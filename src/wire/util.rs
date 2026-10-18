use super::{OpMsgSection, MAX_DOCUMENT_LEN};
use bson::Document;
use eyre::{bail, Result};

pub fn document(bytes: &[u8]) -> Result<(Document, usize)> {
    if bytes.len() < 4 {
        bail!("truncated BSON length");
    }
    let size = i32::from_le_bytes(bytes[..4].try_into()?);
    if size < 5 || size as u32 > MAX_DOCUMENT_LEN || size as usize > bytes.len() {
        bail!("invalid BSON size");
    }
    let size = size as usize;
    let raw = bson::raw::RawDocument::from_bytes(&bytes[..size])?;
    validate_depth(raw, 0)?;
    // Convert raw BSON directly. Serde's BSON visitor interprets nested $oid /
    // $date-shaped documents as Extended JSON even though the wire is typed.
    let doc = Document::try_from(raw)?;
    Ok((doc, size))
}
pub fn cstring(bytes: &[u8]) -> Result<(String, usize)> {
    let size = bytes
        .iter()
        .position(|b| *b == 0)
        .ok_or_else(|| eyre::eyre!("unterminated cstring"))?;
    Ok((std::str::from_utf8(&bytes[..size])?.to_owned(), size + 1))
}
pub fn parse_section(bytes: &[u8]) -> Result<(OpMsgSection, usize)> {
    let kind = *bytes.first().ok_or_else(|| eyre::eyre!("empty section"))?;
    match kind {
        0 => {
            let (doc, size) = document(&bytes[1..])?;
            Ok((
                OpMsgSection {
                    kind,
                    identifier: None,
                    documents: vec![doc],
                },
                1 + size,
            ))
        }
        1 => {
            if bytes.len() < 5 {
                bail!("truncated sequence");
            }
            let size = i32::from_le_bytes(bytes[1..5].try_into()?);
            if size < 5 || size as usize + 1 > bytes.len() {
                bail!("invalid sequence size");
            }
            let end = size as usize + 1;
            let (identifier, len) = cstring(&bytes[5..end])?;
            let mut offset = 5 + len;
            let mut documents = Vec::new();
            while offset < end {
                if documents.len() == 10_000 {
                    bail!("document sequence exceeds 10000 entries");
                }
                let (doc, size) = document(&bytes[offset..end])?;
                documents.push(doc);
                offset += size;
            }
            Ok((
                OpMsgSection {
                    kind,
                    identifier: Some(identifier),
                    documents,
                },
                end,
            ))
        }
        _ => bail!("unknown section kind {kind}"),
    }
}

// Inspect borrowed raw BSON before recursive deserialization or storage encoding.
// This also rejects duplicate keys instead of silently retaining the last value.
fn validate_depth(doc: &bson::raw::RawDocument, depth: usize) -> Result<()> {
    if depth > 64 {
        bail!("BSON nesting exceeds 64 levels");
    }
    let mut keys = std::collections::HashSet::new();
    for entry in doc {
        let (key, value) = entry?;
        if !keys.insert(key) {
            bail!("duplicate BSON field");
        }
        validate_value(value, depth + 1)?;
    }
    Ok(())
}
fn validate_value(value: bson::raw::RawBsonRef<'_>, depth: usize) -> Result<()> {
    use bson::raw::RawBsonRef;
    if depth > 64 {
        bail!("BSON nesting exceeds 64 levels");
    }
    match value {
        RawBsonRef::Document(doc) => validate_depth(doc, depth)?,
        RawBsonRef::Array(values) => {
            for value in values {
                validate_value(value?, depth + 1)?;
            }
        }
        RawBsonRef::JavaScriptCodeWithScope(value) => validate_depth(value.scope, depth)?,
        _ => (),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::document;
    use bson::doc;

    #[test]
    fn wire_documents_do_not_interpret_extended_json_field_names() {
        let original = doc! {
            "literal": {"$oid": "ordinary object"},
            "validLooking": {"$oid": "507f1f77bcf86cd799439011"},
            "array": [{"$numberLong": "123"}, {"$date": "not a date"}],
            "actualId": bson::oid::ObjectId::new(),
        };
        let bytes = bson::to_vec(&original).unwrap();
        let (decoded, length) = document(&bytes).unwrap();
        assert_eq!(length, bytes.len());
        assert_eq!(decoded, original);
    }
}
