use crate::utils::to_cstring;
use crate::wire::{OpMsg, OpMsgSection};
use bson::{ser, Document};
use byteorder::{LittleEndian, ReadBytesExt};
use indoc::indoc;
use std::io::{BufRead, Cursor, Read};

use super::UnknownMessageKindError;

pub fn parse_section(
    bytes: &mut Vec<u8>,
) -> Result<(OpMsgSection, Vec<u8>), UnknownMessageKindError> {
    let kind = bytes[0];
    if kind == 0 {
        return Ok(parse_kind0(bytes.clone()));
    } else if kind == 1 {
        return Ok(parse_kind1(bytes.clone()));
    }

    // FIXME add the kind to the error
    Err(UnknownMessageKindError {})
}

fn parse_kind0(bytes: Vec<u8>) -> (OpMsgSection, Vec<u8>) {
    let mut cursor = Cursor::new(bytes);

    let kind = cursor.read_u8().unwrap();

    let mut new_cursor = cursor.clone();
    let document = Document::from_reader(cursor).unwrap();
    let bson_vec = ser::to_vec(&document).unwrap();
    new_cursor.set_position(new_cursor.position() + bson_vec.len() as u64);

    let mut tail: Vec<u8> = vec![];
    new_cursor.read_to_end(&mut tail).unwrap();

    (
        OpMsgSection {
            kind,
            identifier: None,
            documents: vec![document],
        },
        tail,
    )
}

fn parse_kind1_documents(data: &[u8]) -> Vec<Document> {
    let size = data.len();
    let mut cursor = Cursor::new(data);
    let mut documents = Vec::new();

    let mut read_size = 0;
    while read_size < size as usize {
        let doc_cursor = cursor.clone();
        let document = Document::from_reader(doc_cursor).unwrap();
        let bson_vec = ser::to_vec(&document).unwrap();
        let document_size = bson_vec.len();
        cursor.set_position(cursor.position() + document_size as u64);

        documents.push(document);
        read_size += document_size;
    }

    return documents;
}

fn parse_kind1(bytes: Vec<u8>) -> (OpMsgSection, Vec<u8>) {
    let mut cursor = Cursor::new(bytes);

    // session kind
    let kind = cursor.read_u8().unwrap();

    // session contents size
    let size = cursor.read_u32::<LittleEndian>().unwrap();

    // identifier
    let mut identifier_buffer: Vec<u8> = vec![];
    cursor.read_until(0, &mut identifier_buffer).unwrap();
    let identifier_size: u32 = identifier_buffer.len() as u32;
    let identifier = to_cstring(identifier_buffer);

    // whole section = size - sizeof(size) - sizeof(identifier)
    //                 size - 4 - len(identifier_buffer)
    let remaining_size: u32 = size - 4 - identifier_size;
    let mut section_buffer: Vec<u8> = vec![0u8; remaining_size as usize];
    cursor.read_exact(&mut section_buffer).unwrap();

    let documents = parse_kind1_documents(&section_buffer);
    let mut tail: Vec<u8> = vec![];
    cursor.read_to_end(&mut tail).unwrap();

    (
        OpMsgSection {
            kind,
            identifier: Some(identifier),
            documents,
        },
        tail,
    )
}

#[cfg(test)]
mod tests {
    use crate::utils::hexstring_to_bytes;

    use super::*;

    #[test]
    fn test_parse_sections() {
        let kind1kind0 = indoc! {"
            01 2f 00 00 00 64 6f 63 75 6d 65 6e
            74 73 00 21 00 00 00 07 5f 69 64 00 62 ce d6 9a
            33 78 79 a1 ac c2 9d 40 01 78 00 00 00 00 00 00
            00 f0 3f 00 00 51 00 00 00 02 69 6e 73 65 72 74
            00 04 00 00 00 63 6f 6c 00 08 6f 72 64 65 72 65
            64 00 01 03 6c 73 69 64 00 1e 00 00 00 05 69 64
            00 10 00 00 00 04 e1 54 58 c6 4e 89 4c a3 81 0f
            19 59 d3 a3 2c cf 00 02 24 64 62 00 05 00 00 00
            74 65 73 74 00 00
        "};
        let mut bytes = hexstring_to_bytes(kind1kind0);
        let (section1, mut bytes) = parse_section(&mut bytes).unwrap();
        assert_eq!(section1.kind, 1);
        assert_eq!(section1.identifier.unwrap(), "documents\0");
        assert_eq!(section1.documents.len(), 1);
        assert_eq!(
            section1.documents[0].get_object_id("_id").unwrap(),
            bson::oid::ObjectId::parse_str("62ced69a337879a1acc29d40").unwrap()
        );
        assert_eq!(section1.documents[0].get_f64("x").unwrap(), 1.0);
        assert_eq!(bytes.len(), 82);

        let (section0, bytes) = parse_section(&mut bytes).unwrap();
        assert_eq!(section0.kind, 0);
        assert_eq!(section0.identifier, None);
        assert_eq!(section0.documents.len(), 1);
        assert_eq!(section0.documents[0].get_str("insert").unwrap(), "col");
        assert_eq!(section0.documents[0].get_bool("ordered").unwrap(), true);
        assert_eq!(section0.documents[0].get_str("$db").unwrap(), "test");
        assert_eq!(bytes.len(), 0);
    }

    #[test]
    fn test_parse_kind0_section() {
        let kind0 = indoc! {"
                        00 51 00 00 00 02 69 6e 73 65 72 74
            00 04 00 00 00 63 6f 6c 00 08 6f 72 64 65 72 65
            64 00 01 03 6c 73 69 64 00 1e 00 00 00 05 69 64
            00 10 00 00 00 04 e1 54 58 c6 4e 89 4c a3 81 0f
            19 59 d3 a3 2c cf 00 02 24 64 62 00 05 00 00 00
            74 65 73 74 00 00
        "};

        let mut bytes = hexstring_to_bytes(kind0);
        let (section, _) = parse_section(&mut bytes).unwrap();
        assert_eq!(section.kind, 0);
    }

    #[test]
    fn test_parse_kind1_section() {
        // 96 00 00 00 61 00 00 00 00 00 00 00 dd 07 00 00 -- heade
        // 00 00 00 00 -- flags
        // 01          -- kind
        // size        -- 2f 00 00 00 - 0x0000002f = 47
        // 64 6f 63 75 6d 65 6e

        let kind1 = indoc! {"
            01 2f 00 00 00 64 6f 63 75 6d 65 6e 74 73 00 21 00
            00 00 07 5f 69 64 00 62 ce d6 9a 33 78 79 a1 ac
            c2 9d 40 01 78 00 00 00 00 00 00 00 f0 3f 00 00
            51 00 00 00 02 69 6e 73 65 72 74 00 04 00 00 00
            63 6f 6c 00 08 6f 72 64 65 72 65 64 00 01 03 6c
            73 69 64 00 1e 00 00 00 05 69 64 00 10 00 00 00
            04 e1 54 58 c6 4e 89 4c a3 81 0f 19 59 d3 a3 2c
            cf 00 02 24 64 62 00 05 00 00 00 74 65 73 74 00
            00
        "};

        let mut bytes = hexstring_to_bytes(kind1);
        let (section, _) = parse_section(&mut bytes).unwrap();
        assert_eq!(section.kind, 1);
    }

    #[test]
    fn test_kind1_op_msg() {
        let op_msg_hexstr = indoc! {"
            96 00 00 00 61 00 00 00 00 00 00 00 dd 07 00 00
            00 00 00 00 01 2f 00 00 00 64 6f 63 75 6d 65 6e
            74 73 00 21 00 00 00 07 5f 69 64 00 62 ce d6 9a
            33 78 79 a1 ac c2 9d 40 01 78 00 00 00 00 00 00
            00 f0 3f 00 00 51 00 00 00 02 69 6e 73 65 72 74
            00 04 00 00 00 63 6f 6c 00 08 6f 72 64 65 72 65
            64 00 01 03 6c 73 69 64 00 1e 00 00 00 05 69 64
            00 10 00 00 00 04 e1 54 58 c6 4e 89 4c a3 81 0f
            19 59 d3 a3 2c cf 00 02 24 64 62 00 05 00 00 00
            74 65 73 74 00 00
        "};

        let bytes = hexstring_to_bytes(op_msg_hexstr);
        let op_msg = OpMsg::from_bytes(&bytes).unwrap();

        assert_eq!(op_msg.header.message_length, 150);
        assert_eq!(op_msg.header.request_id, 97);
        assert_eq!(op_msg.header.response_to, 0);
        assert_eq!(op_msg.header.op_code, 2013);

        assert_eq!(op_msg.flags, 0);
        assert_eq!(op_msg.sections.len(), 2);

        let section1 = &op_msg.sections[0];
        let identifier = section1.identifier.clone().unwrap();
        assert_eq!(section1.kind, 1);
        assert_eq!(section1.documents.len(), 1);
        assert_eq!(identifier, "documents\0");
        assert_eq!(
            section1.documents[0].get_object_id("_id").unwrap(),
            bson::oid::ObjectId::parse_str("62ced69a337879a1acc29d40").unwrap()
        );
        assert_eq!(section1.documents[0].get_f64("x").unwrap(), 1.0);

        let section0 = &op_msg.sections[1];
        assert_eq!(section0.kind, 0);
        assert_eq!(section0.identifier, None);
        assert_eq!(section0.documents.len(), 1);
        assert_eq!(section0.documents[0].get_str("insert").unwrap(), "col");
        assert_eq!(section0.documents[0].get_bool("ordered").unwrap(), true);
        assert_eq!(section0.documents[0].get_str("$db").unwrap(), "test");
    }
}
