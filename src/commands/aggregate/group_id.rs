use crate::utils::field_to_jsonb;

use super::sql_statement::SqlStatement;
use bson::{Bson, Document};

pub fn process_id(doc: &mut Document) -> SqlStatement {
    let field = doc.remove("_id").unwrap();

    match field {
        Bson::String(str) => process_id_str(str),
        Bson::Document(doc) => process_id_doc(doc),
        t => unimplemented!("missing implementation for _id with type {:?}", t),
    }
}

fn process_id_str(field: String) -> SqlStatement {
    if let Some(field) = field.strip_prefix("$") {
        let field = field_to_jsonb(field);
        SqlStatement::builder()
            .field(&format!("{} AS _id", field))
            .group(&format!("{}", field))
            .build()
    } else {
        todo!("group by field: {}", field);
    }
}

fn process_id_doc(doc: Document) -> SqlStatement {
    // FIXME the doc must have exactly one key
    // MongoServerError: An object representing an expression must have exactly one field: { $dateToString: { format: "%Y-%m-%d", date: "$date" }, $other: 1 }
    let (key, value) = doc.iter().next().unwrap();
    match key.as_str() {
        "$dateToString" => {
            let value = value.as_document().unwrap();
            let field = value.get_str("date").unwrap().strip_prefix("$").unwrap();
            let field = format!(
                "TO_CHAR(TO_TIMESTAMP(({}->>'$d')::numeric / 1000), 'YYYY-MM-DD') AS _id",
                field_to_jsonb(field)
            );

            SqlStatement::builder().field(&field).group("_id").build()
        }
        _ => todo!("missing group by field: {}", key),
    }
}
