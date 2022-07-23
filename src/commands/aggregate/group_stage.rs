use crate::utils::{collapse_fields, convert_if_numeric, field_to_jsonb};
use bson::{Bson, Document};

use super::sql_statement::SqlStatement;

pub fn process_group(doc: &Document) -> SqlStatement {
    let mut doc = doc.clone();
    let mut sql = SqlStatement::new();

    if doc.contains_key("_id") {
        sql.append(&mut process_id(&mut doc));
    }

    let doc = collapse_fields(&doc);
    for (key, value) in doc.iter() {
        if key.ends_with("$sum") {
            sql.append(&mut process_sum(key, value));
        }
    }

    sql
}

fn process_id(doc: &mut Document) -> SqlStatement {
    let field = doc.remove("_id").unwrap();
    let field = field.as_str().unwrap();
    if let Some(field) = field.strip_prefix("$") {
        let field = field_to_jsonb(field);
        SqlStatement::builder()
            .field(format!("{} AS _id", field))
            .group(format!("{}", field))
            .build()
    } else {
        todo!("group by field: {}", field);
    }
}

fn process_sum(key: &String, value: &Bson) -> SqlStatement {
    // name of the resulting field - AS xxxx
    let key = key.strip_suffix(".$sum").unwrap();

    // what to sum SUM(xxxx)
    let value = if let Some(str_value) = value.as_str() {
        // if it's a string starting with $ we take it as a field name
        if let Some(field_name) = str_value.strip_prefix("$") {
            convert_if_numeric(&field_to_jsonb(field_name))
        } else {
            // FIXME we can't do anything yet for summing other types
            todo!("unsupported sum value without $: {}", str_value);
        }
    } else {
        // if it's not a string, we take its contents as is
        value.to_string()
    };

    SqlStatement::builder()
        .field(format!("SUM({}) AS {}", value, key))
        .build()
}

#[cfg(test)]
mod tests {
    use super::*;

    use bson::doc;

    #[test]
    fn test_process_group_with_sum_int() {
        let doc = doc! { "_id": "$field", "count": { "$sum": 1 } };
        let sql = process_group(&doc);
        assert_eq!(sql.fields[0], "_jsonb->'field' AS _id");
        assert_eq!(sql.fields[1], "SUM(1) AS count");
        assert_eq!(sql.groups[0], "_jsonb->'field'");
    }

    #[test]
    fn test_process_group_with_sum_field() {
        let doc = doc! { "_id": "$other", "qty": { "$sum": "$qty" } };
        let sql = process_group(&doc);
        assert_eq!(sql.fields[0], "_jsonb->'other' AS _id");
        assert_eq!(sql.fields[1], "SUM((CASE WHEN (_jsonb->'qty' ? '$f') THEN (_jsonb->'qty'->>'$f')::numeric ELSE (_jsonb->'qty')::numeric END)) AS qty");
        assert_eq!(sql.groups[0], "_jsonb->'other'");
    }
}
