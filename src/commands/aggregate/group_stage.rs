use super::{group_id::process_id, sql_statement::SqlStatement};
use crate::utils::{collapse_fields, convert_if_numeric, field_to_jsonb};
use bson::{Bson, Document};

pub fn process_group(doc: &Document) -> SqlStatement {
    let mut doc = doc.clone();
    let mut sql = SqlStatement::new();

    if doc.contains_key("_id") {
        sql.append(&mut process_id(&mut doc));
    }

    let doc = collapse_fields(&doc);
    for (key, value) in doc.iter() {
        if key.ends_with("$sum") {
            sql.append(&mut process("sum", "SUM", key, value));
        } else if key.ends_with("$avg") {
            sql.append(&mut process("avg", "AVG", key, value));
        }
    }

    sql
}

fn process(oper: &str, sql_func: &str, key: &String, value: &Bson) -> SqlStatement {
    // name of the resulting field - AS xxxx
    let key = key.strip_suffix(&format!(".${}", oper)).unwrap();

    // what to sum SUM(xxxx)
    let value = if let Some(str_value) = value.as_str() {
        // if it's a string starting with $ we take it as a field name
        if let Some(field_name) = str_value.strip_prefix("$") {
            convert_if_numeric(&field_to_jsonb(field_name))
        } else {
            // FIXME we can't do anything yet for summing other types
            todo!("unsupported {} value without $: {}", oper, str_value);
        }
    } else {
        // if it's not a string, we take its contents as is
        value.to_string()
    };

    SqlStatement::builder()
        .field(&format!("{}({}) AS {}", sql_func, value, key))
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

    #[test]
    fn test_process_group_with_avg_int() {
        let doc = doc! { "_id": "$field", "count": { "$avg": 1 } };
        let sql = process_group(&doc);
        assert_eq!(sql.fields[0], "_jsonb->'field' AS _id");
        assert_eq!(sql.fields[1], "AVG(1) AS count");
        assert_eq!(sql.groups[0], "_jsonb->'field'");
    }

    #[test]
    fn test_process_group_with_avg_field() {
        let doc = doc! { "_id": "$other", "qty": { "$avg": "$qty" } };
        let sql = process_group(&doc);
        assert_eq!(sql.fields[0], "_jsonb->'other' AS _id");
        assert_eq!(sql.fields[1], "AVG((CASE WHEN (_jsonb->'qty' ? '$f') THEN (_jsonb->'qty'->>'$f')::numeric ELSE (_jsonb->'qty')::numeric END)) AS qty");
        assert_eq!(sql.groups[0], "_jsonb->'other'");
    }

    #[test]
    fn test_process_group_with_date_to_str() {
        let doc = doc! { "_id": {
            "$dateToString": {
                "format": "%Y-%m-%d",
                "date": "$date",
            }
        }, "qty": { "$avg": "$qty" } };
        let sql = process_group(&doc);
        assert_eq!(
            sql.fields[0],
            "TO_CHAR(TO_TIMESTAMP((_jsonb->'date'->>'$d')::numeric / 1000), 'YYYY-MM-DD') AS _id"
        );
        assert_eq!(sql.fields[1], "AVG((CASE WHEN (_jsonb->'qty' ? '$f') THEN (_jsonb->'qty'->>'$f')::numeric ELSE (_jsonb->'qty')::numeric END)) AS qty");
        assert_eq!(sql.groups[0], "_id");
    }
}
