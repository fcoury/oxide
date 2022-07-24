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

    for (raw_key, value) in doc.iter() {
        let mut value: Bson = value.to_owned();
        let keys = raw_key.split(".").collect::<Vec<&str>>();
        for key in keys.iter().skip(1).rev() {
            match *key {
                "$sum" | "$avg" => {
                    let oper = key.strip_prefix("$").unwrap();
                    let sql_func = oper.to_ascii_uppercase();
                    match value {
                        Bson::String(str_val) => {
                            value = Bson::String(process(&sql_func, &str_val));
                        }
                        Bson::Int32(i32val) => {
                            value = Bson::String(process(&sql_func, &i32val.to_string()));
                        }
                        t => unimplemented!("missing implementation for {} with type {:?}", key, t),
                    }
                }
                "$add" | "$multiply" | "$subtract" | "$divide" => {
                    let oper = match *key {
                        "$add" => "+",
                        "$multiply" => "*",
                        "$subtract" => "-",
                        "$divide" => "/",
                        _ => unreachable!(),
                    };
                    if let Some(values) = value.as_array() {
                        let items: Vec<String> = values
                            .iter()
                            .map(|v| {
                                convert_if_numeric(&field_to_jsonb(
                                    v.as_str().unwrap().strip_prefix("$").unwrap(),
                                ))
                            })
                            .collect();
                        value = Bson::String(format!("{}", items.join(&format!(" {} ", oper))));
                    }
                }
                key => unimplemented!("missing handling operator: {}", key),
            }
        }

        sql.add_field(&format!("{} AS {}", value.as_str().unwrap(), keys[0]));
    }

    sql
}

fn process(sql_func: &str, value: &str) -> String {
    // if it's a string starting with $ we take it as a field name
    let value = if let Some(field_name) = value.strip_prefix("$") {
        convert_if_numeric(&field_to_jsonb(field_name))
    } else {
        value.to_owned()
    };

    format!("{}({})", sql_func, value)
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
        assert_eq!(sql.groups[0], "_id");
    }

    #[test]
    fn test_process_group_with_sum_field() {
        let doc = doc! { "_id": "$other", "qty": { "$sum": "$qty" } };
        let sql = process_group(&doc);
        assert_eq!(sql.fields[0], "_jsonb->'other' AS _id");
        assert_eq!(sql.fields[1], "SUM((CASE WHEN (_jsonb->'qty' ? '$f') THEN (_jsonb->'qty'->>'$f')::numeric ELSE (_jsonb->'qty')::numeric END)) AS qty");
        assert_eq!(sql.groups[0], "_id");
    }

    #[test]
    fn test_process_group_with_avg_int() {
        let doc = doc! { "_id": "$field", "count": { "$avg": 1 } };
        let sql = process_group(&doc);
        assert_eq!(sql.fields[0], "_jsonb->'field' AS _id");
        assert_eq!(sql.fields[1], "AVG(1) AS count");
        assert_eq!(sql.groups[0], "_id");
    }

    #[test]
    fn test_process_group_with_avg_field() {
        let doc = doc! { "_id": "$other", "qty": { "$avg": "$qty" } };
        let sql = process_group(&doc);
        assert_eq!(sql.fields[0], "_jsonb->'other' AS _id");
        assert_eq!(sql.fields[1], "AVG((CASE WHEN (_jsonb->'qty' ? '$f') THEN (_jsonb->'qty'->>'$f')::numeric ELSE (_jsonb->'qty')::numeric END)) AS qty");
        assert_eq!(sql.groups[0], "_id");
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

    #[test]
    fn test_process_group_with_sum_of_multiply() {
        let doc = doc! { "_id": "$field", "total": { "$sum": { "$multiply": ["$a", "$b"] } } };
        let sql = process_group(&doc);
        assert_eq!(sql.fields[0], "_jsonb->'field' AS _id");
        assert_eq!(sql.fields[1], "SUM((CASE WHEN (_jsonb->'a' ? '$f') THEN (_jsonb->'a'->>'$f')::numeric ELSE (_jsonb->'a')::numeric END) * (CASE WHEN (_jsonb->'b' ? '$f') THEN (_jsonb->'b'->>'$f')::numeric ELSE (_jsonb->'b')::numeric END)) AS total");
        assert_eq!(sql.groups[0], "_id");
    }

    #[test]
    fn test_process_group_with_sum_of_add() {
        let doc = doc! { "_id": "$field", "total": { "$sum": { "$add": ["$a", "$b"] } } };
        let sql = process_group(&doc);
        assert_eq!(sql.fields[0], "_jsonb->'field' AS _id");
        assert_eq!(sql.fields[1], "SUM((CASE WHEN (_jsonb->'a' ? '$f') THEN (_jsonb->'a'->>'$f')::numeric ELSE (_jsonb->'a')::numeric END) + (CASE WHEN (_jsonb->'b' ? '$f') THEN (_jsonb->'b'->>'$f')::numeric ELSE (_jsonb->'b')::numeric END)) AS total");
        assert_eq!(sql.groups[0], "_id");
    }

    #[test]
    fn test_process_group_with_sum_of_subtract() {
        let doc = doc! { "_id": "$field", "total": { "$sum": { "$subtract": ["$a", "$b"] } } };
        let sql = process_group(&doc);
        assert_eq!(sql.fields[0], "_jsonb->'field' AS _id");
        assert_eq!(sql.fields[1], "SUM((CASE WHEN (_jsonb->'a' ? '$f') THEN (_jsonb->'a'->>'$f')::numeric ELSE (_jsonb->'a')::numeric END) - (CASE WHEN (_jsonb->'b' ? '$f') THEN (_jsonb->'b'->>'$f')::numeric ELSE (_jsonb->'b')::numeric END)) AS total");
        assert_eq!(sql.groups[0], "_id");
    }

    #[test]
    fn test_process_group_with_sum_of_divide() {
        let doc = doc! { "_id": "$field", "total": { "$sum": { "$divide": ["$a", "$b"] } } };
        let sql = process_group(&doc);
        assert_eq!(sql.fields[0], "_jsonb->'field' AS _id");
        assert_eq!(sql.fields[1], "SUM((CASE WHEN (_jsonb->'a' ? '$f') THEN (_jsonb->'a'->>'$f')::numeric ELSE (_jsonb->'a')::numeric END) / (CASE WHEN (_jsonb->'b' ? '$f') THEN (_jsonb->'b'->>'$f')::numeric ELSE (_jsonb->'b')::numeric END)) AS total");
        assert_eq!(sql.groups[0], "_id");
    }
}
