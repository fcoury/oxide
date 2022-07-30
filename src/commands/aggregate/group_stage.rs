use super::{group_id::process_id, sql_statement::SqlStatement};
use crate::utils::{collapse_fields, convert_if_numeric, field_to_jsonb};
use bson::{Bson, Document};

#[derive(Debug)]
pub struct InvalidGroupError {
    pub message: String,
}

pub fn process_group(doc: &Document) -> eyre::Result<SqlStatement> {
    let mut doc = doc.clone();
    let mut sql = SqlStatement::new();

    if doc.contains_key("_id") {
        sql.append(&mut process_id(&mut doc)?);
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
                        Bson::Int64(i64val) => {
                            value = Bson::String(process(&sql_func, &i64val.to_string()));
                        }
                        _ => {
                            return Err(eyre::eyre!(
                                "Cannot currently use {} on non-numeric field",
                                oper
                            ));
                        }
                    }
                }
                "$add" | "$multiply" | "$subtract" | "$divide" => {
                    let oper = match *key {
                        "$add" => "+",
                        "$multiply" => "*",
                        "$subtract" => "-",
                        "$divide" => "/",
                        _ => {
                            return Err(eyre::eyre!(
                                "Operation invalid or not yet implemented: {}",
                                key
                            ));
                        }
                    };
                    if let Some(values) = value.as_array() {
                        // checks if values all start with $
                        let items = parse_math_oper_params(values)?;
                        value = Bson::String(format!("{}", items.join(&format!(" {} ", oper))));
                    } else {
                        return Err(eyre::eyre!(
                            "Cannot {} can only take an array, got {:?}",
                            oper,
                            value
                        ));
                    }
                }
                _ => {
                    return Err(eyre::eyre!("Operation missing or not implemented: {}", key));
                }
            }
        }
        match value {
            Bson::String(str_val) => {
                sql.add_field(&format!("{} AS {}", str_val, keys[0]));
            }
            _ => {
                return Err(eyre::eyre!(
                    r#"The field '{}' must be an accumulator object. Try wrapping it on an object like {{ "field": {{ "{}": {} }} }}."#,
                    raw_key,
                    raw_key,
                    value
                ));
            }
        }
    }

    Ok(sql)
}

fn parse_math_oper_params(attributes: &bson::Array) -> eyre::Result<Vec<String>> {
    let mut items: Vec<String> = vec![];
    for attr in attributes.into_iter() {
        match attr {
            Bson::String(str_val) => {
                if !str_val.starts_with("$") {
                    return Err(eyre::eyre!("Prefixing fields with $ is mandatory. Use ${} if you want to use a field as attribute.", str_val));
                }
                items.push(convert_if_numeric(&field_to_jsonb(
                    str_val.strip_prefix("$").unwrap(),
                )));
            }
            _ => {
                return Err(eyre::eyre!("Cannot use {:?} as a parameter", attr));
            }
        }
    }
    Ok(items)
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
        let sql = process_group(&doc).unwrap();
        assert_eq!(sql.fields[0], "_jsonb->'field' AS _id");
        assert_eq!(sql.fields[1], "SUM(1) AS count");
        assert_eq!(sql.groups[0], "_id");
    }

    #[test]
    fn test_process_group_with_sum_field() {
        let doc = doc! { "_id": "$other", "qty": { "$sum": "$qty" } };
        let sql = process_group(&doc).unwrap();
        assert_eq!(sql.fields[0], "_jsonb->'other' AS _id");
        assert_eq!(sql.fields[1], "SUM(CASE WHEN (_jsonb->'qty' ? '$f') THEN (_jsonb->'qty'->>'$f')::numeric ELSE (_jsonb->'qty')::numeric END) AS qty");
        assert_eq!(sql.groups[0], "_id");
    }

    #[test]
    fn test_process_group_with_avg_int() {
        let doc = doc! { "_id": "$field", "count": { "$avg": 1 } };
        let sql = process_group(&doc).unwrap();
        assert_eq!(sql.fields[0], "_jsonb->'field' AS _id");
        assert_eq!(sql.fields[1], "AVG(1) AS count");
        assert_eq!(sql.groups[0], "_id");
    }

    #[test]
    fn test_process_group_with_avg_field() {
        let doc = doc! { "_id": "$other", "qty": { "$avg": "$qty" } };
        let sql = process_group(&doc).unwrap();
        assert_eq!(sql.fields[0], "_jsonb->'other' AS _id");
        assert_eq!(sql.fields[1], "AVG(CASE WHEN (_jsonb->'qty' ? '$f') THEN (_jsonb->'qty'->>'$f')::numeric ELSE (_jsonb->'qty')::numeric END) AS qty");
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
        let sql = process_group(&doc).unwrap();
        assert_eq!(
            sql.fields[0],
            "TO_CHAR(TO_TIMESTAMP((_jsonb->'date'->>'$d')::numeric / 1000), 'YYYY-MM-DD') AS _id"
        );
        assert_eq!(sql.fields[1], "AVG(CASE WHEN (_jsonb->'qty' ? '$f') THEN (_jsonb->'qty'->>'$f')::numeric ELSE (_jsonb->'qty')::numeric END) AS qty");
        assert_eq!(sql.groups[0], "_id");
    }

    #[test]
    fn test_process_group_with_sum_of_multiply() {
        let doc = doc! { "_id": "$field", "total": { "$sum": { "$multiply": ["$a", "$b"] } } };
        let sql = process_group(&doc).unwrap();
        assert_eq!(sql.fields[0], "_jsonb->'field' AS _id");
        assert_eq!(sql.fields[1], "SUM(CASE WHEN (_jsonb->'a' ? '$f') THEN (_jsonb->'a'->>'$f')::numeric ELSE (_jsonb->'a')::numeric END * CASE WHEN (_jsonb->'b' ? '$f') THEN (_jsonb->'b'->>'$f')::numeric ELSE (_jsonb->'b')::numeric END) AS total");
        assert_eq!(sql.groups[0], "_id");
    }

    #[test]
    fn test_process_group_with_sum_of_add() {
        let doc = doc! { "_id": "$field", "total": { "$sum": { "$add": ["$a", "$b"] } } };
        let sql = process_group(&doc).unwrap();
        assert_eq!(sql.fields[0], "_jsonb->'field' AS _id");
        assert_eq!(sql.fields[1], "SUM(CASE WHEN (_jsonb->'a' ? '$f') THEN (_jsonb->'a'->>'$f')::numeric ELSE (_jsonb->'a')::numeric END + CASE WHEN (_jsonb->'b' ? '$f') THEN (_jsonb->'b'->>'$f')::numeric ELSE (_jsonb->'b')::numeric END) AS total");
        assert_eq!(sql.groups[0], "_id");
    }

    #[test]
    fn test_process_group_with_sum_of_subtract() {
        let doc = doc! { "_id": "$field", "total": { "$sum": { "$subtract": ["$a", "$b"] } } };
        let sql = process_group(&doc).unwrap();
        assert_eq!(sql.fields[0], "_jsonb->'field' AS _id");
        assert_eq!(sql.fields[1], "SUM(CASE WHEN (_jsonb->'a' ? '$f') THEN (_jsonb->'a'->>'$f')::numeric ELSE (_jsonb->'a')::numeric END - CASE WHEN (_jsonb->'b' ? '$f') THEN (_jsonb->'b'->>'$f')::numeric ELSE (_jsonb->'b')::numeric END) AS total");
        assert_eq!(sql.groups[0], "_id");
    }

    #[test]
    fn test_process_group_with_sum_of_divide() {
        let doc = doc! { "_id": "$field", "total": { "$sum": { "$divide": ["$a", "$b"] } } };
        let sql = process_group(&doc).unwrap();
        assert_eq!(sql.fields[0], "_jsonb->'field' AS _id");
        assert_eq!(sql.fields[1], "SUM(CASE WHEN (_jsonb->'a' ? '$f') THEN (_jsonb->'a'->>'$f')::numeric ELSE (_jsonb->'a')::numeric END / CASE WHEN (_jsonb->'b' ? '$f') THEN (_jsonb->'b'->>'$f')::numeric ELSE (_jsonb->'b')::numeric END) AS total");
        assert_eq!(sql.groups[0], "_id");
    }
}
