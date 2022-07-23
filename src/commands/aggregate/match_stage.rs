use crate::parser::parse;
use crate::pg::SqlParam;
use crate::utils::expand_fields;
use bson::Document;

pub fn process_match(sp: &SqlParam, doc: &Document) -> String {
    let mut sql = format!("SELECT _jsonb FROM {}", sp.sanitize());

    let filter_doc = expand_fields(&doc).unwrap();
    let filter = parse(filter_doc);
    if filter != "" {
        sql = format!("{} WHERE {}", sql, filter);
    }

    sql
}

#[cfg(test)]
mod tests {
    use super::*;
    use bson::doc;

    #[test]
    fn test_process_match() {
        let sp = SqlParam::new("test", "test");
        let doc = doc! { "age": { "$gt": 20 } };
        let sql = process_match(&sp, &doc);
        assert_eq!(
            sql,
            r#"SELECT _jsonb FROM "test"."test" WHERE (jsonb_typeof(_jsonb->'age') = 'number' OR jsonb_typeof(_jsonb->'age'->'$f') = 'number') AND (CASE WHEN (_jsonb->'age' ? '$f') THEN (_jsonb->'age'->>'$f')::numeric ELSE (_jsonb->'age')::numeric END) > '20'"#
        );
    }
}
