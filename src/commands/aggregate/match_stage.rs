use crate::parser::parse;
use crate::utils::expand_fields;
use bson::Document;

use super::sql_statement::SqlStatement;

pub fn process_match(doc: &Document) -> SqlStatement {
    let mut sql = SqlStatement::builder().build();

    let filter_doc = expand_fields(&doc).unwrap();
    let filter = parse(filter_doc);
    if filter != "" {
        sql.add_filter(&filter);
    }

    sql
}

#[cfg(test)]
mod tests {
    use super::*;

    use bson::doc;

    #[test]
    fn test_process_match() {
        let doc = doc! { "age": { "$gt": 20 } };
        let sql = process_match(&doc);

        assert_eq!(
            sql.filters[0],
            r#"(jsonb_typeof(_jsonb->'age') = 'number' OR jsonb_typeof(_jsonb->'age'->'$f') = 'number') AND CASE WHEN (_jsonb->'age' ? '$f') THEN (_jsonb->'age'->>'$f')::numeric ELSE (_jsonb->'age')::numeric END > '20'"#
        );
    }
}
