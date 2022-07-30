use crate::parser::parse;
use crate::utils::expand_fields;
use bson::Document;
use eyre::Result;

use super::sql_statement::SqlStatement;

pub fn process_match(doc: &Document) -> Result<SqlStatement> {
    let mut sql = SqlStatement::builder().build();

    let filter_doc = expand_fields(&doc)?;
    let filter = parse(filter_doc)?;
    if filter != "" {
        sql.add_filter(&filter);
    }

    Ok(sql)
}

#[cfg(test)]
mod tests {
    use super::*;

    use bson::doc;

    #[test]
    fn test_process_match() {
        let doc = doc! { "age": { "$gt": 20 } };
        let sql = process_match(&doc).unwrap();

        assert_eq!(
            sql.filters[0],
            r#"(jsonb_typeof(_jsonb->'age') = 'number' OR jsonb_typeof(_jsonb->'age'->'$f') = 'number') AND CASE WHEN (_jsonb->'age' ? '$f') THEN (_jsonb->'age'->>'$f')::numeric ELSE (_jsonb->'age')::numeric END > '20'"#
        );
    }
}
