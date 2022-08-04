use super::sql_statement::SqlStatement;
use eyre::Result;

pub fn process_count(count_field: &str) -> Result<SqlStatement> {
    if count_field.contains(".") {
        return Err(eyre::eyre!("the count field cannot contain '.'"));
    }

    if count_field.contains("$") {
        return Err(eyre::eyre!("the count field cannot be a $-prefixed path"));
    }

    let sql = SqlStatement::builder()
        .field(&format!(
            "json_build_object('{}', COUNT(*))::jsonb AS _jsonb",
            count_field
        ))
        .build();
    Ok(sql)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_process_count() {
        let sql = process_count("total").unwrap();

        assert_eq!(
            sql.fields[0],
            r#"json_build_object('total', COUNT(*))::jsonb AS _jsonb"#
        );
    }

    #[test]
    fn test_dot_error() {
        let err = process_count("total.name").unwrap_err();
        assert_eq!(err.to_string(), "the count field cannot contain '.'");
    }

    #[test]
    fn test_dollar_sign_error() {
        let err = process_count("$name").unwrap_err();
        assert_eq!(
            err.to_string(),
            "the count field cannot be a $-prefixed path"
        );
    }
}
