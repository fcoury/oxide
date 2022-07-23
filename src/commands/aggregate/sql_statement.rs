use std::fmt::Display;

use crate::pg::SqlParam;

#[derive(Debug, Clone)]
pub enum FromTypes {
    Table(SqlParam),
    Subquery(Box<SqlStatement>, Option<String>),
}

impl Display for FromTypes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FromTypes::Table(table) => write!(f, "{}", table),
            FromTypes::Subquery(subquery, alias) => {
                if let Some(alias) = alias {
                    write!(f, "({}) AS {}", subquery, alias)
                } else {
                    write!(f, "({})", subquery)
                }
            }
        }
    }
}

#[derive(Default, Debug, Clone)]
pub struct SqlStatement {
    pub fields: Vec<String>,
    pub groups: Vec<String>,
    pub filters: Vec<String>,
    pub from: Option<FromTypes>,
}

impl Display for SqlStatement {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_string())
    }
}

impl SqlStatement {
    pub fn new() -> Self {
        SqlStatement::default()
    }

    pub fn builder() -> SqlStatementBuilder {
        SqlStatementBuilder::default()
    }

    pub fn append(&mut self, other: &mut SqlStatement) {
        self.fields.append(&mut other.fields);
        self.groups.append(&mut other.groups);
        self.filters.append(&mut other.filters);
    }

    pub fn add_filter(&mut self, filter: String) {
        self.filters.push(filter);
    }

    pub fn fields_as_str(&self) -> String {
        if self.fields.is_empty() {
            return "*".to_string();
        }
        self.fields.join(", ")
    }

    pub fn groups_as_str(&self) -> String {
        if self.groups.is_empty() {
            return "".to_string();
        }
        format!("GROUP BY {}", self.groups.join(", "))
    }

    pub fn to_string(&self) -> String {
        let from = match &self.from {
            Some(from) => format!("FROM {}", from),
            None => todo!("table missing"),
        };

        format!("SELECT {} {}", self.fields_as_str(), from)
    }

    pub fn unwrap(&self) -> String {
        SqlStatement::builder()
            .field("row_to_json(t)".to_string())
            .from_subquery_with_alias(self.clone(), "t")
            .build()
            .to_string()
    }
}

#[derive(Default, Debug, Clone)]
pub struct SqlStatementBuilder {
    fields: Vec<String>,
    groups: Vec<String>,
    filters: Vec<String>,
    from: Option<FromTypes>,
}

impl SqlStatementBuilder {
    pub fn new() -> Self {
        SqlStatementBuilder::default()
    }

    pub fn field(mut self, field: String) -> Self {
        self.fields.push(field);
        self
    }

    pub fn group(mut self, group: String) -> Self {
        self.groups.push(group);
        self
    }

    pub fn from(mut self, from: FromTypes) -> Self {
        self.from = Some(from);
        self
    }

    pub fn from_table(mut self, table: SqlParam) -> Self {
        self.from = Some(FromTypes::Table(table));
        self
    }

    pub fn from_subquery(mut self, subquery: SqlStatement) -> Self {
        self.from = Some(FromTypes::Subquery(Box::new(subquery), None));
        self
    }

    pub fn from_subquery_with_alias(mut self, subquery: SqlStatement, alias: &str) -> Self {
        self.from = Some(FromTypes::Subquery(
            Box::new(subquery),
            Some(alias.to_string()),
        ));
        self
    }

    pub fn filters(mut self, filter: String) -> Self {
        self.filters.push(filter);
        self
    }

    pub fn build(self) -> SqlStatement {
        SqlStatement {
            fields: self.fields,
            groups: self.groups,
            filters: self.filters,
            from: self.from,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::pg::SqlParam;

    use super::*;

    #[test]
    fn test_from_table() {
        let sql = SqlStatement::builder()
            .field("_jsonb".to_string())
            .from(FromTypes::Table(SqlParam::new("schema", "table")))
            .build();
        assert_eq!(sql.to_string(), r#"SELECT _jsonb FROM "schema"."table""#);
    }

    #[test]
    fn test_from_subquery() {
        let subquery = SqlStatement::builder()
            .field("b".to_string())
            .from_table(SqlParam::new("schema", "table"))
            .build();
        let sql = SqlStatement::builder()
            .field("alias.b".to_string())
            .from_subquery(subquery)
            .build();
        assert_eq!(
            sql.to_string(),
            r#"SELECT alias.b FROM (SELECT b FROM "schema"."table")"#
        );
    }

    #[test]
    fn test_from_subquery_with_alias() {
        let subquery = SqlStatement::builder()
            .field("b".to_string())
            .from_table(SqlParam::new("schema", "table"))
            .build();
        let sql = SqlStatement::builder()
            .field("alias.b".to_string())
            .from_subquery_with_alias(subquery, "alias")
            .build();
        assert_eq!(
            sql.to_string(),
            r#"SELECT alias.b FROM (SELECT b FROM "schema"."table") AS alias"#
        );
    }
}
