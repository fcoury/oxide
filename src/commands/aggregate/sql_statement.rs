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
    pub orders: Vec<String>,
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
        self.orders.append(&mut other.orders);
    }

    pub fn add_field(&mut self, field: &str) {
        self.fields.push(field.to_string());
    }

    pub fn add_filter(&mut self, filter: &str) {
        self.filters.push(filter.to_string());
    }

    pub fn add_order(&mut self, order: &str, asc: bool) {
        let order = format!("{} {}", order, if asc { "ASC" } else { "DESC" });
        self.orders.push(order.to_string());
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
        format!(" GROUP BY {}", self.groups.join(", "))
    }

    pub fn order_as_str(&self) -> String {
        if self.orders.is_empty() {
            return "".to_string();
        }
        format!(" ORDER BY {}", self.orders.join(", "))
    }

    pub fn set_table(&mut self, table: SqlParam) {
        self.from = Some(FromTypes::Table(table));
    }

    pub fn to_string(&self) -> String {
        let from = match &self.from {
            Some(from) => format!("FROM {}", from),
            // None => todo!("table missing"),
            None => "".to_string(),
        };

        let where_str = if self.filters.len() > 0 {
            format!(" WHERE {}", self.filters.join(" AND "))
        } else {
            "".to_string()
        };

        format!(
            "SELECT {} {}{}{}{}",
            self.fields_as_str(),
            from,
            where_str,
            self.groups_as_str(),
            self.order_as_str(),
        )
    }

    pub fn add_subquery(&mut self, subquery: &mut SqlStatement) {
        self.from = Some(FromTypes::Subquery(Box::new(subquery.clone()), None));
    }

    pub fn add_subquery_with_alias(&mut self, subquery: &mut SqlStatement, alias: &str) {
        self.from = Some(FromTypes::Subquery(
            Box::new(subquery.clone()),
            Some(alias.to_string()),
        ));
    }
}

#[derive(Default, Debug, Clone)]
pub struct SqlStatementBuilder {
    fields: Vec<String>,
    groups: Vec<String>,
    filters: Vec<String>,
    from: Option<FromTypes>,
    orders: Vec<String>,
}

impl SqlStatementBuilder {
    pub fn new() -> Self {
        SqlStatementBuilder::default()
    }

    pub fn field(mut self, field: &str) -> Self {
        self.fields.push(field.to_string());
        self
    }

    pub fn group(mut self, group: &str) -> Self {
        self.groups.push(group.to_string());
        self
    }

    pub fn from(mut self, from: FromTypes) -> Self {
        self.from = Some(from);
        self
    }

    pub fn order(mut self, order: &str, asc: bool) -> Self {
        let order = format!("{} {}", order, if asc { "ASC" } else { "DESC" });
        self.orders.push(order.to_string());
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

    pub fn filter(mut self, filter: &str) -> Self {
        self.filters.push(filter.to_string());
        self
    }

    pub fn build(self) -> SqlStatement {
        SqlStatement {
            fields: self.fields,
            groups: self.groups,
            filters: self.filters,
            from: self.from,
            orders: self.orders,
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
            .field("_jsonb")
            .filter("_jsonb->'count' = 1")
            .from(FromTypes::Table(SqlParam::new("schema", "table")))
            .build();
        assert_eq!(
            sql.to_string(),
            r#"SELECT _jsonb FROM "schema"."table" WHERE _jsonb->'count' = 1"#
        );
    }

    #[test]
    fn test_from_subquery() {
        let subquery = SqlStatement::builder()
            .field("b")
            .from_table(SqlParam::new("schema", "table"))
            .build();
        let sql = SqlStatement::builder()
            .field("alias.b")
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
            .field("b")
            .from_table(SqlParam::new("schema", "table"))
            .build();
        let sql = SqlStatement::builder()
            .field("alias.b")
            .from_subquery_with_alias(subquery, "alias")
            .build();
        assert_eq!(
            sql.to_string(),
            r#"SELECT alias.b FROM (SELECT b FROM "schema"."table") AS alias"#
        );
    }

    #[test]
    fn test_from_nested_subquery() {
        let subquery1 = SqlStatement::builder()
            .field("c")
            .from_table(SqlParam::new("schema", "table"))
            .build();
        let subquery2 = SqlStatement::builder()
            .field("b")
            .from_subquery(subquery1)
            .build();
        let sql = SqlStatement::builder()
            .field("alias.b")
            .from_subquery(subquery2)
            .build();
        assert_eq!(
            sql.to_string(),
            r#"SELECT alias.b FROM (SELECT b FROM (SELECT c FROM "schema"."table"))"#
        );
    }

    #[test]
    fn test_groups() {
        let sql = SqlStatement::builder()
            .field("state")
            .field("sum(1) AS count")
            .group("state")
            .from(FromTypes::Table(SqlParam::new("schema", "table")))
            .build();
        assert_eq!(
            sql.to_string(),
            r#"SELECT state, sum(1) AS count FROM "schema"."table" GROUP BY state"#
        );
    }
}
