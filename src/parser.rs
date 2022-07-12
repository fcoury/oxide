#![allow(dead_code)]
use crate::serializer::PostgresSerializer;
use bson::{Bson, Document};
use mongodb_language_model::{
    Clause, Expression, ExpressionTreeClause, LeafClause, LeafValue, Operator, Value, ValueOperator,
};

pub fn parse(doc: Document) -> String {
    let bson: Bson = doc.into();
    let json = bson.into_psql_json();
    let str = serde_json::to_string(&json).unwrap();
    let expression = mongodb_language_model::parse(&str).unwrap();
    parse_expression(expression)
}

fn parse_expression(expression: Expression) -> String {
    parse_clauses(expression.clauses)
}

fn parse_clauses(clauses: Vec<Clause>) -> String {
    let sql: Vec<String> = clauses
        .into_iter()
        .map(|clause| parse_clause(clause))
        .collect();

    if sql.len() > 1 {
        format!("({})", sql.join(" AND "))
    } else {
        sql[0].to_string()
    }
}

fn parse_clause(clause: Clause) -> String {
    match clause {
        Clause::Leaf(leaf) => parse_leaf(leaf),
        Clause::ExpressionTree(exp_tree) => parse_expression_tree(exp_tree),
    }
}

fn to_field(key: &str) -> String {
    format!("_jsonb->'{}'", key)
}

fn parse_leaf(leaf: LeafClause) -> String {
    let field = to_field(&leaf.key);
    match leaf.value {
        Value::Leaf(leaf_value) => {
            format!("{} = {}", field, parse_leaf_value(leaf_value))
        }
        Value::Operators(val_operators) => {
            format!("{} {}", field, parse_value_operators(val_operators))
        }
    }
}

fn parse_value_operators(val_operators: Vec<Operator>) -> String {
    let values: Vec<String> = val_operators
        .into_iter()
        .map(|operator| parse_operator(operator))
        .collect();
    values.join("")
}

fn parse_operator(oper: Operator) -> String {
    match oper {
        Operator::Value(value_oper) => parse_value_operator(value_oper),
        t => unimplemented!("parse_operator - unimplemented {:?}", t),
    }
}

fn parse_value_operator(value_oper: ValueOperator) -> String {
    let operator = match value_oper.operator.as_str() {
        "$lt" => "<",
        "$lte" => "<=",
        "$gt" => ">",
        "$gte" => ">=",
        "$ne" => "!=",
        "$eq" => "=",
        t => unimplemented!("parse_value_operator - operator unimplemented {:?}", t),
    };
    let value = parse_leaf_value(value_oper.value);
    format!("{} {}", operator, value)
}

fn parse_expression_tree(exp_tree: ExpressionTreeClause) -> String {
    let operator = match exp_tree.operator.as_str() {
        "$and" => "AND",
        "$or" => "OR",
        // "$nor" => "NOR", FIXME
        t => unimplemented!("parse_expression_tree operator unimplemented = {:?}", t),
    };
    let sql: Vec<String> = exp_tree
        .expressions
        .into_iter()
        .map(|exp| parse_expression(exp))
        .collect();

    if sql.len() > 1 {
        format!("({})", sql.join(&format!(" {} ", operator)))
    } else {
        sql[0].to_string()
    }
}

fn parse_leaf_value(leaf_value: LeafValue) -> String {
    let json = leaf_value.value;
    json.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use bson::doc;

    #[test]
    fn test_simple_str() {
        let doc = doc! {"name": "test"};
        assert_eq!(r#"_jsonb->'name' = "test""#, parse(doc))
    }

    #[test]
    fn test_simple_int() {
        let doc = doc! {"age": 12};
        assert_eq!(r#"_jsonb->'age' = 12"#, parse(doc))
    }

    #[test]
    fn test_and() {
        let doc = doc! {"a": "name", "b": 212};
        assert_eq!(
            r#"(_jsonb->'a' = "name" AND _jsonb->'b' = 212)"#,
            parse(doc)
        )
    }

    #[test]
    fn test_simple_or() {
        let doc = doc! {"$or": vec![
            doc! { "a": "name", "b": 212 },
        ]};
        assert_eq!(
            r#"(_jsonb->'a' = "name" AND _jsonb->'b' = 212)"#,
            parse(doc)
        )
    }

    #[test]
    fn test_explicit_and() {
        let doc = doc! {"$and": vec![
            doc! { "a": 1, "b": 2, "c": 3 },
        ]};
        assert_eq!(
            r#"(_jsonb->'a' = 1 AND _jsonb->'b' = 2 AND _jsonb->'c' = 3)"#,
            parse(doc)
        )
    }

    #[test]
    fn test_and_or_combo() {
        let doc = doc! {"$or": vec![
            doc! { "a": "name", "b": 212 },
            doc! { "c": "name", "d": 212 },
        ]};
        assert_eq!(
            r#"((_jsonb->'a' = "name" AND _jsonb->'b' = 212) OR (_jsonb->'c' = "name" AND _jsonb->'d' = 212))"#,
            parse(doc)
        )
    }

    #[test]
    fn test_with_gt_oper() {
        let doc = doc! {"age": {"$gt": 12}};
        assert_eq!(r#"_jsonb->'age' > 12"#, parse(doc))
    }
}
