#![allow(dead_code)]
use crate::serializer::PostgresSerializer;
use bson::{Bson, Document};
use mongodb_language_model::{
    Clause, Expression, ExpressionTreeClause, LeafClause, LeafValue, Operator,
    OperatorExpressionOperator, Value, ValueOperator,
};

pub fn parse(doc: Document) -> String {
    if doc.is_empty() {
        return "".to_string();
    }
    let bson: Bson = doc.into();
    let json = bson.into_psql_json();
    let str = serde_json::to_string(&json).unwrap();
    let expression = mongodb_language_model::parse(&str).unwrap();
    log::debug!("{:#?}", expression);
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

fn field_to_jsonb(key: &str) -> String {
    format!("_jsonb->'{}'", key)
}

fn parse_leaf(leaf: LeafClause) -> String {
    let field = field_to_jsonb(&leaf.key);
    match leaf.value {
        Value::Leaf(leaf_value) => {
            format!("{} = {}", field, parse_leaf_value(leaf_value))
        }
        Value::Operators(val_operators) => {
            // format!("{} {}", field, parse_value_operators(val_operators))
            parse_value_operators(val_operators, field)
        }
    }
}

fn parse_value_operators(val_operators: Vec<Operator>, field: String) -> String {
    let values: Vec<String> = val_operators
        .into_iter()
        .map(|operator| parse_operator(operator, field.clone()))
        .collect();
    values.join("")
}

fn parse_operator(oper: Operator, field: String) -> String {
    match oper {
        Operator::Value(value_oper) => format!("{} {}", field, parse_value_operator(value_oper)),
        Operator::ExpressionOperator(expr_oper) => parse_expression_operator(expr_oper, field),
        t => unimplemented!("parse_operator - unimplemented {:?}", t),
    }
}

fn parse_expression_operator(expr_oper: OperatorExpressionOperator, field: String) -> String {
    let operators: Vec<String> = expr_oper
        .operators
        .into_iter()
        .map(|operator| parse_operator(operator, field.clone()))
        .collect();
    let operators_str = operators.join("");
    match expr_oper.operator.as_str() {
        "$not" => format!("NOT ({})", operators_str),
        t => unimplemented!("parse_expression_operator - unimplemented operator {:?}", t),
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
    // OperatorResult::new(format!("{} {}", operator, value))
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

fn value_to_jsonb(value: String) -> String {
    format!("'{}'", value)
}

fn parse_leaf_value(leaf_value: LeafValue) -> String {
    let json = leaf_value.value;
    value_to_jsonb(json.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use bson::doc;

    #[test]
    fn test_empty() {
        let doc = doc! {};
        assert_eq!("", parse(doc));
    }

    #[test]
    fn test_simple_str() {
        let doc = doc! {"name": "test"};
        assert_eq!(r#"_jsonb->'name' = '"test"'"#, parse(doc))
    }

    #[test]
    fn test_simple_int() {
        let doc = doc! {"age": 12};
        assert_eq!(r#"_jsonb->'age' = '12'"#, parse(doc))
    }

    #[test]
    fn test_and() {
        let doc = doc! {"a": "name", "b": 212};
        assert_eq!(
            r#"(_jsonb->'a' = '"name"' AND _jsonb->'b' = '212')"#,
            parse(doc)
        )
    }

    #[test]
    fn test_simple_or() {
        let doc = doc! {"$or": vec![
            doc! { "a": "name", "b": 212 },
        ]};
        assert_eq!(
            r#"(_jsonb->'a' = '"name"' AND _jsonb->'b' = '212')"#,
            parse(doc)
        )
    }

    #[test]
    fn test_explicit_and() {
        let doc = doc! {"$and": vec![
            doc! { "a": 1, "b": 2, "c": 3 },
        ]};
        assert_eq!(
            r#"(_jsonb->'a' = '1' AND _jsonb->'b' = '2' AND _jsonb->'c' = '3')"#,
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
            r#"((_jsonb->'a' = '"name"' AND _jsonb->'b' = '212') OR (_jsonb->'c' = '"name"' AND _jsonb->'d' = '212'))"#,
            parse(doc)
        )
    }

    #[test]
    fn test_with_gt_oper() {
        let doc = doc! {"age": {"$gt": 12}};
        assert_eq!(r#"_jsonb->'age' > '12'"#, parse(doc))
    }

    #[test]
    fn test_with_simple_unary_not() {
        let doc = doc! { "age": {"$not": {"$gt": 12 } } };
        assert_eq!(r#"NOT (_jsonb->'age' > '12')"#, parse(doc))
    }

    #[test]
    fn test_with_unary_not() {
        let doc = doc! { "x": 1, "age": {"$not": {"$gt": 12} }, "y": 2 };
        assert_eq!(
            r#"(_jsonb->'x' = '1' AND NOT (_jsonb->'age' > '12') AND _jsonb->'y' = '2')"#,
            parse(doc)
        )
    }

    #[test]
    fn test_with_unary_not_and_or() {
        let doc =
            doc! { "age": {"$not": {"$gt": 12} }, "$or": vec! [ doc!{ "y": 2 }, doc!{ "x": 1 } ]};
        assert_eq!(
            r#"(NOT (_jsonb->'age' > '12') AND (_jsonb->'y' = '2' OR _jsonb->'x' = '1'))"#,
            parse(doc)
        )
    }
}
