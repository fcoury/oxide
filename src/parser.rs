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

pub fn field_to_jsonb(key: &str) -> String {
    format!("_jsonb->'{}'", key)
}

fn parse_leaf(leaf: LeafClause) -> String {
    match leaf.value {
        Value::Leaf(leaf_value) => {
            let (field, value) = parse_leaf_value(leaf_value, leaf.key);
            format!("{} = {}", field, value)
        }
        Value::Operators(val_operators) => parse_value_operators(val_operators, leaf.key),
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
        Operator::Value(value_oper) => parse_value_operator(value_oper, field.clone()),
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

fn parse_value_operator(value_oper: ValueOperator, field: String) -> String {
    let operator = match value_oper.operator.as_str() {
        "$lt" => "<",
        "$lte" => "<=",
        "$gt" => ">",
        "$gte" => ">=",
        "$ne" => "!=",
        "$eq" => "=",
        t => unimplemented!("parse_value_operator - operator unimplemented {:?}", t),
    };
    let (field, value) = parse_leaf_value(value_oper.value, field);
    format!("{} {} {}", field, operator, value)
}

fn parse_expression_tree(exp_tree: ExpressionTreeClause) -> String {
    let operator = match exp_tree.operator.as_str() {
        "$and" => "AND",
        "$or" => "OR",
        // FIXME handle $nor expression
        // #17 - https://github.com/fcoury/oxide/issues/17
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

pub fn value_to_jsonb(value: String) -> String {
    format!("'{}'", value)
}

fn parse_leaf_value(leaf_value: LeafValue, f: String) -> (String, String) {
    let json = leaf_value.value;
    let mut field = field_to_jsonb(&f);

    if json.is_number() {
        field = format!(
            "(jsonb_typeof({}) = 'number' OR jsonb_typeof({}->'$f') = 'number') AND (CASE WHEN ({} ? '$f') THEN ({}->>'$f')::numeric ELSE ({})::numeric END)",
            field, field, field, field, field
        );
    }
    (field, value_to_jsonb(json.to_string()))
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
        assert_eq!(
            r#"(jsonb_typeof(_jsonb->'age') = 'number' OR jsonb_typeof(_jsonb->'age'->'$f') = 'number') AND (CASE WHEN (_jsonb->'age' ? '$f') THEN (_jsonb->'age'->>'$f')::numeric ELSE (_jsonb->'age')::numeric END) = '12'"#,
            parse(doc)
        )
    }

    #[test]
    fn test_simple_double() {
        let doc = doc! {"age": Bson::Double(1.2)};
        assert_eq!(
            r#"(jsonb_typeof(_jsonb->'age') = 'number' OR jsonb_typeof(_jsonb->'age'->'$f') = 'number') AND (CASE WHEN (_jsonb->'age' ? '$f') THEN (_jsonb->'age'->>'$f')::numeric ELSE (_jsonb->'age')::numeric END) = '1.2'"#,
            parse(doc)
        )
    }

    #[test]
    fn test_and() {
        let doc = doc! {"a": "name", "b": 212};
        assert_eq!(
            r#"(_jsonb->'a' = '"name"' AND (jsonb_typeof(_jsonb->'b') = 'number' OR jsonb_typeof(_jsonb->'b'->'$f') = 'number') AND (CASE WHEN (_jsonb->'b' ? '$f') THEN (_jsonb->'b'->>'$f')::numeric ELSE (_jsonb->'b')::numeric END) = '212')"#,
            parse(doc)
        )
    }

    #[test]
    fn test_simple_or() {
        let doc = doc! {"$or": vec![
            doc! { "a": "name", "b": 212 },
        ]};
        assert_eq!(
            r#"(_jsonb->'a' = '"name"' AND (jsonb_typeof(_jsonb->'b') = 'number' OR jsonb_typeof(_jsonb->'b'->'$f') = 'number') AND (CASE WHEN (_jsonb->'b' ? '$f') THEN (_jsonb->'b'->>'$f')::numeric ELSE (_jsonb->'b')::numeric END) = '212')"#,
            parse(doc)
        )
    }

    #[test]
    fn test_explicit_and() {
        let doc = doc! {"$and": vec![
            doc! { "a": 1, "b": 2, "c": 3 },
        ]};
        assert_eq!(
            r#"((jsonb_typeof(_jsonb->'a') = 'number' OR jsonb_typeof(_jsonb->'a'->'$f') = 'number') AND (CASE WHEN (_jsonb->'a' ? '$f') THEN (_jsonb->'a'->>'$f')::numeric ELSE (_jsonb->'a')::numeric END) = '1' AND (jsonb_typeof(_jsonb->'b') = 'number' OR jsonb_typeof(_jsonb->'b'->'$f') = 'number') AND (CASE WHEN (_jsonb->'b' ? '$f') THEN (_jsonb->'b'->>'$f')::numeric ELSE (_jsonb->'b')::numeric END) = '2' AND (jsonb_typeof(_jsonb->'c') = 'number' OR jsonb_typeof(_jsonb->'c'->'$f') = 'number') AND (CASE WHEN (_jsonb->'c' ? '$f') THEN (_jsonb->'c'->>'$f')::numeric ELSE (_jsonb->'c')::numeric END) = '3')"#,
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
            r#"((_jsonb->'a' = '"name"' AND (jsonb_typeof(_jsonb->'b') = 'number' OR jsonb_typeof(_jsonb->'b'->'$f') = 'number') AND (CASE WHEN (_jsonb->'b' ? '$f') THEN (_jsonb->'b'->>'$f')::numeric ELSE (_jsonb->'b')::numeric END) = '212') OR (_jsonb->'c' = '"name"' AND (jsonb_typeof(_jsonb->'d') = 'number' OR jsonb_typeof(_jsonb->'d'->'$f') = 'number') AND (CASE WHEN (_jsonb->'d' ? '$f') THEN (_jsonb->'d'->>'$f')::numeric ELSE (_jsonb->'d')::numeric END) = '212'))"#,
            parse(doc)
        )
    }

    #[test]
    fn test_with_gt_oper() {
        let doc = doc! {"age": {"$gt": 12}};
        assert_eq!(
            r#"(jsonb_typeof(_jsonb->'age') = 'number' OR jsonb_typeof(_jsonb->'age'->'$f') = 'number') AND (CASE WHEN (_jsonb->'age' ? '$f') THEN (_jsonb->'age'->>'$f')::numeric ELSE (_jsonb->'age')::numeric END) > '12'"#,
            parse(doc)
        )
    }

    #[test]
    fn test_with_simple_unary_not() {
        let doc = doc! { "age": {"$not": {"$gt": 12 } } };
        assert_eq!(
            r#"NOT ((jsonb_typeof(_jsonb->'age') = 'number' OR jsonb_typeof(_jsonb->'age'->'$f') = 'number') AND (CASE WHEN (_jsonb->'age' ? '$f') THEN (_jsonb->'age'->>'$f')::numeric ELSE (_jsonb->'age')::numeric END) > '12')"#,
            parse(doc)
        )
    }

    #[test]
    fn test_with_unary_not() {
        let doc = doc! { "x": 1, "age": {"$not": {"$gt": 12} }, "y": 2 };
        assert_eq!(
            r#"((jsonb_typeof(_jsonb->'x') = 'number' OR jsonb_typeof(_jsonb->'x'->'$f') = 'number') AND (CASE WHEN (_jsonb->'x' ? '$f') THEN (_jsonb->'x'->>'$f')::numeric ELSE (_jsonb->'x')::numeric END) = '1' AND NOT ((jsonb_typeof(_jsonb->'age') = 'number' OR jsonb_typeof(_jsonb->'age'->'$f') = 'number') AND (CASE WHEN (_jsonb->'age' ? '$f') THEN (_jsonb->'age'->>'$f')::numeric ELSE (_jsonb->'age')::numeric END) > '12') AND (jsonb_typeof(_jsonb->'y') = 'number' OR jsonb_typeof(_jsonb->'y'->'$f') = 'number') AND (CASE WHEN (_jsonb->'y' ? '$f') THEN (_jsonb->'y'->>'$f')::numeric ELSE (_jsonb->'y')::numeric END) = '2')"#,
            parse(doc)
        )
    }

    #[test]
    fn test_with_unary_not_and_or() {
        let doc =
            doc! { "age": {"$not": {"$gt": 12} }, "$or": vec! [ doc!{ "y": 2 }, doc!{ "x": 1 } ]};
        assert_eq!(
            r#"(NOT ((jsonb_typeof(_jsonb->'age') = 'number' OR jsonb_typeof(_jsonb->'age'->'$f') = 'number') AND (CASE WHEN (_jsonb->'age' ? '$f') THEN (_jsonb->'age'->>'$f')::numeric ELSE (_jsonb->'age')::numeric END) > '12') AND ((jsonb_typeof(_jsonb->'y') = 'number' OR jsonb_typeof(_jsonb->'y'->'$f') = 'number') AND (CASE WHEN (_jsonb->'y' ? '$f') THEN (_jsonb->'y'->>'$f')::numeric ELSE (_jsonb->'y')::numeric END) = '2' OR (jsonb_typeof(_jsonb->'x') = 'number' OR jsonb_typeof(_jsonb->'x'->'$f') = 'number') AND (CASE WHEN (_jsonb->'x' ? '$f') THEN (_jsonb->'x'->>'$f')::numeric ELSE (_jsonb->'x')::numeric END) = '1'))"#,
            parse(doc)
        )
    }

    #[test]
    fn test_with_boolean() {
        let doc = doc! {
            "type": false,
        };

        assert_eq!(r#"_jsonb->'type' = 'false'"#, parse(doc),);
    }

    #[test]
    #[ignore = "missing $in and $exists"]
    fn test_with_in_and_exists() {
        let doc = doc! {
            "type": "queue",
            "$or": [
                { "allowedGroups": { "$in": [ "AUTH_GROUP", "Cognito_Admin" ] } },
                { "allowedGroups": { "$exists": false } }
            ]
        };

        let res = parse(doc);
        println!("  res = {}", res);
    }
}
