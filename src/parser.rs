#![allow(dead_code)]
use crate::serializer::PostgresSerializer;
use crate::utils::flatten_object;
use bson::{Bson, Document};
use mongodb_language_model::{
    Clause, Expression, ExpressionTreeClause, LeafClause, LeafValue, ListOperator, Operator,
    OperatorExpressionOperator, Value, ValueOperator,
};
use serde_json::Map;
use std::fmt;

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
        Value::Leaf(leaf_value) => parse_leaf_value(leaf_value, leaf.key, None),
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
        Operator::List(list_oper) => parse_list_operator(list_oper, field),
    }
}

fn parse_list_operator(list_oper: ListOperator, field: String) -> String {
    match list_oper.operator.as_str() {
        "$in" | "$nin" => {
            let values: Vec<serde_json::Value> = list_oper
                .values
                .into_iter()
                .map(|leaf_value| leaf_value.value)
                .collect();
            let jsonb_field = if field.contains(".") {
                let fields = field
                    .split(".")
                    .collect::<Vec<&str>>()
                    .iter()
                    .map(|f| format!("'{}'", f))
                    .collect::<Vec<String>>()
                    .join("->>");
                format!("_jsonb->{}", fields)
            } else {
                format!("_jsonb->>'{}'", field)
            };

            let clause = format!(
                "{} = ANY('{{{}}}')",
                jsonb_field,
                values
                    .iter()
                    .map(|v| v.to_string())
                    .collect::<Vec<String>>()
                    .join(", ")
            );

            if list_oper.operator.as_str() == "$in" {
                clause
            } else {
                format!("NOT ({})", clause)
            }
        }
        t => unimplemented!("parse_list_operator - unimplemented {:?}", t),
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

fn translate_operator(oper: &str) -> &str {
    match oper {
        "$lt" => "<",
        "$lte" => "<=",
        "$gt" => ">",
        "$gte" => ">=",
        "$ne" => "!=",
        "$eq" => "=",
        other => other,
    }
}

fn parse_value_operator(value_oper: ValueOperator, field: String) -> String {
    let operator = match value_oper.operator.as_str() {
        "$lt" | "$lte" | "$gt" | "$gte" | "$ne" | "$eq" => {
            translate_operator(value_oper.operator.as_str())
        }
        "$exists" => {
            let source = "_jsonb".to_string();
            let value = value_oper.value.value;

            let (field, target) = if field.contains(".") {
                let parts = field.split(".").collect::<Vec<&str>>();
                let field = parts[parts.len() - 1].to_string();
                let target = format!("_jsonb->'{}'", parts[0..parts.len() - 1].join("'->'"));
                (field, target)
            } else {
                (field, source)
            };

            let stmt = format!("{} ? '{}'", target, field);
            if (value.is_boolean() && !value.as_bool().unwrap())
                || value.is_number() && value.as_i64().unwrap() == 0
            {
                return format!("NOT ({})", stmt);
            } else {
                return stmt;
            }
        }
        t => unimplemented!("parse_value_operator - operator unimplemented {:?}", t),
    };

    parse_leaf_value(value_oper.value, field, Some(operator))
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

enum OperatorValueType {
    Json(serde_json::Value),
    Field(String),
}

impl fmt::Display for OperatorValueType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            OperatorValueType::Json(json) => write!(f, "{}", json),
            OperatorValueType::Field(field) => write!(f, "{}", field),
        }
    }
}

fn parse_object(field: &str, object: &Map<String, serde_json::Value>) -> String {
    let mut res = vec![];
    let source_flat_obj = flatten_object(object);
    let mut flat_obj: Map<String, serde_json::Value> = Map::new();
    for (key, value) in source_flat_obj {
        flat_obj.insert(format!("{}.{}", field, key), value);
    }

    for (key, v) in flat_obj {
        let mut parts = key.split(".").collect::<Vec<&str>>();
        let mut value = OperatorValueType::Json(v);

        let oper = if parts[parts.len() - 1].starts_with("$") {
            let operator = parts.pop().unwrap();
            match operator {
                "$lt" | "$lte" | "$gt" | "$gte" | "$ne" | "$eq" => translate_operator(operator),
                "$exists" => {
                    value = OperatorValueType::Field(parts.pop().unwrap().to_string());
                    "?"
                }
                t => unimplemented!("parse_object - unimplemented {:?}", t),
            }
        } else {
            "="
        };

        let field = parts
            .iter()
            .map(|f| format!("'{}'", f))
            .collect::<Vec<String>>()
            .join("->");
        let field = format!("_jsonb->{}", field);
        let value = value_to_jsonb(value.to_string());
        res.push(format!("{} {} {}", field, oper, value));
    }

    res.join(" AND ")
}

fn parse_leaf_value(leaf_value: LeafValue, f: String, operator: Option<&str>) -> String {
    let json = leaf_value.value;
    let mut field = field_to_jsonb(&f);

    if json.is_object() {
        let obj = json.as_object().unwrap();
        return parse_object(&f, obj);
    }

    if json.is_number() {
        field = format!(
            "(jsonb_typeof({}) = 'number' OR jsonb_typeof({}->'$f') = 'number') AND (CASE WHEN ({} ? '$f') THEN ({}->>'$f')::numeric ELSE ({})::numeric END)",
            field, field, field, field, field
        );
    }

    let oper = operator.unwrap_or("=");
    format!("{} {} {}", field, oper, value_to_jsonb(json.to_string()))
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

    #[test]
    fn test_exists() {
        assert_eq!(parse(doc! { "a": { "$exists": true } }), r#"_jsonb ? 'a'"#);
        assert_eq!(
            parse(doc! { "a": { "$exists": false } }),
            r#"NOT (_jsonb ? 'a')"#
        );
        assert_eq!(
            parse(doc! { "a.b": { "$exists": 1 } }),
            r#"_jsonb->'a' ? 'b'"#
        );
        assert_eq!(
            parse(doc! { "a.b": { "$exists": 0 } }),
            r#"NOT (_jsonb->'a' ? 'b')"#
        );
        assert_eq!(
            parse(doc! { "a.b.c": { "$exists": 1 } }),
            r#"_jsonb->'a'->'b' ? 'c'"#
        );
        assert_eq!(
            parse(doc! { "a.b.c.d.e.f": { "$exists": 0 } }),
            r#"NOT (_jsonb->'a'->'b'->'c'->'d'->'e' ? 'f')"#
        );
    }

    #[test]
    fn test_nested_find() {
        assert_eq!(
            parse(doc! { "a": { "b": { "c": 1, "d": 2 }, "e": 2 } }),
            r#"_jsonb->'a'->'b'->'c' = '1' AND _jsonb->'a'->'b'->'d' = '2' AND _jsonb->'a'->'e' = '2'"#
        )
    }

    #[test]
    fn test_nested_expression() {
        assert_eq!(
            parse(doc! { "a": { "b": { "$exists": 1 }, "c": { "$gt": 1 }, "e": "Felipe" } }),
            r#"_jsonb->'a' ? 'b' AND _jsonb->'a'->'c' > '1' AND _jsonb->'a'->'e' = '"Felipe"'"#
        )
    }

    #[test]
    fn test_in() {
        assert_eq!(
            parse(doc! { "a": { "$in": [1, 2] } }),
            r#"_jsonb->>'a' = ANY('{1, 2}')"#
        );

        assert_eq!(
            parse(doc! { "a": { "$in": ["a", "b"] } }),
            r#"_jsonb->>'a' = ANY('{"a", "b"}')"#
        );

        assert_eq!(
            parse(doc! { "a.b": { "$in": ["a", "b"] } }),
            r#"_jsonb->'a'->>'b' = ANY('{"a", "b"}')"#
        );
    }

    #[test]
    fn test_nin() {
        assert_eq!(
            parse(doc! { "a": { "$nin": [1, 2] } }),
            r#"NOT (_jsonb->>'a' = ANY('{1, 2}'))"#
        );

        assert_eq!(
            parse(doc! { "a": { "$nin": ["a", "b"] } }),
            r#"NOT (_jsonb->>'a' = ANY('{"a", "b"}'))"#
        );

        assert_eq!(
            parse(doc! { "a.b": { "$nin": ["a", "b"] } }),
            r#"NOT (_jsonb->'a'->>'b' = ANY('{"a", "b"}'))"#
        );
    }
}
