#![allow(dead_code)]
use crate::utils::{field_to_jsonb, flatten_object};
use crate::{serializer::PostgresSerializer, utils::expand_object};
use bson::{Bson, Document};
use eyre::{eyre, Result};
use mongodb_language_model::{
    Clause, Expression, ExpressionTreeClause, LeafClause, LeafValue, ListOperator, Operator,
    OperatorExpressionOperator, Value, ValueOperator,
};
use serde_json::Map;
use std::fmt;

pub use self::update_parser::parse_update;
pub use self::update_parser::InvalidUpdateError;
pub use self::update_parser::UpdateDoc;
pub use self::update_parser::UpdateOper;

mod update_parser;

#[derive(Debug)]
struct UnimplementedError {
    pub kind: String,
    pub target: String,
}

impl UnimplementedError {
    pub fn new(kind: &str, target: &str) -> Self {
        Self {
            kind: kind.to_string(),
            target: target.to_string(),
        }
    }
}

impl std::fmt::Display for UnimplementedError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{} unimplemented: {}", self.kind, self.target)
    }
}

impl std::error::Error for UnimplementedError {}

pub fn parse(doc: Document) -> Result<String> {
    if doc.is_empty() {
        return Ok("".to_string());
    }
    let bson: Bson = doc.into();
    let json = serde_json::Value::Object(
        expand_object(bson.into_psql_json().as_object().unwrap()).unwrap(),
    );
    let str = serde_json::to_string(&json)?;
    let expression = mongodb_language_model::parse(&str)?;
    log::trace!("{:#?}", expression);
    parse_expression(expression)
}

fn parse_expression(expression: Expression) -> Result<String> {
    parse_clauses(expression.clauses)
}

fn parse_clauses(clauses: Vec<Clause>) -> Result<String> {
    let mut sql = vec![];
    for clause in clauses {
        let result = parse_clause(clause);
        if let Ok(clause) = result {
            sql.push(clause);
        } else {
            return result;
        }
    }

    if sql.len() > 1 {
        Ok(format!("({})", sql.join(" AND ")))
    } else {
        Ok(sql[0].to_string())
    }
}

fn parse_clause(clause: Clause) -> Result<String> {
    match clause {
        Clause::Leaf(leaf) => parse_leaf(leaf),
        Clause::ExpressionTree(exp_tree) => parse_expression_tree(exp_tree),
    }
}

fn parse_leaf(leaf: LeafClause) -> Result<String> {
    match leaf.value {
        Value::Leaf(leaf_value) => parse_leaf_value(leaf_value, leaf.key, None),
        Value::Operators(val_operators) => parse_value_operators(val_operators, leaf.key),
    }
}

fn parse_value_operators(val_operators: Vec<Operator>, field: String) -> Result<String> {
    let values: Result<Vec<String>> = val_operators
        .into_iter()
        .map(|operator| parse_operator(operator, field.clone()))
        .collect();

    if let Ok(values) = values {
        Ok(values.join(""))
    } else {
        Err(values.unwrap_err())
    }
}

fn parse_operator(oper: Operator, field: String) -> Result<String> {
    match oper {
        Operator::Value(value_oper) => parse_value_operator(value_oper, field.clone()),
        Operator::ExpressionOperator(expr_oper) => parse_expression_operator(expr_oper, field),
        Operator::List(list_oper) => parse_list_operator(list_oper, field),
    }
}

fn parse_list_operator(list_oper: ListOperator, field: String) -> Result<String> {
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
                Ok(clause)
            } else {
                Ok(format!("NOT ({})", clause))
            }
        }
        t => return Err(eyre!("list operator in parse_list_operator: {}", t)),
    }
}

fn parse_expression_operator(
    expr_oper: OperatorExpressionOperator,
    field: String,
) -> Result<String> {
    let operators: Result<Vec<String>> = expr_oper
        .operators
        .into_iter()
        .map(|operator| parse_operator(operator, field.clone()))
        .collect();
    if let Ok(operators) = operators {
        let operators_str = operators.join("");
        match expr_oper.operator.as_str() {
            "$not" => Ok(format!("NOT ({})", operators_str)),
            t => {
                return Err(eyre!(
                    "parse_expression_operator - unimplemented operator {:?}",
                    t
                ))
            }
        }
    } else {
        return Err(operators.unwrap_err());
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

fn parse_value_operator(value_oper: ValueOperator, field: String) -> Result<String> {
    let operator = match value_oper.operator.as_str() {
        "$lt" | "$lte" | "$gt" | "$gte" | "$ne" | "$eq" => {
            translate_operator(value_oper.operator.as_str())
        }
        "$regex" => "~",
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
                return Ok(format!("NOT ({})", stmt));
            } else {
                return Ok(stmt);
            }
        }
        t => unimplemented!("parse_value_operator - operator unimplemented {:?}", t),
    };

    parse_leaf_value(value_oper.value, field, Some(operator))
}

fn parse_expression_tree(exp_tree: ExpressionTreeClause) -> Result<String> {
    let operator = match exp_tree.operator.as_str() {
        "$and" => "AND",
        "$or" => "OR",
        // FIXME handle $nor expression
        // #17 - https://github.com/fcoury/oxide/issues/17
        // "$nor" => "NOR", FIXME
        t => {
            return Err(eyre!(
                "parse_expression_tree operator unimplemented = {:?}",
                t
            ))
        }
    };
    let sql: Vec<String> = exp_tree
        .expressions
        .into_iter()
        .map(|exp| parse_expression(exp).unwrap())
        .collect();

    if sql.len() > 1 {
        Ok(format!("({})", sql.join(&format!(" {} ", operator))))
    } else {
        Ok(sql[0].to_string())
    }
}

pub fn str_to_jsonb(value: String) -> String {
    format!("'{}'", value)
}

pub fn value_to_jsonb(value: &Bson) -> String {
    let new_value = value.clone().into_psql_json();
    new_value.to_string()
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

pub fn parse_object(field: &str, object: &Map<String, serde_json::Value>) -> Result<String> {
    let mut res = vec![];
    let source_flat_obj = flatten_object(object);
    let mut flat_obj: Map<String, serde_json::Value> = Map::new();
    for (key, value) in source_flat_obj {
        flat_obj.insert(format!("{}.{}", field, key), value);
    }

    for (key, v) in &flat_obj {
        let mut parts = key.split(".").collect::<Vec<&str>>();
        let mut value = OperatorValueType::Json(v.to_owned());
        let mut alternate = "".to_string();

        let oper = if parts[parts.len() - 1].starts_with("$") {
            let operator = parts.pop().unwrap();
            match operator {
                "$lt" | "$lte" | "$gt" | "$gte" | "$ne" | "$eq" => translate_operator(operator),
                "$regex" => {
                    let last = parts.pop().unwrap();
                    let mut field = parts
                        .iter()
                        .map(|f| format!("'{}'", f))
                        .collect::<Vec<String>>();
                    field.insert(0, "_jsonb".to_string());
                    let field = field.join("->");
                    let field = format!("{}->>'{}'", field, last);

                    let options_key = key.replace("$regex", "$options");
                    let options = flat_obj.get(&options_key);

                    let mut oper = "~";
                    if let Some(options) = options {
                        if options.as_str().unwrap().contains('i') {
                            oper = "~*";
                        }
                    }

                    let value = match v {
                        serde_json::Value::String(s) => s,
                        _ => return Err(eyre!("$regex has to be a string")),
                    };

                    return Ok(format!("{} {} '{}'", field, oper, value));
                }
                "$exists" => {
                    let negative = v.is_boolean() && !v.as_bool().unwrap()
                        || v.is_number() && v.as_i64().unwrap() == 0;

                    if negative {
                        let mut clauses = vec![];
                        let mut acc_parts = vec![];
                        for p in &parts {
                            let mut fields = acc_parts
                                .iter()
                                .map(|f| format!("'{}'", f))
                                .collect::<Vec<String>>()
                                .join("->");
                            if !fields.is_empty() {
                                fields = format!("->{}", fields);
                            }
                            clauses.push(format!("NOT _jsonb{} ? '{}'", fields, p));
                            acc_parts.push(p.clone());
                        }
                        res.push(format!("({})", clauses.join(" OR ")));
                        continue;
                    }

                    value = OperatorValueType::Field(parts.pop().unwrap().to_string());
                    "?"
                }
                "$in" | "$nin" => {
                    if !v.is_array() {
                        // FIXME error this out
                        todo!("Return an error when exists doesn't have an array");
                    }

                    let field = parts
                        .iter()
                        .map(|f| format!("'{}'", f))
                        .collect::<Vec<String>>()
                        .join("->>");
                    let field = if parts.len() > 1 {
                        format!("_jsonb->{}", field)
                    } else {
                        format!("_jsonb->>{}", field)
                    };
                    let values = v
                        .as_array()
                        .unwrap()
                        .iter()
                        .map(|v| v.to_string())
                        .collect::<Vec<String>>()
                        .join(", ");

                    let str = format!("{} = ANY('{{{}}}')", field, values);

                    if operator == "$nin" {
                        return Ok(format!("NOT ({})", str));
                    }

                    return Ok(str);
                }
                "$o" => {
                    return Ok(format!("_jsonb->'{}'->'$o' = '{}'", field, v));
                }
                "$d" => {
                    return Ok(format!("_jsonb->'{}'->'$d' = '{}'", field, v));
                }
                t => unimplemented!("parse_object - unimplemented {:?} in ${:?}", t, object),
            }
        } else {
            let mut alt_parts = parts.clone();
            let field = alt_parts.pop().unwrap();
            let str_value = value.to_string();
            let fields_expr = alt_parts
                .iter()
                .map(|p| format!("{p}[*]."))
                .collect::<Vec<String>>()
                .join("");
            alternate = format!(
                " OR jsonb_path_exists(_jsonb, '$[*].{fields_expr}{field} ? (@ == {str_value})')"
            );

            "="
        };

        let field = parts
            .iter()
            .map(|f| format!("'{}'", f))
            .collect::<Vec<String>>()
            .join("->");
        let field = format!("_jsonb->{}", field);
        let value = str_to_jsonb(value.to_string());
        let str = format!("({} {} {}{})", field, oper, value, alternate);
        res.push(str);
    }

    Ok(res.join(" AND "))
}

fn parse_leaf_value(leaf_value: LeafValue, f: String, operator: Option<&str>) -> Result<String> {
    let json = leaf_value.value;
    let mut field = field_to_jsonb(&f);

    if json.is_object() {
        let obj = json.as_object().unwrap();

        if obj.contains_key("$d") {
            let oper = operator.unwrap_or("=");
            return Ok(format!(
                "{}->'$d' {} {}",
                field,
                oper,
                str_to_jsonb(obj["$d"].to_string())
            ));
        } else {
            return parse_object(&f, obj);
        }
    }

    if json.is_number() {
        field = format!(
            "(jsonb_typeof({}) = 'number' OR jsonb_typeof({}->'$f') = 'number') AND CASE WHEN ({} ? '$f') THEN ({}->>'$f')::numeric ELSE ({})::numeric END",
            field, field, field, field, field
        );
    }

    let oper = operator.unwrap_or("=");
    Ok(format!(
        "{} {} {}",
        field,
        oper,
        str_to_jsonb(json.to_string())
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use bson::doc;

    #[test]
    fn test_empty() {
        let doc = doc! {};
        assert_eq!("", parse(doc).unwrap());
    }

    #[test]
    fn test_simple_str() {
        let doc = doc! {"name": "test"};
        assert_eq!(r#"_jsonb->'name' = '"test"'"#, parse(doc).unwrap())
    }

    #[test]
    fn test_simple_int() {
        let doc = doc! {"age": 12};
        assert_eq!(
            r#"(jsonb_typeof(_jsonb->'age') = 'number' OR jsonb_typeof(_jsonb->'age'->'$f') = 'number') AND CASE WHEN (_jsonb->'age' ? '$f') THEN (_jsonb->'age'->>'$f')::numeric ELSE (_jsonb->'age')::numeric END = '12'"#,
            parse(doc).unwrap()
        )
    }

    #[test]
    fn test_simple_double() {
        let doc = doc! {"age": Bson::Double(1.2)};
        assert_eq!(
            r#"(jsonb_typeof(_jsonb->'age') = 'number' OR jsonb_typeof(_jsonb->'age'->'$f') = 'number') AND CASE WHEN (_jsonb->'age' ? '$f') THEN (_jsonb->'age'->>'$f')::numeric ELSE (_jsonb->'age')::numeric END = '1.2'"#,
            parse(doc).unwrap()
        )
    }

    #[test]
    fn test_and() {
        let doc = doc! {"a": "name", "b": 212};
        assert_eq!(
            r#"(_jsonb->'a' = '"name"' AND (jsonb_typeof(_jsonb->'b') = 'number' OR jsonb_typeof(_jsonb->'b'->'$f') = 'number') AND CASE WHEN (_jsonb->'b' ? '$f') THEN (_jsonb->'b'->>'$f')::numeric ELSE (_jsonb->'b')::numeric END = '212')"#,
            parse(doc).unwrap()
        )
    }

    #[test]
    fn test_simple_or() {
        let doc = doc! {"$or": vec![
            doc! { "a": "name", "b": 212 },
        ]};
        assert_eq!(
            r#"(_jsonb->'a' = '"name"' AND (jsonb_typeof(_jsonb->'b') = 'number' OR jsonb_typeof(_jsonb->'b'->'$f') = 'number') AND CASE WHEN (_jsonb->'b' ? '$f') THEN (_jsonb->'b'->>'$f')::numeric ELSE (_jsonb->'b')::numeric END = '212')"#,
            parse(doc).unwrap()
        )
    }

    #[test]
    fn test_explicit_and() {
        let doc = doc! {"$and": vec![
            doc! { "a": 1, "b": 2, "c": 3 },
        ]};
        assert_eq!(
            r#"((jsonb_typeof(_jsonb->'a') = 'number' OR jsonb_typeof(_jsonb->'a'->'$f') = 'number') AND CASE WHEN (_jsonb->'a' ? '$f') THEN (_jsonb->'a'->>'$f')::numeric ELSE (_jsonb->'a')::numeric END = '1' AND (jsonb_typeof(_jsonb->'b') = 'number' OR jsonb_typeof(_jsonb->'b'->'$f') = 'number') AND CASE WHEN (_jsonb->'b' ? '$f') THEN (_jsonb->'b'->>'$f')::numeric ELSE (_jsonb->'b')::numeric END = '2' AND (jsonb_typeof(_jsonb->'c') = 'number' OR jsonb_typeof(_jsonb->'c'->'$f') = 'number') AND CASE WHEN (_jsonb->'c' ? '$f') THEN (_jsonb->'c'->>'$f')::numeric ELSE (_jsonb->'c')::numeric END = '3')"#,
            parse(doc).unwrap()
        )
    }

    #[test]
    fn test_and_or_combo() {
        let doc = doc! {"$or": vec![
            doc! { "a": "name", "b": 212 },
            doc! { "c": "name", "d": 212 },
        ]};
        assert_eq!(
            r#"((_jsonb->'a' = '"name"' AND (jsonb_typeof(_jsonb->'b') = 'number' OR jsonb_typeof(_jsonb->'b'->'$f') = 'number') AND CASE WHEN (_jsonb->'b' ? '$f') THEN (_jsonb->'b'->>'$f')::numeric ELSE (_jsonb->'b')::numeric END = '212') OR (_jsonb->'c' = '"name"' AND (jsonb_typeof(_jsonb->'d') = 'number' OR jsonb_typeof(_jsonb->'d'->'$f') = 'number') AND CASE WHEN (_jsonb->'d' ? '$f') THEN (_jsonb->'d'->>'$f')::numeric ELSE (_jsonb->'d')::numeric END = '212'))"#,
            parse(doc).unwrap()
        )
    }

    #[test]
    fn test_with_gt_oper() {
        let doc = doc! {"age": {"$gt": 12}};
        assert_eq!(
            r#"(jsonb_typeof(_jsonb->'age') = 'number' OR jsonb_typeof(_jsonb->'age'->'$f') = 'number') AND CASE WHEN (_jsonb->'age' ? '$f') THEN (_jsonb->'age'->>'$f')::numeric ELSE (_jsonb->'age')::numeric END > '12'"#,
            parse(doc).unwrap()
        )
    }

    #[test]
    fn test_with_simple_unary_not() {
        let doc = doc! { "age": {"$not": {"$gt": 12 } } };
        assert_eq!(
            r#"NOT ((jsonb_typeof(_jsonb->'age') = 'number' OR jsonb_typeof(_jsonb->'age'->'$f') = 'number') AND CASE WHEN (_jsonb->'age' ? '$f') THEN (_jsonb->'age'->>'$f')::numeric ELSE (_jsonb->'age')::numeric END > '12')"#,
            parse(doc).unwrap()
        )
    }

    #[test]
    fn test_with_unary_not() {
        let doc = doc! { "x": 1, "age": {"$not": {"$gt": 12} }, "y": 2 };
        assert_eq!(
            r#"((jsonb_typeof(_jsonb->'x') = 'number' OR jsonb_typeof(_jsonb->'x'->'$f') = 'number') AND CASE WHEN (_jsonb->'x' ? '$f') THEN (_jsonb->'x'->>'$f')::numeric ELSE (_jsonb->'x')::numeric END = '1' AND NOT ((jsonb_typeof(_jsonb->'age') = 'number' OR jsonb_typeof(_jsonb->'age'->'$f') = 'number') AND CASE WHEN (_jsonb->'age' ? '$f') THEN (_jsonb->'age'->>'$f')::numeric ELSE (_jsonb->'age')::numeric END > '12') AND (jsonb_typeof(_jsonb->'y') = 'number' OR jsonb_typeof(_jsonb->'y'->'$f') = 'number') AND CASE WHEN (_jsonb->'y' ? '$f') THEN (_jsonb->'y'->>'$f')::numeric ELSE (_jsonb->'y')::numeric END = '2')"#,
            parse(doc).unwrap()
        )
    }

    #[test]
    fn test_with_unary_not_and_or() {
        let doc =
            doc! { "age": {"$not": {"$gt": 12} }, "$or": vec! [ doc!{ "y": 2 }, doc!{ "x": 1 } ]};
        assert_eq!(
            r#"(NOT ((jsonb_typeof(_jsonb->'age') = 'number' OR jsonb_typeof(_jsonb->'age'->'$f') = 'number') AND CASE WHEN (_jsonb->'age' ? '$f') THEN (_jsonb->'age'->>'$f')::numeric ELSE (_jsonb->'age')::numeric END > '12') AND ((jsonb_typeof(_jsonb->'y') = 'number' OR jsonb_typeof(_jsonb->'y'->'$f') = 'number') AND CASE WHEN (_jsonb->'y' ? '$f') THEN (_jsonb->'y'->>'$f')::numeric ELSE (_jsonb->'y')::numeric END = '2' OR (jsonb_typeof(_jsonb->'x') = 'number' OR jsonb_typeof(_jsonb->'x'->'$f') = 'number') AND CASE WHEN (_jsonb->'x' ? '$f') THEN (_jsonb->'x'->>'$f')::numeric ELSE (_jsonb->'x')::numeric END = '1'))"#,
            parse(doc).unwrap()
        )
    }

    #[test]
    fn test_with_boolean() {
        let doc = doc! {
            "type": false,
        };

        assert_eq!(r#"_jsonb->'type' = 'false'"#, parse(doc).unwrap(),);
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

        let res = parse(doc).unwrap();
        println!("  res = {}", res);
    }

    #[test]
    fn test_exists() {
        assert_eq!(
            parse(doc! { "a": { "$exists": true } }).unwrap(),
            r#"_jsonb ? 'a'"#
        );
        assert_eq!(
            parse(doc! { "a": { "$exists": false } }).unwrap(),
            r#"NOT (_jsonb ? 'a')"#
        );
        assert_eq!(
            parse(doc! { "a.b": { "$exists": 1 } }).unwrap(),
            r#"(_jsonb->'a' ? 'b')"#
        );
        assert_eq!(
            parse(doc! { "a.b": { "$exists": 0 } }).unwrap(),
            r#"(NOT _jsonb ? 'a' OR NOT _jsonb->'a' ? 'b')"#
        );
        assert_eq!(
            parse(doc! { "a.b.c": { "$exists": 1 } }).unwrap(),
            r#"(_jsonb->'a'->'b' ? 'c')"#
        );
        assert_eq!(
            parse(doc! { "a.b.c.d.e.f": { "$exists": 0 } }).unwrap(),
            r#"(NOT _jsonb ? 'a' OR NOT _jsonb->'a' ? 'b' OR NOT _jsonb->'a'->'b' ? 'c' OR NOT _jsonb->'a'->'b'->'c' ? 'd' OR NOT _jsonb->'a'->'b'->'c'->'d' ? 'e' OR NOT _jsonb->'a'->'b'->'c'->'d'->'e' ? 'f')"#
        );
    }

    #[test]
    fn test_dot_nested() {
        assert_eq!(
            parse(doc! { "config.get.method": "GET" }).unwrap(),
            r#"(_jsonb->'config'->'get'->'method' = '"GET"' OR jsonb_path_exists(_jsonb, '$[*].config[*].get[*].method ? (@ == "GET")'))"#
        )
    }

    #[test]
    fn test_nested_find() {
        assert_eq!(
            parse(doc! { "a": { "b": { "c": 1, "d": 2 }, "e": 2 } }).unwrap(),
            r#"(_jsonb->'a'->'b'->'c' = '1' OR jsonb_path_exists(_jsonb, '$[*].a[*].b[*].c ? (@ == 1)')) AND (_jsonb->'a'->'b'->'d' = '2' OR jsonb_path_exists(_jsonb, '$[*].a[*].b[*].d ? (@ == 2)')) AND (_jsonb->'a'->'e' = '2' OR jsonb_path_exists(_jsonb, '$[*].a[*].e ? (@ == 2)'))"#
        )
    }

    #[test]
    fn test_nested_expression() {
        assert_eq!(
            parse(doc! { "a": { "b": { "$exists": 1 }, "c": { "$gt": 1 }, "e": "Felipe" } })
                .unwrap(),
            r#"(_jsonb->'a' ? 'b') AND (_jsonb->'a'->'c' > '1') AND (_jsonb->'a'->'e' = '"Felipe"' OR jsonb_path_exists(_jsonb, '$[*].a[*].e ? (@ == "Felipe")'))"#
        )
    }

    #[test]
    fn test_nested_exists_false() {
        assert_eq!(
            parse(doc! { "a": { "b": { "c" :{ "$exists": false } } } }).unwrap(),
            r#"(NOT _jsonb ? 'a' OR NOT _jsonb->'a' ? 'b' OR NOT _jsonb->'a'->'b' ? 'c')"#
        )
    }

    #[test]
    fn test_in() {
        assert_eq!(
            parse(doc! { "a": { "$in": [1, 2] } }).unwrap(),
            r#"_jsonb->>'a' = ANY('{1, 2}')"#
        );

        assert_eq!(
            parse(doc! { "a": { "$in": ["a", "b"] } }).unwrap(),
            r#"_jsonb->>'a' = ANY('{"a", "b"}')"#
        );

        assert_eq!(
            parse(doc! { "a.b": { "$in": ["a", "b"] } }).unwrap(),
            r#"_jsonb->'a'->>'b' = ANY('{"a", "b"}')"#
        );

        assert_eq!(
            parse(doc! { "a": { "b": { "$in": ["a", "b"] } } }).unwrap(),
            r#"_jsonb->'a'->>'b' = ANY('{"a", "b"}')"#
        );
    }

    #[test]
    fn test_nin() {
        assert_eq!(
            parse(doc! { "a": { "$nin": [1, 2] } }).unwrap(),
            r#"NOT (_jsonb->>'a' = ANY('{1, 2}'))"#
        );

        assert_eq!(
            parse(doc! { "a": { "$nin": ["a", "b"] } }).unwrap(),
            r#"NOT (_jsonb->>'a' = ANY('{"a", "b"}'))"#
        );

        assert_eq!(
            parse(doc! { "a.b": { "$nin": ["a", "b"] } }).unwrap(),
            r#"NOT (_jsonb->'a'->>'b' = ANY('{"a", "b"}'))"#
        );

        assert_eq!(
            parse(doc! { "a": { "b": { "$nin": ["a", "b"] } } }).unwrap(),
            r#"NOT (_jsonb->'a'->>'b' = ANY('{"a", "b"}'))"#
        );
    }

    #[test]
    fn test_date() {
        assert_eq!(
            parse(doc! { "a": { "$gt": { "$d": Bson::Int64(1659448486285) } } }).unwrap(),
            r#"_jsonb->'a'->'$d' > '1659448486285'"#
        )
    }

    #[test]
    fn test_regex() {
        assert_eq!(
            parse(doc! { "a": { "$regex": "^j" } }).unwrap(),
            r#"_jsonb->>'a' ~ '^j'"#
        )
    }

    #[test]
    fn test_regex_nested() {
        assert_eq!(
            parse(doc! { "a": { "b": { "$regex": "^j", "$options": "i" } } }).unwrap(),
            r#"_jsonb->'a'->>'b' ~* '^j'"#
        )
    }

    #[test]
    fn test_regex_ignore_case() {
        assert_eq!(
            parse(doc! { "a": { "$regex": "^j", "$options": "i" } }).unwrap(),
            r#"_jsonb->>'a' ~* '^j'"#
        )
    }

    #[test]
    fn test_regex_nested_ignore_case() {
        assert_eq!(
            parse(doc! { "a.b": { "$regex": "^j", "$options": "i" } }).unwrap(),
            r#"_jsonb->'a'->>'b' ~* '^j'"#
        )
    }

    #[test]
    fn test_regex_invalid() {
        assert_eq!(
            parse(doc! { "a": { "$regex": 1 } })
                .unwrap_err()
                .to_string(),
            "$regex has to be a string"
        )
    }

    #[test]
    fn test_oid() {
        assert_eq!(
            parse(doc! { "a": { "$o": "62e27ae37d8474ae4ce87c14" } }).unwrap(),
            r#"_jsonb->'a'->'$o' = '"62e27ae37d8474ae4ce87c14"'"#
        )
    }

    #[test]
    fn test_parse_object_with_date() {
        let bson: Bson = doc! { "$d": Bson::Int64(1659448486285) }.into();
        let json = bson.into_psql_json();
        let obj = json.as_object().unwrap();
        assert_eq!(
            parse_object("a", &obj).unwrap(),
            r#"_jsonb->'a'->'$d' = '1659448486285'"#
        )
    }

    #[test]
    fn test_value_to_jsonb_with_date() {
        let date = bson::DateTime::builder()
            .day(1)
            .month(1)
            .year(2019)
            .build()
            .unwrap();
        assert_eq!(value_to_jsonb(&date.into()), "{\"$d\":1546300800000}")
    }
}
