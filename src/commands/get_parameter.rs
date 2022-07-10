use crate::commands::Handler;
use crate::handler::{CommandExecutionError, Request};
use bson::{doc, Bson, Document};

pub struct GetParameter {}

fn get_params(doc: Document) -> (bool, bool) {
    if let Some(param) = doc.get("getParameter") {
        match param {
            Bson::String(str) => (str == "*", false),
            Bson::Document(doc) => (
                doc.get_bool("allParameters").unwrap_or(false),
                doc.get_bool("showDetails").unwrap_or(false),
            ),
            _ => (false, false),
        }
    } else {
        (false, false)
    }
}

impl Handler for GetParameter {
    fn new() -> Self {
        GetParameter {}
    }

    fn handle(
        &self,
        _request: &Request,
        docs: &Vec<Document>,
    ) -> Result<Document, CommandExecutionError> {
        let doc = docs[0].clone();

        let data = doc! {
            "acceptApiVersion2": doc! {
                "value": false,
                "settableAtRuntime": true,
                "settableAtStartup": true,
            },
            "authSchemaVersion": doc! {
                "value": Bson::Int32(5),
                "settableAtRuntime": true,
                "settableAtStartup": true,
            },
            "tlsMode": doc! {
                "value": "disabled",
                "settableAtRuntime": true,
                "settableAtStartup": false,
            },
            "sslMode": doc! {
                "value": "disabled",
                "settableAtRuntime": true,
                "settableAtStartup": false,
            },
            "quiet": doc! {
                "value": false,
                "settableAtRuntime": true,
                "settableAtStartup": true,
            },
            "featureCompatibilityVersion": doc! {
                "value": Bson::Double(5.0),
                "settableAtRuntime": true,
                "settableAtStartup": true,
            },
        };

        let (all_params, show_details) = get_params(doc.clone());
        let selected_keys = if all_params { data.keys() } else { doc.keys() };

        if all_params && show_details {
            let mut doc = data.clone();
            doc.insert("ok", Bson::Double(1.0));
            return Ok(doc);
        }

        // determine what keys from doc we need to return
        let keys: Vec<String> = selected_keys
            .into_iter()
            .filter(|k| {
                k.as_str() != "getParameter" && k.as_str() != "comment" && k.as_str() != "$db"
            })
            .filter(|k| all_params || doc.get(k).is_some())
            .map(|k| k.to_string())
            .collect();

        // filters the keys from data and if show_details is true returns the whole object
        // otherwise just the value of the key
        let mut res = doc! {};
        for key in keys {
            if let Some(value) = data.get(key.clone()) {
                if show_details {
                    res.insert(key.to_string(), value.clone());
                } else {
                    res.insert(
                        key.to_string(),
                        value.as_document().unwrap().get("value").unwrap(),
                    );
                }
            }
        }

        res.insert("ok", Bson::Double(1.0));
        Ok(res)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_params_asterisk_test() {
        let doc = doc! {
            "getParameter": "*"
        };
        let (all, show_details) = get_params(doc);
        assert_eq!(all, true);
        assert_eq!(show_details, false);
    }

    #[test]
    fn get_params_all_only_test() {
        let doc = doc! {
            "getParameter": doc! { "allParameters": true }
        };
        let (all, show_details) = get_params(doc);
        assert_eq!(all, true);
        assert_eq!(show_details, false);
    }

    #[test]
    fn get_params_show_details_only_test() {
        let doc = doc! {
            "getParameter": doc! { "showDetails": true }
        };
        let (all, show_details) = get_params(doc);
        assert_eq!(all, false);
        assert_eq!(show_details, true);
    }

    #[test]
    fn get_params_none_test() {
        let doc = doc! {};
        let (all, show_details) = get_params(doc);
        assert_eq!(all, false);
        assert_eq!(show_details, false);
    }
}
