use oxide::handler::handle;
use oxide::wire::{OpMsg, OP_MSG};
use bson::doc;

#[test]
fn test_list_database() {
  let list_doc = doc!{
    "listDatabases": true,
  };
  let msg = OpMsg::new_with_body_kind(list_doc);
  let res = handle(1, msg).unwrap();
  let res_msg = OpMsg::parse(&res);
  assert_eq!(res_msg.header.op_code, OP_MSG);
  assert_eq!(res_msg.sections[0].kind, 0);
  // assert_eq!(res_msg.sections[0].documents[0]["databases"]["name"].as_str(), "admin");
}
