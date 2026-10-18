use super::*;

#[test]
fn continuation_understands_javascript_literals_and_invalid_input() {
    let syntax = Syntax::new().unwrap();
    for source in [
        "db.items.insertOne({",
        "db.items.aggregate([",
        "const total =",
        "function f() {",
        "const value = 1 +",
        "const x = true &&",
        "const x = false ||",
        "const x = null ??",
        "const x = !",
        "/* comment",
        "const x = 1; /* comment",
        "const t = `unfinished",
        "const t = `hello ${({x: 1})",
    ] {
        assert!(
            syntax.incomplete(source),
            "{source}: {}",
            syntax.parse(source).unwrap().root_node().to_sexp()
        );
    }
    for source in [
        "({text:'[', x:/[{}]/, n:3})",
        "/* [({ */ 42",
        "// {",
        "const t = `literal [ ${1 + 2}`",
        "const = 1;",
        "db.items.find(}",
        "let x = ;",
        "<div />",
        "use demo",
        "show collections",
        "db.items.find({\n",
        "'unterminated",
    ] {
        assert!(
            !syntax.incomplete(source),
            "{source}: {}",
            syntax.parse(source).unwrap().root_node().to_sexp()
        );
    }
    for line in [
        "db.items.insertOne({",
        "db.items.insertOne({\n  name: 'hello',",
    ] {
        assert_eq!(syntax.newline(line, line.len()).as_deref(), Some("\n  "));
    }
}

#[test]
fn highlighting_preserves_unicode_and_ignores_brackets_in_literals() {
    let syntax = Syntax::new().unwrap();
    let source = "({name: 'héllo 😀[', count: NumberLong('42'), ok: true, re: /[{}]/})";
    let rendered = syntax.highlight(source, None);
    assert!(rendered.contains("\x1b[32m"));
    assert!(rendered.contains("\x1b[36m"));
    let ansi = regex::Regex::new(r"\x1b\[[0-9;]*m").unwrap();
    assert_eq!(ansi.replace_all(&rendered, ""), source);
    let tree = syntax.parse(source).unwrap();
    let pairs = brackets(tree.root_node()).pairs;
    assert_eq!(pairs.len(), 3); // outer parentheses, object, NumberLong call
    assert!(syntax.highlight(source, Some(1)).contains("\x1b[7m"));
}

#[test]
fn completion_is_literal_safe_and_scoped_to_the_database() {
    let mut helper = ShellHelper::new(Arc::new(Syntax::new().unwrap()), true);
    let names = [
        "items",
        "odd-name",
        "quote\"name",
        "name",
        "__proto__",
        "isPrototypeOf",
    ];
    helper.refresh("test", Some(names.iter().map(|s| (*s).into()).collect()));
    let history = DefaultHistory::new();
    let context = Context::new(&history);
    let candidates = |helper: &ShellHelper, source: &str| {
        helper
            .complete(source, source.len(), &context)
            .unwrap()
            .1
            .into_iter()
            .map(|p| p.replacement)
            .collect::<Vec<_>>()
    };
    assert_eq!(candidates(&helper, "db.it"), ["db.items"]);
    assert_eq!(
        candidates(&helper, "db.odd"),
        [r#"db.getCollection("odd-name")"#]
    );
    assert_eq!(
        candidates(&helper, "db.quote"),
        [r#"db.getCollection("quote\"name")"#]
    );
    assert_eq!(
        candidates(&helper, "db.name"),
        [r#"db.getCollection("name")"#]
    );
    assert_eq!(
        candidates(&helper, "db.items.ins"),
        ["db.items.insertMany", "db.items.insertOne"]
    );
    assert_eq!(candidates(&helper, "ObjectI"), ["ObjectId"]);
    assert_eq!(
        candidates(&helper, "db.isProto"),
        [r#"db.getCollection("isPrototypeOf")"#]
    );
    assert_eq!(
        candidates(&helper, r#"db.getCollection("odd-name").fi"#),
        [
            r#"db.getCollection("odd-name").find"#,
            r#"db.getCollection("odd-name").findOne"#,
        ]
    );
    assert_eq!(
        candidates(&helper, r#"db.getCollection("日本語").findO"#),
        [r#"db.getCollection("日本語").findOne"#,]
    );
    assert_eq!(candidates(&helper, "é + ObjectI"), ["ObjectId"]);
    assert!(candidates(&helper, "😀").is_empty());
    for input in [
        "'db.it",
        "// db.it",
        "/db.it/",
        "danger().it",
        "db.items.find().it",
        "db.getCollection(danger()).fi",
    ] {
        assert!(candidates(&helper, input).is_empty(), "{input}");
    }
    helper.refresh("test", None);
    assert_eq!(candidates(&helper, "db.it"), ["db.items"]);
    helper.refresh("other", None);
    assert!(candidates(&helper, "db.it").is_empty());
}
