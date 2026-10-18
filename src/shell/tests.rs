use super::*;
use ::bson::{doc, spec::BinarySubtype, Binary, Bson, DateTime, Decimal128};
use rquickjs::CatchResultExt;
use std::{str::FromStr, thread, time::Instant};

fn session(port: u16) -> Shell {
    Shell::new(
        ClientOptions::builder()
            .hosts(vec![ServerAddress::Tcp {
                host: "127.0.0.1".into(),
                port: Some(port),
            }])
            .server_selection_timeout(Duration::from_secs(10))
            .direct_connection(true)
            .retry_writes(false)
            .build(),
    )
    .unwrap()
}

#[test]
fn persistent_session_recovers_from_errors_and_switches_database() {
    let mut shell = session(1);
    shell.eval("let n = 40").unwrap();
    assert_eq!(shell.eval("n += 2").unwrap().as_deref(), Some("42"));
    assert!(shell
        .eval("throw new Error('intentional')")
        .unwrap_err()
        .to_string()
        .contains("intentional"));
    assert!(shell.eval("this is invalid JS").is_err());
    assert_eq!(shell.eval("n").unwrap().as_deref(), Some("42"));
    shell.eval("use sample").unwrap();
    assert_eq!(shell.database().unwrap(), "sample");
    assert!(shell
        .eval("use sample\"); globalThis.compromised = true; //")
        .is_err());
    assert_eq!(
        shell.eval("typeof compromised").unwrap().as_deref(),
        Some("\"undefined\"")
    );
    shell
        .eval("Promise.resolve().then(() => { n += 1 })")
        .unwrap();
    assert_eq!(shell.eval("n").unwrap().as_deref(), Some("43"));
    assert!(shell
        .eval("Promise.resolve().then(() => { db.runCommand({ping:1}); }); throw new Error('stop')")
        .is_err());
    assert!(!shell.runtime.is_job_pending());
    assert_eq!(
        shell.eval("typeof n").unwrap().as_deref(),
        Some("\"undefined\"")
    );
    assert_eq!(shell.eval("6 * 7").unwrap().as_deref(), Some("42"));
}

#[test]
fn bson_round_trip_preserves_types_and_reserved_document_keys() {
    let shell = session(1);
    let values = doc! {
        "int": 1_i32, "long": i64::MAX, "minLong": i64::MIN,
        "double": 1.0_f64, "negativeZero": -0.0_f64,
        "date": DateTime::from_millis(1_700_000_000_123),
        "distantDate": DateTime::from_millis(i64::MAX),
        "decimal": Decimal128::from_str("123.4567890123456789012345678901234").unwrap(),
        "id": ::bson::oid::ObjectId::new(),
        "binary": Binary { subtype: BinarySubtype::UserDefined(128), bytes: vec![0, 255, 3] },
        "array": [Bson::Int64(1), Bson::Null],
        "literal": {"$oid": "ordinary document", "$numberLong": "123"},
        "__proto__": {"polluted": true},
        "regex": ::bson::Regex { pattern: "^hello".into(), options: "i".into() },
        "timestamp": ::bson::Timestamp { time: 123, increment: 4 },
        "min": Bson::MinKey, "max": Bson::MaxKey,
    };
    shell.context.with(|ctx| {
        let original = Bson::Document(values.clone());
        let js = bson::to_js(&ctx, original.clone()).unwrap();
        ctx.globals().set("record", js.clone()).unwrap();
        let actual = bson::from_js(&ctx, js).catch(&ctx).unwrap();
        assert_eq!(original, actual);
        assert_eq!(::bson::to_vec(&values).unwrap(), ::bson::to_vec(actual.as_document().unwrap()).unwrap());
        assert!(!ctx.eval::<bool, _>("({}).polluted === true").unwrap());
        let made: Value = ctx.eval("({ id: ObjectId('507f1f77bcf86cd799439011'), long: NumberLong('9223372036854775807'), bigint: -9223372036854775808n, date: ISODate('2026-01-02T03:04:05Z'), data: BinData(128, 'AP8D'), n: NumberDouble(1), plain: {$oid:'literal'} })").catch(&ctx).unwrap();
        let Bson::Document(made) = bson::from_js(&ctx, made).catch(&ctx).unwrap() else { panic!() };
        assert_eq!(made.get_i64("long").unwrap(), i64::MAX);
        assert_eq!(made.get_i64("bigint").unwrap(), i64::MIN);
        assert_eq!(made.get_f64("n").unwrap(), 1.0);
        assert!(matches!(made.get("date"), Some(Bson::DateTime(_))));
        assert_eq!(made.get_document("plain").unwrap().get_str("$oid").unwrap(), "literal");
    });
}

#[test]
fn conversion_rejects_lossy_and_unbounded_inputs() {
    let mut shell = session(1);
    for input in [
        "({n:9223372036854775808n})",
        "({n:9007199254740992})",
        "({d:new Date(NaN)})",
        "({x:undefined})",
        "({x:() => 1})",
        "({x:new Map()})",
        "(() => {const r = new RegExp('x'.repeat(1000000)); return Array(30).fill(r);})()",
        "({[Symbol('x')]:1})",
        "(() => { let x = {}; x.self = x; return x; })()",
        "(() => { let x = {v:'hello'}; for (let i=0;i<30;i++) x={a:x,b:x}; return x; })()",
    ] {
        shell.context.with(|ctx| {
            let value: Value = ctx.eval(input).unwrap();
            assert!(bson::from_js(&ctx, value).catch(&ctx).is_err(), "{input}");
        });
    }
    assert!(shell.eval("NumberLong(9007199254740992)").is_err());
    assert!(shell.eval("BinData(256, 'AA==')").is_err());
    assert_eq!(shell.eval("2 + 2").unwrap().as_deref(), Some("4"));
}

#[test]
fn interactive_inspection_preserves_bson_identity_and_script_json() {
    let mut shell = session(1);
    assert_eq!(
        shell
            .eval("ObjectId('507f1f77bcf86cd799439011')")
            .unwrap()
            .as_deref(),
        Some("{\n  \"$oid\": \"507f1f77bcf86cd799439011\"\n}")
    );
    let syntax = Arc::new(editor::Syntax::new().unwrap());
    shell.output = Output::Interactive {
        syntax,
        color: false,
    };
    shell.initial_database = "demo".into();
    shell.reset().unwrap();
    assert_eq!(shell.database().unwrap(), "demo");
    for (source, expected) in [
        (
            "ObjectId('507f1f77bcf86cd799439011')",
            "ObjectId('507f1f77bcf86cd799439011')",
        ),
        ("({$oid:'literal'})", "{ $oid: \"literal\" }"),
        (
            "NumberLong('9223372036854775807')",
            "NumberLong('9223372036854775807')",
        ),
        (
            "NumberLong('-9223372036854775808')",
            "NumberLong('-9223372036854775808')",
        ),
        ("NumberDouble('-0')", "NumberDouble('-0')"),
        ("NumberDouble('Infinity')", "NumberDouble('Infinity')"),
        ("NumberDouble('-Infinity')", "NumberDouble('-Infinity')"),
        ("NumberDouble('NaN')", "NumberDouble('NaN')"),
        (
            "NumberDecimal('123.4567890123456789012345678901234')",
            "NumberDecimal('123.4567890123456789012345678901234')",
        ),
        ("BinData(128,'AP8D')", "BinData(128, 'AP8D')"),
        (
            "ISODate('2026-01-02T03:04:05Z')",
            "ISODate(\"2026-01-02T03:04:05.000Z\")",
        ),
        ("9223372036854775808n", "9223372036854775808n"),
        (
            r#"({'odd key': 'line\n\u001b[31m'})"#,
            r#"{ "odd key": "line\n\u001b[31m" }"#,
        ),
        (
            "(() => {const x = {}; x.self=x; return x})()",
            "{ self: [Circular] }",
        ),
        (
            "({get bad() {throw Error('must not run')}})",
            "{ bad: [Getter/Setter] }",
        ),
        ("[,,42,,]", "[ <2 empty items>, 42, <1 empty items> ]"),
        ("(() => {const r=/hi/gi; Object.defineProperty(r,'source',{get(){throw Error('must not run')}}); r.toString=()=>{throw Error('must not run')}; return r})()", "/hi/gi"),
    ] {
        assert_eq!(
            shell.eval(source).unwrap().as_deref(),
            Some(expected),
            "{source}"
        );
    }
    assert!(shell
        .eval("Array(2000).fill(1)")
        .unwrap()
        .unwrap()
        .contains("[Truncated]"));
    assert!(shell
        .eval("(() => {let x={}; for(let i=0;i<20;i++) x={a:x}; return x})()")
        .unwrap()
        .unwrap()
        .contains("[Max depth]"));
    shell.eval("let typed = {id:ObjectId('507f1f77bcf86cd799439011'), n:NumberLong('9223372036854775807')}").unwrap();
    for source in [
        "Array(1000).fill('x'.repeat(1000000))",
        "BinData(128, 'AAAA'.repeat(100000))",
        "(() => {const x={}; for(let i=0;i<2000;i++) Object.defineProperty(x,'k'+i,{enumerable:true,get(){throw Error('must not run')}}); return x})()",
    ] {
        let output = shell.eval(source).unwrap().unwrap();
        assert!(output.contains("[Truncated]"), "{source}");
        assert!(output.len() < 100_000, "{source}: {}", output.len());
        assert_eq!(shell.eval("6*7").unwrap().as_deref(), Some("42"));
    }
    shell.eval("typed").unwrap();
    assert_eq!(shell.eval("JSON.stringify(typed)").unwrap(), shell.eval("JSON.stringify({id:ObjectId('507f1f77bcf86cd799439011'), n:NumberLong('9223372036854775807')})").unwrap());
    shell.eval("use other").unwrap();
    assert!(shell
        .eval("Promise.resolve().then(() => 1); throw Error('reset')")
        .is_err());
    assert_eq!(shell.database().unwrap(), "demo");
    assert_eq!(
        shell.eval("NumberLong('42')").unwrap().as_deref(),
        Some("NumberLong('42')")
    );
}

#[test]
fn interruption_recovers_from_infinite_js_and_blocked_database() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let mut shell = session(listener.local_addr().unwrap().port());
    for code in [
        "while (true) {}",
        "db.runCommand({ping: 1})",
        "function forever() { Promise.resolve().then(forever); } forever()",
        "function fan() { Promise.resolve().then(fan); Promise.resolve().then(fan); } fan()",
    ] {
        let signal = shell.interrupt_handle();
        let sender = thread::spawn(move || {
            thread::sleep(Duration::from_millis(100));
            signal.send_replace(true);
        });
        let started = Instant::now();
        assert!(shell.eval(code).is_err());
        assert!(started.elapsed() < Duration::from_secs(2), "{code}");
        sender.join().unwrap();
        assert_eq!(shell.eval("6 * 7").unwrap().as_deref(), Some("42"));
    }
}
