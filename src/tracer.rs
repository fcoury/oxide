use bson::Document;

pub trait Trace {
    fn trace(&self, doc: &Document, sql: &str);
}

pub struct DbTracer;

impl DbTracer {
    pub fn new() -> Self {
        Self {}
    }
}

impl Trace for DbTracer {
    fn trace(&self, doc: &Document, sql: &str) {
        println!("Trace: {:?} -> {}", doc, sql);
    }
}
