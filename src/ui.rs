use nickel::Nickel;

pub fn start(listen_addr: &str, port: u16) {
    let mut server = Nickel::new();

    server.utilize(router! {
        get "**" => |_req, _res| {
            "Hello world!"
        }
    });

    server.listen(format!("{}:{}", listen_addr, port)).unwrap();
}
