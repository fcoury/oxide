check:
	cargo check --locked --all-features
start:
	cargo run --locked
debug:
	cargo run --locked -- --debug
test:
	cargo test --locked --all-features
web:
	cargo run --locked --features web -- web
