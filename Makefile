NAME = oxide

check:
	cargo check

start:
	cargo watch -x 'run --bin server'

client:
	cargo run --bin client

test:
	cargo nextest run
