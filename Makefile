NAME = oxide

check:
	cargo check

start:
	cargo watch -x 'run'

test:
	cargo test
