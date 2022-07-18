NAME = oxide

check:
	cargo check

start:
	./scripts/start.sh

test:
	# cargo nextest run
	cargo test
