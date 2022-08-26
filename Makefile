NAME = oxide

check:
	cargo check

start:
	./scripts/start.sh

debug:
	./scripts/start.sh --debug

test:
	# cargo nextest run
	cargo test

web:
	./scripts/start.sh web
