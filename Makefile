NAME = oxide

check:
	cargo check

start:
	./scripts/start.sh

debug:
	./scripts/start.sh --debug

shell:
	./scripts/start.sh shell

test:
	# cargo nextest run
	cargo test

web:
	./scripts/start.sh web
