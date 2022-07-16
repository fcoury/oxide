NAME = oxide

check:
	cargo check

start:
	./scripts/start.sh

test:
	cargo nextest run

devweb:
	./scripts/start.sh web
