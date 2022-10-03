FROM rust:1.64 AS builder
COPY . .
RUN cargo build --release

FROM debian:bullseye-slim
COPY --from=builder ./target/release/oxide ./target/release/oxide

EXPOSE 27017 8087
CMD ["/target/release/oxide", "--web", "--listen-addr", "0.0.0.0", "--web-addr", "0.0.0.0:8087"]
