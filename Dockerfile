FROM rust:1.98.1-bookworm AS builder
WORKDIR /src
COPY . .
RUN cargo build --locked --release --no-default-features

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends tini \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /src/target/release/oxide /usr/local/bin/oxide
USER 65534:65534
EXPOSE 27017
# Forward container termination signals to the server instead of ignoring them as PID 1.
ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/oxide"]
CMD ["--listen-addr", "0.0.0.0"]
