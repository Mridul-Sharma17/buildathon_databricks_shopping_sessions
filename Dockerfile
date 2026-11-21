FROM rust:1.85.1-slim-bookworm AS build

WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src/ ./src/
RUN RUSTFLAGS='-C target-cpu=x86-64-v3' cargo build --release --locked

FROM debian:bookworm-slim

COPY --from=build /app/target/release/shopping-sessions /usr/local/bin/shopping-sessions
USER 1000:1000
CMD ["/usr/local/bin/shopping-sessions"]
