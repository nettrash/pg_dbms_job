# syntax=docker/dockerfile:1

FROM rust:1.93-bookworm AS build
WORKDIR /app

COPY Cargo.toml Cargo.lock* ./
COPY src ./src

RUN cargo build --release

FROM debian:bookworm-slim
WORKDIR /app

COPY --from=build /app/target/release/pg_dbms_job /usr/local/bin/pg_dbms_job

ENTRYPOINT ["/usr/local/bin/pg_dbms_job"]
