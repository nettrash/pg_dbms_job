# syntax=docker/dockerfile:1

FROM rust:1.81-bullseye AS build
WORKDIR /app

COPY Cargo.toml Cargo.lock* ./
COPY src ./src

RUN cargo build --release

FROM debian:bullseye-slim
WORKDIR /app

COPY --from=build /app/target/release/pg_dbms_job /usr/local/bin/pg_dbms_job

ENTRYPOINT ["/usr/local/bin/pg_dbms_job"]
