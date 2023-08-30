FROM lukemathwalker/cargo-chef:latest-rust-slim-bookworm as chef
WORKDIR /app

FROM chef as planner
COPY . .
RUN cargo chef prepare  --recipe-path recipe.json

FROM chef as builder
RUN apt-get update && apt-get install -y libssl-dev pkg-config perl
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json
COPY . .
RUN cargo build --release

FROM matrix2016/shush-rs:latest AS runtime
WORKDIR /app
COPY --from=builder /app/target/release/app .
ENTRYPOINT ["shush-rs", "exec", "--", "./app"]
