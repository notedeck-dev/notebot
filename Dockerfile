# syntax=docker/dockerfile:1
FROM rust:1-slim-bookworm AS builder
WORKDIR /build

# ビルド対象の example (自作 bot はこのリポジトリの examples/ に置くか、
# 自分のクレートで同様の Dockerfile を書く)
ARG EXAMPLE=echo

COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY examples ./examples

# 認証は NOTEBOT_TOKEN の直接注入 (トークンを DB に書かない) を使うため、
# コンテナ内で使えない keyring feature は無効化する。
RUN cargo build --release --example "${EXAMPLE}" --no-default-features

FROM debian:bookworm-slim
ARG EXAMPLE=echo
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --create-home bot \
    && mkdir /data && chown bot:bot /data

# dirs::data_dir() がここを見る → /data/notecli/notecli.db (キャッシュのみ、
# トークンは書かれない)
ENV XDG_DATA_HOME=/data

COPY --from=builder "/build/target/release/examples/${EXAMPLE}" /usr/local/bin/bot

USER bot
VOLUME /data
CMD ["bot"]
