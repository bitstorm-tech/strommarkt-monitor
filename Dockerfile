FROM node:22-slim AS frontend
WORKDIR /app
COPY frontend/package.json frontend/package-lock.json ./
RUN npm ci
COPY frontend/ ./
RUN npm run build

FROM rust:1-slim-bookworm AS backend
WORKDIR /app
COPY backend/ ./
# -j1: der Server hat nur 1 GB RAM. libduckdb.so lädt libduckdb-sys herunter (backend/.cargo/config.toml).
RUN cargo build --release -j1 && cp "$(find target/release/build -name libduckdb.so | head -1)" /app/libduckdb.so

FROM debian:bookworm-slim
COPY --from=backend /app/target/release/backend /usr/local/bin/backend
COPY --from=backend /app/libduckdb.so /usr/local/lib/
RUN ldconfig
COPY --from=frontend /app/dist /app/static
# Die Datenbank liegt auf einem Volume unter /data (Import: backend import <zip>)
ENV MASTR_DB=/data/mastr.db STATIC_DIR=/app/static ADDR=0.0.0.0:3000
EXPOSE 3000
CMD ["backend"]
