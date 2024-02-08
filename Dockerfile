FROM alpine:3.19.0

RUN apk add --no-cache libc6-compat
COPY ./out/clickhouse_destination ./clickhouse_destination
EXPOSE 50052
CMD ["./clickhouse_destination"]
