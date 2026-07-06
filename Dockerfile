FROM alpine:3.19.0

RUN apk add --no-cache libc6-compat
COPY ./bin/server ./clickhouse_destination
EXPOSE 50052
CMD ["./clickhouse_destination"]
