FROM golang:1.27-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY *.go ./
COPY internal ./internal
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/opc-ua-consumer .

FROM alpine:3.22
RUN apk add --no-cache supervisor tzdata \
 && adduser -D -H -s /sbin/nologin consumer
COPY --from=build /out/opc-ua-consumer /usr/local/bin/opc-ua-consumer
COPY docker/supervisord.conf /etc/supervisord.conf
COPY config/tags.yaml /etc/opc-ua-consumer/tags.yaml

ENV OPCUA_TAGS_FILE=/etc/opc-ua-consumer/tags.yaml

# Silencia un aviso de deprecación de pkg_resources que emite supervisord.
ENV PYTHONWARNINGS=ignore::UserWarning

# supervisord es el PID 1: arranca el consumidor y lo reinicia si se cae.
CMD ["supervisord", "-c", "/etc/supervisord.conf"]
