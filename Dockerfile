FROM alpine AS builder

ARG KLEPTO_VERSION

RUN apk add --no-cache openssl tar \
    && wget -O klepto.tar.gz https://github.com/usoban/klepto/releases/download/${KLEPTO_VERSION}/klepto_linux-amd64.tar.gz \
    && tar -xzf klepto.tar.gz -C /tmp

# ---

FROM scratch

COPY FROM builder /tmp/klepto_linux-amd64 /

ENTRYPOINT ["/klepto_linux-amd64"]
