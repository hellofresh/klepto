FROM alpine AS builder

ARG KLEPTO_VERSION

RUN apk add --no-cache openssl tar \
    && wget -O klepto.tar.gz https://github.com/hellofresh/klepto/releases/download/v${KLEPTO_VERSION}/klepto_${KLEPTO_VERSION}_linux_amd64.tar.gz \
    && tar -xzf klepto.tar.gz -C /tmp

# ---

FROM scratch

COPY --from=builder /tmp/klepto /

ENTRYPOINT ["/klepto"]
