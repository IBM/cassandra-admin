FROM openresty/openresty:alpine
WORKDIR /app

RUN apk update && apk add luarocks5.1 && ln -sf /usr/bin/luarocks-5.1 /usr/bin/luarocks
RUN luarocks install lua-resty-template \
    && luarocks install lua-resty-reqargs \
    && luarocks install inspect

RUN adduser -D -g 'appuser' appuser

RUN mkdir -p /run/app/logs && chown -R appuser:appuser /run/app
RUN mkdir -p /etc/cassandra-admin && chown -R appuser:appuser /etc/cassandra-admin

USER appuser

CMD ["/bin/sh", "-c", "/app/_docker_entrypoint.lua > /run/app/nginx.conf && exec nginx -p /run/app -c /run/app/nginx.conf"]
