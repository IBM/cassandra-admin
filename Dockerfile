FROM openresty/openresty:1.21.4.1-0-bullseye-fat
WORKDIR /app
RUN apt update -y && apt install -y luarocks git
RUN luarocks install lua-resty-template \
    && luarocks install lua-resty-reqargs \
    && luarocks install inspect
CMD ["nginx"]