FROM debian:bookworm-slim AS build

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl gcc libc6-dev make cmake pkg-config \
    libjson-c-dev liburing-dev zlib1g-dev libzstd-dev liblz4-dev

RUN curl -fsSL https://codeload.github.com/Vitruves/carquet/tar.gz/refs/tags/v0.8.0 -o /tmp/carquet.tar.gz \
    && echo 'fc29437cffcdef0c310c076fd7eb8cc2188fc3e99efc339e2039668dfb8d5015  /tmp/carquet.tar.gz' | sha256sum -c - \
    && mkdir /opt/carquet \
    && tar -xzf /tmp/carquet.tar.gz --strip-components=1 -C /opt/carquet \
    && cmake -S /opt/carquet -B /opt/carquet-build -DCMAKE_BUILD_TYPE=Release \
        -DCARQUET_BUILD_CLI=OFF -DCARQUET_ENABLE_BROTLI=OFF -DCMAKE_DISABLE_FIND_PACKAGE_OpenMP=ON \
    && cmake --build /opt/carquet-build --parallel 8

COPY native/ /app/
RUN make -C /app LIBS='-Wl,-Bstatic -lz -lzstd -llz4 -ljson-c -luring -Wl,-Bdynamic -lm'

FROM debian:bookworm-slim

COPY --from=build /app/shopping-sessions /usr/local/bin/shopping-sessions
COPY --from=build /opt/carquet/LICENSE /usr/share/doc/shopping-sessions/carquet-LICENSE
USER 1000:1000
CMD ["/usr/local/bin/shopping-sessions"]
