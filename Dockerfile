# syntax=docker/dockerfile:1

FROM gocv/opencv:latest AS builder
ENV GOTOOLCHAIN=auto
WORKDIR /src
RUN apt-get update && apt-get install -y --no-install-recommends \
    libtesseract-dev libleptonica-dev pkg-config \
    && rm -rf /var/lib/apt/lists/*
COPY . .
RUN go build -o /out/emulator ./emulator

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
    libtesseract5 tesseract-ocr-eng liblept5 \
    libavcodec59 libavformat59 libavutil57 libswscale6 \
    libgtk2.0-0 libpng16-16 libtiff6 libwebp7 libwebpdemux2 libtbb12 \
    libgdk-pixbuf2.0-0 libcairo2 libharfbuzz0b libfreetype6 \
    && rm -rf /var/lib/apt/lists/*
# gocv builds OpenCV 4.13 from source into /usr/local/lib; bookworm's apt
# repo only ships 4.6, so the runtime libs are copied from the builder
# stage instead of installed, and ldconfig re-indexes the linker cache.
COPY --from=builder /usr/local/lib/libopencv_*.so* /usr/local/lib/
RUN ldconfig
COPY --from=builder /out/emulator /usr/local/bin/emulator
ENTRYPOINT ["/usr/local/bin/emulator"]
