# syntax=docker/dockerfile:1

FROM gocv/opencv:latest AS builder
WORKDIR /src
RUN apt-get update && apt-get install -y --no-install-recommends \
    libtesseract-dev libleptonica-dev pkg-config \
    && rm -rf /var/lib/apt/lists/*
COPY . .
RUN go build -o /out/emulator ./emulator

# NOTE: exact OpenCV runtime package names/versions vary by Debian release
# and may not match bookworm-slim's repo exactly (e.g. the libopencv-core
# time64-ABI package suffix) — verify these against `apt-cache search
# libopencv` inside this base image and adjust before relying on this build.
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
    libtesseract5 tesseract-ocr-eng libleptonica6 \
    libopencv-core4.6 libopencv-imgproc4.6 libopencv-imgcodecs4.6 \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /out/emulator /usr/local/bin/emulator
ENTRYPOINT ["/usr/local/bin/emulator"]
