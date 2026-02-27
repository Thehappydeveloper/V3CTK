FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive

# Minimal runtime dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg \
    python3 \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

# ----------------------------------------------------
# 1. Working directory
# ----------------------------------------------------
WORKDIR /workspace

# ----------------------------------------------------
# 2. Expect TMC2 binaries to be mounted from builder
# ----------------------------------------------------
WORKDIR /workspace/TMC2
ENV PATH="/workspace/TMC2/build/bin:${PATH}"

# ----------------------------------------------------
# 3. External dataset & output folders
# ----------------------------------------------------
VOLUME ["/data"]

WORKDIR /data

CMD ["/bin/bash"]

