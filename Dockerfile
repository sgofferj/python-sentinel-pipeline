# base image with CUDA support
FROM nvidia/cuda:12.4.1-runtime-ubuntu22.04

# Avoid interactive prompts during apt install
ENV DEBIAN_FRONTEND=noninteractive

# Install system dependencies
RUN apt-get update && apt-get install -y \
    python3.10 \
    python3-pip \
    python3-gdal \
    gdal-bin \
    libgdal-dev \
    cron \
    vim \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy requirements and install
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Setup Directories
RUN mkdir -p temp output/visual output/analytic output/logs output/legends

# Create the entrypoint script
RUN echo '#!/bin/bash\n\
if [ -n "$CRON_SCHEDULE" ]; then\n\
    echo "$CRON_SCHEDULE . /app/.env_docker && /usr/bin/python3 /app/pipelines.py >> /app/output/logs/cron.log 2>&1" > /etc/cron.d/pipeline-cron\n\
    chmod 0644 /etc/cron.d/pipeline-cron\n\
    crontab /etc/cron.d/pipeline-cron\n\
    # Save env for cron\n\
    printenv | sed "s/^\\([^=]*\\)=\\(.*\\)$/export \\1=\\x27\\2\\x27/" > /app/.env_docker\n\
    echo "Cron scheduled: $CRON_SCHEDULE"\n\
    cron -f\n\
else\n\
    echo "No CRON_SCHEDULE provided. Running once..."\n\
    python3 pipelines.py\n\
fi' > /app/entrypoint.sh && chmod +x /app/entrypoint.sh

# Default environment variables
ENV PIPELINES="S1,S2"
ENV PIPELINE_WORKERS=2
ENV ENABLE_GPU_WARP="false"
ENV CRON_SCHEDULE="0 4 * * *"

ENTRYPOINT ["/app/entrypoint.sh"]
