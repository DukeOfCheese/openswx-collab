# Published as ghcr.io/dukeofcheese/openswx-collab-server (amd64, arm64) by .github/workflows/release.yml.
# Build from the repository root:  docker build -f deploy/Dockerfile -t openswx-collab-server .
FROM python:3.15-slim

LABEL org.opencontainers.image.source=https://github.com/DukeOfCheese/openswx-collab
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app
COPY protocol /app/protocol
COPY server /app/server
RUN pip install --no-cache-dir /app/protocol "/app/server[postgres,s3]"

RUN useradd --system --create-home collab && mkdir -p /data && chown collab /data
USER collab
ENV OSWX_COLLAB_DATABASE_URL=sqlite+aiosqlite:////data/collab.db \
    OSWX_COLLAB_STORAGE_URL=/data/storage
VOLUME /data
EXPOSE 8787
HEALTHCHECK --interval=30s --timeout=5s CMD python -c "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8787/api/health').status==200 else 1)"
# One process only: open documents live in this process's memory.
CMD ["openswx-collab-server", "serve", "--host", "0.0.0.0", "--port", "8787"]
