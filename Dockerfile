FROM ghcr.io/astral-sh/uv:alpine

WORKDIR /tgzr.contextconf
COPY pyproject.toml .
COPY README.md .
COPY src ./src
RUN ls -la .

# ENTRYPOINT ["tail", "-f", "/dev/null"]
CMD ["uv", "run", "-p", "3.13", "--extra", "GUI", "fastapi", "run", "./src/tgzr/contextual_settings/demo/app.py", "--proxy-headers", "--port", "8080"]