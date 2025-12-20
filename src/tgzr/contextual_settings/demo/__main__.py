import uvicorn

from .config import get_config


def main():
    config = get_config()
    uvicorn.run(
        "tgzr.contextual_settings.demo.app:app",
        host=config.host,
        port=config.port,
        reload=config.reload,
    )


if __name__ == "__main__":
    main()
