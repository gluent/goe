# Standard Library
import logging
import sys


def validate_config():
    """"""
    try:

        # Gluent
        from gluentlib.config.config_checks import check_cli_path  # noqa: WPS433 F401

        check_cli_path()
    except Exception:
        print(
            "failed to validate configuration.  Please check your installation"
        )  # noqa: WPS421
        sys.exit(1)


async def validate_cache():
    """"""
    # Gluent
    from gluentlib.listener import utils  # noqa: WPS433 F401
    from gluentlib.listener.config import settings  # noqa: WPS433 F401
    from gluentlib.listener.config.logging import Logger  # noqa: WPS433 F401

    try:
        logger = Logger.configure_logger()
        if settings.cache_enabled:
            cache = utils.cache.get_client()
            await cache.ping()
            logger.info("✅  successfully validated Redis connectivity.")
    except Exception as exc:
        logger.error("⚠️  Could not connect to redis backend.  retrying...")
        raise exc
    finally:
        if cache and settings.cache_enabled:
            await utils.cache.close_client()


def prestart() -> None:
    # Third Party Libraries
    from tenacity import after_log  # noqa: WPS433 F401
    from tenacity import before_log  # noqa: WPS433 F401
    from tenacity import retry  # noqa: WPS433 F401
    from tenacity import wait_fixed  # noqa: WPS433 F401

    # Gluent
    from gluentlib.listener.config.logging import Logger  # noqa: WPS433 F401
    from gluentlib_contrib.asyncer import runnify  # noqa: WPS433 F401

    logger = Logger.configure_logger()
    # max_tries = 60
    wait_seconds = 5

    @retry(
        # stop=stop_after_attempt(max_tries),
        wait=wait_fixed(wait_seconds),
        before=before_log(logger, logging.DEBUG),
        after=after_log(logger, logging.DEBUG),
    )
    async def _prestart():
        """"""
        await validate_cache()

    validate_config()
    runnify(_prestart)()


if __name__ == "__main__":
    prestart()
