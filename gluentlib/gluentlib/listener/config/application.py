# Standard Library
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, Optional

# Third Party Libraries
from pydantic import validator
from pydantic.types import SecretStr

# Gluent
from gluentlib.config.orchestration_config import OrchestrationConfig
from gluentlib.listener.schemas.base import BaseSettings

BASE_DIR: Path = Path(__file__).resolve().parent.parent.parent.parent
APP_DIR: Path = Path(BASE_DIR / "gluentlib" / "listener")
CONFIG_DIR: Path = Path(APP_DIR / "config")
FRONTEND_DIR: Path = Path(APP_DIR / "web")


class ListenerSettings(BaseSettings):
    """Listener Configuration Object"""

    global_config: OrchestrationConfig = OrchestrationConfig.as_defaults(do_not_connect=True)

    host: str = global_config.listener_host or "0.0.0.0"
    port: int = global_config.listener_port or 8085
    http_workers: int = 2
    reload: bool = False
    static_url: str = "/"
    static_path: Optional[str]
    background_workers: int = 2
    shared_token: Optional[SecretStr] = global_config.listener_shared_token
    gunicorn_conf: str = str(Path(CONFIG_DIR / "gunicorn.conf.py"))
    certfile: Optional[str] = None
    keyfile: Optional[str] = None
    ssl_enabled: Optional[bool] = False
    heartbeat_interval: int = global_config.listener_heartbeat_interval or 30
    redis_host: Optional[str] = global_config.listener_redis_host
    redis_port: int = global_config.listener_redis_port or 6379
    redis_db: int = global_config.listener_redis_db or 0
    redis_username: Optional[str] = global_config.listener_redis_username
    redis_password: Optional[SecretStr] = global_config.listener_redis_password
    redis_ssl: Optional[bool] = global_config.listener_redis_use_ssl
    redis_ssl_cert: Optional[str] = global_config.listener_redis_ssl_cert
    redis_use_sentinel: Optional[bool] = False
    redis_sentinel_master: str = "gluent-listener"

    @property
    def redis_url(self) -> str:
        """Returns a redis url to connect to"""
        proto = "rediss" if self.redis_ssl else "redis"
        if self.redis_password is None:
            return f"{proto}://{self.redis_host}:{self.redis_port}/{self.redis_db}"
        return f"{proto}://:{self.redis_password}@{self.redis_host}:{self.redis_port}/{self.redis_db}"

    @property
    def cache_enabled(self) -> bool:
        """Returns if redis is enabled"""
        return True if self.redis_host else False

    @validator("static_path", pre=True)
    def assemble_static_path(
        cls,
        value: Optional[str],
        values: Dict[str, Any],
    ) -> str:
        """Parses a list of origins"""
        if value:
            return str
        return str(Path(FRONTEND_DIR / "public"))

    # @validator("ssl_enabled", pre=True)
    # def determine_if_ssl_enabled(
    #     cls,
    #     value: Optional[str],
    #     values: Dict[str, Any],
    # ) -> str:
    #     """Parses a list of origins"""
    #     return "certfile" and "keyfile" in values


@lru_cache(maxsize=1)
def get_app_settings() -> ListenerSettings:
    """
    Cache app settings

    This function returns a configured instance of settings.

    LRU Cache decorator has been used to limit the number of instances to 1.
    This effectively turns this into a singleton class.

    Maybe there are better approaches for this?
    """
    return ListenerSettings()


settings = get_app_settings()
