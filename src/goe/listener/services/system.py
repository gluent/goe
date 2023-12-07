# Standard Library
import logging
from typing import Any, Dict, List, Optional, Union
from uuid import NAMESPACE_DNS, uuid3

# Third Party Libraries
from pydantic import UUID3

# Gluent
from goe.gluent import version as gluent_version
from goe.config.orchestration_config import OrchestrationConfig
from goe.listener import utils
from goe.listener.config import settings
from goe.offload.offload_messages import OffloadMessages
from goe.orchestration.execution_id import ExecutionId
from goe.persistence.factory.orchestration_repo_client_factory import (
    orchestration_repo_client_factory,
)
from goe.persistence.orchestration_repo_client import (
    OrchestrationRepoClientInterface,
)

logger = logging.getLogger(__name__)


class SystemService(object):
    """API for accessing metadata about databases"""

    def __init__(self):
        self.config = OrchestrationConfig.as_defaults()
        self.messages = OffloadMessages()

    @staticmethod
    def get_repo(
        config: OrchestrationConfig, messages: OffloadMessages
    ) -> OrchestrationRepoClientInterface:
        return orchestration_repo_client_factory(
            config, messages, dry_run=bool(not config.execute)
        )

    def generate_listener_group_id(self) -> UUID3:
        return uuid3(NAMESPACE_DNS, f"{self.config.rdbms_dsn}")

    def generate_listener_endpoint_id(self) -> UUID3:
        return uuid3(
            NAMESPACE_DNS,
            f"{self.config.rdbms_dsn}/{utils.system.get_ip_address()}:{settings.port}",
        )

    async def get_active_listener_endpoints(self):
        _, keys = await utils.cache.scan("gluent:listener:endpoints:*")
        return await utils.cache.mget(keys)

    def get_db_unique_name(self) -> str:
        return self.config._get_frontend_connection().get_db_unique_name()

    def get_backend_type(self) -> str:
        return self.config.backend_distribution

    def get_frontend_type(self) -> str:
        return self.config.db_type

    def get_version(self) -> str:
        return gluent_version()

    def get_schemas(self) -> List[Dict[str, Union[str, Any]]]:
        return self.get_repo(self.config, self.messages).get_offloadable_schemas()

    def get_schema_tables(self, schema_name: str) -> List[Dict[str, Union[str, Any]]]:
        return self.get_repo(self.config, self.messages).get_schema_tables(schema_name)

    def get_table_columns(
        self, schema_name: str, table_name: str
    ) -> List[Dict[str, Union[str, Any]]]:
        return self.get_repo(self.config, self.messages).get_table_columns(
            schema_name, table_name
        )

    def get_table_partitions(
        self, schema_name: str, table_name: str
    ) -> List[Dict[str, Union[str, Any]]]:
        return self.get_repo(self.config, self.messages).get_table_partitions(
            schema_name, table_name
        )

    def get_table_subpartitions(
        self, schema_name: str, table_name: str
    ) -> List[Dict[str, Union[str, Any]]]:
        return self.get_repo(self.config, self.messages).get_table_subpartitions(
            schema_name, table_name
        )

    def get_command_executions(self) -> List[Dict[str, Union[str, Any]]]:
        return self.get_repo(self.config, self.messages).get_command_executions()

    def get_command_execution(
        self, execution_id: ExecutionId
    ) -> Dict[str, Union[str, Any]]:
        return self.get_repo(self.config, self.messages).get_command_execution(
            execution_id
        )

    def get_command_execution_steps(
        self, execution_id: Optional[ExecutionId]
    ) -> List[Dict[str, Union[str, Any]]]:
        return self.get_repo(self.config, self.messages).get_command_execution_steps(
            execution_id
        )


system = SystemService()
