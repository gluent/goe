# -*- coding: utf-8 -*-
"""GOE Listener Exceptions."""
# GOE
from goe.listener.exceptions.base import (
    ApplicationError,
    BaseApplicationError,
    HTTPException,
    app_error_handler,
    cache_connectivity_error,
    database_connectivity_error,
    http422_error_handler,
    http_error_handler,
    system_error_exception_handler,
)
from goe.listener.exceptions.errors import (
    CommandExecutionNotFound,
    CredentialValidationError,
    DatabaseConnectivityError,
    HybridViewMetadataNotFoundError,
    LogFileNotFoundError,
)

__all__ = [
    "HTTPException",
    "http_error_handler",
    "app_error_handler",
    "http422_error_handler",
    "BaseApplicationError",
    "ApplicationError",
    "DatabaseConnectivityError",
    "CredentialValidationError",
    "HybridViewMetadataNotFoundError",
    "LogFileNotFoundError",
    "CommandExecutionNotFound",
    "system_error_exception_handler",
    "cache_connectivity_error",
    "database_connectivity_error",
]
