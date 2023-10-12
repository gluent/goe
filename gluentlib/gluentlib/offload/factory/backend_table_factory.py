#! /usr/bin/env python3
""" LICENSE_TEXT
"""

from gluentlib.offload.offload_constants import DBTYPE_BIGQUERY, DBTYPE_IMPALA, DBTYPE_HIVE,\
    DBTYPE_SNOWFLAKE, DBTYPE_SYNAPSE
from gluentlib.persistence.orchestration_metadata import OrchestrationMetadata


def backend_table_factory(db_name, table_name, backend_type, orchestration_options, messages,
                          orchestration_operation=None, hybrid_metadata=None, data_gov_client=None, dry_run=None,
                          hybrid_owner_override=None, hybrid_view_override=None, existing_backend_api=None):
    """ Return a BackendTable object for the appropriate backend.
        hybrid_owner_override/hybrid_view_override are for use from modules that do not have
        an orchestration_operation or a means of getting hybrid_metadata.
    """
    if dry_run is None:
        if hasattr(orchestration_options, 'execute'):
            dry_run = bool(not orchestration_options.execute)
        else:
            dry_run = False

    if not hybrid_metadata:
        if orchestration_operation:
            hybrid_metadata = orchestration_operation.get_hybrid_metadata()
        elif hybrid_owner_override and hybrid_view_override:
            hybrid_metadata = OrchestrationMetadata.from_name(hybrid_owner_override, hybrid_view_override,
                                                              connection_options=orchestration_options,
                                                              messages=messages)

    if backend_type == DBTYPE_HIVE:
        from gluentlib.offload.hadoop.hive_backend_table import BackendHiveTable
        return BackendHiveTable(db_name, table_name, backend_type, orchestration_options,
                                messages, orchestration_operation=orchestration_operation,
                                hybrid_metadata=hybrid_metadata, data_gov_client=data_gov_client, dry_run=dry_run,
                                existing_backend_api=existing_backend_api)
    elif backend_type == DBTYPE_IMPALA:
        from gluentlib.offload.hadoop.impala_backend_table import BackendImpalaTable
        return BackendImpalaTable(db_name, table_name, backend_type, orchestration_options,
                                  messages, orchestration_operation=orchestration_operation,
                                  hybrid_metadata=hybrid_metadata, data_gov_client=data_gov_client, dry_run=dry_run,
                                  existing_backend_api=existing_backend_api)
    elif backend_type == DBTYPE_BIGQUERY:
        from gluentlib.offload.bigquery.bigquery_backend_table import BackendBigQueryTable
        return BackendBigQueryTable(db_name, table_name, backend_type, orchestration_options,
                                    messages, orchestration_operation=orchestration_operation,
                                    hybrid_metadata=hybrid_metadata, data_gov_client=data_gov_client, dry_run=dry_run,
                                    existing_backend_api=existing_backend_api)
    elif backend_type == DBTYPE_SNOWFLAKE:
        from gluentlib.offload.snowflake.snowflake_backend_table import BackendSnowflakeTable
        return BackendSnowflakeTable(db_name, table_name, backend_type, orchestration_options,
                                     messages, orchestration_operation=orchestration_operation,
                                     hybrid_metadata=hybrid_metadata, data_gov_client=data_gov_client, dry_run=dry_run,
                                     existing_backend_api=existing_backend_api)
    elif backend_type == DBTYPE_SYNAPSE:
        from gluentlib.offload.microsoft.synapse_backend_table import BackendSynapseTable
        return BackendSynapseTable(db_name, table_name, backend_type, orchestration_options,
                                   messages, orchestration_operation=orchestration_operation,
                                   hybrid_metadata=hybrid_metadata, data_gov_client=data_gov_client, dry_run=dry_run,
                                   existing_backend_api=existing_backend_api)
    else:
        raise NotImplementedError('Unsupported backend system type: %s' % backend_type)


def get_backend_table_from_metadata(hybrid_metadata, options, messages, offload_operation=None):
    return backend_table_factory(hybrid_metadata.backend_owner, hybrid_metadata.backend_table, options.target,
                                 options, messages, orchestration_operation=offload_operation,
                                 hybrid_metadata=hybrid_metadata)
