#! /usr/bin/env python3
""" LICENSE_TEXT
"""

from goe.offload.offload_messages import VERBOSE, VVERBOSE
from goe.offload.offload_transport import (
    OFFLOAD_TRANSPORT_METHOD_QUERY_IMPORT,
    OFFLOAD_TRANSPORT_METHOD_SQOOP,
    OFFLOAD_TRANSPORT_METHOD_SQOOP_BY_QUERY,
    OFFLOAD_TRANSPORT_METHOD_SPARK_BATCHES_GCLOUD,
    OFFLOAD_TRANSPORT_METHOD_SPARK_DATAPROC_GCLOUD,
    OFFLOAD_TRANSPORT_METHOD_SPARK_LIVY,
    OFFLOAD_TRANSPORT_METHOD_SPARK_SUBMIT,
    OFFLOAD_TRANSPORT_METHOD_SPARK_THRIFT,
)


def offload_transport_factory(
    offload_transport_method,
    offload_source_table,
    offload_target_table,
    offload_operation,
    offload_options,
    messages,
    dfs_client,
    incremental_update_extractor=None,
    rdbms_columns_override=None,
):
    """Constructs and returns an appropriate data transport object based on user inputs and RDBMS table"""
    if offload_transport_method == OFFLOAD_TRANSPORT_METHOD_QUERY_IMPORT:
        from goe.offload.offload_transport import OffloadTransportQueryImport

        messages.log(
            "Data transport method: OffloadTransportQueryImport", detail=VVERBOSE
        )
        return OffloadTransportQueryImport(
            offload_source_table,
            offload_target_table,
            offload_operation,
            offload_options,
            messages,
            dfs_client,
            incremental_update_extractor,
            rdbms_columns_override=rdbms_columns_override,
        )
    elif offload_transport_method == OFFLOAD_TRANSPORT_METHOD_SQOOP:
        from goe.offload.hadoop.sqoop_offload_transport import OffloadTransportStandardSqoop

        messages.log(
            "Data transport method: OffloadTransportStandardSqoop", detail=VVERBOSE
        )
        return OffloadTransportStandardSqoop(
            offload_source_table,
            offload_target_table,
            offload_operation,
            offload_options,
            messages,
            dfs_client,
            incremental_update_extractor,
            rdbms_columns_override=rdbms_columns_override,
        )
    elif offload_transport_method == OFFLOAD_TRANSPORT_METHOD_SQOOP_BY_QUERY:
        from goe.offload.hadoop.sqoop_offload_transport import OffloadTransportSqoopByQuery

        messages.log(
            "Data transport method: OffloadTransportSqoopByQuery", detail=VVERBOSE
        )
        return OffloadTransportSqoopByQuery(
            offload_source_table,
            offload_target_table,
            offload_operation,
            offload_options,
            messages,
            dfs_client,
            incremental_update_extractor,
            rdbms_columns_override=rdbms_columns_override,
        )
    elif offload_transport_method == OFFLOAD_TRANSPORT_METHOD_SPARK_THRIFT:
        from goe.offload.offload_transport import OffloadTransportSparkThrift

        messages.log(
            "Data transport method: OffloadTransportSparkThrift", detail=VVERBOSE
        )
        return OffloadTransportSparkThrift(
            offload_source_table,
            offload_target_table,
            offload_operation,
            offload_options,
            messages,
            dfs_client,
            incremental_update_extractor,
            rdbms_columns_override=rdbms_columns_override,
        )
    elif offload_transport_method == OFFLOAD_TRANSPORT_METHOD_SPARK_SUBMIT:
        from goe.offload.offload_transport import OffloadTransportSparkSubmit

        messages.log(
            "Data transport method: OffloadTransportSparkSubmit", detail=VVERBOSE
        )
        return OffloadTransportSparkSubmit(
            offload_source_table,
            offload_target_table,
            offload_operation,
            offload_options,
            messages,
            dfs_client,
            incremental_update_extractor,
            rdbms_columns_override=rdbms_columns_override,
        )
    elif offload_transport_method == OFFLOAD_TRANSPORT_METHOD_SPARK_DATAPROC_GCLOUD:
        from goe.offload.spark.dataproc_offload_transport import OffloadTransportSparkDataprocGcloud

        messages.log(
            "Data transport method: OffloadTransportSparkDataprocGcloud",
            detail=VVERBOSE,
        )
        return OffloadTransportSparkDataprocGcloud(
            offload_source_table,
            offload_target_table,
            offload_operation,
            offload_options,
            messages,
            dfs_client,
            incremental_update_extractor,
            rdbms_columns_override=rdbms_columns_override,
        )
    elif offload_transport_method == OFFLOAD_TRANSPORT_METHOD_SPARK_BATCHES_GCLOUD:
        from goe.offload.spark.dataproc_offload_transport import OffloadTransportSparkBatchesGcloud

        messages.log(
            "Data transport method: OffloadTransportSparkBatchesGcloud", detail=VVERBOSE
        )
        return OffloadTransportSparkBatchesGcloud(
            offload_source_table,
            offload_target_table,
            offload_operation,
            offload_options,
            messages,
            dfs_client,
            incremental_update_extractor,
            rdbms_columns_override=rdbms_columns_override,
        )
    elif offload_transport_method == OFFLOAD_TRANSPORT_METHOD_SPARK_LIVY:
        from goe.offload.spark.livy_offload_transport import OffloadTransportSparkLivy

        messages.log(
            "Data transport method: OffloadTransportSparkLivy", detail=VVERBOSE
        )
        return OffloadTransportSparkLivy(
            offload_source_table,
            offload_target_table,
            offload_operation,
            offload_options,
            messages,
            dfs_client,
            incremental_update_extractor,
            rdbms_columns_override=rdbms_columns_override,
        )
    else:
        raise NotImplementedError(
            "Offload transport method not implemented: %s" % offload_transport_method
        )


def spark_thrift_jdbc_connectivity_checker(offload_options, messages):
    """Connect needs a cut down client to simply check RDBMS connectivity from Spark
    back to the source RDBMS is correctly configured
    """
    from goe.offload.offload_transport import OffloadTransportSparkThriftCanary

    messages.log("Invoking OffloadTransportSparkThriftCanary", detail=VVERBOSE)
    return OffloadTransportSparkThriftCanary(offload_options, messages)


def spark_submit_jdbc_connectivity_checker(offload_options, messages):
    """Connect needs a cut down client to simply check RDBMS connectivity from Spark
    back to the source RDBMS is correctly configured
    """
    from goe.offload.offload_transport import OffloadTransportSparkSubmitCanary

    messages.log("Invoking OffloadTransportSparkSubmitCanary", detail=VVERBOSE)
    return OffloadTransportSparkSubmitCanary(offload_options, messages)


def spark_livy_jdbc_connectivity_checker(offload_options, messages):
    """Connect needs a cut down client to simply check RDBMS connectivity from Spark
    back to the source RDBMS is correctly configured
    """
    from goe.offload.spark.livy_offload_transport import OffloadTransportSparkLivyCanary

    messages.log("Invoking OffloadTransportSparkLivyCanary", detail=VVERBOSE)
    return OffloadTransportSparkLivyCanary(offload_options, messages)


def spark_dataproc_jdbc_connectivity_checker(offload_options, messages):
    """Connect needs a cut down client to simply check RDBMS connectivity from Dataproc
    back to the source RDBMS is correctly configured
    """
    from goe.offload.spark.dataproc_offload_transport import OffloadTransportSparkDataprocGcloudCanary

    messages.log("Invoking OffloadTransportSparkDataprocGcloudCanary", detail=VVERBOSE)
    return OffloadTransportSparkDataprocGcloudCanary(offload_options, messages)


def spark_dataproc_batches_jdbc_connectivity_checker(offload_options, messages):
    """Connect needs a cut down client to simply check RDBMS connectivity from Dataproc Batches
    back to the source RDBMS is correctly configured
    """
    from goe.offload.spark.dataproc_offload_transport import OffloadTransportSparkBatchesGcloudCanary

    messages.log("Invoking OffloadTransportSparkBatchesGcloudCanary", detail=VVERBOSE)
    return OffloadTransportSparkBatchesGcloudCanary(offload_options, messages)


def sqoop_jdbc_connectivity_checker(offload_options, messages):
    """Connect needs a cut down client to simply check RDBMS connectivity from Sqoop
    back to the source RDBMS is correctly configured
    """
    from goe.offload.hadoop.sqoop_offload_transport import OffloadTransportSqoopCanary

    messages.log("Invoking OffloadTransportSqoopCanary", detail=VVERBOSE)
    return OffloadTransportSqoopCanary(offload_options, messages)
