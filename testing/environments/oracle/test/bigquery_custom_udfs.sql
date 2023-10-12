
/*
 These UDFs have been pre-loaded into a dataset named UDFS in gluent-dev, gluent-teamcity and
 any other BigQuery projects that need them. This is because the build_test_schemas.sh doesn't have the ability
 to create backend objects other than with offload and offloading the (at the time of writing) PayPal
 mockup customer test data requires these functions to be in place.

 The functions have also been created in the UDFS_PAYPAL dataset which has location us-west3.
*/

CREATE FUNCTION UDFS.PAYPAL_CAK_TO_INT64 (cak BIGNUMERIC)
RETURNS INT64
AS (
   CAST(FLOOR(cak/POWER(2,72)) AS INT64)
);

CREATE FUNCTION UDFS.PAYPAL_TUID_TO_INT64(tuid STRING)
RETURNS INT64
AS (
   CAST(CONCAT('0x', SUBSTR(TRANSLATE(tuid,'-',''),2,15)) AS INT64)
);

CREATE FUNCTION UDFS.PAYPAL_LTID_TO_INT64(ltid BIGNUMERIC)
RETURNS INT64
AS (
   CAST(FLOOR(ltid/POWER(2,84)) AS INT64)
);
