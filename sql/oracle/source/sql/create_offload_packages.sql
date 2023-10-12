-- create_offload_packages.sql
--
-- LICENSE_TEXT
--

-- Specifications...
@@create_offload_package_spec.sql
SHOW ERRORS
--@@create_offload_tools_package_spec.sql
--SHOW ERRORS

-- Bodies...
@@create_offload_package_body.sql
SHOW ERRORS
--@@create_offload_tools_package_body.sql
--SHOW ERRORS
