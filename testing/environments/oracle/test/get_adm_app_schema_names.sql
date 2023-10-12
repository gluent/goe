set termout off
column adm_owner new_value _gluent_adm_schema
column app_owner new_value _gluent_app_schema
select owner                                  as adm_owner
,      regexp_replace(owner, '_ADM$', '_APP') as app_owner
from   dba_objects
where  object_name = 'OFFLOAD'
and    object_type = 'PACKAGE';
set termout on
