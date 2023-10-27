select /*+ PARALLEL(2) &_test_name */ id from gl_backend_type_mapping where id between 1 and 5
