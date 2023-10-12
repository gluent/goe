create table gl_binary
( id       number
, raw_col  raw(2000)
, blob_col blob
, src_file varchar2(30)
);

declare
  v_bfile       bfile;
  v_blob        blob;
  v_raw         raw(2000);
  v_dest_offset integer;
  v_src_offset  integer;
  v_amount      integer;
  v_files       sys.dbms_debug_vc2coll := sys.dbms_debug_vc2coll('gl_binary.aifc', 'gl_binary.mp4', 'gl_binary.png', 'gl_binary.txt', 'gl_binary.zip');
begin
  for i in 1 .. v_files.count loop

      v_dest_offset := 1;
      v_src_offset  := 1;
      v_amount      := 2000;
      v_raw         := null;

      insert into gl_binary
        (id, blob_col, src_file)
      values
        (i, empty_blob(), v_files(i))
      returning blob_col into v_blob;

      v_bfile := bfilename('OFFLOAD_LOG', v_files(i));
      dbms_lob.fileopen(v_bfile, dbms_lob.file_readonly);
      dbms_lob.loadblobfromfile(dest_lob => v_blob, src_bfile => v_bfile, amount => dbms_lob.lobmaxsize, dest_offset => v_dest_offset, src_offset => v_src_offset);
      dbms_lob.close(v_bfile);

      dbms_lob.read(lob_loc => v_blob, amount => v_amount, offset => 1, buffer => v_raw);
      update gl_binary
      set    raw_col = v_raw
      where  id = i;

  end loop;
  commit;
end;
/
