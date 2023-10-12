create table gl_strings
( id number
, c_short char(8)
, c_short_c char(8 char)
, c_long char(200) /* Maximum size is 2000 bytes or characters */
, c_long_c char(100 char)
, vc_short varchar2(10)
, vc_short_c varchar2(10 char)
, vc_long varchar2(200)
, vc_long_c varchar2(100 char)
, nc_short nchar(8) /* upper limit of 2000 bytes */
, nc_long nchar(100)
, nvc_short nvarchar2(10)
, nvc_long nvarchar2(100)
);

insert into gl_strings
select rownum id
, v.ascii_lang /* c_short */
, v.unicode_lang /* c_short_c */
, v.ascii_words /* c_long */
, v.unicode_words /* c_long_c */
, v.ascii_lang /* vc_short */
, v.unicode_lang /* vc_short_c */
, v.ascii_words /* vc_long */
, v.unicode_words /* vc_long_c */
, v.unicode_lang /* nc_short */
, v.unicode_words /* nc_long */
, v.unicode_lang /* nvc_short */
, v.unicode_words /* nvc_long */
from  (
  select 'Greek' as ascii_lang
      , 'Ελληνικά' as unicode_lang
      , 'Γαζέες καὶ μυρτιὲς δὲν θὰ βρῶ πιὰ στὸ χρυσαφὶ ξέφωτο' as unicode_words
      , 'No more shall I see acacias or myrtles in the golden clearing' as ascii_words
  from dual
  union all
  select 'Russian'
      , 'русский'
      , 'Съешь же ещё этих мягких французских булок да выпей чаю'
      , 'Eat some more of these fresh French loafs and have some tea'
  from dual
) v
;
