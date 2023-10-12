create table gl_exotic_chars
( /* Table name referenced in test. */
  id number
, lang varchar2(100)
, words varchar2(1000 char)
, trans varchar2(1000)
);

insert into gl_exotic_chars
select rownum id, v.lang, v.words, v.trans
from  (
  select 'Danish (da)' lang
      , 'Quizdeltagerne spiste jordbær med fløde, mens cirkusklovnen Wolther spillede på xylofon.' words
      , 'Quiz contestants were eating strawbery with cream while Wolther the circus clown played on xylophone.' trans
  from dual
  union all
  select 'German (de)'
      , 'Falsches Üben von Xylophonmusik quält jeden größeren Zwerg'
      , 'Wrongful practicing of xylophone music tortures every larger dwarf'
  from dual
  union all
  select 'German (de)'
      , 'Zwölf Boxkämpfer jagten Eva quer über den Sylter Deich'
      , 'Twelve boxing fighters hunted Eva across the dike of Sylt'
  from dual
  union all
  select 'Greek (el)'
      , 'Γαζέες καὶ μυρτιὲς δὲν θὰ βρῶ πιὰ στὸ χρυσαφὶ ξέφωτο'
      , 'No more shall I see acacias or myrtles in the golden clearing'
  from dual
  union all
  select 'Hungarian (hu)'
      , 'Árvíztűrő tükörfúrógép'
      , 'flood-proof mirror-drilling machine'
  from dual
  union all
  select 'Japanese (jp)'
      , 'イロハニホヘト チリヌルヲ ワカヨタレソ ツネナラム ウヰノオクヤマ ケフコエテ アサキユメミシ ヱヒモセスン'
      , '?'
  from dual
  union all
  select 'Turkish (tr)'
      , 'Pijamalı hasta, yağız şoföre çabucak güvendi.'
      , 'Patient with pajamas, trusted swarthy driver quickly'
  from dual
  union all
  select 'Russian (ru)'
      , 'Съешь же ещё этих мягких французских булок да выпей чаю'
      , 'Eat some more of these fresh French loafs and have some tea'
  from dual
) v
;
