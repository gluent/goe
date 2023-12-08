create table gl_exotic_chars
( /* Table name referenced in test. */
  id bigint
, lang varchar(100)
, words varchar(1000) character set unicode
, trans varchar(1000)
);

insert into gl_exotic_chars
with da as (
  select 'Danish (da)' lang
      , 'Quizdeltagerne spiste jordbær med fløde, mens cirkusklovnen Wolther spillede på xylofon.' words
      , 'Quiz contestants were eating strawbery with cream while Wolther the circus clown played on xylophone.' trans
)
, de1 as (
  select 'German (de)' lang
      , 'Falsches Üben von Xylophonmusik quält jeden größeren Zwerg' words
      , 'Wrongful practicing of xylophone music tortures every larger dwarf' trans
)
, de2 as (
  select 'German (de)' lang
      , 'Zwölf Boxkämpfer jagten Eva quer über den Sylter Deich' words
      , 'Twelve boxing fighters hunted Eva across the dike of Sylt' trans
)
, el as (
  select 'Greek (el)' lang
      , 'Γαζέες καὶ μυρτιὲς δὲν θὰ βρῶ πιὰ στὸ χρυσαφὶ ξέφωτο' words
      , 'No more shall I see acacias or myrtles in the golden clearing' trans
)
, hu as (
  select 'Hungarian (hu)' lang
      , 'Árvíztűrő tükörfúrógép' words
      , 'flood-proof mirror-drilling machine' trans
)
, jp as (
  select 'Japanese (jp)' lang
      , 'イロハニホヘト チリヌルヲ ワカヨタレソ ツネナラム ウヰノオクヤマ ケフコエテ アサキユメミシ ヱヒモセスン' words
      , '?' trans
)
, tr as (
  select 'Turkish (tr)' lang
      , 'Pijamalı hasta, yağız şoföre çabucak güvendi.' words
      , 'Patient with pajamas, trusted swarthy driver quickly' trans
)
, ru as (
  select 'Russian (ru)' lang
      , 'Съешь же ещё этих мягких французских булок да выпей чаю' words
      , 'Eat some more of these fresh French loafs and have some tea' trans
)
select 1, lang, words, trans from da
union all
select 1, lang, words, trans from de1
union all
select 1, lang, words, trans from de2
union all
select 1, lang, words, trans from el
union all
select 1, lang, words, trans from hu
union all
select 1, lang, words, trans from jp
union all
select 1, lang, words, trans from tr
union all
select 1, lang, words, trans from ru
;
