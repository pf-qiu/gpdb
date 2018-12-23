cmake_minimum_required(VERSION 3.12)

set(CHILD_FILES
    ts_locale.c ts_parse.c wparser.c wparser_def.c dict.c
	dict_simple.c dict_synonym.c dict_thesaurus.c
	dict_ispell.c regis.c spell.c
	to_tsany.c ts_selfuncs.c ts_typanalyze.c ts_utils.c
    PARENT_SCOPE
)

install(FILES synonym_sample.syn thesaurus_sample.ths hunspell_sample.affix
	ispell_sample.affix ispell_sample.dict
	DESTINATION share/postgresql/tsearch_data)