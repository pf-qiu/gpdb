cmake_minimum_required(VERSION 3.12)

set(CHILD_FILES 
    acl.c arrayfuncs.c array_selfuncs.c array_typanalyze.c
	array_userfuncs.c arrayutils.c bool.c
	cash.c char.c complex_type.c gp_dump_oids.c gp_optimizer_functions.c
	gp_partition_functions.c interpolate.c matrix.c orderedsetaggs.c
	pivot.c date.c datetime.c datum.c domains.c
	enum.c float.c format_type.c
	geo_ops.c geo_selfuncs.c int.c int8.c json.c jsonfuncs.c like.c
	lockfuncs.c misc.c nabstime.c name.c numeric.c numutils.c
	oid.c oracle_compat.c pseudotypes.c rangetypes.c rangetypes_gist.c
	rowtypes.c regexp.c regproc.c ruleutils.c selfuncs.c
	tid.c timestamp.c varbit.c varchar.c varlena.c version.c xid.c
	network.c mac.c inet_cidr_ntop.c inet_net_pton.c
	ri_triggers.c pg_lzcompress.c pg_locale.c formatting.c
	ascii.c quote.c pgstatfuncs.c encode.c dbsize.c genfile.c trigfuncs.c
	tsginidx.c tsgistidx.c tsquery.c tsquery_cleanup.c tsquery_gist.c
	tsquery_op.c tsquery_rewrite.c tsquery_util.c tsrank.c
	tsvector.c tsvector_op.c tsvector_parser.c
	txid.c uuid.c windowfuncs.c xml.c rangetypes_spgist.c
	rangetypes_typanalyze.c rangetypes_selfuncs.c
    PARENT_SCOPE)
