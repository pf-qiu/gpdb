/*-------------------------------------------------------------------------
 *
 * ext_fdw.c
 *		  foreign-data wrapper for server-side flat files (or programs).
 *
 * Copyright (c) 2010-2019, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  contrib/ext_fdw/ext_fdw.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <sys/stat.h>
#include <unistd.h>

#include "access/htup_details.h"
#include "access/reloptions.h"
#include "access/sysattr.h"
#include "access/table.h"
#include "access/url.h"
#include "access/formatter.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_foreign_table.h"
#include "commands/copy.h"
#include "commands/defrem.h"
#include "commands/explain.h"
#include "commands/vacuum.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/extensible.h"
#include "nodes/readfuncs.h"
#include "nodes/value.h"
#include "optimizer/cost.h"
#include "optimizer/optimizer.h"
#include "optimizer/pathnode.h"
#include "optimizer/planmain.h"
#include "optimizer/restrictinfo.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/sampling.h"
#include "utils/uri.h"
#include "utils/lsyscache.h"
#include "cdb/cdbvars.h"
#include "parser/parse_func.h"

PG_MODULE_MAGIC;
/*
 * SQL functions
 */
PG_FUNCTION_INFO_V1(ext_fdw_handler);
PG_FUNCTION_INFO_V1(ext_fdw_validator);

typedef struct
{
	List *locations;
	// Options not used by framework.
	List *app_options;
	char *format;
	char exec_location;
} ExternalCommonInfo;

static StringInfo SerializeExternalCommonInfo(ExternalCommonInfo *info)
{
	StringInfo str = makeStringInfo();
	char buffer[0x20];
	buffer[0] = info->exec_location;
	buffer[1] = ':';
	appendBinaryStringInfo(str, buffer, 2);
	int l = strlen(info->format);
	int n = snprintf(buffer, sizeof(buffer), "%d:", l);
	appendBinaryStringInfo(str, buffer, n);
	appendBinaryStringInfo(str, info->format, l);
	ListCell *cell;
	foreach (cell, info->locations)
	{
		const char *s = strVal(lfirst(cell));
		l = strlen(s);
		n = snprintf(buffer, sizeof(buffer), "%d:", l);
		appendBinaryStringInfo(str, buffer, n);
		appendBinaryStringInfo(str, s, l);
	}
	return str;
}

static char *extract_string(const char *s, int *l)
{
	const char *begin = s;
	if (1 != sscanf(s, "%d", l))
		return NULL;
	while (*s != ':')
		s++;
	s++;
	char *str = palloc(*l + 1);
	memcpy(str, s, *l);
	str[*l] = 0;
	*l += s - begin;
	return str;
}

static ExternalCommonInfo *DeserializeExternalCommonInfo(const char *str)
{
	ExternalCommonInfo *info = palloc0(sizeof(ExternalCommonInfo));
	const char *s = str;
	const char *end = str + strlen(str);
	info->exec_location = s[0];
	s += 2;
	int l = 0;
	char *format = extract_string(s, &l);
	info->format = format;
	s += l;
	while (s < end)
	{
		char *loc = extract_string(s, &l);
		if (loc == NULL)
			break;
		info->locations = lappend(info->locations, loc);
		s += l;
	}
	return info;
}

typedef bool (*ValidateLocation)(const char *);

typedef struct
{
	ValidateLocation validate_location;
} ExternalAPIRoutines;

static ExternalCommonInfo *MakeExternalCommonInfo(List *server_options, List *table_options, char exec_location)
{
	ExternalCommonInfo *info = palloc0(sizeof(ExternalCommonInfo));
	ListCell *cell;
	info->locations = NIL;
	foreach (cell, table_options)
	{
		DefElem *def = (DefElem *)lfirst(cell);
		if (strcmp(def->defname, "location") == 0)
		{
			char *str = defGetString(def);
			List *locations = lappend(info->locations, makeString(str));
			info->locations = locations;
		}
		else if (strcmp(def->defname, "format") == 0)
		{
			info->format = defGetString(def);
		}
	}
	if (info->locations == NIL)
	{
		elog(ERROR, "missing location");
	}

	if (info->format == NULL)
	{
		elog(ERROR, "missing format");
	}

	info->exec_location = exec_location;
	return info;
}

static void
exttable_GetForeignRelSize(PlannerInfo *root,
						   RelOptInfo *baserel,
						   Oid foreigntableid)
{
	elog(NOTICE, "exttable_GetForeignRelSize: %d", GpIdentity.segindex);
	set_baserel_size_estimates(root, baserel);
}

/*
 * cost_externalscan
 *	  Determines and returns the cost of scanning an external relation.
 *
 *	  Right now this is not very meaningful at all but we'll probably
 *	  want to make some good estimates in the future.
 */
static void
cost_externalscan(ForeignPath *path, PlannerInfo *root,
				  RelOptInfo *baserel, ParamPathInfo *param_info)
{
	Cost startup_cost = 0;
	Cost run_cost = 0;
	Cost cpu_per_tuple;

	/* Should only be applied to external relations */
	Assert(baserel->relid > 0);
	Assert(baserel->rtekind == RTE_RELATION);

	/* Mark the path with the correct row estimate */
	if (param_info)
		path->path.rows = param_info->ppi_rows;
	else
		path->path.rows = baserel->rows;

	/*
	 * disk costs
	 */
	run_cost += seq_page_cost * baserel->pages;

	/* CPU costs */
	startup_cost += baserel->baserestrictcost.startup;
	cpu_per_tuple = cpu_tuple_cost + baserel->baserestrictcost.per_tuple;
	run_cost += cpu_per_tuple * baserel->tuples;

	path->path.startup_cost = startup_cost;
	path->path.total_cost = startup_cost + run_cost;
}

static void
exttable_GetForeignPaths(PlannerInfo *root,
						 RelOptInfo *baserel,
						 Oid foreigntableid)
{
	elog(NOTICE, "exttable_GetForeignPaths: %d", GpIdentity.segindex);
	ForeignPath *pathnode;
	ExternalCommonInfo *info;

	/* it should be an foreign rel... */
	Assert(baserel->rtekind == RTE_RELATION);
	ForeignTable *ft = GetForeignTable(foreigntableid);
	ForeignServer *fs = GetForeignServer(ft->serverid);
	GpPolicy *policy = GpPolicyFetch(foreigntableid);
	char exec_location = policy->ptype == POLICYTYPE_ENTRY ? 
		FTEXECLOCATION_MASTER : FTEXECLOCATION_ALL_SEGMENTS;

	info = MakeExternalCommonInfo(fs->options, ft->options, exec_location);
	StringInfo infostr = SerializeExternalCommonInfo(info);
	List *infoopt = list_make1(makeString(infostr->data));
	pathnode = create_foreignscan_path(root,
									   baserel,
									   NULL, /* default pathtarget */
									   0,	 /* rows, filled in later */
									   0,	 /* startup_cost, later */
									   0,	 /* total_cost, later */
									   NIL,	 /* external scan has unordered result */
									   NULL, /* no outer rel either */
									   NULL, /* no extra plan */
									   infoopt);

	// Control number of segments
	//CdbPathLocus_MakeStrewn(&pathnode->path.locus, getgpsegmentCount());
	pathnode->path.locus = cdbpathlocus_from_baserel(root, baserel);
	pathnode->path.motionHazard = false;

	/*
	 * Mark external tables as non-rescannable. While rescan is possible,
	 * it can lead to surprising results if the external table produces
	 * different results when invoked twice.
	 */
	pathnode->path.rescannable = false;
	pathnode->path.sameslice_relids = baserel->relids;

	cost_externalscan(pathnode, root, baserel, pathnode->path.param_info);

	add_path(baserel, (Path *)pathnode);
	set_cheapest(baserel);
}

/*
 * create_externalscan_plan
 *	 Returns an externalscan plan for the base relation scanned by 'best_path'
 *	 with restriction clauses 'scan_clauses' and targetlist 'tlist'.
 *
 *	 The external plan also includes the data format specification and file
 *	 location specification. Here is where we do the mapping of external file
 *	 to segment database and add it to the plan (or bail out of the mapping
 *	 rules are broken)
 *
 *	 Mapping rules
 *	 -------------
 *	 - 'file' protocol: each location (URI of local file) gets mapped to one
 *						and one only primary segdb.
 *	 - 'http' protocol: each location (URI of http server) gets mapped to one
 *						and one only primary segdb.
 *	 - 'gpfdist' and 'gpfdists' protocols: all locations (URI of gpfdist(s) client) are mapped
 *						to all primary segdbs. If there are less URIs than
 *						segdbs (usually the case) the URIs are duplicated
 *						so that there will be one for each segdb. However, if
 *						the GUC variable gp_external_max_segs is set to a num
 *						less than (total segdbs/total URIs) then we make sure
 *						that no URI gets mapped to more than this GUC number by
 *						skipping some segdbs randomly.
 *	 - 'exec' protocol: all segdbs get mapped to execute the command (this is
 *						soon to be changed though).
 */
static ForeignScan *
exttable_GetForeignPlan(PlannerInfo *root,
						RelOptInfo *baserel,
						Oid foreigntableid,
						ForeignPath *best_path,
						List *tlist,
						List *scan_clauses,
						Plan *outer_plan)
{
	elog(NOTICE, "exttable_GetForeignPlan: %d", GpIdentity.segindex);
	Index scan_relid = best_path->path.parent->relid;
	ForeignScan *scan_plan;

	Assert(scan_relid > 0);

	/* Reduce RestrictInfo list to bare expressions; ignore pseudoconstants */
	scan_clauses = extract_actual_clauses(scan_clauses, false);

	scan_plan = make_foreignscan(tlist,
								 scan_clauses,
								 scan_relid,
								 NIL, /* fdw_exprs */
								 best_path->fdw_private,
								 NIL, /* fdw_scan_tlist */
								 NIL, /* fdw_recheck_quals */
								 NULL /* outer_plan */);

	return scan_plan;
}

typedef struct _ext_fdw_state
{
	bool iscustom;
	CopyState cstate;

	URL_FILE *file;
	char *raw_buffer;
	int raw_buf_len;
	bool eof;
	StringInfo full_buffer;

	FmgrInfo *formatter_func;
	FormatterData *formatter_data;
} ext_fdw_state;

static int
external_getdata_callback(void *outbuf, int minread, int maxread, void *extra)
{
	int bytesread;
	ext_fdw_state *state = (ext_fdw_state *)extra;
	/*
	 * CK: this code is very delicate. The caller expects this: - if url_fread
	 * returns something, and the EOF is reached, it this call must return
	 * with both the content and the reached_eof flag set. - failing to do so will
	 * result in skipping the last line.
	 */
	bytesread = url_fread((void *)outbuf, maxread, state->file, state->cstate);

	if (url_feof(state->file, bytesread))
	{
		state->cstate->reached_eof = true;
	}

	if (bytesread <= 0)
	{
		if (url_ferror(state->file, bytesread, NULL, 0))
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not read from extCREATE EXTENSION ext_fdwernal file: %m")));
	}

	return bytesread;
}

static void
exttable_BeginForeignScan(ForeignScanState *node, int eflags)
{
	elog(NOTICE, "exttable_BeginForeignScan: %d", GpIdentity.segindex);
	ForeignScan *scan = (ForeignScan *)node->ss.ps.plan;
	const char *infostr = strVal(linitial(scan->fdw_private));

	ExternalCommonInfo *info = DeserializeExternalCommonInfo(infostr);
	if (info->exec_location == FTEXECLOCATION_ALL_SEGMENTS &&
		Gp_role == GP_ROLE_DISPATCH)
		return;

	// Control location segment mapping
	char *url = (char *)linitial(info->locations);
	extvar_t extvar;

	/* set up extvar */
	memset(&extvar, 0, sizeof(extvar));
	external_set_env_vars_ext(&extvar,
							  url,
							  true,
							  "\"",
							  "\"",
							  0,
							  false,
							  0,
							  NIL);
	URL_FILE *file = url_fopen(url, false, &extvar, 0, 0);
	ext_fdw_state *state = palloc(sizeof(ext_fdw_state));
	state->file = file;
	state->raw_buffer = palloc(RAW_BUF_SIZE);
	state->raw_buf_len = 0;
	state->eof = false;
	state->full_buffer = makeStringInfo();
	node->fdw_state = state;

	if (strcmp(info->format, "csv") == 0)
	{
		state->iscustom = false;
		CopyState cstate = cstate = BeginCopyFrom(NULL,
												  node->ss.ss_currentRelation,
												  0,
												  false,
												  external_getdata_callback,
												  state, NIL, NIL);
		cstate->csv_mode = true;
		cstate->quote = "\"";
		cstate->escape = "\"";
		cstate->delim = ",";
		state->cstate = cstate;
	}
	else
	{
		state->iscustom = true;
		/*
	 	 * Custom format: get formatter name and find it in the catalog
	 	 */
		Oid argList[1];

		Oid procOid = LookupFuncName(list_make1(info->format), 0, argList, true);
		if (!OidIsValid(procOid))
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_FUNCTION),
					 errmsg("formatter function \"%s\" of type %s was not found",
							info->format, "readable"),
					 errhint("Create it with CREATE FUNCTION.")));

		/* we found our function. set it up for calling  */
		state->formatter_func = palloc(sizeof(FmgrInfo));
		fmgr_info(procOid, state->formatter_func);

		//extInsertDesc->ext_custom_formatter_params = custom_formatter_params;
		FormatterData *formatter_data = (FormatterData *)palloc0(sizeof(FormatterData));
		formatter_data->fmt_perrow_ctx = AllocSetContextCreate(CurrentMemoryContext,
															   "ExtTableMemCxt",
															   ALLOCSET_DEFAULT_MINSIZE,
															   ALLOCSET_DEFAULT_INITSIZE,
															   ALLOCSET_DEFAULT_MAXSIZE);

		TupleDesc tupDesc = RelationGetDescr(node->ss.ss_currentRelation);
		int num_phys_attrs = tupDesc->natts;
		FmgrInfo *convFuncs = (FmgrInfo *)palloc(num_phys_attrs * sizeof(FmgrInfo));
		Oid *typioparams = (Oid *)palloc(num_phys_attrs * sizeof(Oid));
		for (int i = 0; i < num_phys_attrs; i++)
		{
			Oid func_oid;
			Form_pg_attribute attr = TupleDescAttr(tupDesc, i);
			getTypeInputInfo(attr->atttypid, &func_oid, &typioparams[i]);
			fmgr_info(func_oid, &convFuncs[i]);
		}

		formatter_data->type = T_FormatterData;
		formatter_data->fmt_relation = node->ss.ss_currentRelation;
		formatter_data->fmt_tupDesc = tupDesc;
		formatter_data->fmt_notification = FMT_NONE;
		formatter_data->fmt_badrow_len = 0;
		formatter_data->fmt_badrow_num = 0;
		formatter_data->fmt_args = NIL;
		formatter_data->fmt_conv_funcs = convFuncs;
		formatter_data->fmt_saw_eof = false;
		formatter_data->fmt_typioparams = typioparams;
		formatter_data->fmt_perrow_ctx = AllocSetContextCreate(CurrentMemoryContext,
															   "ExtTableMemCxt",
															   ALLOCSET_DEFAULT_MINSIZE,
															   ALLOCSET_DEFAULT_INITSIZE,
															   ALLOCSET_DEFAULT_MAXSIZE);

		formatter_data->fmt_needs_transcoding = false;
		formatter_data->fmt_conversion_proc = false;
		formatter_data->fmt_external_encoding = 0;
		state->formatter_data = formatter_data;
	}

	// Transaction hook?
}

static HeapTuple
externalgettup(ext_fdw_state *state)
{
	LOCAL_FCINFO(fcinfo, 0);
	InitFunctionCallInfoData(*fcinfo, state->formatter_func, 0, InvalidOid, (Node *)state->formatter_data, 0);

	while (state->raw_buf_len != 0 || !state->eof)
	{
		if (state->raw_buf_len == 0)
		{
			int bytesread = url_fread(state->raw_buffer, RAW_BUF_SIZE, state->file, 0);
			if (url_feof(state->file, bytesread))
			{
				state->eof = true;
			}
			state->raw_buf_len = bytesread;
		}

		while (state->raw_buf_len > 0)
		{
			bool error_caught = false;
			PG_TRY();
			{
				FormatterData *formatter = state->formatter_data;
				formatter->fmt_notification = FMT_NONE;
				formatter->fmt_badrow_len = 0;
				formatter->fmt_badrow_num = 0;
				formatter->fmt_saw_eof = state->eof;
				(void)FunctionCallInvoke(fcinfo);
			}
			PG_CATCH();
			{
				// Error handling hook here.
				error_caught = true;
			}
			PG_END_TRY();
			if (!error_caught)
			{
				switch (state->formatter_data->fmt_notification)
				{
				case FMT_NONE:
					return state->formatter_data->fmt_tuple;
				case FMT_NEED_MORE_DATA:
					state->raw_buf_len = 0;
					continue;
				};
			}
		}
	}
	return NULL;
}

static TupleTableSlot *
exttable_IterateForeignScan(ForeignScanState *node)
{
	//elog(NOTICE, "exttable_IterateForeignScan: %d", GpIdentity.segindex);
	TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;
	ext_fdw_state *state = (ext_fdw_state *)node->fdw_state;
	if (state->iscustom)
	{
		//MemoryContext oldcxt = MemoryContextSwitchTo(estate->es_query_cxt);
		HeapTuple tuple = externalgettup(state);
		if (!tuple)
		{
			ExecClearTuple(slot);
		}
		else
		{
			ExecStoreHeapTuple(tuple, slot, true);
		}
	}
	else
	{
		ExecClearTuple(slot);
		bool found = NextCopyFrom(state->cstate, NULL,
								  slot->tts_values, slot->tts_isnull);
		if (found)
			ExecStoreVirtualTuple(slot);
	}

	return slot;
}

static void
exttable_EndForeignScan(ForeignScanState *node)
{
	elog(NOTICE, "exttable_EndForeignScan: %d", GpIdentity.segindex);
	if (Gp_role == GP_ROLE_DISPATCH)
		return;
	ext_fdw_state *state = (ext_fdw_state *)node->fdw_state;
	if (state->file)
	{
		url_fclose(state->file, true, 0);
		if (!state->iscustom)
			EndCopyFrom(state->cstate);
	}

	// Transaction hook?
}

static void extExplainForeignModify(ModifyTableState *mtstate,
									ResultRelInfo *rinfo,
									List *fdw_private,
									int subplan_index,
									ExplainState *es)
{
	if (es->verbose)
	{
	}
}

static void extBeginForeignModify(ModifyTableState *mtstate, ResultRelInfo *rinfo,
								  List *fdw_private, int subplan_index, int eflags)
{
	elog(NOTICE, "extBeginForeignModify: %d", GpIdentity.segindex);
	Relation rel = rinfo->ri_RelationDesc;
	ForeignTable *ft = GetForeignTable(rel->rd_id);
	ForeignServer *fs = GetForeignServer(ft->serverid);
	ExternalCommonInfo *info;
	GpPolicy *policy = GpPolicyFetch(rel->rd_id);
	char exec_location = policy->ptype == POLICYTYPE_ENTRY ? 
		FTEXECLOCATION_MASTER : FTEXECLOCATION_ALL_SEGMENTS;
	info = MakeExternalCommonInfo(fs->options, ft->options, exec_location);
	if (info->exec_location == FTEXECLOCATION_ALL_SEGMENTS &&
		Gp_role == GP_ROLE_DISPATCH)
		return;

	// Control location segment mapping
	char *url = strVal(linitial(info->locations));
	extvar_t extvar;

	/* set up extvar */
	memset(&extvar, 0, sizeof(extvar));
	external_set_env_vars_ext(&extvar,
							  url,
							  true,
							  "\"",
							  "\"",
							  0,
							  false,
							  0,
							  NIL);
	URL_FILE *file = url_fopen(url, true, &extvar, 0, 0);
	ext_fdw_state *state = palloc(sizeof(ext_fdw_state));
	state->file = file;
	state->raw_buffer = palloc(RAW_BUF_SIZE);
	state->raw_buf_len = 0;
	state->eof = false;
	state->full_buffer = makeStringInfo();
	rinfo->ri_FdwState = state;

	if (strcmp(info->format, "csv") == 0)
	{
		Value *csv = makeString("csv");
		List *options = list_make1(makeDefElem("format", (Node *)csv, 0));

		state->iscustom = false;
		CopyState cstate = cstate = BeginCopyToForeignTable(
			rinfo->ri_RelationDesc, options);
		cstate->csv_mode = true;
		cstate->quote = "\"";
		cstate->escape = "\"";

		TupleDesc tupDesc = RelationGetDescr(cstate->rel);
		int num_phys_attrs = tupDesc->natts;
		cstate->out_functions = (FmgrInfo *)palloc(num_phys_attrs * sizeof(FmgrInfo));
		ListCell *cur;
		foreach (cur, cstate->attnumlist)
		{
			int attnum = lfirst_int(cur);
			Form_pg_attribute attr = TupleDescAttr(tupDesc, attnum - 1);
			Oid out_func_oid;
			bool isvarlena;

			if (cstate->binary)
				getTypeBinaryOutputInfo(attr->atttypid,
										&out_func_oid,
										&isvarlena);
			else
				getTypeOutputInfo(attr->atttypid,
								  &out_func_oid,
								  &isvarlena);
			fmgr_info(out_func_oid, &cstate->out_functions[attnum - 1]);
		}
		/* and 'fe_mgbuf' */
		cstate->fe_msgbuf = makeStringInfo();

		cstate->rowcontext = AllocSetContextCreate(CurrentMemoryContext,
												   "ExtTableMemCxt",
												   ALLOCSET_DEFAULT_MINSIZE,
												   ALLOCSET_DEFAULT_INITSIZE,
												   ALLOCSET_DEFAULT_MAXSIZE);
		state->cstate = cstate;
	}
}

static TupleTableSlot *extExecForeignInsert(EState *estate, ResultRelInfo *rinfo,
											TupleTableSlot *slot, TupleTableSlot *planSlot)
{
	//elog(NOTICE, "extExecForeignInsert: %d", GpIdentity.segindex);
	ext_fdw_state *state = (ext_fdw_state *)rinfo->ri_FdwState;
	if (state->cstate)
	{
		CopyState pstate = state->cstate;
		CopyOneRowTo(pstate, slot);
		CopySendEndOfRow(pstate);

		StringInfo fe_msgbuf = pstate->fe_msgbuf;
		static char ebuf[512] = {0};
		int ebuflen = 512;
		size_t nwrote = url_fwrite((void *)fe_msgbuf->data, fe_msgbuf->len, state->file, pstate);

		if (url_ferror(state->file, nwrote, ebuf, ebuflen))
		{
			if (*ebuf && strlen(ebuf) > 0)
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not write to external resource: %s",
								ebuf)));
			else
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not write to external resource: %m")));
		}

		pstate->fe_msgbuf->len = 0;
		pstate->fe_msgbuf->data[0] = '\0';
	}
	return slot;
}

static void extEndForeignModify(EState *estate, ResultRelInfo *rinfo)
{
	elog(NOTICE, "extEndForeignModify: %d", GpIdentity.segindex);
	ext_fdw_state *state = (ext_fdw_state *)rinfo->ri_FdwState;
	if (state && state->file)
	{
		char *relname = RelationGetRelationName(rinfo->ri_RelationDesc);
		url_fflush(state->file, state->cstate);
		url_fclose(state->file, true, relname);
		EndCopyFrom(state->cstate);
	}
	// Transaction hook?
}

static List *extPlanForeignModify (PlannerInfo *root,
											 ModifyTable *plan,
											 Index resultRelation,
											 int subplan_index)
{
	elog(NOTICE, "extPlanForeignModify: %d", GpIdentity.segindex);
	return NULL;
}

/*
 * Foreign-data wrapper handler function: return a struct with pointers
 * to my callback routines.
 */
Datum ext_fdw_handler(PG_FUNCTION_ARGS)
{
	elog(NOTICE, "ext_fdw_handler: %d", GpIdentity.segindex);
	FdwRoutine *fdwroutine = makeNode(FdwRoutine);

	fdwroutine->GetForeignRelSize = exttable_GetForeignRelSize;
	fdwroutine->GetForeignPaths = exttable_GetForeignPaths;
	fdwroutine->GetForeignPlan = exttable_GetForeignPlan;
	fdwroutine->BeginForeignScan = exttable_BeginForeignScan;
	fdwroutine->IterateForeignScan = exttable_IterateForeignScan;
	fdwroutine->EndForeignScan = exttable_EndForeignScan;

	fdwroutine->PlanForeignModify = extPlanForeignModify;
	fdwroutine->BeginForeignModify = extBeginForeignModify;
	fdwroutine->ExecForeignInsert = extExecForeignInsert;
	fdwroutine->EndForeignModify = extEndForeignModify;
	fdwroutine->ExplainForeignModify = extExplainForeignModify;

	PG_RETURN_POINTER(fdwroutine);
}

/*
 * Validate the generic options given to a FOREIGN DATA WRAPPER, SERVER,
 * USER MAPPING or FOREIGN TABLE that uses ext_fdw.
 *
 * Raise an ERROR if the option or its value is considered invalid.
 */
Datum ext_fdw_validator(PG_FUNCTION_ARGS)
{
	List *options_list = untransformRelOptions(PG_GETARG_DATUM(0));
	ListCell *cell;

	/*
	 * Check that only options supported by ext_fdw, and allowed for the
	 * current object type, are given.
	 */
	foreach (cell, options_list)
	{
		// DefElem    *def = (DefElem *) lfirst(cell);

		// if (!is_valid_option(def->defname, catalog))
		// {

		// }
	}

	PG_RETURN_VOID();
}
