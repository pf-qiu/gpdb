/*-------------------------------------------------------------------------
 *
 * gpss_fdw.c
 *		  foreign-data wrapper for server-side flat files.
 *
 * Copyright (c) 2010-2016, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  contrib/gpss_fdw/gpss_fdw.c
 *
 *-------------------------------------------------------------------------
 */
extern "C"
{
#include "postgres.h"

#include <sys/stat.h>
#include <unistd.h>

#include "access/htup_details.h"
#include "access/reloptions.h"
#include "access/sysattr.h"
#include "catalog/pg_foreign_table.h"
#include "commands/copy.h"
#include "commands/defrem.h"
#include "commands/explain.h"
#include "commands/vacuum.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "optimizer/cost.h"
#include "optimizer/pathnode.h"
#include "optimizer/planmain.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/var.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/sampling.h"
#include "utils/builtins.h"
#include "catalog/pg_proc.h"
#include "utils/lsyscache.h"
#include "parser/parse_func.h"
}
#include "gpss_rpc.h"

PG_MODULE_MAGIC;

/*
 * Describes the valid options for objects that use this wrapper.
 */
struct GpssFdwOption
{
	const char *optname;
	Oid optcontext; /* Oid of catalog in which option may appear */
};

/*
 * Valid options for gpss_fdw.
 * These options are based on the options for the COPY FROM command.
 * But note that force_not_null and force_null are handled as boolean options
 * attached to a column, not as table options.
 *
 * Note: If you are adding new option for user mapping, you need to modify
 * gpssGetOptions(), which currently doesn't bother to look at user mappings.
 */
static const struct GpssFdwOption valid_options[] = {
	/* File options */
	{"address", ForeignTableRelationId},

	/* Format options */
	/* oids option is not supported */
	{"formatter", ForeignTableRelationId},

	/*
	 * force_quote is not supported by gpss_fdw because it's for COPY TO.
	 */

	/* Sentinel */
	{NULL, InvalidOid}};

/*
 * FDW-specific information for RelOptInfo.fdw_private.
 */
typedef struct GpssFdwPlanState
{
	char *address;
	List *options;
	BlockNumber pages;
	double ntuples;
} GpssFdwPlanState;

/*
 * FDW-specific information for ForeignScanState.fdw_state.
 */
typedef struct GpssFdwExecutionState
{
	char *address;
	List *options;
	void *gpssrpc;
	FmgrInfo fi;
} GpssFdwExecutionState;

/*
 * SQL functions
 */
PG_FUNCTION_INFO_V1(gpss_fdw_handler);
PG_FUNCTION_INFO_V1(gpss_fdw_validator);

/*
 * FDW callback routines
 */
static void gpssGetForeignRelSize(PlannerInfo *root,
								  RelOptInfo *baserel,
								  Oid foreigntableid);
static void gpssGetForeignPaths(PlannerInfo *root,
								RelOptInfo *baserel,
								Oid foreigntableid);
static ForeignScan *gpssGetForeignPlan(PlannerInfo *root,
									   RelOptInfo *baserel,
									   Oid foreigntableid,
									   ForeignPath *best_path,
									   List *tlist,
									   List *scan_clauses,
									   Plan *outer_plan);
static void gpssExplainForeignScan(ForeignScanState *node, ExplainState *es);
static void gpssBeginForeignScan(ForeignScanState *node, int eflags);
static TupleTableSlot *gpssIterateForeignScan(ForeignScanState *node);
static void gpssReScanForeignScan(ForeignScanState *node);
static void gpssEndForeignScan(ForeignScanState *node);
static bool gpssAnalyzeForeignTable(Relation relation,
									AcquireSampleRowsFunc *func,
									BlockNumber *totalpages);
static bool gpssIsForeignScanParallelSafe(PlannerInfo *root, RelOptInfo *rel,
										  RangeTblEntry *rte);

/*
 * Helper functions
 */
static bool is_valid_option(const char *option, Oid context);
static void gpssGetOptions(Oid foreigntableid,
						   char **address, char **funcname, List **other_options);

static void estimate_size(PlannerInfo *root, RelOptInfo *baserel,
						  GpssFdwPlanState *fdw_private);
static void estimate_costs(PlannerInfo *root, RelOptInfo *baserel,
						   GpssFdwPlanState *fdw_private,
						   Cost *startup_cost, Cost *total_cost);
static Oid lookupCustomTransform(char *formatter_name);
/*
 * Foreign-data wrapper handler function: return a struct with pointers
 * to my callback routines.
 */
Datum gpss_fdw_handler(PG_FUNCTION_ARGS)
{
	FdwRoutine *fdwroutine = makeNode(FdwRoutine);

	fdwroutine->GetForeignRelSize = gpssGetForeignRelSize;
	fdwroutine->GetForeignPaths = gpssGetForeignPaths;
	fdwroutine->GetForeignPlan = gpssGetForeignPlan;
	fdwroutine->ExplainForeignScan = gpssExplainForeignScan;
	fdwroutine->BeginForeignScan = gpssBeginForeignScan;
	fdwroutine->IterateForeignScan = gpssIterateForeignScan;
	fdwroutine->ReScanForeignScan = gpssReScanForeignScan;
	fdwroutine->EndForeignScan = gpssEndForeignScan;
	fdwroutine->AnalyzeForeignTable = gpssAnalyzeForeignTable;
	fdwroutine->IsForeignScanParallelSafe = gpssIsForeignScanParallelSafe;

	PG_RETURN_POINTER(fdwroutine);
}

/*
 * Validate the generic options given to a FOREIGN DATA WRAPPER, SERVER,
 * USER MAPPING or FOREIGN TABLE that uses gpss_fdw.
 *
 * Raise an ERROR if the option or its value is considered invalid.
 */
Datum gpss_fdw_validator(PG_FUNCTION_ARGS)
{
	List *options_list = untransformRelOptions(PG_GETARG_DATUM(0));
	Oid catalog = PG_GETARG_OID(1);
	char *address = NULL;
	List *other_options = NIL;
	ListCell *cell;

	/*
	 * Check that only options supported by gpss_fdw, and allowed for the
	 * current object type, are given.
	 */
	foreach (cell, options_list)
	{
		DefElem *def = (DefElem *)lfirst(cell);

		if (!is_valid_option(def->defname, catalog))
		{
			const struct GpssFdwOption *opt;
			StringInfoData buf;

			/*
			 * Unknown option specified, complain about it. Provide a hint
			 * with list of valid options for the object.
			 */
			initStringInfo(&buf);
			for (opt = valid_options; opt->optname; opt++)
			{
				if (catalog == opt->optcontext)
					appendStringInfo(&buf, "%s%s", (buf.len > 0) ? ", " : "",
									 opt->optname);
			}

			ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
					 errmsg("invalid option \"%s\"", def->defname),
					 buf.len > 0
						 ? errhint("Valid options in this context are: %s",
								   buf.data)
						 : errhint("There are no valid options in this context.")));
		}

		/*
		 * Separate out address and column-specific options, since
		 * ProcessCopyOptions won't accept them.
		 */

		if (strcmp(def->defname, "address") == 0)
		{
			if (address)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));
			address = defGetString(def);
		}
		else
			other_options = lappend(other_options, def);
	}

	/*
	 * Address option is required for gpss_fdw foreign tables.
	 */
	if (catalog == ForeignTableRelationId && address == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FDW_DYNAMIC_PARAMETER_VALUE_NEEDED),
				 errmsg("address is required for gpss_fdw foreign tables")));

	PG_RETURN_VOID();
}

/*
 * Check if the provided option is one of the valid options.
 * context is the Oid of the catalog holding the object the option is for.
 */
static bool
is_valid_option(const char *option, Oid context)
{
	const struct GpssFdwOption *opt;

	for (opt = valid_options; opt->optname; opt++)
	{
		if (context == opt->optcontext && strcmp(opt->optname, option) == 0)
			return true;
	}
	return false;
}

/*
 * Fetch the options for a gpss_fdw foreign table.
 *
 * We have to separate out "address" from the other options because
 * it must not appear in the options list passed to the core COPY code.
 */
static void
gpssGetOptions(Oid foreigntableid,
			   char **address, char **funcname, List **other_options)
{
	ForeignTable *table;
	ForeignServer *server;
	ForeignDataWrapper *wrapper;
	List *options;
	ListCell *lc,
		*prev;

	/*
	 * Extract options from FDW objects.  We ignore user mappings because
	 * gpss_fdw doesn't have any options that can be specified there.
	 *
	 * (XXX Actually, given the current contents of valid_options[], there's
	 * no point in examining anything except the foreign table's own options.
	 * Simplify?)
	 */
	table = GetForeignTable(foreigntableid);
	server = GetForeignServer(table->serverid);
	wrapper = GetForeignDataWrapper(server->fdwid);

	options = NIL;
	options = list_concat(options, wrapper->options);
	options = list_concat(options, server->options);
	options = list_concat(options, table->options);

	/*
	 * Separate out the address.
	 */
	*address = NULL;
	prev = NULL;
	foreach (lc, options)
	{
		DefElem *def = (DefElem *)lfirst(lc);

		if (strcmp(def->defname, "address") == 0)
		{
			*address = defGetString(def);
			options = list_delete_cell(options, lc, prev);
			break;
		}
		prev = lc;
	}

	/*
	 * The validator should have checked that a address was included in the
	 * options, but check again, just in case.
	 */
	if (*address == NULL)
		elog(ERROR, "address is required for gpss_fdw foreign tables");

	if (table->exec_location == FTEXECLOCATION_ALL_SEGMENTS)
	{
		/*
		 * pass the on_segment option to COPY, which will replace the required
		 * placeholder "<SEGID>" in address
		 */
		options = list_append_unique(options, makeDefElem((char*)"on_segment", (Node *)makeInteger(TRUE)));
	}
	else if (table->exec_location == FTEXECLOCATION_ANY)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("gpss_fdw does not support mpp_execute option 'any'")));
	}

	*other_options = options;
}

/*
 * gpssGetForeignRelSize
 *		Obtain relation size estimates for a foreign table
 */
static void
gpssGetForeignRelSize(PlannerInfo *root,
					  RelOptInfo *baserel,
					  Oid foreigntableid)
{
	GpssFdwPlanState *fdw_private;

	/*
	 * Fetch options.  We only need address at this point, but we might as
	 * well get everything and not need to re-fetch it later in planning.
	 */
	fdw_private = (GpssFdwPlanState *)palloc(sizeof(GpssFdwPlanState));
	gpssGetOptions(foreigntableid,
				   &fdw_private->address, NULL, &fdw_private->options);
	baserel->fdw_private = (void *)fdw_private;

	/* Estimate relation size */
	estimate_size(root, baserel, fdw_private);
}

/*
 * gpssGetForeignPaths
 *		Create possible access paths for a scan on the foreign table
 *
 *		Currently we don't support any push-down feature, so there is only one
 *		possible access path, which simply returns all records in the order in
 *		the data file.
 */
static void
gpssGetForeignPaths(PlannerInfo *root,
					RelOptInfo *baserel,
					Oid foreigntableid)
{
	GpssFdwPlanState *fdw_private = (GpssFdwPlanState *)baserel->fdw_private;
	Cost startup_cost;
	Cost total_cost;

	/* Estimate costs */
	estimate_costs(root, baserel, fdw_private,
				   &startup_cost, &total_cost);

	/*
	 * Create a ForeignPath node and add it as only possible path.  We use the
	 * fdw_private list of the path to carry the convert_selectively option;
	 * it will be propagated into the fdw_private list of the Plan node.
	 */
	add_path(baserel, (Path *)
						  create_foreignscan_path(root, baserel,
												  NULL, /* default pathtarget */
												  baserel->rows,
												  startup_cost,
												  total_cost,
												  NIL,  /* no pathkeys */
												  NULL, /* no outer rel either */
												  NULL, /* no extra plan */
												  NIL));

	/*
	 * If data file was sorted, and we knew it somehow, we could insert
	 * appropriate pathkeys into the ForeignPath node to tell the planner
	 * that.
	 */
}

/*
 * gpssGetForeignPlan
 *		Create a ForeignScan plan node for scanning the foreign table
 */
static ForeignScan *
gpssGetForeignPlan(PlannerInfo *root,
				   RelOptInfo *baserel,
				   Oid foreigntableid,
				   ForeignPath *best_path,
				   List *tlist,
				   List *scan_clauses,
				   Plan *outer_plan)
{
	Index scan_relid = baserel->relid;

	/*
	 * We have no native ability to evaluate restriction clauses, so we just
	 * put all the scan_clauses into the plan node's qual list for the
	 * executor to check.  So all we have to do here is strip RestrictInfo
	 * nodes from the clauses and ignore pseudoconstants (which will be
	 * handled elsewhere).
	 */
	scan_clauses = extract_actual_clauses(scan_clauses, false);

	/* Create the ForeignScan node */
	return make_foreignscan(tlist,
							scan_clauses,
							scan_relid,
							NIL, /* no expressions to evaluate */
							best_path->fdw_private,
							NIL, /* no custom tlist */
							NIL, /* no remote quals */
							outer_plan);
}

/*
 * gpssExplainForeignScan
 *		Produce extra output for EXPLAIN
 */
static void
gpssExplainForeignScan(ForeignScanState *node, ExplainState *es)
{
	char *address;
	List *options;

	/* Fetch options --- we only need address at this point */
	gpssGetOptions(RelationGetRelid(node->ss.ss_currentRelation),
				   &address, NULL, &options);

	ExplainPropertyText("Foreign File", address, es);

	/* Suppress file size if we're not showing cost details */
	if (es->costs)
	{
		struct stat stat_buf;

		if (stat(address, &stat_buf) == 0)
			ExplainPropertyLong("Foreign File Size", (long)stat_buf.st_size,
								es);
	}
}

/*
 * gpssBeginForeignScan
 *		Initiate access to the file by creating CopyState
 */
static void
gpssBeginForeignScan(ForeignScanState *node, int eflags)
{
	ForeignScan *plan = (ForeignScan *)node->ss.ps.plan;
	char *address;
	char *name;
	List *options;
	GpssFdwExecutionState *festate;

	/*
	 * Do nothing in EXPLAIN (no ANALYZE) case.  node->fdw_state stays NULL.
	 */
	if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
		return;

	/* Fetch options of foreign table */
	gpssGetOptions(RelationGetRelid(node->ss.ss_currentRelation),
				   &address, &name, &options);

	/* Add any options from the plan (currently only convert_selectively) */
	options = list_concat(options, plan->fdw_private);

	/*
	 * Save state in node->fdw_state.  We must save enough information to call
	 * BeginCopyFrom() again.
	 */
	festate = (GpssFdwExecutionState *)palloc(sizeof(GpssFdwExecutionState));
	festate->address = address;
	festate->options = options;
	festate->gpssrpc = create_gpss_stub(address);

	Oid func = lookupCustomTransform(name);
	fmgr_info(func, &festate->fi);

	node->fdw_state = (void *)festate;
}

/*
 * gpssIterateForeignScan
 *		Read next record from the data file and store it into the
 *		ScanTupleSlot as a virtual tuple
 */
static TupleTableSlot *
gpssIterateForeignScan(ForeignScanState *node)
{
	GpssFdwExecutionState *festate = (GpssFdwExecutionState *)node->fdw_state;
	TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;
	
	/*
	 * The protocol for loading a virtual tuple into a slot is first
	 * ExecClearTuple, then fill the values/isnull arrays, then
	 * ExecStoreVirtualTuple.  If we don't find another row in the file, we
	 * just skip the last step, leaving the slot empty as required.
	 *
	 * We can pass ExprContext = NULL because we read all columns from the
	 * file, so no need to evaluate default expressions.
	 *
	 * We can also pass tupleOid = NULL because we don't allow oids for
	 * foreign tables.
	 */
	ExecClearTuple(slot);
	StringInfoData str;
	initStringInfoOfSize(&str, 4096);
	if (gpssfdw_stream_data(festate->gpssrpc, "", &str))
	{
		text *data = cstring_to_text_with_len(str.data, str.len);
		Datum v = PointerGetDatum(data);
		Datum *values = slot_get_values(slot);
		bool *isnull = slot_get_isnull(slot);
		HeapTupleHeader tup = (HeapTupleHeader)FunctionCall1(&festate->fi, v);
		HeapTupleData tuple;
		tuple.t_len = HeapTupleHeaderGetDatumLength(tup);
		ItemPointerSetInvalid(&(tuple.t_self));
		tuple.t_data = tup;

		TupleDesc desc = RelationGetDescr(node->ss.ss_currentRelation);
		heap_deform_tuple(&tuple, desc, values, isnull);
		ExecStoreVirtualTuple(slot);
	}

	return slot;
}

/*
 * gpssReScanForeignScan
 *		Rescan table, possibly with new parameters
 */
static void
gpssReScanForeignScan(ForeignScanState *node)
{
	
}

/*
 * gpssEndForeignScan
 *		Finish scanning foreign table and dispose objects used for this scan
 */
static void
gpssEndForeignScan(ForeignScanState *node)
{
	GpssFdwExecutionState *festate = (GpssFdwExecutionState *)node->fdw_state;

	/* if festate is NULL, we are in EXPLAIN; nothing to do */
	if (festate)
	{
		delete_gpss_stub(festate->gpssrpc);
	}
}

/*
 * gpssAnalyzeForeignTable
 *		Test whether analyzing this foreign table is supported
 */
static bool
gpssAnalyzeForeignTable(Relation relation,
						AcquireSampleRowsFunc *func,
						BlockNumber *totalpages)
{
	return false;
}

/*
 * gpssIsForeignScanParallelSafe
 *		Reading a file in a parallel worker should work just the same as
 *		reading it in the leader, so mark scans safe.
 */
static bool
gpssIsForeignScanParallelSafe(PlannerInfo *root, RelOptInfo *rel,
							  RangeTblEntry *rte)
{
	return false;
}

/*
 * Estimate size of a foreign table.
 *
 * The main result is returned in baserel->rows.  We also set
 * fdw_private->pages and fdw_private->ntuples for later use in the cost
 * calculation.
 */
static void
estimate_size(PlannerInfo *root, RelOptInfo *baserel,
			  GpssFdwPlanState *fdw_private)
{
	BlockNumber pages;
	double ntuples;
	double nrows;

	void *gpss = create_gpss_stub(fdw_private->address);
	if (gpss == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("gpss: could not connect to gpss at \"%s\"", fdw_private->address)));

	int64 bytes = gpssfdw_estimate_size(gpss, "");
	delete_gpss_stub(gpss);

	if (bytes == 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("gpss: could not estimate size")));

	/*
	 * Convert size to pages for use in I/O cost estimate later.
	 */
	pages = (bytes + (BLCKSZ - 1)) / BLCKSZ;
	if (pages < 1)
		pages = 1;
	fdw_private->pages = pages;

	/*
	 * Estimate the number of tuples in the file.
	 */
	if (baserel->pages > 0)
	{
		/*
		 * We have # of pages and # of tuples from pg_class (that is, from a
		 * previous ANALYZE), so compute a tuples-per-page estimate and scale
		 * that by the current file size.
		 */
		double density;

		density = baserel->tuples / (double)baserel->pages;
		ntuples = clamp_row_est(density * (double)pages);
	}
	else
	{
		/*
		 * Otherwise we have to fake it.  We back into this estimate using the
		 * planner's idea of the relation width; which is bogus if not all
		 * columns are being read, not to mention that the text representation
		 * of a row probably isn't the same size as its internal
		 * representation.  Possibly we could do something better, but the
		 * real answer to anyone who complains is "ANALYZE" ...
		 */
		int tuple_width;

		tuple_width = MAXALIGN(baserel->reltarget->width) +
					  MAXALIGN(SizeofHeapTupleHeader);
		ntuples = clamp_row_est((double)bytes /
								(double)tuple_width);
	}
	fdw_private->ntuples = ntuples;

	/*
	 * Now estimate the number of rows returned by the scan after applying the
	 * baserestrictinfo quals.
	 */
	nrows = ntuples *
			clauselist_selectivity(root,
								   baserel->baserestrictinfo,
								   0,
								   JOIN_INNER,
								   NULL,
								   false);

	nrows = clamp_row_est(nrows);

	/* Save the output-rows estimate for the planner */
	baserel->rows = nrows;
}

/*
 * Estimate costs of scanning a foreign table.
 *
 * Results are returned in *startup_cost and *total_cost.
 */
static void
estimate_costs(PlannerInfo *root, RelOptInfo *baserel,
			   GpssFdwPlanState *fdw_private,
			   Cost *startup_cost, Cost *total_cost)
{
	BlockNumber pages = fdw_private->pages;
	double ntuples = fdw_private->ntuples;
	Cost run_cost = 0;
	Cost cpu_per_tuple;

	/*
	 * We estimate costs almost the same way as cost_seqscan(), thus assuming
	 * that I/O costs are equivalent to a regular table file of the same size.
	 * However, we take per-tuple CPU costs as 10x of a seqscan, to account
	 * for the cost of parsing records.
	 */
	run_cost += seq_page_cost * pages;

	*startup_cost = baserel->baserestrictcost.startup;
	cpu_per_tuple = cpu_tuple_cost * 10 + baserel->baserestrictcost.per_tuple;
	run_cost += cpu_per_tuple * ntuples;
	*total_cost = *startup_cost + run_cost;
}


static Oid
lookupCustomTransform(char *formatter_name)
{
        List       *funcname = NIL;
        Oid                     procOid = InvalidOid;
        Oid                     argList[1];

        funcname = lappend(funcname, makeString(formatter_name));

        argList[0] = JSONOID; //json, see pg_type.h
        procOid = LookupFuncName(funcname, 1, argList, true);

        if (!OidIsValid(procOid))
                ereport(ERROR,
                                (errcode(ERRCODE_UNDEFINED_FUNCTION),
                                 errmsg("function \"%s\" was not found", formatter_name),
                                 errhint("Create it with CREATE FUNCTION.")));

        /* check allowed volatility */
        if (func_volatile(procOid) != PROVOLATILE_IMMUTABLE)
                ereport(ERROR,
                                (errcode(ERRCODE_INVALID_FUNCTION_DEFINITION),
                                 errmsg("formatter function %s is not declared IMUUTABLE",
                                                formatter_name)));

        return procOid;
}