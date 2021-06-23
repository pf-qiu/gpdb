#include <unistd.h>

#include "postgres.h"
#include "fmgr.h"
#include "funcapi.h"

#include "access/reloptions.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_foreign_table.h"
#include "cdb/cdbvars.h"
#include "cdb/cdbutil.h"
#include "commands/defrem.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "nodes/makefuncs.h"
#include "optimizer/cost.h"
#include "optimizer/optimizer.h"
#include "optimizer/pathnode.h"
#include "optimizer/planmain.h"
#include "optimizer/restrictinfo.h"
#include "utils/lsyscache.h"

#include "libpq-fe.h"

PG_MODULE_MAGIC;


extern Datum gp2gp_fdw_handler(PG_FUNCTION_ARGS);
extern Datum gp2gp_fdw_validator(PG_FUNCTION_ARGS);

extern Datum pg_exttable(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(gp2gp_fdw_handler);
PG_FUNCTION_INFO_V1(gp2gp_fdw_validator);

static void
gp2gp_GetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid)
{

}
/*
 * gp2gp_GetForeignPaths
 *		Create possible access paths for a scan on the foreign table
 *
 *		Currently we don't support any push-down feature, so there is only one
 *		possible access path, which simply returns all records in the order in
 *		the data file.
 */
static void
gp2gp_GetForeignPaths(PlannerInfo *root,
					RelOptInfo *baserel,
					Oid foreigntableid)
{
	ForeignPath *pathnode = create_foreignscan_path(root, baserel,
									 NULL,	/* default pathtarget */
									 baserel->rows,
									 0,
									 0,
									 NIL,	/* no pathkeys */
									 baserel->lateral_relids,
									 NULL,	/* no extra plan */
									 NIL);
	/* Set path locus for random distribution */
	CdbPathLocus_MakeStrewn(&pathnode->path.locus, getgpsegmentCount());
	pathnode->path.motionHazard = false;
	add_path(baserel, (Path*)pathnode);
}

/*
 * gp2gpGetForeignPlan
 *		Create a ForeignScan plan node for scanning the foreign table
 */
static ForeignScan *
gp2gp_GetForeignPlan(PlannerInfo *root,
				   RelOptInfo *baserel,
				   Oid foreigntableid,
				   ForeignPath *best_path,
				   List *tlist,
				   List *scan_clauses,
				   Plan *outer_plan)
{
	Index		scan_relid = baserel->relid;

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
							NIL,	/* no expressions to evaluate */
							NIL,
							NIL,	/* no custom tlist */
							NIL,	/* no remote quals */
							outer_plan);
}

typedef struct {
	const char* host;
	const char* port;
	const char* db;
	const char* user;
	const char* password;
	const char* options;
	const char* query;
} ConnParameters;

static const char* GetParameterFromList(List* options_list, const char* name)
{
	ListCell   *cell;
	foreach(cell, options_list)
	{
		DefElem    *def = (DefElem *) lfirst(cell);
		const char *key = def->defname;
		if (strcmp(name,key) == 0)
		{
			return defGetString(def);
		}
	}

	ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					errmsg("missing parameter: %s", name)));
}

static void ParseConnParameter(ForeignServer *server, ConnParameters *param)
{
	List* options = server->options;
	param->host = GetParameterFromList(options, "host");
	param->port = GetParameterFromList(options, "port");
	param->db = GetParameterFromList(options, "db");
	param->user = GetParameterFromList(options, "user");
	param->options = GetParameterFromList(options, "options");
	param->password = NULL;
}

static void
check_prepare_conn(PGconn *conn, const char *dbName)
{
	/* check to see that the backend connection was successfully made */
	if (PQstatus(conn) != CONNECTION_OK)
	{
		ereport(ERROR, (errcode(ERRCODE_CONNECTION_FAILURE), 
				errmsg("Connection to database \"%s\" failed: %s",
				dbName, PQerrorMessage(conn))));
	}

	/*
	 * Set always-secure search path, so malicous users can't take
	 * control.
	 */
	PGresult *res = PQexec(conn,
			   "SELECT pg_catalog.set_config('search_path', '', false)");
	ExecStatusType status = PQresultStatus(res);
	PQclear(res);

	if (status != PGRES_TUPLES_OK)
	{
		ereport(ERROR, (errcode(ERRCODE_CONNECTION_EXCEPTION), 
			errmsg("SET failed: %s", PQerrorMessage(conn))));
	}
}

/* execute sql and check it is a command without result set returned */
static void
exec_sql_without_resultset(PGconn *conn, const char *sql)
{
	elog(NOTICE, "gp2gp_fdw exec: %s", sql);

	PGresult *res = PQexec(conn, sql);
	ExecStatusType status = PQresultStatus(res);
	PQclear(res);
	if (status != PGRES_COMMAND_OK)
	{
		ereport(ERROR, (errcode(ERRCODE_CONNECTION_EXCEPTION), 
				errmsg("execute sql failed: \"%s\"\nfailed %s",
					sql, PQerrorMessage(conn))));
	}
}

typedef struct {
	const char *host;
	const char *port;
	const char *db;
	const char *user;
	const char *token;
	const char *endpoint;
} CursorInfo;

typedef struct {
	List *cursors;
	ListCell *next_cursor;
	PGconn *current_conn;
	CursorInfo current_info;

	PGresult   *res;
	int next_row;

	TupleDesc desc;
	FmgrInfo   *in_functions;
	Oid		   *typioparams;
} ParallelCursorWorkingState;

/* assign_cursor is the segment cursor finding algorithm. Here we use
 * a simple same modular group algorithm: (dest segid) % (src cluster
 * size) == current segid.
 * As a result there may be 3 scenarios depending on the cluster size.
 * 1. src = dst. Each segment has exactly one cursor of same segid.
 * 2. src < dst. Each segment may use more than one cursor.
 * 3. src > dst. Some segments have no cursor to work with.
 */
static List* assign_cursor(List *fdw_private)
{
	List *cursors = NIL;
	ListCell *lc;
	int segid;
	DefElem *de;
	foreach(lc, fdw_private)
	{
		void *n = lfirst(lc);
		if (nodeTag(n) != T_DefElem) continue;
		de = n;
		if (0 != sscanf(de->defname, "seg%d", &segid) &&
			segid % getgpsegmentCount() == GpIdentity.segindex)
		{
			/* Target segment cursor belongs to this segment. */
			cursors = lappend(cursors, de);
		}
	}
	return cursors;
}


/* terminate current string at comma, return string after comma */
static char* next_csv_part(char* str)
{
	char *comma = strchr(str, ',');
	if (comma == NULL)
		return NULL;
	*comma = '\0';
	return comma + 1;
}
/* simple csv parser */
static void parse_cursor_info(DefElem *e, CursorInfo *info)
{
	/* cursor info format: host,port,token */
	char *str = defGetString(e);
	elog(NOTICE, "paramstr: %s, %s", e->defname, str);
	char *p = str;
	info->host = p;

	p = next_csv_part(p);
	if (p == NULL)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			errmsg("cursor info missing port %s", str)));
	info->port = p;

	p = next_csv_part(p);
	if (p == NULL)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			errmsg("cursor info missing db %s", str)));
	info->db = p;

	p = next_csv_part(p);
	if (p == NULL)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			errmsg("cursor info missing user %s", str)));
	info->user = p;

	p = next_csv_part(p);
	if (p == NULL)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			errmsg("cursor info missing token %s", str)));
	info->token = p;

	p = next_csv_part(p);
	if (p == NULL)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			errmsg("cursor info missing endpoint %s", str)));
	info->endpoint = p;

	p = next_csv_part(p);
	if (p != NULL)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			errmsg("cursor info extra fields %s", str)));
}

static void setup_next_conn(ParallelCursorWorkingState *state)
{
	if (state->current_conn != NULL)
	{
		PQfinish(state->current_conn);
		state->current_conn = NULL;
	}
	if (state->next_cursor == NULL)
	{
		return;
	}

	DefElem *e = lfirst(state->next_cursor);
	CursorInfo *info = &state->current_info;
	parse_cursor_info(e, info);
	StringInfo si = makeStringInfo();
	appendStringInfo(si, "-c gp_retrieve_token=%s", info->token);

	PGconn *conn = PQsetdbLogin(info->host, info->port, si->data,
							 false, info->db,
							 info->user, NULL);
	if (PQstatus(conn) != CONNECTION_OK)
	{
		char *msg = pstrdup(PQerrorMessage(conn));
		PQfinish(conn);
		ereport(ERROR, (errcode(ERRCODE_CONNECTION_FAILURE), 
				errmsg("Connection to database \"%s\" failed: %s",
				info->db, msg)));
	}
	state->current_conn = conn;

	state->next_cursor = lnext(state->next_cursor);
}

static void setup_func_calls(ParallelCursorWorkingState *state, TupleDesc tupDesc)
{
	state->desc = tupDesc;
	int n = tupDesc->natts;
	state->in_functions = palloc0(sizeof(FmgrInfo) * n);
	state->typioparams = palloc0(sizeof(Oid) * n);
	for (int i = 0; i < n; i++)
	{
		Oid func_oid;
		Oid typio;
		Form_pg_attribute att = TupleDescAttr(tupDesc, i);
		getTypeInputInfo(att->atttypid, &func_oid, &typio);
		fmgr_info(func_oid, state->in_functions + i);
		state->typioparams[i] = typio;
	}
}

static void
gp2gp_BeginForeignScan(ForeignScanState *node, int eflags)
{
	ForeignTable *table = GetForeignTable(RelationGetRelid(node->ss.ss_currentRelation));
	ForeignServer *server = GetForeignServer(table->serverid);
	ForeignScan *plan = (ForeignScan *) node->ss.ps.plan;
	node->fdw_state = NULL;
	if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
		return;

	if (Gp_role == GP_ROLE_DISPATCH)
	{
		/* Path for QD.
		 * Execute query and declare parallel cursor, retrive configuration for each segment,
		 * append to fdw_private and propagate to QE.
		 */
		ConnParameters param;
		ParseConnParameter(server, &param);
		PGconn *master_conn = PQsetdbLogin(param.host, param.port, param.options, NULL, param.db, param.user, param.password);

		PG_TRY();
		{
			check_prepare_conn(master_conn, param.db);

			exec_sql_without_resultset(master_conn, "BEGIN;");

			param.query = GetParameterFromList(table->options, "query");
			StringInfo si = makeStringInfo();
			appendStringInfo(si, "DECLARE myportal PARALLEL RETRIEVE CURSOR FOR %s;", param.query);
			exec_sql_without_resultset(master_conn, si->data);

			/*
			 * get the endpoints info of this PARALLEL RETRIEVE CURSOR
			 */
			const char *q = "select hostname,port,auth_token,endpointname,gp_segment_id from pg_catalog.gp_endpoints() where cursorname='myportal';";
			PGresult *res = PQexec(master_conn, q);
			if (PQresultStatus(res) != PGRES_TUPLES_OK)
			{
				char* msg = pstrdup(PQerrorMessage(master_conn));
				PQclear(res);
				ereport(ERROR, (errcode(ERRCODE_CONNECTION_EXCEPTION),
					errmsg("execute sql failed: %s", msg)));
			}
			else
			{
				int	ntup = PQntuples(res);
				if (ntup <= 0)
				{
					elog(NOTICE, "select gp_endpoints view doesn't return rows");
				}
				else
				{
					for (int i = 0; i < ntup; i++)
					{
						char *host = PQgetvalue(res, i, 0);
						if (strchr(host, ',') != NULL)
							ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
								errmsg("host name must not contain comma: %s", host)));
						char *port = PQgetvalue(res, i, 1);
						char *token = pstrdup(PQgetvalue(res, i, 2));
						char *endpoint = pstrdup(PQgetvalue(res, i, 3));
						char *segid = PQgetvalue(res, i, 4);;

						resetStringInfo(si);
						appendStringInfo(si, "seg%s", segid);
						char *segbuf = pstrdup(si->data);
						resetStringInfo(si);
						appendStringInfo(si, "%s,%s,%s,%s,%s,%s", host, port, param.db, param.user, token, endpoint);
						char *paramstr = pstrdup(si->data);
						/* fdw_private is a list that will be propagated to segments
						 * as long as every node is a subclass of Node structure.
						 */
						Node *opt = (Node*)makeDefElem(segbuf, (Node*)makeString(paramstr), -1);
						plan->fdw_private = lappend(plan->fdw_private, opt);
					}
				}
				PQclear(res);
			}
		}
		PG_CATCH();
		{
			PQfinish(master_conn);
			PG_RE_THROW();
		}
		PG_END_TRY();
		node->fdw_state = master_conn;
	}
	else if (Gp_role == GP_ROLE_EXECUTE)
	{
		/* Path for QE. Retrive cursor configuration from fdw_private.
		 * Find cursor that belongs to current segment depending on size of
		 * source and destination cluster. */
		ParallelCursorWorkingState* state = palloc0(sizeof(ParallelCursorWorkingState));
		state->cursors = assign_cursor(plan->fdw_private);
		state->next_cursor = list_head(state->cursors);
		setup_next_conn(state);
		setup_func_calls(state, node->ss.ps.ps_ResultTupleDesc);
		node->fdw_state = state;
	}
}

static int retrive_rows = 100000;

static TupleTableSlot *
gp2gp_IterateForeignScan(ForeignScanState *node)
{
	ParallelCursorWorkingState *state = node->fdw_state;
	TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;
	TupleDesc desc = node->ss.ps.ps_ResultTupleDesc;
	ExecClearTuple(slot);

	/* We may have finished working on one or more cursors if some are empty.
	 * Iterate until we get some tuple or complete scan on all cursors.
	 */
	while(state->current_conn != NULL)
	{
		if (state->res == NULL)
		{
			/* first scan for this cursor, setup query and result.*/
			PGconn *conn = state->current_conn;
			StringInfo sql = makeStringInfo();
			if (retrive_rows <= 0)
				appendStringInfo(sql, "RETRIEVE ALL FROM ENDPOINT %s;", state->current_info.endpoint);
			else
				appendStringInfo(sql, "RETRIEVE %d FROM ENDPOINT %s;", retrive_rows, state->current_info.endpoint);
			PGresult *res = PQexec(conn, sql->data);
			if (PQresultStatus(res) != PGRES_TUPLES_OK)
			{
				ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), 
								errmsg("Query didn't return tuples properly: %s",
									PQerrorMessage(conn))));
			}
			int fields = PQnfields(res);
			if (fields != desc->natts)
			{
				ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), 
								errmsg("local field %d, remote field %d",
								desc->natts, fields)));
			}
			state->res = res;
			state->next_row = 0;
		}

		int nrows = PQntuples(state->res);
		if (nrows == 0)
		{
			PQclear(state->res);
			state->res = NULL;
			setup_next_conn(state);
			continue;
		}
		if (state->next_row == nrows)
		{
			/* finished scanning from one cursor */
			PQclear(state->res);
			state->res = NULL;
			continue;
		}
		else
		{
			/* normal scan route, process one tuple(row) each time. */
			for (int i = 0; i < desc->natts; i++)
			{
				if (PQgetisnull(state->res, state->next_row, i))
				{
					slot->tts_isnull[i] = true;
					slot->tts_values[i] = 0;
				}
				else
				{
					char *value = PQgetvalue(state->res, state->next_row, i);
					slot->tts_isnull[i] = false;
					Datum d = InputFunctionCall(&state->in_functions[i], value, 
						state->typioparams[i], desc->attrs[i].atttypmod);
					slot->tts_values[i] = d;
				}
			}
			state->next_row++;
			ExecStoreVirtualTuple(slot);
			break;
		}
	}
	return slot;
}

static void
gp2gp_EndForeignScan(ForeignScanState *node)
{
	if (Gp_role == GP_ROLE_DISPATCH)
	{
		PGconn* master_conn = node->fdw_state;
		if (master_conn)
			PQfinish(master_conn);
	}
	else
	{
		/* Normally cursor connections should be closed gracefully during scan. */
		ParallelCursorWorkingState *state = node->fdw_state;
		if (state)
		{
			if (state->in_functions)
				pfree(state->in_functions);
			if (state->typioparams)
				pfree(state->typioparams);
			pfree(state);
		}
	}
}

/* FDW validator for external tables */
Datum
gp2gp_fdw_validator(PG_FUNCTION_ARGS)
{
	List	   *options_list = untransformRelOptions(PG_GETARG_DATUM(0));
	ListCell   *cell;

	/*
	 * Check options
	 */
	foreach(cell, options_list)
	{
		// DefElem    *def = (DefElem *) lfirst(cell);
		// const char *name = def->defname;
		// const char *value = defGetString(def);
	}
	PG_RETURN_VOID();
}

Datum
gp2gp_fdw_handler(PG_FUNCTION_ARGS)
{
	FdwRoutine *routine = makeNode(FdwRoutine);

	routine->GetForeignRelSize = gp2gp_GetForeignRelSize;
	routine->GetForeignPaths = gp2gp_GetForeignPaths;
	routine->GetForeignPlan = gp2gp_GetForeignPlan;
	routine->BeginForeignScan = gp2gp_BeginForeignScan;
	routine->IterateForeignScan = gp2gp_IterateForeignScan;
	routine->EndForeignScan = gp2gp_EndForeignScan;

	PG_RETURN_POINTER(routine);
};
