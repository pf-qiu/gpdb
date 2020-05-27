/*
 * cdbendpointutils.c
 *
 * Utility functions for endpoints implementation.
 *
 * Copyright (c) 2019 - Present Pivotal Software, Inc.
 *
 * IDENTIFICATION
 *		src/backend/cdb/cdbendpointutils.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "cdb/cdbdisp_query.h"
#include "cdb/cdbdispatchresult.h"
#include "cdb/cdbendpoint.h"
#include "cdb/cdbutil.h"
#include "cdb/cdbvars.h"
#include "cdbendpointinternal.h"
#include "funcapi.h"
#include "libpq-fe.h"
#include "utils/builtins.h"
#include "utils/portal.h"
#ifdef FAULT_INJECTOR
#include "utils/faultinjector.h"
#endif

#define atooid(x)  ((Oid) strtoul((x), NULL, 10))

#define GP_ENDPOINTS_INFO_ATTRNUM 9

/*
 * EndpointStatus, EndpointsInfo and EndpointsStatusInfo structures are used
 * in UDFs(gp_endpoints_info, gp_endpoints_status_info) that show endpoint and
 * token information.
 */
typedef struct
{
	char		name[NAMEDATALEN];
	char		cursorName[NAMEDATALEN];
	int8		token[ENDPOINT_TOKEN_LEN];
	int			dbid;
	enum AttachStatus attachStatus;
	pid_t		senderPid;
	Oid			userId;
	int			sessionId;
}	EndpointStatus;

typedef struct
{
	/* current index in shared token list. */
	int			curTokenIdx;
	CdbComponentDatabases *cdbs;
	/* current index of node (master + segment) id */
	int			currIdx;
	EndpointStatus *status;
	int			status_num;
}	EndpointsInfo;

typedef struct
{
	int			endpointsNum;	/* number of EndpointDesc in the list */
	int			currentIdx;		/* current index of EndpointDesc in the list */
}	EndpointsStatusInfo;

extern Datum gp_check_parallel_retrieve_cursor(PG_FUNCTION_ARGS);
extern Datum gp_wait_parallel_retrieve_cursor(PG_FUNCTION_ARGS);
extern Datum gp_endpoints_info(PG_FUNCTION_ARGS);
extern Datum gp_endpoints_status_info(PG_FUNCTION_ARGS);

/* Used in UDFs */
static char *status_enum_to_string(enum AttachStatus status);
static enum AttachStatus status_string_to_enum(const char *status);
static bool check_parallel_retrieve_cursor(const char *cursorName, bool isWait);

/* Endpoint control information for current session. */
struct EndpointControl EndpointCtl = {
PARALLEL_RETRIEVE_NONE, InvalidSession,.sender = {NULL},.receiver = {NULL}};

/*
 * Convert the string tk0123456789 to int 0123456789 and save it into
 * the given token pointer.
 */
void
endpoint_parse_token(int8 *token /* out */ , const char *tokenStr)
{
	const char *msg = "Retrieve auth token is invalid";

	if (tokenStr[0] == 't' && tokenStr[1] == 'k' &&
		strlen(tokenStr) == ENDPOINT_TOKEN_STR_LEN)
	{
		hex_decode(tokenStr + 2, ENDPOINT_TOKEN_LEN * 2, (char *) token);
	}
	else
	{
		ereport(FATAL, (errcode(ERRCODE_INVALID_PASSWORD), errmsg("%s", msg)));
	}
}

/*
 * Generate a string tk0123456789 from int 0123456789
 *
 * Note: need to pfree() the result
 */
char *
endpoint_print_token(const int8 *token)
{
	const size_t len =
	ENDPOINT_TOKEN_STR_LEN + 1; /* 2('tk') + HEX string length + 1('\0') */
	char	   *res = palloc(len);

	res[0] = 't';
	res[1] = 'k';
	hex_encode((const char *) token, ENDPOINT_TOKEN_LEN, res + 2);
	res[len - 1] = 0;

	return res;
}

/*
 * Set the role of endpoint, sender or receiver.
 * When call RETRIEVE statement in PQprepare() & PQexecPrepared(), this
 * func will be called 2 times.
 */
void
SetParallelRtrvCursorExecRole(enum ParallelRtrvCursorExecRole role)
{
	if (EndpointCtl.GpParallelRtrvRole != PARALLEL_RETRIEVE_NONE && EndpointCtl.GpParallelRtrvRole != role)
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
						errmsg("endpoint role %s is already set to %s",
					 endpoint_role_to_string(EndpointCtl.GpParallelRtrvRole),
							   endpoint_role_to_string(role))));

	elog(DEBUG3, "CDB_ENDPOINT: set endpoint role to %s",
		 endpoint_role_to_string(role));

	EndpointCtl.GpParallelRtrvRole = role;
}

/*
 * Clear the role of endpoint
 */
void
ClearParallelRtrvCursorExecRole(void)
{
	elog(DEBUG3, "CDB_ENDPOINT: unset endpoint role %s",
		 endpoint_role_to_string(EndpointCtl.GpParallelRtrvRole));

	EndpointCtl.GpParallelRtrvRole = PARALLEL_RETRIEVE_NONE;
}

/*
 * Return the value of static variable GpPrceRole
 */
enum ParallelRtrvCursorExecRole
GetParallelRtrvCursorExecRole(void)
{
	return EndpointCtl.GpParallelRtrvRole;
}

const char *
endpoint_role_to_string(enum ParallelRtrvCursorExecRole role)
{
	switch (role)
	{
		case PARALLEL_RETRIEVE_SENDER:
			return "[END POINT SENDER]";

		case PARALLEL_RETRIEVE_RECEIVER:
			return "[END POINT RECEIVER]";

		case PARALLEL_RETRIEVE_NONE:
			return "[END POINT NONE]";

		default:
			Assert(false);
			return "";
	}
}

/*
 * Returns true if the two given endpoint tokens are equal.
 */
bool
endpoint_token_equals(const int8 *token1, const int8 *token2)
{
	Assert(token1);
	Assert(token2);

	/*
	 * memcmp should be good enough. Timing attack would not be a concern
	 * here.
	 */
	return memcmp(token1, token2, ENDPOINT_TOKEN_LEN) == 0;
}

bool
endpoint_name_equals(const char *name1, const char *name2)
{
	return strncmp(name1, name2, NAMEDATALEN) == 0;
}

/*
 * gp_check_parallel_retrieve_cursor
 *
 * Check whether given parallel retrieve cursor is finished immediately.
 *
 * Return true means finished.
 * Error out when parallel retrieve cursor has exception raised.
 */
Datum
gp_check_parallel_retrieve_cursor(PG_FUNCTION_ARGS)
{
	const char *cursorName = NULL;

	cursorName = text_to_cstring(PG_GETARG_TEXT_P(0));

	PG_RETURN_BOOL(check_parallel_retrieve_cursor(cursorName, false));
}

/*
 * gp_check_parallel_retrieve_cursor
 *
 * Wait until given parallel retrieve cursor is finished.
 *
 * Return true means finished.
 * Error out when parallel retrieve cursor has exception raised.
 */
Datum
gp_wait_parallel_retrieve_cursor(PG_FUNCTION_ARGS)
{
	const char *cursorName = NULL;

	cursorName = text_to_cstring(PG_GETARG_TEXT_P(0));

	PG_RETURN_BOOL(check_parallel_retrieve_cursor(cursorName, true));
}

/*
 * check_parallel_retrieve_cursor
 *
 * Support function for UDFs:
 * gp_check_parallel_retrieve_cursor
 * gp_wait_parallel_retrieve_cursor
 *
 * Check whether given parallel retrieve cursor is finished.
 * If isWait is true, hang until parallel retrieve cursor finished.
 *
 * Return true means finished.
 * Error out when parallel retrieve cursor has exception raised.
 */
static bool
check_parallel_retrieve_cursor(const char *cursorName, bool isWait)
{
	bool		retVal = false;
	Portal		portal;
	EState	   *estate = NULL;

	/* get the portal from the portal name */
	portal = GetPortalByName(cursorName);
	if (!PortalIsValid(portal))
	{
		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_CURSOR),
						errmsg("cursor \"%s\" does not exist", cursorName)));
		return false;			/* keep compiler happy */
	}
	if (!PortalIsParallelRetrieve())
	{
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
			   errmsg("this UDF only works for PARALLEL RETRIEVE CURSOR.")));
		return false;
	}
	estate = portal->queryDesc->estate;
	retVal = cdbdisp_checkDispatchAckMessage(estate->dispatcherState, ENDPOINT_FINISHED_ACK, isWait);

#ifdef FAULT_INJECTOR
	HOLD_INTERRUPTS();
	SIMPLE_FAULT_INJECTOR("check_parallel_retrieve_cursor_after_udf");
	RESUME_INTERRUPTS();
#endif

	check_parallel_cursor_errors(estate);
	return retVal;
}

/*
 * check_parallel_cursor_errors - Check the PARALLEL RETRIEVE CURSOR execution
 * status
 *
 * If get error, then rethrow the error.
 */
void
check_parallel_cursor_errors(EState *estate)
{
	CdbDispatcherState *ds;

	Assert(estate);

	ds = estate->dispatcherState;

	/*
	 * If QD, wait for QEs to finish and check their results.
	 */
	if (cdbdisp_checkResultsErrcode(ds->primaryResults))
	{
		ErrorData  *qeError = NULL;

		cdbdisp_getDispatchResults(ds, &qeError);
		Assert(qeError);
		estate->dispatcherState = NULL;
		cdbdisp_cancelDispatch(ds);
		ReThrowError(qeError);
	}
}

/*
 * On QD, display all the endpoints information in shared memory.
 * When allSessions is false, only parallel retrieve cursors created
 * in current session will be listed. Otherwise, all parallel retrieve
 * cursors will be listed.
 *
 * Note:
 * As a superuser, it can list all endpoints info of all users', but for
 * non-superuser, it can only list the current user's endpoints info for
 * security reason.
 */
Datum
gp_endpoints_info(PG_FUNCTION_ARGS)
{
	if (Gp_role != GP_ROLE_DISPATCH)
		ereport(
				ERROR, (errcode(ERRCODE_GP_COMMAND_ERROR),
						errmsg(
			 "gp_endpoints_info() only can be called on query dispatcher")));

	bool		allSessions = PG_GETARG_BOOL(0);
	FuncCallContext *funcctx;
	EndpointsInfo *mystatus;
	MemoryContext oldcontext;
	Datum		values[GP_ENDPOINTS_INFO_ATTRNUM];
	bool		nulls[GP_ENDPOINTS_INFO_ATTRNUM] = {true};
	HeapTuple	tuple;
	int			res_number = 0;

	if (SRF_IS_FIRSTCALL())
	{
		/* create a function context for cross-call persistence */
		funcctx = SRF_FIRSTCALL_INIT();

		/* switch to memory context appropriate for multiple function calls */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		/* build tuple descriptor */
		TupleDesc	tupdesc =
		CreateTemplateTupleDesc(GP_ENDPOINTS_INFO_ATTRNUM, false);

		TupleDescInitEntry(tupdesc, (AttrNumber) 1, "auth_token", TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 2, "cursorname", TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 3, "sessionid", INT4OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 4, "hostname", TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 5, "port", INT4OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 6, "dbid", INT4OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 7, "userid", OIDOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 8, "status", TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 9, "endpointname", TEXTOID, -1, 0);

		funcctx->tuple_desc = BlessTupleDesc(tupdesc);
		mystatus = (EndpointsInfo *) palloc0(sizeof(EndpointsInfo));
		funcctx->user_fctx = (void *) mystatus;
		mystatus->curTokenIdx = 0;
		mystatus->cdbs = cdbcomponent_getCdbComponents();
		mystatus->currIdx = 0;
		mystatus->status = NULL;
		mystatus->status_num = 0;

		CdbPgResults cdb_pgresults = {NULL, 0};

		CdbDispatchCommand(
		 "SELECT endpointname,cursorname,auth_token,dbid,status,senderpid,userid,"
					  "sessionid FROM pg_catalog.gp_endpoints_status_info()",
					  DF_WITH_SNAPSHOT | DF_CANCEL_ON_ERROR, &cdb_pgresults);

		if (cdb_pgresults.numResults == 0)
		{
			ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("gp_endpoints_info didn't get back any data "
								"from the segDBs")));
		}
		for (int i = 0; i < cdb_pgresults.numResults; i++)
		{
			if (PQresultStatus(cdb_pgresults.pg_results[i]) != PGRES_TUPLES_OK)
			{
				cdbdisp_clearCdbPgResults(&cdb_pgresults);
				ereport(
						ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg(
					 "gp_endpoints_info(): resultStatus is not tuples_Ok")));
			}
			res_number += PQntuples(cdb_pgresults.pg_results[i]);
		}

		if (res_number > 0)
		{
			mystatus->status =
				(EndpointStatus *) palloc0(sizeof(EndpointStatus) * res_number);
			mystatus->status_num = res_number;
			int			idx = 0;

			for (int i = 0; i < cdb_pgresults.numResults; i++)
			{
				struct pg_result *result = cdb_pgresults.pg_results[i];

				for (int j = 0; j < PQntuples(result); j++)
				{
					StrNCpy(mystatus->status[idx].name, PQgetvalue(result, j, 0), NAMEDATALEN);
					StrNCpy(mystatus->status[idx].cursorName, PQgetvalue(result, j, 1), NAMEDATALEN);
					endpoint_parse_token(mystatus->status[idx].token, PQgetvalue(result, j, 2));
					mystatus->status[idx].dbid = atoi(PQgetvalue(result, j, 3));
					mystatus->status[idx].attachStatus = status_string_to_enum(PQgetvalue(result, j, 4));
					mystatus->status[idx].senderPid = atoi(PQgetvalue(result, j, 5));
					mystatus->status[idx].userId = atooid(PQgetvalue(result, j, 6));
					mystatus->status[idx].sessionId = atoi(PQgetvalue(result, j, 7));
					idx++;
				}
			}
		}

		/* get endpoint status on master */
		LWLockAcquire(ParallelCursorEndpointLock, LW_SHARED);
		int			cnt = 0;

		for (int i = 0; i < MAX_ENDPOINT_SIZE; i++)
		{
			const EndpointDesc *entry = get_endpointdesc_by_index(i);

			if (!entry->empty && (superuser() || entry->userID == GetUserId()))
				cnt++;
		}
		if (cnt != 0)
		{
			int			idx = mystatus->status_num;

			mystatus->status_num += cnt;
			if (mystatus->status)
			{
				mystatus->status = (EndpointStatus *) repalloc(
															mystatus->status,
							  sizeof(EndpointStatus) * mystatus->status_num);
			}
			else
			{
				mystatus->status = (EndpointStatus *) palloc(
							  sizeof(EndpointStatus) * mystatus->status_num);
			}

			for (int i = 0; i < MAX_ENDPOINT_SIZE; i++)
			{
				const EndpointDesc *entry = get_endpointdesc_by_index(i);

				/*
				 * Only allow current user to get own endpoints. Or let
				 * superuser get all endpoints.
				 */
				if (!entry->empty && (superuser() || entry->userID == GetUserId()))
				{
					EndpointStatus *status = &mystatus->status[idx];

					StrNCpy(status->name, entry->name, NAMEDATALEN);
					StrNCpy(status->cursorName, entry->cursorName, NAMEDATALEN);
					get_token_by_session_id(entry->sessionID, entry->userID,
											status->token);
					status->dbid = contentid_get_dbid(
					MASTER_CONTENT_ID, GP_SEGMENT_CONFIGURATION_ROLE_PRIMARY,
													  false);
					status->attachStatus = entry->attachStatus;
					status->senderPid = entry->senderPid;
					status->userId = entry->userID;
					status->sessionId = entry->sessionID;
					idx++;
				}
			}
		}
		LWLockRelease(ParallelCursorEndpointLock);

		/* return to original context when allocating transient memory */
		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();
	mystatus = funcctx->user_fctx;

	while (mystatus->currIdx < mystatus->status_num)
	{
		Datum		result;
		EndpointStatus *qe_status = &mystatus->status[mystatus->currIdx++];

		Assert(qe_status);

		/*
		 * If allSessions is true, show all endpoints in mystatus->status.
		 * Otherwise, only show endpoints in mystatus->status that sessionId
		 * equals to current session.
		 */
		if (!allSessions && qe_status->sessionId != gp_session_id)
		{
			continue;
		}

		GpSegConfigEntry *segCnfInfo = dbid_get_dbinfo(qe_status->dbid);

		memset(values, 0, sizeof(values));
		memset(nulls, 0, sizeof(nulls));

		char	   *token = endpoint_print_token(qe_status->token);

		values[0] = CStringGetTextDatum(token);
		pfree(token);
		nulls[0] = false;
		values[1] = CStringGetTextDatum(qe_status->cursorName);
		nulls[1] = false;
		values[2] = Int32GetDatum(qe_status->sessionId);
		nulls[2] = false;
		values[3] = CStringGetTextDatum(segCnfInfo->hostname);
		nulls[3] = false;
		values[4] = Int32GetDatum(segCnfInfo->port);
		nulls[4] = false;
		values[5] = Int32GetDatum(segCnfInfo->dbid);
		nulls[5] = false;
		values[6] = ObjectIdGetDatum(qe_status->userId);
		nulls[6] = false;

		/*
		 * find out the status of end-point
		 */
		values[7] =
			CStringGetTextDatum(status_enum_to_string(qe_status->attachStatus));
		nulls[7] = false;

		if (qe_status)
		{
			values[8] = CStringGetTextDatum(qe_status->name);
			nulls[8] = false;
		}
		else
		{
			nulls[8] = true;
		}

		tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
		result = HeapTupleGetDatum(tuple);

		SRF_RETURN_NEXT(funcctx, result);
	}
	SRF_RETURN_DONE(funcctx);
}

/*
 * Display the status of all valid EndpointDesc of current
 * backend in shared memory.
 * If current user is superuser, list all endpoints on this segment.
 * Or only show current user's endpoints on this segment.
 */
Datum
gp_endpoints_status_info(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	EndpointsStatusInfo *mystatus;
	MemoryContext oldcontext;
	Datum		values[10];
	bool		nulls[10] = {true};
	HeapTuple	tuple;

	if (SRF_IS_FIRSTCALL())
	{
		/* create a function context for cross-call persistence */
		funcctx = SRF_FIRSTCALL_INIT();

		/* switch to memory context appropriate for multiple function calls */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		/* build tuple descriptor */
		TupleDesc	tupdesc = CreateTemplateTupleDesc(10, false);

		TupleDescInitEntry(tupdesc, (AttrNumber) 1, "auth_token", TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 2, "databaseid", INT4OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 3, "senderpid", INT4OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 4, "receiverpid", INT4OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 5, "status", TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 6, "dbid", INT4OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 7, "sessionid", INT4OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 8, "userid", OIDOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 9, "endpointname", TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 10, "cursorname", TEXTOID, -1, 0);


		funcctx->tuple_desc = BlessTupleDesc(tupdesc);

		mystatus = (EndpointsStatusInfo *) palloc0(sizeof(EndpointsStatusInfo));
		funcctx->user_fctx = (void *) mystatus;
		mystatus->endpointsNum = MAX_ENDPOINT_SIZE;
		mystatus->currentIdx = 0;

		/* return to original context when allocating transient memory */
		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();
	mystatus = funcctx->user_fctx;

	LWLockAcquire(ParallelCursorEndpointLock, LW_SHARED);
	while (mystatus->currentIdx < mystatus->endpointsNum)
	{
		memset(values, 0, sizeof(values));
		memset(nulls, 0, sizeof(nulls));
		Datum		result;

		const EndpointDesc *entry =
		get_endpointdesc_by_index(mystatus->currentIdx);

		/*
		 * Only allow current user to list his own endpoints. Or let superuser
		 * list all endpoints.
		 */
		if (!entry->empty && (superuser() || entry->userID == GetUserId()))
		{
			char	   *status = NULL;
			int8		token[ENDPOINT_TOKEN_LEN];

			get_token_by_session_id(entry->sessionID, entry->userID, token);
			char	   *tokenStr = endpoint_print_token(token);

			values[0] = CStringGetTextDatum(tokenStr);
			nulls[0] = false;
			values[1] = Int32GetDatum(entry->databaseID);
			nulls[1] = false;
			values[2] = Int32GetDatum(entry->senderPid);
			nulls[2] = false;
			values[3] = Int32GetDatum(entry->receiverPid);
			nulls[3] = false;
			status = status_enum_to_string(entry->attachStatus);
			values[4] = CStringGetTextDatum(status);
			nulls[4] = false;
			values[5] = Int32GetDatum(GpIdentity.dbid);
			nulls[5] = false;
			values[6] = Int32GetDatum(entry->sessionID);
			nulls[6] = false;
			values[7] = ObjectIdGetDatum(entry->userID);
			nulls[7] = false;
			values[8] = CStringGetTextDatum(entry->name);
			nulls[8] = false;
			values[9] = CStringGetTextDatum(entry->cursorName);
			nulls[9] = false;
			tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
			result = HeapTupleGetDatum(tuple);
			mystatus->currentIdx++;
			LWLockRelease(ParallelCursorEndpointLock);
			pfree(tokenStr);
			SRF_RETURN_NEXT(funcctx, result);
		}
		mystatus->currentIdx++;
	}
	LWLockRelease(ParallelCursorEndpointLock);
	SRF_RETURN_DONE(funcctx);
}

static char *
status_enum_to_string(enum AttachStatus status)
{
	char	   *result = NULL;

	switch (status)
	{
		case Status_Ready:
			result = GP_ENDPOINT_STATUS_READY;
			break;
		case Status_Retrieving:
			result = GP_ENDPOINT_STATUS_RETRIEVING;
			break;
		case Status_Attached:
			result = GP_ENDPOINT_STATUS_ATTACHED;
			break;
		case Status_Finished:
			result = GP_ENDPOINT_STATUS_FINISHED;
			break;
		case Status_Released:
			result = GP_ENDPOINT_STATUS_RELEASED;
			break;
		case Status_Invalid:

			/*
			 * This function is called when displays endpoint's information.
			 * Only valid endpoints will be printed out. So the status of the
			 * endpoint shouldn't be invalid.
			 */
			elog(ERROR, "invalid status of endpoint");
			break;
	}
	Assert(result != NULL);
	return result;
}

static enum AttachStatus
status_string_to_enum(const char *status)
{
	Assert(status);
	if (strcmp(status, GP_ENDPOINT_STATUS_READY) == 0)
	{
		return Status_Ready;
	}
	else if (strcmp(status, GP_ENDPOINT_STATUS_RETRIEVING) == 0)
	{
		return Status_Retrieving;
	}
	else if (strcmp(status, GP_ENDPOINT_STATUS_ATTACHED) == 0)
	{
		return Status_Attached;
	}
	else if (strcmp(status, GP_ENDPOINT_STATUS_FINISHED) == 0)
	{
		return Status_Finished;
	}
	else if (strcmp(status, GP_ENDPOINT_STATUS_RELEASED) == 0)
	{
		return Status_Released;
	}
	else
	{
		elog(ERROR, "unknown endpoint status %s", status);
		return Status_Invalid;
	}
}
