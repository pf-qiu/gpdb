/*-------------------------------------------------------------------------
 * cdbendpoint.h
 *	  Functions supporting the Greenplum Endpoint PARALLEL RETRIEVE CURSOR.
 *
 * The PARALLEL RETRIEVE CURSOR is introduced to reduce the heavy burdens of
 * master node. If possible it will not gather the result to master, and
 * redirect the result to segments. However some query may still need to
 * gather to the master. So the ENDPOINT is introduced to present these
 * node entities that when the PARALLEL RETRIEVE CURSOR executed, the query result
 * will be redirected to, not matter they are one master or some segments
 * or all segments.
 *
 * When the PARALLEL RETRIEVE CURSOR executed, user can setup retrieve mode connection
 * (in retrieve mode connection, the libpq authentication will not depends on
 * pg_hba) to all endpoints for retrieving result data parallelly. The RETRIEVE
 * statement behavior is similar to the "FETCH count" statement, while it only
 * can be executed in retrieve mode connection to endpoint.
 *
 * #NOTE: Orca is not support PARALLEL RETRIEVE CURSOR for now. It should fall back
 * to postgres optimizer.
 *
 * Copyright (c) 2019-Present Pivotal Software, Inc
 *
 *
 * IDENTIFICATION
 *		src/include/cdb/cdbendpoint.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CDBENDPOINT_H
#define CDBENDPOINT_H

#include "executor/tqueue.h"
#include "storage/shm_toc.h"
#include "nodes/execnodes.h"

/*
 * Endpoint allocate positions.
 */
enum EndPointExecPosition
{
	ENDPOINT_ON_ENTRY_DB,
	ENDPOINT_ON_SINGLE_QE,
	ENDPOINT_ON_SOME_QE,
	ENDPOINT_ON_ALL_QE
};

/*
 * The state information for parallel retrieve cursor
 */
typedef struct EndpointExecState
{
	struct EndpointDesc *endpoint;      /* endpoint entry */
	DestReceiver        *dest;
	dsm_segment         *dsmSeg;        /* dsm_segment pointer */
} EndpointExecState;

extern bool am_cursor_retrieve_handler;
extern bool retrieve_conn_authenticated;

/* cbdendpoint.c */
/* Endpoint shared memory context init */
extern Size EndpointShmemSize(void);
extern void EndpointCTXShmemInit(void);

/*
 * Below functions should run on dispatcher.
 */
extern enum EndPointExecPosition GetParallelCursorEndpointPosition(PlannedStmt *plan);
extern void WaitEndpointReady(EState *estate);
extern void AtAbort_EndpointExecState(void);
extern EndpointExecState *allocEndpointExecState(void);

/*
 * Below functions should run on Endpoints(QE/Entry DB).
 */
extern void CreateTQDestReceiverForEndpoint(TupleDesc tupleDesc,
		const char *cursorName, EndpointExecState *state);
extern void DestroyTQDestReceiverForEndpoint(EndpointExecState *state);

/* cdbendpointretrieve.c */
/*
 * Below functions should run on retrieve role backend.
 */
extern bool AuthEndpoint(Oid userID, const char *tokenStr);
extern TupleDesc GetRetrieveStmtTupleDesc(const RetrieveStmt *stmt);
extern void ExecRetrieveStmt(const RetrieveStmt *stmt, DestReceiver *dest);

#endif   /* CDBENDPOINT_H */
