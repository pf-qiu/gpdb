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

#include <inttypes.h>

#include "executor/tqueue.h"
#include "executor/execdesc.h"
#include "storage/dsm.h"
#include "storage/shm_toc.h"
#include "nodes/parsenodes.h"
#include "tcop/dest.h"
#include "storage/lwlock.h"
#include "nodes/execnodes.h"

/*
 * Roles that used in PARALLEL RETRIEVE CURSOR execution.
 *
 * PARALLEL_RETRIEVE_SENDER behaves like a store, the client could retrieve
 * results from it. The PARALLEL_RETRIEVE_SENDER could be on master or
 * some/all segments, depending on the query of the PARALLEL RETRIEVE CURSOR.
 *
 * PARALLEL_RETRIEVE_RECEIVER connect to each PARALLEL_RETRIEVE_SENDER via
 * "retrieve" mode to retrieve results.
 */
enum ParallelRtrvCursorExecRole
{
	PARALLEL_RETRIEVE_SENDER = 1,
	PARALLEL_RETRIEVE_RECEIVER,
	PARALLEL_RETRIEVE_NONE
};

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

/* cbdendpoint.c */
/* Endpoint shared memory context init */
extern Size EndpointShmemSize(void);
extern void EndpointCTXShmemInit(void);

/*
 * Below functions should run on dispatcher.
 */
extern enum EndPointExecPosition GetParallelCursorEndpointPosition(PlannedStmt *plan);
extern void WaitEndpointReady(EState *estate);

/*
 * Below functions should run on Endpoints(QE/Entry DB).
 */
extern DestReceiver* CreateTQDestReceiverForEndpoint(TupleDesc tupleDesc,
		const char *cursorName, struct ParallelRtrvCursorSenderState *state);
extern void DestroyTQDestReceiverForEndpoint(DestReceiver *endpointDest,
		struct ParallelRtrvCursorSenderState *state);

/* cdbendpointretrieve.c */
/*
 * Below functions should run on retrieve role backend.
 */
extern bool AuthEndpoint(Oid userID, const char *tokenStr);
extern TupleDesc GetRetrieveStmtTupleDesc(const RetrieveStmt *stmt);
extern void ExecRetrieveStmt(const RetrieveStmt *stmt, DestReceiver *dest);

/* cdbendpointutils.c */
/* Utility functions */
extern void SetParallelRtrvCursorExecRole(enum ParallelRtrvCursorExecRole role);
extern void ClearParallelRtrvCursorExecRole(void);
extern enum ParallelRtrvCursorExecRole GetParallelRtrvCursorExecRole(void);
extern void ClearParallelRtrvCursorSenderState(struct ParallelRtrvCursorSenderState *state);

extern void AllocParallelRtrvCursorSenderState(EState *state);
extern void FreeParallelRtrvCursorSenderState(EState *state);

#endif   /* CDBENDPOINT_H */
