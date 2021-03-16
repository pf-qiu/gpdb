/*-------------------------------------------------------------------------
 *
 * cdbendpointinternal.h
 *	  Internal routines for parallel retrieve cursor.
 *
 *
 * Copyright (c) 2019-Present Pivotal Software, Inc
 *
 * src/backend/cdb/endpoints/cdbendpointinternal.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef CDBENDPOINTINTERNAL_H
#define CDBENDPOINTINTERNAL_H

#define MAX_ENDPOINT_SIZE				 1024
#define ENDPOINT_TOKEN_LEN				 16
#define ENDPOINT_TOKEN_STR_LEN			 (2 + ENDPOINT_TOKEN_LEN * 2) // "tk0A1B...4E5F"
#define InvalidSession					 (-1)

#define GP_ENDPOINT_STATE_READY		 "READY"
#define GP_ENDPOINT_STATE_RETRIEVING	 "RETRIEVING"
#define GP_ENDPOINT_STATE_ATTACHED		 "ATTACHED"
#define GP_ENDPOINT_STATE_FINISHED		 "FINISHED"
#define GP_ENDPOINT_STATE_RELEASED		 "RELEASED"

#define ENDPOINT_KEY_TUPLE_DESC_LEN		1
#define ENDPOINT_KEY_TUPLE_DESC			2
#define ENDPOINT_KEY_TUPLE_QUEUE		3

#define ENDPOINT_MSG_QUEUE_MAGIC		0x1949100119980802U

/*
 * Naming rules for endpoint:
 * cursorname_sessionIdHex_segIndexHex
 */

/* ACK NOTICE MESSAGE FROM ENDPOINT QE/Entry DB to QD */
#define ENDPOINT_READY_ACK "ENDPOINT_READY"
#define ENDPOINT_FINISHED_ACK "ENDPOINT_FINISHED"
#define ENDPOINT_NAME_SESSIONID_LEN 8
#define ENDPOINT_NAME_RANDOM_LEN 10
#define ENDPOINT_NAME_CURSOR_LEN (NAMEDATALEN - 1 - ENDPOINT_NAME_SESSIONID_LEN - ENDPOINT_NAME_RANDOM_LEN)

/*
 * Endpoint attach status, used by parallel retrieve cursor.
 */
typedef enum EndpointState
{
	ENDPOINTSTATE_INVALID,
	ENDPOINTSTATE_READY,
	ENDPOINTSTATE_RETRIEVING,
	ENDPOINTSTATE_ATTACHED,
	ENDPOINTSTATE_FINISHED,
	ENDPOINTSTATE_RELEASED,
} EndpointState;

/*
 * Endpoint Description, used by parallel retrieve cursor.
 * Entries are maintained in shared memory.
 */
typedef struct EndpointDesc
{
	char		name[NAMEDATALEN];		/* Endpoint name */
	char		cursorName[NAMEDATALEN];		/* Parallel cursor name */
	Oid			databaseID;		/* Database OID */
	pid_t		senderPid;		/* The PID of EPR_SENDER(endpoint), set before
								 * endpoint sends data */
	pid_t		receiverPid;	/* The retrieve role's PID that connect to
								 * current endpoint */
	dsm_handle	mqDsmHandle;	/* DSM handle, which contains shared message
								 * queue */
	Latch		ackDone;		/* Latch to sync EPR_SENDER and EPR_RECEIVER
								 * status */
	EndpointState state;		/* The state of the endpoint */
	int			sessionID;		/* Connection session id */
	Oid			userID;			/* User ID of the current executed PARALLEL
								 * RETRIEVE CURSOR */
	bool		empty;			/* Whether current EndpointDesc slot in DSM is
								 * free */
	dsm_handle	sessionDsmHandle;	/* DSM handle, which contains per-session
									 * DSM (see session.c). */
}	EndpointDesc;

/*
 * Retrieve role status.
 */
enum RetrieveStatus
{
	RETRIEVE_STATUS_INIT,
	RETRIEVE_STATUS_GET_TUPLEDSCR,
	RETRIEVE_STATUS_GET_DATA,
	RETRIEVE_STATUS_FINISH,
};

typedef struct MsgQueueStatusEntry MsgQueueStatusEntry;

/*
 * Local structure to record current PARALLEL RETRIEVE CURSOR token and other info.
 */
typedef struct EndpointControl
{
	/*
	 * Which session that the endpoint is created in. For senders, this is the
	 * same with gp_session_id. For receivers, this is decided by the auth
	 * token.
	 */
	int			sessionID;

	struct receiver
	{
		/* Track current msg queue entry for running RETRIEVE statement */
		MsgQueueStatusEntry *currentMQEntry;
	}			receiver;
}	EndpointControl;

typedef EndpointDesc *Endpoint;

extern EndpointControl EndpointCtl;		/* Endpoint ctrl */

extern void check_parallel_cursor_errors(EState *estate);

/* Endpoint shared memory utility functions in "cdbendpoint.c" */
extern EndpointDesc *get_endpointdesc_by_index(int index);
extern EndpointDesc *find_endpoint(const char *endpointName, int sessionID);
extern void get_token_by_session_id(int sessionId, Oid userID, int8 *token /* out */ );
extern int	get_session_id_for_auth(Oid userID, const int8 *token);

/* utility functions in "cdbendpointutilities.c" */
extern bool endpoint_token_equals(const int8 *token1, const int8 *token2);
extern bool endpoint_name_equals(const char *name1, const char *name2);
extern void endpoint_parse_token(int8 *token /* out */ , const char *tokenStr);
extern char *endpoint_print_token(const int8 *token);	/* Need to pfree the result */

#endif   /* CDBENDPOINTINTERNAL_H */
