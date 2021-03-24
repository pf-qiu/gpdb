/*-------------------------------------------------------------------------
 *
 * cdbendpoint_private.h
 *	  Internal routines for parallel retrieve cursor.
 *
 *
 * Copyright (c) 2019-Present Pivotal Software, Inc
 *
 * src/backend/cdb/endpoints/cdbendpoint_private.h
 *
 *-------------------------------------------------------------------------
 */

#include "cdb/cdbendpoint.h"

#ifndef CDBENDPOINTINTERNAL_H
#define CDBENDPOINTINTERNAL_H

#define MAX_ENDPOINT_SIZE				 1024
#define ENDPOINT_TOKEN_LEN				 16
#define ENDPOINT_TOKEN_STR_LEN			 (2 + ENDPOINT_TOKEN_LEN * 2) // "tk0A1B...4E5F"
#define InvalidEndpointSessionId					 (-1)

#define ENDPOINT_KEY_TUPLE_DESC_LEN		1
#define ENDPOINT_KEY_TUPLE_DESC			2
#define ENDPOINT_KEY_TUPLE_QUEUE		3

#define ENDPOINT_MSG_QUEUE_MAGIC		0x1949100119980802U

/*
 * Naming rules for endpoint:
 * cursorname_sessionIdHex_segIndexHex
 */

/* ACK NOTICE MESSAGE FROM ENDPOINT QE/Entry DB to QD */
#define ENDPOINT_READY_ACK			"ENDPOINT_READY"
#define ENDPOINT_FINISHED_ACK		"ENDPOINT_FINISHED"
#define ENDPOINT_NAME_SESSIONID_LEN	8
#define ENDPOINT_NAME_RANDOM_LEN	10
#define ENDPOINT_NAME_CURSOR_LEN (NAMEDATALEN - 1 - ENDPOINT_NAME_SESSIONID_LEN - ENDPOINT_NAME_RANDOM_LEN)

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

	/* Track current msg queue entry for running RETRIEVE statement */
	MsgQueueStatusEntry *rxMQEntry;
}	EndpointControl;

extern EndpointControl EndpointCtl;

extern void check_parallel_cursor_errors(EState *estate);

/* Endpoint shared memory utility functions in "cdbendpoint.c" */
extern Endpoint get_endpointdesc_by_index(int index);
extern Endpoint find_endpoint(const char *endpointName, int sessionID);
extern void get_token_by_session_id(int sessionId, Oid userID, int8 *token /* out */ );
extern int	get_session_id_for_auth(Oid userID, const int8 *token);

/* utility functions in "cdbendpointutilities.c" */
extern bool endpoint_token_equals(const int8 *token1, const int8 *token2);
extern bool endpoint_name_equals(const char *name1, const char *name2);
extern void endpoint_parse_token(int8 *token /* out */ , const char *tokenStr);
extern char *endpoint_print_token(const int8 *token);	/* Need to pfree the result */
extern char *state_enum_to_string(EndpointState state);

#endif   /* CDBENDPOINTINTERNAL_H */
