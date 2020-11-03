/*-------------------------------------------------------------------------
 *
 * url.c
 *	  Core support for opening external relations via a URL
 *
 * Portions Copyright (c) 2007-2008, Greenplum inc
 * Portions Copyright (c) 2012-Present VMware, Inc. or its affiliates.
 *
 * IDENTIFICATION
 *	  src/backend/access/external/url.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/url.h"
#include "cdb/cdbdtxcontextinfo.h"
#include "cdb/cdbvars.h"
#include "cdb/cdbtm.h"
#include "commands/dbcommands.h"
#include "libpq/libpq-be.h"
#include "miscadmin.h"
#include "postmaster/postmaster.h"		/* postmaster port */
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/uri.h"

static void base16_encode(char *raw, int len, char *encoded);
static char *get_eol_delimiter(List *params);

void
external_set_env_vars(extvar_t *extvar, char *uri, bool csv, char *escape, char *quote, bool header, uint32 scancounter)
{
	external_set_env_vars_ext(extvar, uri, csv, escape, quote, EOL_UNKNOWN, header, scancounter, NULL);
}

void
external_set_env_vars_ext(extvar_t *extvar, char *uri, bool csv, char *escape, char *quote, EolType eol_type, bool header,
						  uint32 scancounter, List *params)
{
	time_t		now = time(0);
	struct tm  *tm = localtime(&now);
	char	   *result = (char *) palloc(7);	/* sign, 5 digits, '\0' */

	char	   *encoded_delim;
	int			line_delim_len;

	snprintf(extvar->GP_CSVOPT, sizeof(extvar->GP_CSVOPT),
			"m%1dx%3dq%3dn%1dh%1d",
			csv ? 1 : 0,
			escape ? 255 & *escape : 0,
			quote ? 255 & *quote : 0,
			eol_type,
			header ? 1 : 0);

	if (Gp_role != GP_ROLE_DISPATCH)
	{
		pg_ltoa(qdPostmasterPort, result);
		extvar->GP_MASTER_PORT = result;
		extvar->GP_MASTER_HOST = qdHostname;
	}
	else
	{
		CdbComponentDatabaseInfo *qdinfo =
				cdbcomponent_getComponentInfo(MASTER_CONTENT_ID);

		pg_ltoa(qdinfo->config->port, result);
		extvar->GP_MASTER_PORT = result;

		if (qdinfo->config->hostip != NULL)
			extvar->GP_MASTER_HOST = pstrdup(qdinfo->config->hostip);
		else
			extvar->GP_MASTER_HOST = pstrdup(qdinfo->config->hostname);
	}

	if (MyProcPort)
		extvar->GP_USER = MyProcPort->user_name;
	else
		extvar->GP_USER = "";

	extvar->GP_DATABASE = get_database_name(MyDatabaseId);
	extvar->GP_SEG_PG_CONF = ConfigFileName;	/* location of the segments
												 * pg_conf file  */
	extvar->GP_SEG_DATADIR = DataDir;	/* location of the segments
												 * datadirectory */
	sprintf(extvar->GP_DATE, "%04d%02d%02d",
			1900 + tm->tm_year, 1 + tm->tm_mon, tm->tm_mday);
	sprintf(extvar->GP_TIME, "%02d%02d%02d",
			tm->tm_hour, tm->tm_min, tm->tm_sec);

	/*
	 * read-only query don't have a valid distributed transaction ID, use
	 * "session id"-"command id" to identify the transaction.
	 */
	if (!getDistributedTransactionIdentifier(extvar->GP_XID))
		sprintf(extvar->GP_XID, "%u-%.10u", gp_session_id, gp_command_count);

	sprintf(extvar->GP_CID, "%x", gp_command_count);
	sprintf(extvar->GP_SN, "%x", scancounter);
	sprintf(extvar->GP_SEGMENT_ID, "%d", GpIdentity.segindex);
	sprintf(extvar->GP_SEG_PORT, "%d", PostPortNumber);
	sprintf(extvar->GP_SESSION_ID, "%d", gp_session_id);
	sprintf(extvar->GP_SEGMENT_COUNT, "%d", getgpsegmentCount());

	extvar->GP_QUERY_STRING = (char *)debug_query_string;

	if (NULL != params)
	{
		char	   *line_delim_str = get_eol_delimiter(params);

		line_delim_len = (int) strlen(line_delim_str);
		if (line_delim_len > 0)
		{
			encoded_delim = (char *) (palloc(line_delim_len * 2 + 1));
			base16_encode(line_delim_str, line_delim_len, encoded_delim);
		}
		else
		{
			line_delim_len = -1;
			encoded_delim = "";
		}
	}
	else
	{
		switch(eol_type)
		{
			case EOL_CR:
				encoded_delim = "0D";
				line_delim_len = 1;
				break;
			case EOL_NL:
				encoded_delim = "0A";
				line_delim_len = 1;
				break;
			case EOL_CRNL:
				encoded_delim = "0D0A";
				line_delim_len = 2;
				break;
			default:
				encoded_delim = "";
				line_delim_len = -1;
				break;
		}
	}
	extvar->GP_LINE_DELIM_STR = pstrdup(encoded_delim);
	sprintf(extvar->GP_LINE_DELIM_LENGTH, "%d", line_delim_len);
}

static void
base16_encode(char *raw, int len, char *encoded)
{
	const char *raw_bytes = raw;
	char	   *encoded_bytes = encoded;
	int			remaining = len;

	for (; remaining--; encoded_bytes += 2)
	{
		sprintf(encoded_bytes, "%02x", *(raw_bytes++));
	}
}

static char *
get_eol_delimiter(List *params)
{
	ListCell   *lc = params->head;

	while (lc)
	{
		if (pg_strcasecmp(((DefElem *) lc->data.ptr_value)->defname, "line_delim") == 0)
			return pstrdup(((Value *) ((DefElem *) lc->data.ptr_value)->arg)->val.str);
		lc = lc->next;
	}
	return pstrdup("");
}