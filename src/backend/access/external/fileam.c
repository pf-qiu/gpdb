/*-------------------------------------------------------------------------
 *
 * fileam.c
 *	  file access method routines
 *
 * This access layer mimics the heap access API with respect to how it
 * communicates with its respective scan node (external scan node) but
 * instead of accessing the heap pages, it actually "scans" data by
 * reading it from a local flat file or a remote data source.
 *
 * The actual data access, whether local or remote, is done with the
 * curl c library ('libcurl') which uses a 'c-file like' API but behind
 * the scenes actually does all the work of parsing the URI and communicating
 * with the target. In this case if the URI uses the file protocol (file://)
 * curl will try to open the specified file locally. If the URI uses the
 * http protocol (http://) then curl will reach out to that address and
 * get the data from there.
 *
 * As data is being read it gets parsed with the COPY command parsing rules,
 * as if it is data meant for COPY. Therefore, currently, with the lack of
 * single row error handling the first error will raise an error and the
 * query will terminate.
 *
 * Portions Copyright (c) 2007-2008, Greenplum inc
 * Portions Copyright (c) 2012-Present Pivotal Software, Inc.
 *
 *
 * IDENTIFICATION
 *	    src/backend/access/external/fileam.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <fstream/gfile.h>

#include "funcapi.h"
#include "access/fileam.h"
#include "access/formatter.h"
#include "access/heapam.h"
#include "access/valid.h"
#include "catalog/pg_exttable.h"
#include "catalog/pg_extprotocol.h"
#include "catalog/pg_proc.h"
#include "commands/copy.h"
#include "commands/dbcommands.h"
#include "libpq/libpq-be.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "pgstat.h"
#include "parser/parse_func.h"
#include "postmaster/postmaster.h" /* postmaster port */
#include "utils/relcache.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/uri.h"
#include "utils/builtins.h"

#include "cdb/cdbsreh.h"
#include "cdb/cdbtm.h"
#include "cdb/cdbutil.h"
#include "cdb/cdbvars.h"

static HeapTuple externalgettup(FileScanDesc scan, ScanDirection dir);

static void open_external_readable_source(FileScanDesc scan);
static void close_external_readable_source(FileScanDesc scan);
static void external_scan_error_callback(void *arg);

static HeapTuple InvokeExtProtocol(ExtProtocolDesc *file, bool last_call);

/* ----------------------------------------------------------------
*				   external_ interface functions
* ----------------------------------------------------------------
*/

#ifdef FILEDEBUGALL
#define FILEDEBUG_1                                      \
	elog(DEBUG2, "external_getnext([%s],dir=%d) called", \
		 RelationGetRelationName(scan->fs_rd), (int)direction)
#define FILEDEBUG_2 \
	elog(DEBUG2, "external_getnext returning EOS")
#define FILEDEBUG_3 \
	elog(DEBUG2, "external_getnext returning tuple")
#else
#define FILEDEBUG_1
#define FILEDEBUG_2
#define FILEDEBUG_3
#endif /* !defined(FILEDEBUGALL) */

/* ----------------
*		external_beginscan	- begin file scan
* ----------------
*/
FileScanDesc
external_beginscan(Relation relation, uint32 scancounter,
				   List *uriList, List *fmtOpts, char fmtType, bool isMasterOnly,
				   int rejLimit, bool rejLimitInRows, Oid fmterrtbl, int encoding)
{
	FileScanDesc scan;
	TupleDesc tupDesc = NULL;
	int attnum;
	int segindex = GpIdentity.segindex;
	char *uri = NULL;

	/*
	 * increment relation ref count while scanning relation
	 *
	 * This is just to make really sure the relcache entry won't go away while
	 * the scan has a pointer to it.  Caller should be holding the rel open
	 * anyway, so this is redundant in all normal scenarios...
	 */
	RelationIncrementReferenceCount(relation);

	/*
	 * allocate and initialize scan descriptor
	 */
	scan = (FileScanDesc)palloc(sizeof(FileScanDescData));

	scan->fs_inited = false;
	scan->fs_ctup.t_data = NULL;
	ItemPointerSetInvalid(&scan->fs_ctup.t_self);
	scan->fs_cbuf = InvalidBuffer;
	scan->fs_rd = relation;
	scan->fs_scancounter = scancounter;
	scan->fs_noop = false;
	scan->fs_file = NULL;
	scan->fs_formatter = NULL;
	scan->fs_constraintExprs = NULL;
	if (relation->rd_att->constr != NULL && relation->rd_att->constr->num_check > 0)
	{
		scan->fs_hasConstraints = true;
	}
	else
	{
		scan->fs_hasConstraints = false;
	}

	/*
	 * get the external URI assigned to us.
	 *
	 * The URI assigned for this segment is normally in the uriList list at
	 * the index of this segment id. However, if we are executing on MASTER
	 * ONLY the (one and only) entry which is destined for the master will be
	 * at the first entry of the uriList list.
	 */
	if (Gp_role == GP_ROLE_EXECUTE)
	{
		/* this is the normal path for most ext tables */
		Value *v;
		int idx = segindex;

		/*
		 * Segindex may be -1, for the following case. A slice is executed on
		 * entry db, (for example, gp_segment_configuration), then external table is
		 * executed on another slice. Entry db slice will still call
		 * ExecInitExternalScan (probably we should fix this?), then segindex
		 * = -1 will bomb out here.
		 */
		if (isMasterOnly && idx == -1)
			idx = 0;

		if (idx >= 0)
		{
			v = (Value *)list_nth(uriList, idx);

			if (v->type == T_Null)
				uri = NULL;
			else
				uri = (char *)strVal(v);
		}
	}
	else if (Gp_role == GP_ROLE_DISPATCH && isMasterOnly)
	{
		/* this is a ON MASTER table. Only get uri if we are the master */
		if (segindex == -1)
		{
			Value *v = list_nth(uriList, 0);

			if (v->type == T_Null)
				uri = NULL;
			else
				uri = (char *)strVal(v);
		}
	}

	/*
	 * if a uri is assigned to us - get a reference to it. Some executors
	 * don't have a uri to scan (if # of uri's < # of primary segdbs). in
	 * which case uri will be NULL. If that's the case for this segdb set to
	 * no-op.
	 */
	if (uri)
	{
		/* set external source (uri) */
		scan->fs_uri = uri;

		/*
		 * NOTE: we delay actually opening the data source until
		 * external_getnext()
		 */
	}
	else
	{
		/* segdb has no work to do. set to no-op */
		scan->fs_noop = true;
		scan->fs_uri = NULL;
	}

	tupDesc = RelationGetDescr(relation);
	scan->fs_tupDesc = tupDesc;
	scan->attr = tupDesc->attrs;
	scan->num_phys_attrs = tupDesc->natts;

	scan->values = (Datum *)palloc(scan->num_phys_attrs * sizeof(Datum));
	scan->nulls = (bool *)palloc(scan->num_phys_attrs * sizeof(bool));

	/*
	 * Pick up the required catalog information for each attribute in the
	 * relation, including the input function and the element type (to pass to
	 * the input function).
	 */
	scan->in_functions = (FmgrInfo *)palloc(scan->num_phys_attrs * sizeof(FmgrInfo));
	scan->typioparams = (Oid *)palloc(scan->num_phys_attrs * sizeof(Oid));

	for (attnum = 1; attnum <= scan->num_phys_attrs; attnum++)
	{
		/* We don't need info for dropped attributes */
		if (scan->attr[attnum - 1]->attisdropped)
			continue;

		getTypeInputInfo(scan->attr[attnum - 1]->atttypid,
						 &scan->in_func_oid, &scan->typioparams[attnum - 1]);
		fmgr_info(scan->in_func_oid, &scan->in_functions[attnum - 1]);
	}

	return scan;
}

/* ----------------
*		external_rescan  - (re)start a scan of an external file
* ----------------
*/
void external_rescan(FileScanDesc scan)
{
	/* Close previous scan if it was already open */
	external_stopscan(scan);
}

/* ----------------
*		external_endscan - end a scan
* ----------------
*/
void external_endscan(FileScanDesc scan)
{
	char *relname = pstrdup(RelationGetRelationName(scan->fs_rd));

	if (scan->values)
	{
		pfree(scan->values);
		scan->values = NULL;
	}
	if (scan->nulls)
	{
		pfree(scan->nulls);
		scan->nulls = NULL;
	}
	if (scan->in_functions)
	{
		pfree(scan->in_functions);
		scan->in_functions = NULL;
	}
	if (scan->typioparams)
	{
		pfree(scan->typioparams);
		scan->typioparams = NULL;
	}

	/*
	 * Close the external file
	 */
	if (!scan->fs_noop && scan->fs_file)
	{
		/*
		 * QueryFinishPending == true means QD have got
		 * enough tuples and query can return correctly,
		 * so slient errors when closing external file.
		 */
		close_external_readable_source(scan);
		
		scan->fs_file = NULL;
	}

	pfree(relname);
}

/* ----------------
*		external_stopscan - closes an external resource without dismantling the scan context
* ----------------
*/
void external_stopscan(FileScanDesc scan)
{
	/*
	 * Close the external file
	 */
	if (!scan->fs_noop && scan->fs_file)
	{
		close_external_readable_source(scan);
		scan->fs_file = NULL;
	}
}

/* ----------------------------------------------------------------
*		external_getnext
*
*		Parse a data file and return its rows in heap tuple form
* ----------------------------------------------------------------
*/
HeapTuple
external_getnext(FileScanDesc scan, ScanDirection direction)
{
	HeapTuple tuple;

	if (scan->fs_noop)
		return NULL;

	/*
	 * open the external source (local file or http).
	 *
	 * NOTE: external_beginscan() seems like the natural place for this call.
	 * However, in queries with more than one gang each gang will initialized
	 * all the nodes of the plan (but actually executed only the nodes in it's
	 * local slice) This means that external_beginscan() (and
	 * external_endscan() too) will get called more than needed and we'll end
	 * up opening too many http connections when they are not expected (see
	 * MPP-1261). Therefore we instead do it here on the first time around
	 * only.
	 */
	if (!scan->fs_file)
		open_external_readable_source(scan);

	/* Note: no locking manipulations needed */
	FILEDEBUG_1;

	tuple = externalgettup(scan, direction);

	if (tuple == NULL)
	{
		FILEDEBUG_2; /* external_getnext returning EOS */

		return NULL;
	}

	/*
	 * if we get here it means we have a new current scan tuple
	 */
	FILEDEBUG_3; /* external_getnext returning tuple */

	pgstat_count_heap_getnext(scan->fs_rd);

	return tuple;
}

/* ----------------
*		externalgettup	form another tuple from the data file.
*		This is the workhorse - make sure it's fast!
*
*		Initialize the scan if not already done.
*		Verify that we are scanning forward only.
*
* ----------------
*/
static HeapTuple
externalgettup(FileScanDesc scan,
			   ScanDirection dir __attribute__((unused)))
{
	ExtProtocolDesc *ext = scan->fs_file;
	HeapTuple tup = NULL;
	ErrorContextCallback externalscan_error_context;

	Assert(ScanDirectionIsForward(dir));

	externalscan_error_context.callback = external_scan_error_callback;
	externalscan_error_context.arg = (void *)RelationGetRelationName(scan->fs_rd);
	externalscan_error_context.previous = error_context_stack;

	error_context_stack = &externalscan_error_context;

	if (!scan->fs_inited)
	{
		/* more init stuff here... */
		scan->fs_inited = true;
	}
	else
	{
		/* continue from previously returned tuple */
		/* (set current state...) */
	}

	tup = InvokeExtProtocol(ext, false);

	/* Restore the previous error callback */
	error_context_stack = externalscan_error_context.previous;

	return tup;
}

static char *
get_proto_name(char *url)
{
	int i = 0;
	char c = 0;
	while ((c = url[i]))
	{
		if (c == ':')
			break;
		i++;
	}

	char *prot = palloc(i + 1);
	strncpy(prot, url, i);
	prot[i] = 0;
	return prot;
}

static void
open_external_readable_source(FileScanDesc scan)
{
	char *prot_name;
	Oid procOid;
	MemoryContext oldcontext;
	ExtProtocolDesc *ext;

	ext = palloc0(sizeof(ExtProtocolDesc));
	ext->rel = scan->fs_rd;
	ext->url = scan->fs_uri;

	initStringInfo(&ext->data_buffer);

	prot_name = get_proto_name(scan->fs_uri);
	procOid = LookupExtProtocolFunction(prot_name, EXTPTC_FUNC_READER, true);
	pfree(prot_name);
	ext->protcxt = AllocSetContextCreate(TopTransactionContext,
										  "CustomProtocolMemCxt",
										  ALLOCSET_DEFAULT_MINSIZE,
										  ALLOCSET_DEFAULT_INITSIZE,
										  ALLOCSET_DEFAULT_MAXSIZE);

	oldcontext = MemoryContextSwitchTo(ext->protcxt);
		
	ext->protocol_udf = palloc(sizeof(FmgrInfo));
	ext->extprotocol = (ExtProtocolData *) palloc0 (sizeof(ExtProtocolData));
	ext->extprotocol->conv_funcs = scan->in_functions;
	ext->extprotocol->typioparams = scan->typioparams;

	/* we found our function. set it in custom file handler */
	fmgr_info(procOid, ext->protocol_udf);

	MemoryContextSwitchTo(oldcontext);
	scan->fs_file = ext;
}

static void
close_external_readable_source(FileScanDesc scan)
{
	ExtProtocolDesc *ext = (ExtProtocolDesc *)scan->fs_file;
		/* last call. let the user close custom resources */
	if (ext->protocol_udf)
		(void) InvokeExtProtocol(ext, true);

	/* now clean up everything not cleaned by user */
	MemoryContextDelete(ext->protcxt);

	pfree(ext);
}

/*
 * error context callback for external table scan
 */
static void
external_scan_error_callback(void *arg)
{
	char* cur_relname = (char*)arg;

	/*
	 * early exit for custom format error. We don't have metadata to report
	 * on. TODO: this actually will override any errcontext that the user
	 * wants to set. maybe another approach is needed here.
	 */

	errcontext("External table %s", cur_relname);
	return;
}

static HeapTuple
InvokeExtProtocol(ExtProtocolDesc *file, bool last_call)
{
	FunctionCallInfoData	fcinfo;
	ExtProtocolData *extprotocol = file->extprotocol;
	FmgrInfo	   *extprotocol_udf = file->protocol_udf;
	MemoryContext			oldcontext;
	HeapTuple tup;

	/* must have been created during url_fopen() */
	Assert(extprotocol);
	
	extprotocol->type = T_ExtProtocolData;
	extprotocol->prot_url = file->url;
	extprotocol->prot_relation = (last_call ? NULL : file->rel);
	extprotocol->prot_last_call = last_call;
	extprotocol->prot_linebuf  = &file->data_buffer;
	
	InitFunctionCallInfoData(/* FunctionCallInfoData */ fcinfo,
							 /* FmgrInfo */ extprotocol_udf,
							 /* nArgs */ 0,
							 /* Call Context */ (Node *) extprotocol,
							 /* ResultSetInfo */ NULL);
	
	/* invoke the protocol within a designated memory context */
	oldcontext = MemoryContextSwitchTo(file->protcxt);
	tup = FunctionCallInvoke(&fcinfo);
	MemoryContextSwitchTo(oldcontext);

	/* We do not expect a null result */
	if (fcinfo.isnull)
		elog(ERROR, "function %u returned NULL", fcinfo.flinfo->fn_oid);

	return tup;
}


/*
 * external_insert_init
 *
 * before using external_insert() to insert tuples we need to call
 * this function to initialize our various structures and state..
 */
ExternalInsertDesc
external_insert_init(Relation rel)
{
	elog(ERROR, "Not implemented");
	return NULL;
}


/*
 * external_insert - format the tuple into text and write to the external source
 *
 * Note the following major differences from heap_insert
 *
 * - wal is always bypassed here.
 * - transaction information is of no interest.
 * - tuples are sent always to the destination (local file or remote target).
 *
 * Like heap_insert(), this function can modify the input tuple!
 */
Oid
external_insert(ExternalInsertDesc extInsertDesc, HeapTuple instup)
{
	return HeapTupleGetOid(instup);
}

/*
 * external_insert_finish
 *
 * when done inserting all the data via external_insert() we need to call
 * this function to flush all remaining data in the buffer into the file.
 */
void
external_insert_finish(ExternalInsertDesc extInsertDesc)
{

}


void
gfile_printf_then_putc_newline(const char *format,...)
{
	char	   *a;
	va_list		va;
	int			i;

	va_start(va, format);
	i = vsnprintf(0, 0, format, va);
	va_end(va);

	if (i < 0)
		elog(NOTICE, "gfile_printf_then_putc_newline vsnprintf failed.");
	else if (!(a = palloc(i + 1)))
		elog(NOTICE, "gfile_printf_then_putc_newline palloc failed.");
	else
	{
		va_start(va, format);
		vsnprintf(a, i + 1, format, va);
		va_end(va);
		elog(NOTICE, "%s", a);
		pfree(a);
	}
}

void *
gfile_malloc(size_t size)
{
	return palloc(size);
}

void
gfile_free(void *a)
{
	pfree(a);
}

char *
linenumber_atoi(char buffer[20], int64 linenumber)
{
	if (linenumber < 0)
		return "N/A";

	snprintf(buffer, 20, INT64_FORMAT, linenumber);

	return buffer;
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

static void
external_set_env_vars_ext(extvar_t *extvar, char *uri, bool csv, char *escape, char *quote, int eol_type, bool header,
						  uint32 scancounter, List *params)
{
	time_t		now = time(0);
	struct tm  *tm = localtime(&now);
	char	   *result = (char *) palloc(7);	/* sign, 5 digits, '\0' */

	char	   *encoded_delim;
	int			line_delim_len;

	sprintf(extvar->GP_CSVOPT,
			"m%dx%dq%dn%dh%d",
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
		CdbComponentDatabases *cdb_component_dbs = getCdbComponentDatabases();
		CdbComponentDatabaseInfo *qdinfo = &cdb_component_dbs->entry_db_info[0];

		pg_ltoa(qdinfo->port, result);
		extvar->GP_MASTER_PORT = result;

		if (qdinfo->hostip != NULL)
			extvar->GP_MASTER_HOST = pstrdup(qdinfo->hostip);
		else
			extvar->GP_MASTER_HOST = pstrdup(qdinfo->hostname);

		freeCdbComponentDatabases(cdb_component_dbs);
	}

	if (MyProcPort)
		extvar->GP_USER = MyProcPort->user_name;
	else
		extvar->GP_USER = "";

	extvar->GP_DATABASE = get_database_name(MyDatabaseId);
	extvar->GP_SEG_PG_CONF = ConfigFileName;	/* location of the segments
												 * pg_conf file  */
	extvar->GP_SEG_DATADIR = data_directory;	/* location of the segments
												 * datadirectory */
	sprintf(extvar->GP_DATE, "%04d%02d%02d",
			1900 + tm->tm_year, 1 + tm->tm_mon, tm->tm_mday);
	sprintf(extvar->GP_TIME, "%02d%02d%02d",
			tm->tm_hour, tm->tm_min, tm->tm_sec);
	if (!getDistributedTransactionIdentifier(extvar->GP_XID))
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("cannot get distributed transaction identifier while %s", uri)));

	sprintf(extvar->GP_CID, "%x", QEDtxContextInfo.curcid);
	sprintf(extvar->GP_SN, "%x", scancounter);
	sprintf(extvar->GP_SEGMENT_ID, "%d", Gp_segment);
	sprintf(extvar->GP_SEG_PORT, "%d", PostPortNumber);
	sprintf(extvar->GP_SESSION_ID, "%d", gp_session_id);
	sprintf(extvar->GP_SEGMENT_COUNT, "%d", GpIdentity.numsegments);

	/*
	 * Hadoop Connector env var
	 *
	 * Those has to be set into the env because the gphdfs env setup script
	 * (hadoop_env.sh) relies on those to set the classpath to the connector
	 * jar as well as the Hadoop jar.
	 *
	 * Setting these var here (instead of inside gphdfs protocol) allows
	 * ordinary "execute" external table to run hadoop connector jar for other
	 * purposes.
	 */
	extvar->GP_HADOOP_CONN_JARDIR = gp_hadoop_connector_jardir;
	extvar->GP_HADOOP_CONN_VERSION = gp_hadoop_connector_version;
	extvar->GP_HADOOP_HOME = gp_hadoop_home;

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
			case EOL_LF:
				encoded_delim = "0A";
				line_delim_len = 1;
				break;
			case EOL_CRLF:
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

void
external_set_env_vars(extvar_t *extvar, char *uri, bool csv, char *escape, char *quote, bool header, uint32 scancounter)
{
	external_set_env_vars_ext(extvar, uri, csv, escape, quote, EOL_UNKNOWN, header, scancounter, NULL);
}

int readable_external_table_timeout = 0;