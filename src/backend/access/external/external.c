/*-------------------------------------------------------------------------
 *
 * external.c
 *	  routines for getting external info from external table fdw.
 *
 * Portions Copyright (c) 2020-Present VMware, Inc. or its affiliates.
 *
 * IDENTIFICATION
 *	    src/backend/access/external/external.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <fstream/gfile.h>

#include "access/external.h"
#include "access/reloptions.h"
#include "access/table.h"
#include "catalog/indexing.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_foreign_table.h"
#include "cdb/cdbsreh.h"
#include "cdb/cdbvars.h"
#include "commands/defrem.h"
#include "foreign/foreign.h"
#include "mb/pg_wchar.h"
#include "nodes/makefuncs.h"
#include "optimizer/optimizer.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/uri.h"

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

/* transform the locations string to a list */
List*
TokenizeLocationUris(char *uris)
{
	char *uri = NULL;
	List *result = NIL;

	Assert(uris != NULL);

	while ((uri = strsep(&uris, "|")) != NULL)
	{
		result = lappend(result, makeString(uri));
	}

	return result;
}

/*
 * Get the entry for an exttable relation (from pg_foreign_table)
 */
ExtTableEntry*
GetExtTableEntry(Oid relid)
{
	ExtTableEntry *extentry;

	extentry = GetExtTableEntryIfExists(relid);
	if (!extentry)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("missing pg_foreign_table entry for relation \"%s\"",
						get_rel_name(relid))));
	return extentry;
}

/*
 * Like GetExtTableEntry(Oid), but returns NULL instead of throwing
 * an error if no pg_foreign_table entry is found.
 */
ExtTableEntry*
GetExtTableEntryIfExists(Oid relid)
{
	Relation	pg_foreign_table_rel;
	ScanKeyData ftkey;
	SysScanDesc ftscan;
	HeapTuple	fttuple;
	ExtTableEntry *extentry;
	bool		isNull;
	List		*ftoptions_list = NIL;;

	pg_foreign_table_rel = table_open(ForeignTableRelationId, RowExclusiveLock);

	ScanKeyInit(&ftkey,
				Anum_pg_foreign_table_ftrelid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(relid));

	ftscan = systable_beginscan(pg_foreign_table_rel, ForeignTableRelidIndexId,
								true, NULL, 1, &ftkey);
	fttuple = systable_getnext(ftscan);

	if (!HeapTupleIsValid(fttuple))
	{
		systable_endscan(ftscan);
		heap_close(pg_foreign_table_rel, RowExclusiveLock);

		return NULL;
	}

	/* get the foreign table options */
	Datum ftoptions = heap_getattr(fttuple,
						   Anum_pg_foreign_table_ftoptions,
						   RelationGetDescr(pg_foreign_table_rel),
						   &isNull);

	if (isNull)
	{
		/* options array is always populated, {} if no options set */
		elog(ERROR, "could not find options for external protocol");
	}
	else
	{
		ftoptions_list = untransformRelOptions(ftoptions);
	}

	extentry = GetExtFromForeignTableOptions(ftoptions_list, relid);

	/* Finish up scan and close catalogs */
	systable_endscan(ftscan);
	table_close(pg_foreign_table_rel, RowExclusiveLock);

	return extentry;
}

ExtTableEntry *
GetExtFromForeignTableOptions(List *ftoptons, Oid relid)
{
	ExtTableEntry	   *extentry;
	ListCell		   *lc;
	List			   *entryOptions = NIL;
	char			   *arg;
	bool				fmtcode_found = false;
	bool				rejectlimit_found = false;
	bool				rejectlimittype_found = false;
	bool				logerrors_found = false;
	bool				encoding_found = false;
	bool				iswritable_found = false;
	bool				locationuris_found = false;
	bool				command_found = false;

	extentry = (ExtTableEntry *) palloc0(sizeof(ExtTableEntry));

	foreach(lc, ftoptons)
	{
		DefElem    *def = (DefElem *) lfirst(lc);

		if (pg_strcasecmp(def->defname, "location_uris") == 0)
		{
			extentry->urilocations = TokenizeLocationUris(defGetString(def));
			locationuris_found = true;
			continue;
		}

		if (pg_strcasecmp(def->defname, "execute_on") == 0)
		{
			extentry->execlocations = list_make1(makeString(defGetString(def)));
			continue;
		}

		if (pg_strcasecmp(def->defname, "command") == 0)
		{
			extentry->command = defGetString(def);
			command_found = true;
			continue;
		}

		if (pg_strcasecmp(def->defname, "format_type") == 0)
		{
			arg = defGetString(def);
			extentry->fmtcode = arg[0];
			fmtcode_found = true;
			continue;
		}

		/* only CSV format needs this for ProcessCopyOptions(), will do it later */
		if (pg_strcasecmp(def->defname, "format") == 0)
		{
			continue;
		}

		if (pg_strcasecmp(def->defname, "reject_limit") == 0)
		{
			extentry->rejectlimit = atoi(defGetString(def));
			rejectlimit_found = true;
			continue;
		}

		if (pg_strcasecmp(def->defname, "reject_limit_type") == 0)
		{
			arg = defGetString(def);
			extentry->rejectlimittype = arg[0];
			rejectlimittype_found = true;
			continue;
		}

		if (pg_strcasecmp(def->defname, "log_errors") == 0)
		{
			arg = defGetString(def);
			extentry->logerrors = arg[0];
			logerrors_found = true;
			continue;
		}

		if (pg_strcasecmp(def->defname, "encoding") == 0)
		{
			extentry->encoding = atoi(defGetString(def));
			encoding_found = true;
			continue;
		}

		if (pg_strcasecmp(def->defname, "is_writable") == 0)
		{
			extentry->iswritable = defGetBoolean(def);
			iswritable_found = true;
			continue;
		}

		entryOptions = lappend(entryOptions, makeDefElem(def->defname, (Node *) makeString(pstrdup(defGetString(def))), -1));
	}

	/* If CSV format was chosen, make it visible to ProcessCopyOptions. */
	if (fmttype_is_csv(extentry->fmtcode))
		entryOptions = lappend(entryOptions, makeDefElem("format", (Node *) makeString("csv"), -1));

	/*
	 * external table syntax does have these for sure, but errors could happen
	 * if using foreign table syntax
	 */
	if (!fmtcode_found || !logerrors_found || !encoding_found || !iswritable_found)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("missing format, logerrors, encoding or iswritable options for relation \"%s\"",
						get_rel_name(relid))));

	if (locationuris_found && command_found)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("locationuris and command options conflict with each other")));

	if (!fmttype_is_custom(extentry->fmtcode) &&
		!fmttype_is_csv(extentry->fmtcode) &&
		!fmttype_is_text(extentry->fmtcode))
		elog(ERROR, "unsupported format type %d for external table", extentry->fmtcode);

	if (!rejectlimit_found) {
		/* mark that no SREH requested */
		extentry->rejectlimit = -1;
	}

	if (rejectlimittype_found)
	{
		if (extentry->rejectlimittype != 'r' && extentry->rejectlimittype != 'p')
			elog(ERROR, "unsupported reject limit type %c for external table",
				 extentry->rejectlimittype);
	}
	else
		extentry->rejectlimittype = -1;

	if (!PG_VALID_ENCODING(extentry->encoding))
		elog(ERROR, "invalid encoding found for external table");

	extentry->options = entryOptions;

	return extentry;
}
