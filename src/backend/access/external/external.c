/*-------------------------------------------------------------------------
 *
 * external.c
 *	  routines for getting external info from external table fdw.
 *
 * Portions Copyright (c) 2020-Present Pivotal Software, Inc.
 *
 * IDENTIFICATION
 *	    src/backend/access/external/external.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "nodes/pg_list.h"
#include "nodes/value.h"

#include <fstream/gfile.h>


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