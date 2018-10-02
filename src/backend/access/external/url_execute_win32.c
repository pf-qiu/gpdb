/*-------------------------------------------------------------------------
 *
 * url_execute.c
 *	  Core support for opening external relations via a URL execute
 *
 * Portions Copyright (c) 2007-2008, Greenplum inc
 * Portions Copyright (c) 2012-Present Pivotal Software, Inc.
 *
 * IDENTIFICATION
 *	  src/backend/access/external/url_execute.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include "access/fileam.h"
#include "cdb/cdbtimer.h"
#include "cdb/cdbvars.h"
#include "libpq/pqsignal.h"
#include "utils/resowner.h"

#define EXEC_DATA_P 0 /* index to data pipe */
#define EXEC_ERR_P 1 /* index to error pipe  */

/*
 * This struct encapsulates the resources that need to be explicitly cleaned up
 * on error. We use the resource owner mechanism to make sure
 * these are not leaked. When a ResourceOwner is released, our hook will
 * walk the list of open curlhandles, and releases any that were owned by
 * the released resource owner.
 *
 * On abort, we need to close the pipe FDs, and wait for the subprocess to
 * exit.
 */
typedef struct execute_handle_t
{
	/*
	 * PID of the open sub-process, and pipe FDs to communicate with it.
	 */
	int			pid;
	int			pipes[2];		/* only out and err needed */

	ResourceOwner owner;	/* owner of this handle */
	struct execute_handle_t *next;
	struct execute_handle_t *prev;
} execute_handle_t;

/*
 * Private state for an EXECUTE external table.
 */
typedef struct URL_EXECUTE_FILE
{
	URL_FILE	common;

	char	   *shexec;			/* shell command-line */

	execute_handle_t *handle;	/* ResourceOwner-tracked stuff */
} URL_EXECUTE_FILE;

/**
 * execute_fopen()
 *
 * refactor the fopen code for execute into this routine
 */
URL_FILE *
url_execute_fopen(char *url, bool forwrite, extvar_t *ev, CopyState pstate)
{
	elog(ERROR, "Not implemented on Win32");
}

void
url_execute_fclose(URL_FILE *file, bool failOnError, const char *relname)
{
	elog(ERROR, "Not implemented on Win32");
}

bool
url_execute_feof(URL_FILE *file, int bytesread)
{
	return (bytesread == 0);
}

bool
url_execute_ferror(URL_FILE *file, int bytesread, char *ebuf, int ebuflen)
{
	elog(ERROR, "Not implemented on Win32");
}

size_t
url_execute_fread(void *ptr, size_t size, URL_FILE *file, CopyState pstate)
{
	elog(ERROR, "Not implemented on Win32");
}

size_t
url_execute_fwrite(void *ptr, size_t size, URL_FILE *file, CopyState pstate)
{
	elog(ERROR, "Not implemented on Win32");
}
