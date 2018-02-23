/*-------------------------------------------------------------------------
*
* fileam.h
*	  external file access method definitions.
*
* Portions Copyright (c) 2007-2008, Greenplum inc
 * Portions Copyright (c) 2012-Present Pivotal Software, Inc.
 *
 *
 * IDENTIFICATION
 *	    src/include/access/fileam.h
*
*-------------------------------------------------------------------------
*/
#ifndef FILEAM_H
#define FILEAM_H

#include "access/url.h"
#include "access/formatter.h"
#include "access/relscan.h"
#include "access/sdir.h"
#include "access/extprotocol.h"
#include "utils/rel.h"

typedef struct ExtProtocolDesc
{
	Relation rel;
	char *url;
	FmgrInfo   *protocol_udf;
	ExtProtocol extprotocol;
	MemoryContext protcxt;
	StringInfoData data_buffer;
} ExtProtocolDesc;

/*
 * ExternalInsertDescData is used for storing state related
 * to inserting data into a writable external table.
 */
typedef struct ExternalInsertDescData
{
	Relation	ext_rel;
	ExtProtocolDesc   *ext_data;
	char	   *ext_uri;		/* "command:<cmd>" or "tablespace:<path>" */
	bool		ext_noop;		/* no op. this segdb needs to do nothing (e.g.
								 * mirror seg) */

	TupleDesc	ext_tupDesc;
	Datum	   *ext_values;
	bool	   *ext_nulls;

	FormatterData *ext_formatter_data;

	struct CopyStateData *ext_pstate;	/* data parser control chars and state */

} ExternalInsertDescData;

typedef ExternalInsertDescData *ExternalInsertDesc;

typedef enum DataLineStatus
{
	LINE_OK,
	LINE_ERROR,
	NEED_MORE_DATA,
	END_MARKER
} DataLineStatus;

extern FileScanDesc external_beginscan(Relation relation,
				   uint32 scancounter, List *uriList,
				   List *fmtOpts, char fmtType, bool isMasterOnly,
				   int rejLimit, bool rejLimitInRows,
				   Oid fmterrtbl, int encoding);
extern void external_rescan(FileScanDesc scan);
extern void external_endscan(FileScanDesc scan);
extern void external_stopscan(FileScanDesc scan);
extern HeapTuple external_getnext(FileScanDesc scan, ScanDirection direction);

#endif   /* FILEAM_H */
