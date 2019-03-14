extern "C" 
{
#include "postgres.h"
#include "fmgr.h"
#include "funcapi.h"

#include "access/extprotocol.h"
#include "catalog/pg_proc.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/memutils.h"

#include "catalog/pg_exttable.h"

#include "cdb/cdbtm.h"
#include "cdb/cdbutil.h"
#include "cdb/cdbvars.h"


/* Do the module magic dance */
PG_MODULE_MAGIC;
PG_FUNCTION_INFO_V1(gpss_export);
PG_FUNCTION_INFO_V1(gpss_import);
PG_FUNCTION_INFO_V1(gpss_validate_urls);

Datum gpss_export(PG_FUNCTION_ARGS);
Datum gpss_import(PG_FUNCTION_ARGS);
Datum gpss_validate_urls(PG_FUNCTION_ARGS);
}

#include <grpc/grpc.h>
#include <grpcpp/channel.h>
#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>
#include "stream.grpc.pb.h"

using namespace std;
using namespace gpss;
using namespace grpc;


static char *ParseGpssUri(const char *uri_str);

struct worker_context
{	
	int message_index;
	int message_offset;
	StreamMessages messages;

	unique_ptr<GreenplumStreamServer::Stub> stub;
	ClientContext ctx;
	unique_ptr<ClientReader<StreamMessages>> reader;
	Status status;
};

worker_context* new_stream_worker()
{
	worker_context* ctx = new worker_context;
	ctx->message_index = 0;
	ctx->message_offset = 0;
	return ctx;
}

void delete_stream_worker(worker_context* ctx)
{
	delete ctx;
}

int init_stream_worker(worker_context* ctx, const char* address)
{
	auto channel = CreateChannel(address, InsecureChannelCredentials());
	if (!channel)
		return 0;

	ctx->stub = GreenplumStreamServer::NewStub(channel);
	if (!ctx->stub)
		return 0;

	ConsumeRequest req;
	char GP_XID[TMGIDSIZE];		/* global transaction id */

	if (!getDistributedTransactionIdentifier(GP_XID))
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("cannot get distributed transaction identifier")));

	req.set_xid(GP_XID);
	req.set_cid(QEDtxContextInfo.curcid);
	req.set_sessionid(gp_session_id);
	req.set_segmentid(GpIdentity.segindex);
	req.set_segmentcount(getgpsegmentCount());
	ctx->reader = ctx->stub->Consume(&ctx->ctx, req);
	if (!ctx->reader)
		return 0;

	return 1;
}

int next_message(worker_context* ctx, char* buffer, int len)
{
	while (ctx->message_index == ctx->messages.messages_size())
	{
		ctx->messages.clear_messages();
		ctx->message_index = 0;
		if (!ctx->reader->Read(&ctx->messages))
		{
			ctx->status = ctx->reader->Finish();
			return 0;
		}
	}
	
	const auto& m = ctx->messages.messages()[ctx->message_index];
	if (ctx->message_offset + len < m.size())
	{
		memcpy(buffer, m.c_str() + ctx->message_offset, len);
		ctx->message_offset += len;		
	}
	else
	{
		len = m.size() - ctx->message_offset;
		memcpy(buffer, m.c_str() + ctx->message_offset, len);
		ctx->message_index++;
		ctx->message_offset = 0;
	}
	return len;
}


const char protocol_name[] = "gpss";

/*
 * Import data into GPDB.
 */
Datum 
gpss_import(PG_FUNCTION_ARGS)
{
	worker_context  *myData;
	char			*data;
	int				 datlen;
	size_t			 nread = 0;

	/* Must be called via the external table format manager */
	if (!CALLED_AS_EXTPROTOCOL(fcinfo))
		elog(ERROR, "extprotocol_import: not called by external protocol manager");

	/* Get our internal description of the protocol */
	myData = (worker_context*) EXTPROTOCOL_GET_USER_CTX(fcinfo);

	if(EXTPROTOCOL_IS_LAST_CALL(fcinfo))
	{
		/* we're done receiving data. close our connection */
		if(myData)
			delete_stream_worker(myData);

		PG_RETURN_INT32(0);
	}

	if (myData == NULL)
	{
		/* first call. do any desired init */

		char		*address;
		char		*url = EXTPROTOCOL_GET_URL(fcinfo);

		myData 			 = new_stream_worker();
		address 		 = ParseGpssUri(url);
		int success = init_stream_worker(myData, address);
		if (success == 0)
		{
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("gpss_import: could not connect to gpss %s",
							address)));
		}
		pfree(address);
		
		EXTPROTOCOL_SET_USER_CTX(fcinfo, myData);
	}

	/* =======================================================================
	 *                            DO THE IMPORT
	 * ======================================================================= */
	
	data 	= EXTPROTOCOL_GET_DATABUF(fcinfo);
	datlen 	= EXTPROTOCOL_GET_DATALEN(fcinfo);

	if(datlen > 0)
	{
		nread = next_message(myData, data, datlen);
		if (nread == 0)
		{
			if (!myData->status.ok())
			{
				ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("gpss_import: failed to read message %s",
							myData->status.error_message().c_str())));
			}
		}
	}
	
	PG_RETURN_INT32((int)nread);
}

/*
 * Export data out of GPDB.
 */
Datum 
gpss_export(PG_FUNCTION_ARGS)
{
	elog(ERROR, "Unimplemented");
}

Datum 
gpss_validate_urls(PG_FUNCTION_ARGS)
{
	int					nurls;
	int					i;
	ValidatorDirection	direction;
	
	/* Must be called via the external table format manager */
	if (!CALLED_AS_EXTPROTOCOL_VALIDATOR(fcinfo))
		elog(ERROR, "gpss_validate_urls: not called by external protocol manager");

	nurls 		= EXTPROTOCOL_VALIDATOR_GET_NUM_URLS(fcinfo);
	direction 	= EXTPROTOCOL_VALIDATOR_GET_DIRECTION(fcinfo);

	if (nurls > getgpsegmentCount())
		elog(ERROR, "gpss_validate_urls: not called by external protocol manager");
	
	PG_RETURN_VOID();
}

/* --- utility functions --- */

static 
char *ParseGpssUri(const char *uri_str)
{
	int protocol_len;
	char* hostport;
	/*
	 * parse protocol
	 */
	const char *post_protocol = strstr(uri_str, "://");

	if(!post_protocol)
	{
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("invalid gpss protocol URI \'%s\'", uri_str)));
	}

	protocol_len = post_protocol - uri_str;
	if (strncmp(uri_str, protocol_name, sizeof(protocol_name) - 1) != 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("invalid gpss protocol URI \'%s\'", uri_str)));	
	}

	/* make sure there is more to the uri string */
	if (strlen(uri_str) <= protocol_len)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
		errmsg("invalid gpss protocol URI \'%s\' : missing domain", uri_str)));

	/*
	 * host, port
	 */
	const char* path = post_protocol + strlen("://");
	const char* slash = path;
	while(*slash && *slash != '/')
		slash++;

	int domain_length = slash - path;
	hostport = (char*)palloc(domain_length + 1);
	strncpy(hostport, path, domain_length);
	hostport[domain_length] = '\0';

	return hostport;
}
