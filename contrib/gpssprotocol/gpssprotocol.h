#include <grpc/grpc.h>
#include <grpcpp/channel.h>
#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>
#include "stream.grpc.pb.h"

using gpss::StreamMessages;
using gpss::GreenplumStreamServer;
using std::unique_ptr;
using grpc::Status;
using grpc::ClientContext;
using grpc::ClientReader;

struct worker_context
{	
	int message_index;
	int message_offset;
	StreamMessages messages;

	unique_ptr<GreenplumStreamServer::Stub> stub;
	ClientContext ctx;
	unique_ptr<ClientReader<StreamMessages>> reader;
	Status status;

    bool init_stream_worker(const char* address);
    int next_message(char* buffer, int len);
};
