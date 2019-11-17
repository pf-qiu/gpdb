
#include <grpcpp/grpcpp.h>
#include "gpss.grpc.pb.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using namespace gpssfdw;

struct GpssRpc {
    std::unique_ptr<GpssFdw::Stub> stub;

};

void* create_gpss_stub(const char* address)
{
	auto ch = grpc::CreateChannel(address, grpc::InsecureChannelCredentials());
    if (!ch)
        return nullptr;

    GpssRpc *rpc = new GpssRpc;
    rpc->stub = gpssfdw::GpssFdw::NewStub(ch);

    return rpc;
}

void delete_gpss_stub(void* p)
{
    GpssRpc* rpc = (GpssRpc*)p;
    delete rpc;
}

int64_t gpssfdw_estimate_size(void* p, const char* id)
{
    GpssRpc* rpc = (GpssRpc*)p;
    grpc::ClientContext ctx;
    EstimateSizeRequest req;
    EstimateSizeResponse res;
    Status s = rpc->stub->EstimateSize(&ctx, req, &res);
    if (s.ok())
    {
        return res.bytes();
    }
    else
    {
        return 0;
    }
}

int64_t gpssfdw_stream_data(void* p, const char* id)
{
    GpssRpc* rpc = (GpssRpc*)p;
    grpc::ClientContext ctx;
    StreamDataRequest req;
    StreamDataResponse res;
    auto h = rpc->stub->StreamData(&ctx, req);
    if (h->Read(&res))
    {
        
    }
}