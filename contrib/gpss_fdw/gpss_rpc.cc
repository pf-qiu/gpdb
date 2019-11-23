
#include <grpcpp/grpcpp.h>
#include "gpss.grpc.pb.h"
#include "gpss_rpc.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using namespace gpssfdw;

struct GpssRpc
{
    std::unique_ptr<GpssFdw::Stub> stub;
    std::unique_ptr<grpc::ClientReader<StreamDataResponse>> stream;
    std::unique_ptr<grpc::ClientContext> ctx;
};

void *create_gpss_stub(const char *address)
{
    auto ch = grpc::CreateChannel(address, grpc::InsecureChannelCredentials());
    if (!ch)
        return nullptr;

    GpssRpc *rpc = new GpssRpc;
    rpc->stub = gpssfdw::GpssFdw::NewStub(ch);

    return rpc;
}

void delete_gpss_stub(void *p)
{
    if (p)
    {
        GpssRpc *rpc = (GpssRpc *)p;
        delete rpc;
    }
}

int64 gpssfdw_estimate_size(void *p, const char *id)
{
    GpssRpc *rpc = (GpssRpc *)p;
    grpc::ClientContext ctx;
    EstimateSizeRequest req;
    EstimateSizeResponse res;
    Status s = rpc->stub->EstimateSize(&ctx, req, &res);
    if (s.ok())
    {
        return res.estimate_size();
    }
    else
    {
        return 0;
    }
}

bool gpssfdw_stream_data(void *p, const char *id, int segid, StringInfo str)
{
    GpssRpc *rpc = (GpssRpc *)p;
    if (!rpc->stream)
    {
        StreamDataRequest req;
        req.set_id(id);
        req.set_segid(segid);
        
        rpc->ctx.reset(new grpc::ClientContext());
        rpc->stream = rpc->stub->StreamData(rpc->ctx.get(), req);
    }

    StreamDataResponse res;
    if (rpc->stream->Read(&res))
    {
        appendBinaryStringInfo(str, res.msg().data(), res.msg().size());
        return true;
    }
    else
    {
        rpc->stream->Finish();
        rpc->stream.release();
        rpc->ctx.release();
        return false;
    }
}