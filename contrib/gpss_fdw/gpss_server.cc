
#include <grpcpp/grpcpp.h>
#include "gpss.grpc.pb.h"
#include "gpss_server_utils.h"
#include "gpss_server_kafka.h"

using namespace gpssfdw;

using grpc::Status;
using grpc::StatusCode;

char json1[] = "{\"a\":1, \"b\": \"msg1\"}";
char json2[] = "{\"a\":2, \"b\": \"msg2\"}";

// Logic and data behind the server's behavior.
class GpssFdwImpl : public GpssFdw::Service
{
public:
    Status EstimateSize(grpc::ServerContext *context, const EstimateSizeRequest *request, EstimateSizeResponse *response)
    {
        response->set_estimate_size(1024);
        return Status::OK;
    }
    Status StartKafkaStream(grpc::ServerContext *context, const StartKafkaStreamRequest *request, StartKafkaStreamResponse *response)
    {
        std::string errmsg;
        auto c = KafkaConsumer::NewConsumer(request->brokers(), request->topic(), errmsg);
        if (c)
        {
            RandomID id;
            std::string idstr = id.String();
            consumers[idstr] = std::move(c);
            response->set_id(std::move(idstr));
            c->StartConsume();
            return Status::OK;
        }
        else
        {
            return Status(StatusCode::INVALID_ARGUMENT, errmsg);
        }
    }
    Status StopKafkaStream(grpc::ServerContext *context, const StopKafkaStreamRequest *request, StopKafkaStreamResponse *response)
    {
        auto it = consumers.find(request->id());
        if (it != consumers.end())
        {
            it->second->StopConsume();
            consumers.erase(it);
        }
        return Status::OK;
    }
    Status StreamData(grpc::ServerContext *context, const StreamDataRequest *request, grpc::ServerWriter<StreamDataResponse> *writer)
    {
        auto it = consumers.find(request->id());
        if (it == consumers.end())
        {
            return Status(StatusCode::NOT_FOUND, "invalid id");
        }

        it->second->Consume([](KafkaMessage& km, void* p)
        {
            grpc::ServerWriter<StreamDataResponse>* writer;
            writer = (grpc::ServerWriter<StreamDataResponse>*)p;
            StreamDataResponse res;
            res.set_msg(km.value);
            writer->Write(res);
        }, writer);

        return Status::OK;
    }

private:
    std::map<std::string, KafkaConsumer::KafkaConsumerHandle> consumers;
};

int main()
{
    GpssFdwImpl gpss;
    grpc::ServerBuilder builder;
    builder.AddListeningPort("0.0.0.0:5000", grpc::InsecureServerCredentials());
    builder.RegisterService(&gpss);

    std::unique_ptr<grpc::Server> server = builder.BuildAndStart();
    server->Wait();
    return 0;
}