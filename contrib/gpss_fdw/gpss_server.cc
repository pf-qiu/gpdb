#include "gpss.grpc.pb.h"
#include <grpcpp/grpcpp.h>
#include <librdkafka/rdkafka.h>
using namespace gpssfdw;
using namespace grpc;

char json1[] = "{\"a\":1, \"b\": \"msg1\"}";
char json2[] = "{\"a\":2, \"b\": \"msg2\"}";

// Logic and data behind the server's behavior.
class GpssFdwImpl final : public GpssFdw::Service {
    Status EstimateSize(ServerContext* context, const EstimateSizeRequest* request, EstimateSizeResponse* response)
    {
        response->set_estimate_size(1024);
        return Status::OK;
    }
    Status StreamData(ServerContext* context, const StreamDataRequest* request, ServerWriter< StreamDataResponse>* writer)
    {
        StreamDataResponse res;
        
        res.set_msg(json1);
        writer->Write(res);
        res.set_msg(json2);
        writer->WriteLast(res, WriteOptions());
        return Status::OK;
    }
};

int main()
{
    char errstr[0x100];

    rd_kafka_conf_t *conf = rd_kafka_conf_new();
    rd_kafka_t *rk = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr));
    rd_kafka_brokers_add(rk, "127.0.0.1:9092");
    rd_kafka_topic_t *topic = rd_kafka_topic_new(rk, "test", 0);
    rd_kafka_consume_start(topic, 0, 0);

    GpssFdwImpl gpss;
    ServerBuilder builder;
    builder.AddListeningPort("0.0.0.0:5000",InsecureServerCredentials());
    builder.RegisterService(&gpss);

    std::unique_ptr<Server> server = builder.BuildAndStart();
    server->Wait();
    return 0;
}