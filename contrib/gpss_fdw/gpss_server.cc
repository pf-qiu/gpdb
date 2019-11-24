#include <librdkafka/rdkafka.h>
#include <map>
#include <string>
#include <grpcpp/grpcpp.h>
#include "gpss.grpc.pb.h"
#include "gpss_server_utils.h"

using namespace gpssfdw;

using grpc::Status;
using grpc::StatusCode;

char json1[] = "{\"a\":1, \"b\": \"msg1\"}";
char json2[] = "{\"a\":2, \"b\": \"msg2\"}";

struct KafkaBroker
{
    KafkaBroker(rd_kafka_metadata_broker *broker)
    {
        host = broker->host;
        port = broker->port;
        id = broker->id;
    }
    std::string host;
    int id;
    int port;
};

struct KafkaPartition
{
    KafkaPartition(rd_kafka_metadata_partition *partition)
    {
        id = partition->id;
        leader = partition->leader;
        isrs.reserve(partition->isr_cnt);
        for (int i = 0; i < partition->isr_cnt; i++)
        {
            isrs.emplace_back(partition->isrs[i]);
        }
        replicas.reserve(partition->replica_cnt);
        for (int i = 0; i < partition->replica_cnt; i++)
        {
            replicas.emplace_back(partition->replicas[i]);
        }
    }
    int id;
    int leader;
    std::vector<int> isrs;
    std::vector<int> replicas;
};

struct KafkaConsumer
{
    KafkaConsumer() : rk(nullptr), rkt(nullptr) {}
    ~KafkaConsumer()
    {
        if (rkt)
        {
            rd_kafka_topic_destroy(rkt);
            rkt = nullptr;
        }

        if (rk)
        {
            rd_kafka_destroy(rk);
            rk = nullptr;
        }
    }
    KafkaConsumer(const KafkaConsumer&) = delete;
    KafkaConsumer(KafkaConsumer&&) = delete;
    rd_kafka_t *rk;
    rd_kafka_topic_t *rkt;
    std::vector<KafkaBroker> brokers;
    std::vector<KafkaPartition> partitions;
};

class RandomID
{
public:
    RandomID()
    {
        GenerateID(data, sizeof(data));
    }
    RandomID(const RandomID& id)
    {
        memcpy(data, id.data, sizeof(data));
    }
    std::string String()
    {
        std::string id;
        id.reserve(sizeof(data) * 2 + 1);
        for (unsigned char c : data)
        {
            id.push_back(hextable[c >> 4]);
            id.push_back(hextable[c & 0xF]);
        }
        return id;
    }
private:
    unsigned char data[16];
    const char hextable[16] =
    {
        '0', '1', '2', '3', '4', '5', '6', '7',
        '8', '9', 'a', 'b', 'c', 'd', 'e', 'f',
    };
};

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
        char errstr[0x100];
        rd_kafka_t *rk = rd_kafka_new(RD_KAFKA_CONSUMER, nullptr, errstr, sizeof(errstr));
        if (rk == nullptr)
        {
            return Status(StatusCode::INTERNAL, errstr);
        }

        rd_kafka_brokers_add(rk, request->brokers().c_str());
        rd_kafka_topic_t *rkt = rd_kafka_topic_new(rk, request->topic().c_str(), nullptr);

        std::unique_ptr<KafkaConsumer> c(new KafkaConsumer());
        c->rk = rk;
        c->rkt = rkt;
        const struct rd_kafka_metadata *meta;
        rd_kafka_resp_err_t err = rd_kafka_metadata(rk, false, rkt, &meta, 1000);
        if (err != RD_KAFKA_RESP_ERR_NO_ERROR)
        {
            return Status(StatusCode::NOT_FOUND, rd_kafka_err2str(err));
        }

        c->brokers.reserve(meta->broker_cnt);
        for (int i = 0; i < meta->broker_cnt; i++)
        {
            c->brokers.emplace_back(meta->brokers + i);
        }

        c->partitions.reserve(meta->topics->partition_cnt);
        for (int i = 0; i < meta->topics->partition_cnt; i++)
        {
            c->partitions.emplace_back(meta->topics->partitions + i);
        }

        rd_kafka_metadata_destroy(meta);

        RandomID id;
        std::string idstr = id.String();
        consumers[idstr] = std::move(c);
        response->set_id(std::move(idstr));
        return Status::OK;
    }
    Status StopKafkaStream(grpc::ServerContext *context, const StopKafkaStreamRequest *request, StopKafkaStreamResponse *response)
    {
        return Status::OK;
    }
    Status StreamData(grpc::ServerContext *context, const StreamDataRequest *request, grpc::ServerWriter<StreamDataResponse> *writer)
    {
        StreamDataResponse res;

        res.set_msg(json1);
        writer->Write(res);
        res.set_msg(json2);
        writer->WriteLast(res, grpc::WriteOptions());
        return Status::OK;
    }

private:
    std::map<std::string, std::unique_ptr<KafkaConsumer>> consumers;
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
    grpc::ServerBuilder builder;
    builder.AddListeningPort("0.0.0.0:5000", grpc::InsecureServerCredentials());
    builder.RegisterService(&gpss);

    std::unique_ptr<grpc::Server> server = builder.BuildAndStart();
    server->Wait();
    return 0;
}