#include "gpss_server_kafka.h"
#include <librdkafka/rdkafka.h>

KafkaBroker NewKafkaBroker(rd_kafka_metadata_broker *broker)
{
    return KafkaBroker{broker->id, broker->port, broker->host};
}

KafkaPartition NewKafkaPartition(rd_kafka_metadata_partition *partition)
{
    int id = partition->id;
    int leader = partition->leader;
    std::vector<int> isrs;
    isrs.reserve(partition->isr_cnt);
    for (int i = 0; i < partition->isr_cnt; i++)
    {
        isrs.emplace_back(partition->isrs[i]);
    }
    std::vector<int> replicas;
    replicas.reserve(partition->replica_cnt);
    for (int i = 0; i < partition->replica_cnt; i++)
    {
        replicas.emplace_back(partition->replicas[i]);
    }
    return KafkaPartition{id, leader, isrs, replicas};
}

bool KafkaMessage::IsEOF()
{
    return err == RD_KAFKA_RESP_ERR__PARTITION_EOF;
}

std::string KafkaMessage::Error()
{
    if (err == RD_KAFKA_RESP_ERR_NO_ERROR)
    {
        return std::string();
    }
    return rd_kafka_err2str((rd_kafka_resp_err_t)err);
}

KafkaConsumer::KafkaConsumer() : rk(nullptr), rkt(nullptr) {}
KafkaConsumer::~KafkaConsumer()
{
    if (rkt)
    {
        rd_kafka_topic_destroy((rd_kafka_topic_t*)rkt);
        rkt = nullptr;
    }

    if (rk)
    {
        rd_kafka_destroy((rd_kafka_t*)rk);
        rk = nullptr;
    }
}

void KafkaConsumer::StartConsume()
{
}

void KafkaConsumer::StopConsume()
{
}

int KafkaConsumer::Consume(ConsumeCallback cb, void *p)
{
    int n = next.fetch_add(1);
    ConsumeParameter cp;
    cp.cb = cb;
    cp.p = p;
    rd_kafka_topic_t* rkt = (rd_kafka_topic_t*)(this->rkt);

    return rd_kafka_consume_callback(rkt, n, 1000, [](rd_kafka_message_t *msg, void *op) {
        ConsumeParameter *cp = (ConsumeParameter *)op;
        KafkaMessage km;
        km.err = msg->err;
        km.partition = msg->partition;
        km.offset = msg->offset;

        if (msg->key_len > 0)
        {
            char *buf = (char *)msg->key;
            km.key.assign(buf, buf + msg->key_len);
        }

        if (msg->len > 0)
        {
            char *buf = (char *)msg->payload;
            km.value.assign(buf, buf + msg->len);
        }

        cp->cb(km, cp->p);
    },
                                     &cp);
}

KafkaConsumer::KafkaConsumerHandle KafkaConsumer::NewConsumer(std::string brokers, std::string topic, std::string& errmsg)
{
    char errstr[0x100];
    rd_kafka_t *rk = rd_kafka_new(RD_KAFKA_CONSUMER, nullptr, errstr, sizeof(errstr));
    if (rk == nullptr)
    {
        errmsg = errstr;
        return nullptr;
    }

    rd_kafka_brokers_add(rk, brokers.c_str());
    rd_kafka_topic_t *rkt = rd_kafka_topic_new(rk, topic.c_str(), nullptr);

    std::unique_ptr<KafkaConsumer> c(new KafkaConsumer());
    c->rk = rk;
    c->rkt = rkt;
    const struct rd_kafka_metadata *meta;
    rd_kafka_resp_err_t err = rd_kafka_metadata(rk, false, rkt, &meta, 1000);
    if (err != RD_KAFKA_RESP_ERR_NO_ERROR)
    {
        errmsg = rd_kafka_err2str(err);
        return nullptr;
    }

    c->brokers.reserve(meta->broker_cnt);
    for (int i = 0; i < meta->broker_cnt; i++)
    {
        rd_kafka_metadata_broker* broker = meta->brokers + i;
        c->brokers.emplace_back(NewKafkaBroker(broker));
    }

    c->partitions.reserve(meta->topics->partition_cnt);
    for (int i = 0; i < meta->topics->partition_cnt; i++)
    {
        rd_kafka_metadata_partition* partition = meta->topics->partitions + i;
        c->partitions.emplace_back(NewKafkaPartition(partition));
    }

    rd_kafka_metadata_destroy(meta);
    return c;
}