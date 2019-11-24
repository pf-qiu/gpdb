#pragma once

#include <map>
#include <vector>
#include <string>
#include <atomic>
#include <memory>

struct KafkaBroker
{
    int id;
    int port;
    std::string host;
};

struct KafkaPartition
{
    int id;
    int leader;
    std::vector<int> isrs;
    std::vector<int> replicas;
};

struct KafkaMessage
{
    bool IsEOF();
    std::string Error();

    int err;
    int partition;
    int64_t offset;
    std::string key;
    std::string value;
};


struct KafkaConsumer
{
    typedef std::unique_ptr<KafkaConsumer> KafkaConsumerHandle;
    typedef void (*ConsumeCallback)(KafkaMessage &, void*);

    struct ConsumeParameter
    {
        ConsumeCallback cb;
        void* p;
    };

    KafkaConsumer();
    ~KafkaConsumer();
    KafkaConsumer(const KafkaConsumer &) = delete;
    KafkaConsumer(KafkaConsumer &&) = delete;

    void StartConsume();
    void StopConsume();
    int Consume(ConsumeCallback cb, void* p);

    void *rk;
    void *rkt;
    std::atomic<int> next;
    std::vector<KafkaBroker> brokers;
    std::vector<KafkaPartition> partitions;

    static KafkaConsumerHandle NewConsumer(std::string brokers, std::string topic, std::string& errmsg);
};