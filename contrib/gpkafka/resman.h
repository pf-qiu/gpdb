#include "postgres.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/resowner.h"

#include "librdkafka/rdkafka.h"


#define KAFKA_CONSUMER 0
#define KAFKA_PRODUCER 1

typedef struct gpkafkaResHandle
{
    rd_kafka_t *kafka;
    rd_kafka_topic_t *topic;
    int partition;
    int mode;
    
    StringInfo messageData;
    ResourceOwner owner; /* owner of this handle */

    struct gpkafkaResHandle *next;
    struct gpkafkaResHandle *prev;
} gpkafkaResHandle;

extern gpkafkaResHandle *createGpkafkaResHandle(void);
extern void destroyGpkafkaResHandle(gpkafkaResHandle *resHandle);
extern void registerResourceManagerCallback(void);
extern void gpkafkaAbortCallback(ResourceReleasePhase phase, bool isCommit, bool isTopLevel, void *arg);