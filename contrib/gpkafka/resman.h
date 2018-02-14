#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/resowner.h"

typedef struct gpkafkaResHandle
{
    ResourceOwner owner; /* owner of this handle */

    struct gpkafkaResHandle *next;
    struct gpkafkaResHandle *prev;
} gpkafkaResHandle;

extern gpkafkaResHandle *createGpkafkaResHandle(void);
extern void destroyGpkafkaResHandle(gpkafkaResHandle *resHandle);
extern void registerResourceManagerCallback(void);