#include "resman.h"

// Linked list of opened "handles", which are allocated in TopMemoryContext, and tracked by resource
// owners.
static gpkafkaResHandle *openedResHandles;

gpkafkaResHandle *createGpkafkaResHandle(void) {
    gpkafkaResHandle *resHandle;

    resHandle = (gpkafkaResHandle *)MemoryContextAlloc(TopMemoryContext, sizeof(gpkafkaResHandle));

    resHandle->owner = CurrentResourceOwner;
    resHandle->next = openedResHandles;
    resHandle->prev = NULL;

    if (openedResHandles) {
        openedResHandles->prev = resHandle;
    }

    openedResHandles = resHandle;

    return resHandle;
}


void destroyGpkafkaResHandle(gpkafkaResHandle *resHandle) {
    if (resHandle == NULL) return;

    /* unlink from linked list first */
    if (resHandle->prev)
        resHandle->prev->next = resHandle->next;
    else
        openedResHandles = resHandle->next;

    if (resHandle->next) {
        resHandle->next->prev = resHandle->prev;
    }

    if (resHandle->kafka != NULL) {
        rd_kafka_destroy(resHandle->kafka);
    }
    pfree(resHandle);
}

/*
 * Close any open handles on abort.
 */
void gpkafkaAbortCallback(ResourceReleasePhase phase, bool isCommit, bool isTopLevel,
                                 void *arg) {
    gpkafkaResHandle *curr;
    gpkafkaResHandle *next;

    if (phase != RESOURCE_RELEASE_AFTER_LOCKS) return;

    next = openedResHandles;
    while (next) {
        curr = next;
        next = curr->next;

        if (curr->owner == CurrentResourceOwner) {
            if (isCommit)
                elog(WARNING, "gpkafka external table reference leak: %p still referenced", curr);

            destroyGpkafkaResHandle(curr);
        }
    }
}

void registerResourceManagerCallback() {
    static bool isCallbackRegisterd;
    if (isCallbackRegisterd) return;
    RegisterResourceReleaseCallback(gpkafkaAbortCallback, NULL);
    isCallbackRegisterd = true;
}