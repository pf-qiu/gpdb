#include "postgres.h"
#include "meta.h"

KafkaMeta* GetUrlMeta(const char *url)
{
    KafkaMeta* meta = (KafkaMeta*)palloc(sizeof(KafkaMeta));
    const char proto[] = "gpkafka://";
    if (pg_strncasecmp(url, proto, sizeof(proto) - 1) != 0)
    {
        elog(ERROR, "unknown protocol: %s", url);
    }
    const char* start = url + sizeof(proto) - 1;
    const char* hostend = strchr(start, '/');
    if (hostend == NULL)
    {
        elog(ERROR, "invalid url: %s", url);
    }
    size_t len = hostend - start;
    char* broker = palloc(len + 1);
    strncpy(broker, start, len);
    broker[len] = 0;
    meta->broker = broker;

    hostend++;
    len = strlen(hostend);
    char* topic = palloc(len + 1);
    strncpy(topic, hostend, len);
    topic[len] = 0;
    meta->topic = topic;

    return meta;
}
/*

typedef struct CurlBuffer
{
    char *buffer;
    size_t current;
    size_t size;
} CurlBuffer;

// curl's write function callback.
static size_t curlGetCallback(char *ptr, size_t size, size_t nmemb, void *userp) {
    size_t realsize = size * nmemb;
    CurlBuffer *cb = (CurlBuffer *)userp;
    if (cb->current + realsize < cb->size)
    {
        memcpy(cb->buffer + cb->current, ptr, realsize);
        cb->current += realsize;
        return realsize;
    }
    else
    {
        return -1;
    }
}

static KafkaMeta* parseKafkaMeta(char* buffer)
{
    char* topic = strchr(buffer, '/');
    if (topic == NULL)
    {
        elog(ERROR, "Invalid meta response: %s", buffer);
    }
    *topic = 0;

    KafkaMeta* meta = (KafkaMeta*)palloc(sizeof(KafkaMeta));
    meta->broker = buffer;
    meta->topic = topic + 1;
    return meta;
}

KafkaMeta* RequestMetaFromCoordinator(const char *url)
{
    CURL *curl = curl_easy_init();
    if(curl) 
    {
        CurlBuffer cb;
        cb.buffer = palloc(0x1000);
        cb.current = 0;
        cb.size = 0x1000 - 1; // Last byte for zero.

        curl_easy_setopt(curl, CURLOPT_URL, url);
        curl_easy_setopt(curl, CURLOPT_WRITEDATA, (void *)&cb);
        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, curlGetCallback);

        CURLcode res = curl_easy_perform(curl);

        if(res != CURLE_OK) 
        {
            curl_easy_cleanup(curl);
            elog(ERROR, "curl_easy_perform() failed: %s", curl_easy_strerror(res));
        }

        curl_easy_cleanup(curl);

        return parseKafkaMeta(cb.buffer);
    }
    return NULL;
}
*/