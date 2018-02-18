#include <curl/curl.h>

typedef struct KafkaMeta {
    char* broker;
    char* topic;
} KafkaMeta;

KafkaMeta* RequestMetaFromCoordinator(const char *url);