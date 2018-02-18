#include "postgres.h"
#include "fmgr.h"
#include "funcapi.h"

#include "access/extprotocol.h"
#include "access/xact.h"
#include "catalog/pg_exttable.h"
#include "cdb/cdbvars.h"

#include "librdkafka/rdkafka.h"
#include "resman.h"
#include "meta.h"

/* Do the module magic dance */
PG_MODULE_MAGIC;
PG_FUNCTION_INFO_V1(gpkafka_import);
PG_FUNCTION_INFO_V1(gpkafka_export);

Datum gpkafka_import(PG_FUNCTION_ARGS);
Datum gpkafka_export(PG_FUNCTION_ARGS);

static bool QueryAbortInProgress(void)
{
    return QueryCancelPending || IsAbortInProgress();
}

static int consume_message(gpkafkaResHandle *gpkafka, char *databuf, int datalen)
{
    StringInfo data = gpkafka->messageData;
    
    if (data->len == data->cursor)
    {
        // No more data left, fetch new message.
        // Keep trying to poll from kafka, stop if message consumed or cancel is requested.
        while (!QueryAbortInProgress())
        {
            rd_kafka_poll(gpkafka->kafka, 0);
            rd_kafka_message_t *msg = rd_kafka_consume(gpkafka->topic, Gp_segment, 100);
            if (msg)
            {
                if (msg->err == 0)
                {
                    enlargeStringInfo(data, msg->len);
                    resetStringInfo(data);
                    appendBinaryStringInfo(data, msg->payload, msg->len);
                    rd_kafka_message_destroy(msg);
                    break;
                }
                else if (msg->err == RD_KAFKA_RESP_ERR__PARTITION_EOF)
                {
                    rd_kafka_message_destroy(msg);
                    elog(DEBUG5, "partition reach end");
                    return 0;
                }
                else
                {
                    rd_kafka_message_destroy(msg);
                    elog(ERROR, "kafka consumer error: %s", rd_kafka_err2str(msg->err));
                }
            }
        }
    }

    int remain = data->len - data->cursor;
    int consumed = remain < datalen ? remain : datalen;
    memcpy(databuf, data->data + data->cursor, consumed);
    data->cursor += consumed;
    return consumed;
}

Datum gpkafka_import(PG_FUNCTION_ARGS)
{
    /* Must be called via the external table format manager */
    if (!CALLED_AS_EXTPROTOCOL(fcinfo))
        elog(ERROR, "extprotocol_import: not called by external protocol manager");

    if (Gp_segment > 0)
    {
        PG_RETURN_INT32(0);
    }
    /* Get our internal description of the protocol */
    gpkafkaResHandle *resHandle = (gpkafkaResHandle *)EXTPROTOCOL_GET_USER_CTX(fcinfo);

    /* last call. destroy reader */
    if (EXTPROTOCOL_IS_LAST_CALL(fcinfo))
    {
        destroyGpkafkaResHandle(resHandle);

        EXTPROTOCOL_SET_USER_CTX(fcinfo, NULL);
        PG_RETURN_INT32(0);
    }

    char *databuf = EXTPROTOCOL_GET_DATABUF(fcinfo);
    int datalen = EXTPROTOCOL_GET_DATALEN(fcinfo);

    /* first call. do any desired init */
    if (resHandle == NULL)
    {
        registerResourceManagerCallback();
        resHandle = createGpkafkaResHandle();
        EXTPROTOCOL_SET_USER_CTX(fcinfo, resHandle);
        const char *url = EXTPROTOCOL_GET_URL(fcinfo);
        KafkaMeta *meta = RequestMetaFromCoordinator(url);

        rd_kafka_conf_t *conf = rd_kafka_conf_new();
        char errstr[512];

        if (rd_kafka_conf_set(conf, "group.id", "gpkafka", errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK)
        {
            elog(ERROR, "rd_kafka_conf_set failed: %s", errstr);
        }

        rd_kafka_t *kafka = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr));
        if (kafka == NULL)
        {
            elog(ERROR, "rd_kafka_new failed: %s", errstr);
        }

        resHandle->kafka = kafka;

        if (rd_kafka_brokers_add(kafka, meta->broker) > 0)
        {
            rd_kafka_topic_t *topic = rd_kafka_topic_new(kafka, meta->topic, NULL);
            if (rd_kafka_consume_start(topic, Gp_segment, RD_KAFKA_OFFSET_BEGINNING) == -1)
            {
                rd_kafka_resp_err_t err = rd_kafka_last_error();
                elog(ERROR, "rd_kafka_consume_start failed: %s", rd_kafka_err2str(err));
            }
            resHandle->topic = topic;
            resHandle->partition = Gp_segment;
        }
        else
        {
            elog(ERROR, "rd_kafka_brokers_add failed: %s", meta->broker);
        }
    }

    int consumed = consume_message(resHandle, databuf, datalen);
    PG_RETURN_INT32(consumed);
}