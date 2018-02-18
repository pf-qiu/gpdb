#include "postgres.h"
#include "fmgr.h"
#include "funcapi.h"

#include "access/extprotocol.h"
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

static int consume_message(gpkafkaResHandle *gpkafka, char *databuf, int datalen)
{
    StringInfo data = gpkafka->messageData;
    int remain = data->len - data->cursor;
    if (remain == 0)
    {
        // No more data left, fetch new message
        rd_kafka_message_t *msg = rd_kafka_consumer_poll(gpkafka->kafka, 1000);
        if (msg)
        {
            if (msg->err == 0)
            {
                enlargeStringInfo(data, msg->len);
                resetStringInfo(data);
                appendBinaryStringInfo(data, msg->payload, msg->len);
                rd_kafka_message_destroy(msg);
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
        else
        {
            elog(ERROR, "can't pull message from kafka");
        }
    }

    int consumed = remain < datalen ? remain : datalen;
    memcpy(databuf, data->data + data->cursor, consumed);
    data->cursor += consumed;
    return consumed;
}

Datum
gpkafka_import(PG_FUNCTION_ARGS)
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
        elog(INFO, "Kafka url: %s", url);

        KafkaMeta *meta = RequestMetaFromCoordinator(url);
        rd_kafka_conf_t *conf = rd_kafka_conf_new();
        rd_kafka_t *kafka = rd_kafka_new(RD_KAFKA_CONSUMER, conf, NULL, 0);
        resHandle->kafka = kafka;

        if (rd_kafka_brokers_add(kafka, meta->broker) == 0)
        {
            rd_kafka_topic_partition_list_t *list = rd_kafka_topic_partition_list_new(1);
            rd_kafka_topic_partition_list_add(list, meta->topic, Gp_segment);
            
            rd_kafka_resp_err_t err;
            if ((err = rd_kafka_subscribe(kafka, list)))
            {
                elog(ERROR, "rd_kafka_subscribe: %s", rd_kafka_err2str(err));
            }
        }
        else
        {
            elog(ERROR, "rd_kafka_brokers_add failed: %s", meta->broker);
        }
    }

    int consumed = consume_message(resHandle, databuf, datalen);
    PG_RETURN_INT32(consumed);
}