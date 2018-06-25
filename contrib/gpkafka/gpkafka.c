#include "postgres.h"
#include "access/extprotocol.h"
#include "cdb/cdbvars.h"

#include "librdkafka/rdkafka.h"
#include "resman.h"
#include "meta.h"

/* Do the module magic dance */
PG_MODULE_MAGIC;
PG_FUNCTION_INFO_V1(gpkafka_import);
PG_FUNCTION_INFO_V1(gpkafka_export);

Datum gpkafka_import(PG_FUNCTION_ARGS);

static bool QueryAbortInProgress(void)
{
    return QueryCancelPending || IsAbortInProgress();
}

static rd_kafka_t *kafka;
static int max_partition;

static int consume_message(gpkafkaResHandle *gpkafka)
{
    while (!QueryAbortInProgress())
    {
        rd_kafka_poll(kafka, 0);
        rd_kafka_message_t *msg = rd_kafka_consume(gpkafka->topic, gpkafka->partition, 1000);
        if (msg)
        {
            if (msg->err == 0)
            {
                resetStringInfo(gpkafka->messageData);
                appendBinaryStringInfo(gpkafka->messageData, msg->payload, msg->len);
                appendStringInfoChar(gpkafka->messageData, '\n');
                int len = msg->len+1;
                rd_kafka_message_destroy(msg);
                return len;
            }
            else if (msg->err == RD_KAFKA_RESP_ERR__PARTITION_EOF)
            {
                rd_kafka_message_destroy(msg);
                elog(DEBUG5, "partition reach end");
                rd_kafka_resp_err_t err;
                if (rd_kafka_consume_stop(gpkafka->topic, gpkafka->partition) != 0)
                {
                    err = rd_kafka_last_error();
                    elog(ERROR, "rd_kafka_consume_stop failed: %s", rd_kafka_err2str(err));
                }
                return 0;
            }
            else
            {
                rd_kafka_message_destroy(msg);
                elog(ERROR, "kafka consumer error: %s", rd_kafka_err2str(msg->err));
            }
        }
    }
    return 0;
}

Datum gpkafka_import(PG_FUNCTION_ARGS)
{
    /* Must be called via the external table format manager */
    if (!CALLED_AS_EXTPROTOCOL(fcinfo))
        elog(ERROR, "extprotocol_import: not called by external protocol manager");

    /* Get our internal description of the protocol */
    gpkafkaResHandle *resHandle = (gpkafkaResHandle *)EXTPROTOCOL_GET_USER_CTX(fcinfo);

    /* last call. destroy reader */
    if (EXTPROTOCOL_IS_LAST_CALL(fcinfo))
    {
        destroyGpkafkaResHandle(resHandle);

        EXTPROTOCOL_SET_USER_CTX(fcinfo, NULL);
        PG_RETURN_INT32(0);
    }

    /* first call. do any desired init */
    if (resHandle == NULL)
    {
        registerResourceManagerCallback();
        resHandle = createGpkafkaResHandle();
        EXTPROTOCOL_SET_USER_CTX(fcinfo, resHandle);
        const char *url = EXTPROTOCOL_GET_URL(fcinfo);

        KafkaMeta *meta = GetUrlMeta(url);
        if (kafka == NULL)
        {

            rd_kafka_conf_t *conf = rd_kafka_conf_new();
            char errstr[512];
            rd_kafka_conf_set(conf, "queued.min.messages", "1000000", NULL, 0);


            kafka = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr));
            if (kafka == NULL)
            {
                elog(ERROR, "rd_kafka_new failed: %s", errstr);
            }

            if (rd_kafka_brokers_add(kafka, meta->broker) == 0)
            {
                elog(ERROR, "rd_kafka_brokers_add failed: %s", meta->broker);
            }
        }

        rd_kafka_topic_t *topic = rd_kafka_topic_new(kafka, meta->topic, NULL);
        const struct rd_kafka_metadata *topicmeta;
        rd_kafka_resp_err_t err = rd_kafka_metadata(kafka, 0, topic, &topicmeta, 100);
        if (err != RD_KAFKA_RESP_ERR_NO_ERROR)
        {
            elog(ERROR, "rd_kafka_metadata failed: %s", rd_kafka_err2str(err));
        }
        max_partition = topicmeta->topics->partition_cnt - 1;
        rd_kafka_metadata_destroy(topicmeta);
        
        int segid = GpIdentity.segindex;
        if (segid > max_partition)
        {
            rd_kafka_topic_destroy(topic);
            PG_RETURN_INT32(0);
        }

        if (rd_kafka_consume_start(topic, segid, RD_KAFKA_OFFSET_BEGINNING) != 0)
        {
            err = rd_kafka_last_error();
            elog(ERROR, "rd_kafka_consume_start failed: %s", rd_kafka_err2str(err));
        }
        resHandle->topic = topic;
        resHandle->partition = segid;
    }

    char *data 	= EXTPROTOCOL_GET_DATABUF(fcinfo);
    int datalen 	= EXTPROTOCOL_GET_DATALEN(fcinfo);
    if (datalen == 0) return 0;
    StringInfo buf = resHandle->messageData;
    if (buf->cursor == buf->len)
    {
        int consumed = consume_message(resHandle);
        if (consumed == 0)
        {
            //EOF
            PG_RETURN_INT32(0);
        }
    }

    int remain = buf->len - buf->cursor;
    datalen = datalen < remain ? datalen : remain;
    memcpy(data, buf->data, datalen);
    buf->cursor += datalen;
    PG_RETURN_INT32(datalen);
}
