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
Datum gpkafka_export(PG_FUNCTION_ARGS);

static bool QueryAbortInProgress(void)
{
    return QueryCancelPending || IsAbortInProgress();
}

static int max_partition;

static int consume_message(gpkafkaResHandle *gpkafka)
{
    while (!QueryAbortInProgress())
    {
        rd_kafka_poll(gpkafka->kafka, 0);
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
        if (resHandle->kafka == NULL)
        {
            rd_kafka_conf_t *conf = rd_kafka_conf_new();
            char errstr[512];
            rd_kafka_conf_set(conf, "queued.min.messages", "1000000", NULL, 0);


            rd_kafka_t *kafka = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr));
            if (kafka == NULL)
            {
                elog(ERROR, "rd_kafka_new failed: %s", errstr);
            }

            if (rd_kafka_brokers_add(kafka, meta->broker) == 0)
            {
                elog(ERROR, "rd_kafka_brokers_add failed: %s", meta->broker);
            }

            resHandle->kafka = kafka;
            resHandle->mode = KAFKA_CONSUMER;
        }

        rd_kafka_topic_t *topic = rd_kafka_topic_new(resHandle->kafka, meta->topic, NULL);
        const struct rd_kafka_metadata *topicmeta;
        rd_kafka_resp_err_t err = rd_kafka_metadata(resHandle->kafka, 0, topic, &topicmeta, 100);
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

Datum gpkafka_export(PG_FUNCTION_ARGS)
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

    char *data_buf = EXTPROTOCOL_GET_DATABUF(fcinfo);
    int32 data_len = EXTPROTOCOL_GET_DATALEN(fcinfo);
//    if (GpIdentity.segindex != 0)
//    {
//    PG_RETURN_INT32(data_len);
//    }
    /* first call. do any desired init */
    if (resHandle == NULL)
    {
        registerResourceManagerCallback();
        resHandle = createGpkafkaResHandle();
        EXTPROTOCOL_SET_USER_CTX(fcinfo, resHandle);
        const char *url = EXTPROTOCOL_GET_URL(fcinfo);

        KafkaMeta *meta = GetUrlMeta(url);
        if (resHandle->kafka == NULL)
        {

            rd_kafka_conf_t *conf = rd_kafka_conf_new();
            char errstr[512];
            rd_kafka_conf_set(conf, "queued.min.messages", "1000000", NULL, 0);

            rd_kafka_t *kafka = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
            if (kafka == NULL)
            {
                elog(ERROR, "rd_kafka_new failed: %s", errstr);
            }

            if (rd_kafka_brokers_add(kafka, meta->broker) == 0)
            {
                elog(ERROR, "rd_kafka_brokers_add failed: %s", meta->broker);
            }

            resHandle->kafka = kafka;
            resHandle->mode = KAFKA_PRODUCER;
        }

        rd_kafka_topic_t *topic = rd_kafka_topic_new(resHandle->kafka, meta->topic, NULL);
        int segid = GpIdentity.segindex;

        resHandle->topic = topic;
        resHandle->partition = segid;
    }
    int len = data_len;
    if (data_buf[len - 1] == '\n') len--;
    while(rd_kafka_produce(
        /* Topic object */
        resHandle->topic,
        /* Use builtin partitioner to select partition*/
        resHandle->partition,
        /* Make a copy of the payload. */
        RD_KAFKA_MSG_F_COPY,
        /* Message payload (value) and length */
        data_buf, len,
        /* Optional key and its length */
        NULL, 0,
        /* Message opaque, provided in
         * delivery report callback as
         * msg_opaque. */
        NULL) == -1) 
        {
            /* Poll to handle delivery reports */
            if (rd_kafka_last_error() ==
                RD_KAFKA_RESP_ERR__QUEUE_FULL) {
                    /* If the internal queue is full, wait for
                     * messages to be delivered and then retry.
                     * The internal queue represents both
                     * messages to be sent and messages that have
                     * been sent or failed, awaiting their
                     * delivery report callback to be called.
                     *
                     * The internal queue is limited by the
                     * configuration property
                     * queue.buffering.max.messages */
                    rd_kafka_poll(resHandle->kafka, 1000/*block for max 1000ms*/);
                    continue;
            } else {
                elog(ERROR, "Failed to produce to topic %s: %s\n",
                                rd_kafka_topic_name(resHandle->topic),
                                rd_kafka_err2str(rd_kafka_last_error()));
            }
        }

    PG_RETURN_INT32(data_len);
}
