#include "postgres.h"
#include "fmgr.h"
#include "funcapi.h"

#include "access/extprotocol.h"
#include "access/xact.h"
#include "access/heapam.h"
#include "catalog/pg_exttable.h"
#include "catalog/pg_class.h"
#include "utils/tqual.h"
#include "utils/fmgroids.h"
#include "cdb/cdbvars.h"

#include "librdkafka/rdkafka.h"
#include "resman.h"
#include "meta.h"

/* Do the module magic dance */
PG_MODULE_MAGIC;
PG_FUNCTION_INFO_V1(gpkafka_import);
PG_FUNCTION_INFO_V1(gpkafka_export);

HeapTuple gpkafka_import(PG_FUNCTION_ARGS);

static bool QueryAbortInProgress(void)
{
    return QueryCancelPending || IsAbortInProgress();
}

static rd_kafka_t *kafka;
static int max_partition;

static int consume_message(gpkafkaResHandle *gpkafka, StringInfo data)
{
    while (!QueryAbortInProgress())
    {
        rd_kafka_poll(kafka, 0);
        rd_kafka_message_t *msg = rd_kafka_consume(gpkafka->topic, gpkafka->partition, 100);
        if (msg)
        {
            if (msg->err == 0)
            {
                resetStringInfo(data);
                appendBinaryStringInfo(data, msg->payload, msg->len);
                rd_kafka_message_destroy(msg);
                return data->len;
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
                int next = gpkafka->partition + GpIdentity.numsegments;
                gpkafka->partition = -1;

                if (next > max_partition)
                {
                    return 0;
                }

                if (rd_kafka_consume_start(gpkafka->topic, next, RD_KAFKA_OFFSET_BEGINNING) != 0)
                {
                    err = rd_kafka_last_error();
                    elog(ERROR, "rd_kafka_consume_start failed: %s", rd_kafka_err2str(err));
                }
                gpkafka->partition = next;

                continue;
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

HeapTuple gpkafka_import(PG_FUNCTION_ARGS)
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
        return NULL;
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

            if (rd_kafka_conf_set(conf, "group.id", "gpkafka", errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK)
            {
                elog(ERROR, "rd_kafka_conf_set failed: %s", errstr);
            }

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

        if (Gp_segment > max_partition)
        {
            rd_kafka_topic_destroy(topic);
            return NULL;
        }

        if (rd_kafka_consume_start(topic, Gp_segment, RD_KAFKA_OFFSET_BEGINNING) != 0)
        {
            err = rd_kafka_last_error();
            elog(ERROR, "rd_kafka_consume_start failed: %s", rd_kafka_err2str(err));
        }
        resHandle->topic = topic;
        resHandle->partition = Gp_segment;

        resHandle->desc = RelationGetDescr(EXTPROTOCOL_GET_RELATION(fcinfo));
        int ncolumns = resHandle->desc->natts;
        resHandle->isnull = palloc(sizeof(bool) * ncolumns);
        resHandle->values = palloc(sizeof(Datum) * ncolumns);
    }

    StringInfo linebuf = EXTPROTOCOL_GET_LINEBUF(fcinfo);
    int consumed = consume_message(resHandle, linebuf);
    if (consumed > 0)
    {
        FmgrInfo *funcs = EXTPROTOCOL_GET_CONV_FUNCS(fcinfo);
        Oid* typioparams = EXTPROTOCOL_GET_TYPIOPARAMS(fcinfo);

        int n = resHandle->desc->natts;
        char *buf = linebuf->data;
        Datum *values = resHandle->values;
        bool *isnull = resHandle->isnull;
        TupleDesc desc = resHandle->desc;

        int i;
        for (i = 0; i < n - 1; i++)
        {
            char *p = strchr(buf, ',');
            if (p == NULL)
            {
                elog(ERROR, "invalid data");
            }
            *p = '\0';
            if (p == buf + 1)
            {
                isnull[i] = true;
            }
            else
            {
            values[i] = InputFunctionCall(&funcs[i], buf, typioparams[i], desc->attrs[i]->atttypmod);
            isnull[i] = false;
            }
            buf = p + 1;
        }

        if (*buf)
        {
            values[i] = InputFunctionCall(&funcs[i], buf, typioparams[i], desc->attrs[i]->atttypmod);
            isnull[i] = false;
        }
        else
        {
            isnull[i] = true;
        }
        return heap_form_tuple(desc, values, isnull);
    }
    else
    {
        return NULL;
    }
}