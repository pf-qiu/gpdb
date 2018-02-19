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

Datum gpkafka_import(PG_FUNCTION_ARGS);
Datum gpkafka_export(PG_FUNCTION_ARGS);

static bool QueryAbortInProgress(void)
{
    return QueryCancelPending || IsAbortInProgress();
}

static bool is_custom_format(FunctionCallInfo fcinfo)
{
    static ExtTableEntry *exttbl;

    if (exttbl == NULL)
    {
        Relation rel = EXTPROTOCOL_GET_RELATION(fcinfo);
        exttbl = GetExtTableEntry(rel->rd_id);
    }
    return fmttype_is_custom(exttbl->fmtcode);
}

static rd_kafka_t *kafka;

static int consume_message(gpkafkaResHandle *gpkafka, StringInfo data, bool custom)
{
    while (!QueryAbortInProgress())
    {
        rd_kafka_poll(kafka, 0);
        rd_kafka_message_t *msg = rd_kafka_consume(gpkafka->topic, Gp_segment, 100);
        if (msg)
        {
            if (msg->err == 0)
            {
                resetStringInfo(data);
                appendBinaryStringInfo(data, msg->payload, msg->len);

                if (!custom)
                {
                    /* text and csv format needs line end */
                    appendStringInfoChar(data, '\n');
                }
                rd_kafka_message_destroy(msg);
                return data->len;
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
    return 0;
}

static Oid lookup_oid(const char *table)
{
    Oid oid;
    Relation rel;
    ScanKeyData keys[2];
    HeapScanDesc scan;
    HeapTuple tuple;

    oid = InvalidOid;
    rel = heap_open(RelationRelationId, AccessShareLock);
    ScanKeyInit(&keys[0], Anum_pg_class_relname, BTEqualStrategyNumber, F_NAMEEQ, CStringGetDatum(table));
    ScanKeyInit(&keys[1], Anum_pg_class_reltype, BTEqualStrategyNumber, F_CHAREQ, CharGetDatum(RELKIND_RELATION));

    scan = heap_beginscan(rel, SnapshotNow, sizeof(keys), keys);
    if ((tuple = heap_getnext(scan, ForwardScanDirection)))
    {
        oid = HeapTupleGetOid(tuple);
    }

    heap_endscan(scan);
    heap_close(rel, AccessShareLock);
    return oid;
}

#define InvalidOffset ((int64)-1)

static int64 lookup_offset(Oid table)
{
    Relation rel;
    ScanKeyData key;
    HeapScanDesc scan;
    HeapTuple tuple;
    TupleDesc desc;
    bool isnull;
    Datum offset;
    rel = heap_open(table, AccessShareLock);
    ScanKeyInit(&key, 1, BTEqualStrategyNumber, F_INT4EQ, Int32GetDatum(Gp_segment));
    scan = heap_beginscan(rel, SnapshotNow, 1, &key);
    if ((tuple = heap_getnext(scan, ForwardScanDirection)))
    {
        desc = RelationGetDescr(rel);
        isnull = false;
        offset = heap_getattr(tuple, 2, desc, &isnull);
        if (isnull)
        {
            offset = InvalidOffset;
        }
    }

    heap_endscan(scan);
    heap_close(rel, AccessShareLock);
    return DatumGetInt64(offset);
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

        KafkaMeta *meta = RequestMetaFromCoordinator(url);
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
        if (rd_kafka_consume_start(topic, Gp_segment, RD_KAFKA_OFFSET_BEGINNING) == -1)
        {
            rd_kafka_resp_err_t err = rd_kafka_last_error();
            elog(ERROR, "rd_kafka_consume_start failed: %s", rd_kafka_err2str(err));
        }
        resHandle->topic = topic;
        resHandle->partition = Gp_segment;
    }

    StringInfo linebuf = EXTPROTOCOL_GET_LINEBUF(fcinfo);
    bool custom = is_custom_format(fcinfo);
    int consumed = consume_message(resHandle, linebuf, custom);
    if (consumed > 0)
    {
        /* Tell gpdb that no line detection is needed. */
        EXTPROTOCOL_SET_IS_COMPLETE_RECORD(fcinfo);
    }
    PG_RETURN_INT32(consumed);
}