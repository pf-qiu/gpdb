CREATE OR REPLACE FUNCTION gpkafka_import() RETURNS integer AS
        '$libdir/gpkafka.so', 'gpkafka_import' LANGUAGE C STABLE;
CREATE OR REPLACE FUNCTION gpkafka_export() RETURNS integer AS
        '$libdir/gpkafka.so', 'gpkafka_export' LANGUAGE C STABLE;

CREATE PROTOCOL gpkafka (
        readfunc = gpkafka_import,
        writefunc = gpkafka_export
);
