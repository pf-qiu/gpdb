CREATE OR REPLACE FUNCTION gpkafka_import() RETURNS integer AS
        '$libdir/gpkafka.so', 'gpkafka_import' LANGUAGE C STABLE;

CREATE PROTOCOL gpkafka (
        readfunc = gpkafka_import
);
