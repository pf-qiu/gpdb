CREATE EXTENSION IF NOT EXISTS file_fdw;
CREATE SERVER IF NOT EXISTS file_server FOREIGN DATA WRAPPER file_fdw;
CREATE TABLE sales (id int, date date, amt decimal(10,2))
DISTRIBUTED BY (id)
PARTITION BY RANGE (date)
( START (date '2020-01-01') INCLUSIVE
   END (date '2022-01-01') EXCLUSIVE
   EVERY (INTERVAL '1 YEAR') );

INSERT INTO sales SELECT generate_series(1, 1000), '2020-01-01', 0;
INSERT INTO sales SELECT generate_series(1, 1000), '2021-01-01', 0;
SELECT COUNT(*), date  FROM sales GROUP BY (date);

CREATE FOREIGN TABLE sales_2020(id int, date date, amt decimal(10,2))
SERVER file_server OPTIONS( filename '/tmp/<SEGID>.csv', format 'csv')
DISTRIBUTED BY (id);

INSERT INTO sales_2020 SELECT * FROM sales_1_prt_1;

ALTER TABLE sales EXCHANGE PARTITION "1"
WITH TABLE sales_2020 WITHOUT VALIDATION;

SELECT COUNT(*), date FROM sales GROUP BY (date);
DROP TABLE sales_2020;