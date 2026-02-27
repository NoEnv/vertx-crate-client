-- World table
DROP TABLE IF EXISTS World;
CREATE TABLE  World (
  id integer NOT NULL,
  randomNumber integer NOT NULL default 0,
  PRIMARY KEY  (id)
);

INSERT INTO World (id, randomnumber)
SELECT x.id, random() * 10000 + 1 FROM generate_series(1,10000) as x(id);

REFRESH TABLE World;

-- Fortune table
DROP TABLE IF EXISTS Fortune;
CREATE TABLE Fortune (
  id integer NOT NULL,
  message varchar(2048) NOT NULL,
  PRIMARY KEY  (id)
);

INSERT INTO Fortune (id, message) VALUES (1, 'fortune: No such file or directory');
INSERT INTO Fortune (id, message) VALUES (2, 'A computer scientist is someone who fixes things that aren''t broken.');
INSERT INTO Fortune (id, message) VALUES (3, 'After enough decimal places, nobody gives a damn.');
INSERT INTO Fortune (id, message) VALUES (4, 'A bad random number generator: 1, 1, 1, 1, 1, 4.33e+67, 1, 1, 1');
INSERT INTO Fortune (id, message) VALUES (5, 'A computer program does what you tell it to do, not what you want it to do.');
INSERT INTO Fortune (id, message) VALUES (6, 'Emacs is a nice operating system, but I prefer UNIX. — Tom Christaensen');
INSERT INTO Fortune (id, message) VALUES (7, 'Any program that runs right is obsolete.');
INSERT INTO Fortune (id, message) VALUES (8, 'A list is only as strong as its weakest link. — Donald Knuth');
INSERT INTO Fortune (id, message) VALUES (9, 'Feature: A bug with seniority.');
INSERT INTO Fortune (id, message) VALUES (10, 'Computers make very fast, very accurate mistakes.');
INSERT INTO Fortune (id, message) VALUES (11, '<script>alert("This should not be displayed in a browser alert box.");</script>');
INSERT INTO Fortune (id, message) VALUES (12, 'フレームワークのベンチマーク');

REFRESH TABLE Fortune;

-- All purpose testing table
DROP TABLE IF EXISTS Test;
CREATE TABLE Test (
  id integer NOT NULL,
  val varchar(2048) NOT NULL,
  PRIMARY KEY  (id)
);

DROP TABLE IF EXISTS "NumericDataType";
CREATE TABLE "NumericDataType" (
  "id" INTEGER NOT NULL PRIMARY KEY,
  "Short" INT2,
  "Integer" INT4,
  "Long" INT8,
  "Float" FLOAT4,
  "Double" FLOAT8,
  "Boolean" BOOLEAN
);

INSERT INTO "NumericDataType" ("id", "Short", "Integer", "Long", "Float", "Double", "Boolean")
VALUES (1, 32767, 2147483647, 9223372036854775806, 3.4028235E38, 1.7976931348623157E308, true);
INSERT INTO "NumericDataType" ("id", "Short", "Integer", "Long", "Float", "Double", "Boolean")
VALUES (2, 32767, 2147483647, 9223372036854775806, 3.4028235E38, 1.7976931348623157E308, true);

DROP TABLE IF EXISTS "TemporalDataType";
CREATE TABLE "TemporalDataType" ("id" INTEGER NOT NULL PRIMARY KEY, "Timestamp" timestamp without time zone, "TimestampTz" timestamp with time zone);
INSERT INTO "TemporalDataType" ("id" ,"Timestamp", "TimestampTz") VALUES (1 ,'2017-05-14 19:35:58.237666', '2017-05-14 23:59:59.237666-03');
INSERT INTO "TemporalDataType" ("id" ,"Timestamp", "TimestampTz") VALUES (2 ,'1909-05-14 19:35:58.237666', '1909-05-14 22:35:58.237666-03');
INSERT INTO "TemporalDataType" ("id" ,"Timestamp", "TimestampTz") VALUES (3 ,'1800-01-01 23:57:53.237666', '1800-01-01 23:59:59.237666-03');
INSERT INTO "TemporalDataType" ("id" ,"Timestamp", "TimestampTz") VALUES (4 ,'1800-01-01 23:57:53.237666', '1800-01-01 23:59:59.237666-03');

DROP TABLE IF EXISTS "CharacterDataType";
CREATE TABLE "CharacterDataType" (
  "id" INTEGER NOT NULL PRIMARY KEY,
  "Name" NAME,
  "SingleChar" CHAR,
  "FixedChar" CHAR(3),
  "Text" TEXT,
  "VarCharacter" VARCHAR,
  "uuid" UUID
);
INSERT INTO "CharacterDataType" ("id" ,"Name", "SingleChar", "FixedChar", "Text", "VarCharacter", "uuid") VALUES (1, 'What is my name ?', 'A', 'YES', 'Hello World', 'Great!', '6f790482-b5bd-438b-a8b7-4a0bed747011');
INSERT INTO "CharacterDataType" ("id" ,"Name", "SingleChar", "FixedChar", "Text", "VarCharacter", "uuid") VALUES (2, 'What is my name ?', 'A', 'YES', 'Hello World', 'Great!', '6f790482-b5bd-438b-a8b7-4a0bed747011');

CREATE TABLE "AllDataTypes"
(
  boolean     BOOLEAN,
  int2        INT2,
  int4        INT4,
  int8        INT8,
  float4      FLOAT4,
  float8      FLOAT8,
  char        CHAR,
  varchar     VARCHAR,
  text        TEXT,
  name        NAME,
  uuid        UUID,
  timestamp   TIMESTAMP,
  timestamptz TIMESTAMPTZ
);

-- TCK usage --
-- immutable for select query testing --
DROP TABLE IF EXISTS immutable;
CREATE TABLE immutable
(
  id      integer       NOT NULL,
  message varchar(2048) NOT NULL,
  PRIMARY KEY (id)
);

INSERT INTO immutable (id, message)
VALUES (1, 'fortune: No such file or directory');
INSERT INTO immutable (id, message)
VALUES (2, 'A computer scientist is someone who fixes things that aren''t broken.');
INSERT INTO immutable (id, message)
VALUES (3, 'After enough decimal places, nobody gives a damn.');
INSERT INTO immutable (id, message)
VALUES (4, 'A bad random number generator: 1, 1, 1, 1, 1, 4.33e+67, 1, 1, 1');
INSERT INTO immutable (id, message)
VALUES (5, 'A computer program does what you tell it to do, not what you want it to do.');
INSERT INTO immutable (id, message)
VALUES (6, 'Emacs is a nice operating system, but I prefer UNIX. — Tom Christaensen');
INSERT INTO immutable (id, message)
VALUES (7, 'Any program that runs right is obsolete.');
INSERT INTO immutable (id, message)
VALUES (8, 'A list is only as strong as its weakest link. — Donald Knuth');
INSERT INTO immutable (id, message)
VALUES (9, 'Feature: A bug with seniority.');
INSERT INTO immutable (id, message)
VALUES (10, 'Computers make very fast, very accurate mistakes.');
INSERT INTO immutable (id, message)
VALUES (11, '<script>alert("This should not be displayed in a browser alert box.");</script>');
INSERT INTO immutable (id, message)
VALUES (12, 'フレームワークのベンチマーク');
-- immutable for select query testing --

-- mutable for insert,update,delete query testing
DROP TABLE IF EXISTS mutable;
CREATE TABLE mutable
(
  id  integer       NOT NULL,
  val varchar(2048) NOT NULL,
  PRIMARY KEY (id)
);
-- mutable for insert,update,delete query testing

-- unstable table for table schema changing testing
DROP TABLE IF EXISTS unstable;
CREATE TABLE unstable
(
  id      integer       NOT NULL,
  message varchar(2048) NOT NULL,
  PRIMARY KEY (id)
);

INSERT INTO unstable (id, message)
VALUES (1, 'fortune: No such file or directory');
-- unstable table for table schema changing testing

-- table for test ANSI SQL data type codecs
DROP TABLE IF EXISTS basicdatatype;
CREATE TABLE basicdatatype
(
    id           INTEGER,
    test_int_2   SMALLINT,
    test_int_4   INTEGER,
    test_int_8   BIGINT,
    test_float_4 REAL,
    test_float_8 DOUBLE PRECISION,
    test_numeric NUMERIC(5, 2),
    test_boolean BOOLEAN,
    test_char    CHAR(8),
    test_varchar VARCHAR(20)
);
INSERT INTO basicdatatype(id, test_int_2, test_int_4, test_int_8, test_float_4, test_float_8, test_numeric,
                          test_boolean, test_char, test_varchar)
VALUES (1, 32767, 2147483647, 9223372036854775806, 3.40282E38, 1.7976931348623157E308, 999.99,
        TRUE, 'testchar', 'testvarchar');
INSERT INTO basicdatatype(id, test_int_2, test_int_4, test_int_8, test_float_4, test_float_8, test_numeric,
                          test_boolean, test_char, test_varchar)
VALUES (2, 32767, 2147483647, 9223372036854775806, 3.40282E38, 1.7976931348623157E308, 999.99,
        TRUE, 'testchar', 'testvarchar');
INSERT INTO basicdatatype(id, test_int_2, test_int_4, test_int_8, test_float_4, test_float_8, test_numeric,
                          test_boolean, test_char, test_varchar)
VALUES (3, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);

REFRESH TABLE basicdatatype;
-- table for test ANSI SQL data type codecs

-- Collector API testing
DROP TABLE IF EXISTS collector_test;
CREATE TABLE collector_test
(
    id           INT4,
    test_int_2   INT2,
    test_int_4   INT4,
    test_int_8   INT8,
    test_float   FLOAT4,
    test_double  FLOAT8,
    test_varchar VARCHAR(20)
);

INSERT INTO collector_test
VALUES (1, 32767, 2147483647, 9223372036854775806, 123.456, 1.234567, 'HELLO,WORLD');
INSERT INTO collector_test
VALUES (2, 32767, 2147483647, 9223372036854775806, 123.456, 1.234567, 'hello,world');

-- function example --
create function crate_sleep(integer)
returns integer
language JAVASCRIPT
as 'function crate_sleep(t) { var end = new Date().getTime() + t; var a = 1; while( new Date().getTime() < end ) { a++; } return 1; }';

