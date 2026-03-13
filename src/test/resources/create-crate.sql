-- World table
DROP TABLE IF EXISTS "world";
CREATE TABLE "world" (
  "id" INTEGER NOT NULL,
  "randomnumber" INTEGER DEFAULT 0 NOT NULL,
  PRIMARY KEY ("id")
)
CLUSTERED BY ("id") INTO 4 SHARDS
WITH (
  column_policy = 'strict',
  number_of_replicas = '0-1'
);

DROP VIEW IF EXISTS "top_of_the_world";
CREATE VIEW "top_of_the_world" AS
  SELECT * FROM "world" WHERE id > 9000;

INSERT INTO "world" (id, randomnumber) SELECT x.id, random() * 10000 + 1 FROM generate_series(1,10000) AS x(id);

REFRESH TABLE "world";

-- Fortune table
DROP TABLE IF EXISTS "fortune";
CREATE TABLE "fortune" (
  "id" INTEGER NOT NULL,
  "message" VARCHAR(2048) NOT NULL,
  PRIMARY KEY ("id")
)
CLUSTERED BY ("id") INTO 4 SHARDS
WITH (
  column_policy = 'strict',
  number_of_replicas = '0-1'
);

INSERT INTO "fortune" (id, message) VALUES
(1, 'fortune: No such file or directory'),
(2, 'A computer scientist is someone who fixes things that aren''t broken.'),
(3, 'After enough decimal places, nobody gives a damn.'),
(4, 'A bad random number generator: 1, 1, 1, 1, 1, 4.33e+67, 1, 1, 1'),
(5, 'A computer program does what you tell it to do, not what you want it to do.'),
(6, 'Emacs is a nice operating system, but I prefer UNIX. — Tom Christaensen'),
(7, 'Any program that runs right is obsolete.'),
(8, 'A list is only as strong as its weakest link. — Donald Knuth'),
(9, 'Feature: A bug with seniority.'),
(10, 'Computers make very fast, very accurate mistakes.'),
(11, '<script>alert("This should not be displayed in a browser alert box.");</script>'),
(12, 'フレームワークのベンチマーク');

REFRESH TABLE "fortune";

-- All purpose testing table
DROP TABLE IF EXISTS "test";
CREATE TABLE "test" (
  id INTEGER NOT NULL,
  val VARCHAR(2048) NOT NULL,
  PRIMARY KEY ("id")
)
CLUSTERED BY ("id") INTO 4 SHARDS
WITH (
  column_policy = 'strict',
  number_of_replicas = '0-1'
);

DROP TABLE IF EXISTS "numericdatatype";
CREATE TABLE "numericdatatype" (
  "id" INTEGER NOT NULL,
  "Short" SMALLINT,
  "Integer" INTEGER,
  "Long" BIGINT,
  "Float" REAL,
  "Double" DOUBLE PRECISION,
  "Numeric" NUMERIC(5, 2),
  "Boolean" BOOLEAN,
  "FloatVector" FLOAT_VECTOR(3),
  PRIMARY KEY ("id")
)
CLUSTERED BY ("id") INTO 4 SHARDS
WITH (
  column_policy = 'strict',
  number_of_replicas = '0-1'
);

INSERT INTO "numericdatatype" ("id", "Short", "Integer", "Long", "Float", "Double", "Numeric", "Boolean", "FloatVector") VALUES
(1, 32767, 2147483647, 9223372036854775806, 3.4028235E38, 1.7976931348623157E308, 123.45, true, [3.14, 27.34, 38.4]),
(2, 32767, 2147483647, 9223372036854775806, 3.4028235E38, 1.7976931348623157E308, 123.45, true, [3.14, 27.34, 38.4]);

REFRESH TABLE "numericdatatype";

DROP TABLE IF EXISTS "temporaldatatype";
CREATE TABLE "temporaldatatype" (
  "id" INTEGER NOT NULL,
  "Timestamp" TIMESTAMP WITHOUT TIME ZONE,
  "TimestampTz" TIMESTAMP WITH TIME ZONE,
  PRIMARY KEY ("id")
)
CLUSTERED BY ("id") INTO 4 SHARDS
WITH (
  column_policy = 'strict',
  number_of_replicas = '0-1'
);

INSERT INTO "temporaldatatype" ("id", "Timestamp", "TimestampTz") VALUES
(1 ,'2017-05-14 19:35:58.237666', '2017-05-14 23:59:59.237666-03'),
(2 ,'1909-05-14 19:35:58.237666', '1909-05-14 22:35:58.237666-03'),
(3 ,'1800-01-01 23:57:53.237666', '1800-01-01 23:59:59.237666-03'),
(4 ,'1800-01-01 23:57:53.237666', '1800-01-01 23:59:59.237666-03');

REFRESH TABLE "temporaldatatype";

DROP TABLE IF EXISTS "characterdatatype";
CREATE TABLE "characterdatatype" (
  "id" INTEGER NOT NULL,
  "SingleChar" CHARACTER,
  "FixedChar" CHARACTER(3),
  "Text" TEXT,
  "VarCharacter" VARCHAR(256),
  "uuid" UUID,
  PRIMARY KEY ("id")
)
CLUSTERED BY ("id") INTO 4 SHARDS
WITH (
  column_policy = 'strict',
  number_of_replicas = '0-1'
);

INSERT INTO "characterdatatype" ("id", "SingleChar", "FixedChar", "Text", "VarCharacter", "uuid") VALUES
(1, 'A', 'YES', 'Hello World', 'Great!', '6f790482-b5bd-438b-a8b7-4a0bed747011'),
(2, 'A', 'YES', 'Hello World', 'Great!', '6f790482-b5bd-438b-a8b7-4a0bed747011');

REFRESH TABLE "characterdatatype";

DROP TABLE IF EXISTS "specialdatatype";
CREATE TABLE "specialdatatype" (
  "id" INTEGER NOT NULL,
  "Ip" IP,
  "Bit" BIT,
  "BitMask" BIT(4),
  "ObjectDynamic" OBJECT(DYNAMIC),
  "ObjectStrict" OBJECT(STRICT) AS (
    "age" INTEGER,
    "name" TEXT,
    "details" OBJECT(STRICT) AS (
      "birthday" TIMESTAMP WITH TIME ZONE
    )
  ),
  "ObjectIgnored" OBJECT(IGNORED),
  "Tags" ARRAY(TEXT),
  "Objects" ARRAY(OBJECT(DYNAMIC) AS (
    "age" INTEGER,
    "name" TEXT
  )),
  PRIMARY KEY ("id")
)
CLUSTERED BY ("id") INTO 4 SHARDS
WITH (
  column_policy = 'strict',
  number_of_replicas = '0-1'
);

INSERT INTO "specialdatatype" ("id", "Ip", "Bit", "BitMask", "ObjectDynamic", "ObjectStrict", "ObjectIgnored", "Tags", "Objects") VALUES
(1, '127.0.0.1', B'0', B'0110', {"age" = 20, "name" = 'Bob'}, {"age" = 10, "name" = 'Alice', "details" = {"birthday" = '1852-05-04T00:00Z'::TIMESTAMPTZ}}, {"age" = 60, "name" = 'Frank'}, ['foo', 'bar'], [{"name" = 'Alice', "age" = 33}, {"name" = 'Bob', "age" = 45}]),
(2, 'ff:0:ff:ff:0:ffff:c0a8:64', B'1', B'1001', {"age" = 30, "name" = 'Charlie'}, {"age" = 40, "name" = 'David', "details" = {"birthday" = '1952-02-05T01:00Z'::TIMESTAMPTZ}}, {"age" = 70, "name" = 'Grace'}, ['arr1', 'arr2'], [{"name" = 'Heidi', "age" = 80}, {"name" = 'Ivan', "age" = 90}]);

REFRESH TABLE "specialdatatype";

DROP TABLE IF EXISTS "geojsondatatype";
CREATE TABLE "geojsondatatype" (
  "id" INTEGER NOT NULL,
  "Pin" GEO_POINT,
  "Area" GEO_SHAPE INDEX USING GEOHASH,
  PRIMARY KEY ("id")
)
CLUSTERED BY ("id") INTO 4 SHARDS
WITH (
  column_policy = 'strict',
  number_of_replicas = '0-1'
);

INSERT INTO "geojsondatatype" ("id", "Pin", "Area") VALUES
(1, [13.46738, 52.50463], 'POINT (9.7417 47.4108)'),
(2, [13.46738, 52.50463], 'MULTIPOINT (47.4108 9.7417, 9.7483 47.4106)'),
(3, [13.46738, 52.50463], 'LINESTRING (47.4108 9.7417, 9.7483 47.4106)'),
(4, [13.46738, 52.50463], 'MULTILINESTRING ((47.4108 9.7417, 9.7483 47.4106), (52.50463 13.46738, 52.51000 13.47000))'),
(5, [13.46738, 52.50463], 'POLYGON ((47.4108 9.7417, 9.7483 47.4106, 9.7426 47.4142, 47.4108 9.7417))'),
(6, [13.46738, 52.50463], 'MULTIPOLYGON (((5 5, 10 5, 10 10, 5 5)), ((6 6, 10 5, 10 10, 6 6)))'),
(7, [13.46738, 52.50463], 'GEOMETRYCOLLECTION (POINT (9.7417 47.4108), MULTIPOINT (47.4108 9.7417, 9.7483 47.4106))');

REFRESH TABLE "geojsondatatype";

DROP TABLE IF EXISTS "alldatatypes";
CREATE TABLE "alldatatypes" (
  "boolean" BOOLEAN,
  "int2" SMALLINT,
  "integer" INTEGER,
  "int8" BIGINT,
  "float4" REAL,
  "float8" DOUBLE PRECISION,
  """char""" CHARACTER(1),
  "varchar" VARCHAR(2048),
  "text" TEXT,
  "uuid" UUID,
  "timestamp" TIMESTAMP WITHOUT TIME ZONE,
  "timestamptz" TIMESTAMP WITH TIME ZONE
)
CLUSTERED INTO 4 SHARDS
WITH (
  column_policy = 'strict',
  number_of_replicas = '0-1'
);

-- table for test ANSI SQL data type codecs
DROP TABLE IF EXISTS "basicdatatype";
CREATE TABLE "basicdatatype" (
  "id" INTEGER,
  "test_int_2" SMALLINT,
  "test_int_4" INTEGER,
  "test_int_8" BIGINT,
  "test_float_4" REAL,
  "test_float_8" DOUBLE PRECISION,
  "test_numeric" NUMERIC(5, 2),
  "test_boolean" BOOLEAN,
  "test_char" CHARACTER(8),
  "test_varchar" VARCHAR(20)
)
CLUSTERED INTO 4 SHARDS
WITH (
  column_policy = 'strict',
  number_of_replicas = '0-1'
);

INSERT INTO basicdatatype(id, test_int_2, test_int_4, test_int_8, test_float_4, test_float_8, test_numeric, test_boolean, test_char, test_varchar) VALUES
(1, 32767, 2147483647, 9223372036854775806, 3.40282E38, 1.7976931348623157E308, 999.99,TRUE, 'testchar', 'testvarchar'),
(2, 32767, 2147483647, 9223372036854775806, 3.40282E38, 1.7976931348623157E308, 999.99,TRUE, 'testchar', 'testvarchar'),
(3, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);

REFRESH TABLE basicdatatype;

-- blob table
DROP BLOB TABLE IF EXISTS "blobdatatype";
CREATE BLOB TABLE "blobdatatype"
CLUSTERED INTO 4 SHARDS
WITH (
  number_of_replicas = '0'
);

-- function example
CREATE FUNCTION subtract_function(INTEGER, INTEGER)
RETURNS INTEGER
LANGUAGE JAVASCRIPT
AS 'function subtract_function(a, b) { return a - b; }';

-- analyzer example
CREATE ANALYZER "example_analyzer" (
  TOKENIZER whitespace,
  TOKEN_FILTERS (
    lowercase,
    kstem
  ),
  CHAR_FILTERS (
    mymapping WITH (
      type='mapping',
      mappings = ['ph=>f', 'qu=>q', 'foo=>bar']
    )
  )
);

-- search table with fulltext index
DROP TABLE IF EXISTS "searchtable";
CREATE TABLE "searchtable" (
  "id" INTEGER NOT NULL,
  "title" TEXT,
  "body" TEXT,
  INDEX "title_body_ft" USING FULLTEXT ("title", "body") WITH (analyzer = 'example_analyzer'),
  PRIMARY KEY ("id")
)
CLUSTERED BY ("id") INTO 4 SHARDS
WITH (
  column_policy = 'strict',
  number_of_replicas = '0-1'
);

-- schema and table for testing Default-Schema header (query with Default-Schema: test_schema, unqualified "opt_test")
CREATE SCHEMA IF NOT EXISTS test_schema;
DROP TABLE IF EXISTS test_schema.opt_test;
CREATE TABLE test_schema.opt_test (
  "id" INTEGER NOT NULL,
  PRIMARY KEY ("id")
)
CLUSTERED BY ("id") INTO 1 SHARDS
WITH (number_of_replicas = '0-1');
INSERT INTO test_schema.opt_test (id) VALUES (42);
REFRESH TABLE test_schema.opt_test;
