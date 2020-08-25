-- Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
-- and the EPL 1.0 (https://h2database.com/html/license.html).
-- Initial Developer: H2 Group
--

CREATE TABLE TEST(B1 BINARY, B2 BINARY(10));
> ok

SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_OCTET_LENGTH FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = 'TEST' ORDER BY ORDINAL_POSITION;
> COLUMN_NAME DATA_TYPE CHARACTER_OCTET_LENGTH
> ----------- --------- ----------------------
> B1          BINARY    1
> B2          BINARY    10
> rows (ordered): 2

DROP TABLE TEST;
> ok

SELECT CAST(X'11' AS BINARY) || CAST(NULL AS BINARY);
>> null

SELECT CAST(NULL AS BINARY) || CAST(X'11' AS BINARY);
>> null

EXPLAIN VALUES CAST(X'01' AS BINARY);
>> VALUES (CAST(X'01' AS BINARY(1)))

CREATE TABLE T(C BINARY(0));
> exception INVALID_VALUE_2

VALUES CAST(X'0102' AS BINARY);
>> X'01'

CREATE TABLE T1(A BINARY(1048576));
> ok

CREATE TABLE T2(A BINARY(1048577));
> exception INVALID_VALUE_PRECISION

SET TRUNCATE_LARGE_LENGTH TRUE;
> ok

CREATE TABLE T2(A BINARY(1048577));
> ok

SELECT TABLE_NAME, CHARACTER_OCTET_LENGTH FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'PUBLIC';
> TABLE_NAME CHARACTER_OCTET_LENGTH
> ---------- ----------------------
> T1         1048576
> T2         1048576
> rows: 2

SET TRUNCATE_LARGE_LENGTH FALSE;
> ok

DROP TABLE T1, T2;
> ok
