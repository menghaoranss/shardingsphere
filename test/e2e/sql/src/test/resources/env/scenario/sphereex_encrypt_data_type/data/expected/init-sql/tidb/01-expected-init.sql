--
-- Licensed to the Apache Software Foundation (ASF) under one or more
-- contributor license agreements.  See the NOTICE file distributed with
-- this work for additional information regarding copyright ownership.
-- The ASF licenses this file to You under the Apache License, Version 2.0
-- (the "License"); you may not use this file except in compliance with
-- the License.  You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.
--

--  SPEX ADDED: BEGIN
SET character_set_database='utf8';
SET character_set_server='utf8';

DROP DATABASE IF EXISTS expected_dataset;
CREATE DATABASE expected_dataset;

CREATE TABLE expected_dataset.t_user (user_id INT PRIMARY KEY, user_name VARCHAR(200), age int NOT NULL);

CREATE TABLE expected_dataset.t_number (id INT, asmallint SMALLINT(100), aMEDIUMINT MEDIUMINT(100) UNSIGNED, aintunsigned INT(100) UNSIGNED, aint INT, ainteger INTEGER, aintegerunsigned INTEGER UNSIGNED, abigint BIGINT, abigintunsigned BIGINT UNSIGNED, adecimal DECIMAL(10,5), anumeric NUMERIC(10,6), afixed FIXED(10,6), afloat FLOAT(11), afloatbig FLOAT(53), afloutdouble FLOAT(10,4), adouble DOUBLE(10,6), adoubleprecision DOUBLE PRECISION(10,6), areal REAL);
--  SPEX ADDED: END
