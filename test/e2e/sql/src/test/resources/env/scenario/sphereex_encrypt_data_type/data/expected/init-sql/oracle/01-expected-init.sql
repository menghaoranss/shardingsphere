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
CREATE USER expected_dataset identified by expected_dataset;
GRANT ALL PRIVILEGES TO expected_dataset;
ALTER USER expected_dataset QUOTA UNLIMITED ON USERS;

CREATE TABLE expected_dataset.T_NUMBER_ORACLE (ID INT, A_NUMBER NUMBER, A_NUMBER_P NUMBER(20), A_NUMBER_PS NUMBER(20, 6), A_NUMBER_PS_LE NUMBER(4, 6), A_FLOAT FLOAT, A_FLOAT_P FLOAT(100), A_BINARY_FLOAT BINARY_FLOAT, A_BINARY_DOUBLE BINARY_DOUBLE, A_NUMERIC NUMERIC, A_NUMERIC_P NUMERIC(10), A_NUMERIC_PS NUMERIC(20,5), A_DECIMAL DECIMAL, A_DECIMAL_P DECIMAL(20), A_DECIMAL_PS DECIMAL(20,6),  A_DEC DEC, A_DEC_P DEC(20), A_DEC_PS DEC(20,6), A_INTEGER INTEGER, A_INT INT, A_SMALLINT SMALLINT, A_DOUBLE_PRECISION DOUBLE PRECISION, A_REAL REAL);
CREATE TABLE expected_dataset.T_INSERT_DATA_TYPE (ID INT, NAME_CLOB CLOB, PASSWORD VARCHAR2(100));
CREATE TABLE expected_dataset.T_SELECT_DATA_TYPE (ID INT, NAME_CLOB CLOB, PASSWORD VARCHAR2(100));
--  SPEX ADDED: END
