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
CREATE USER sphereex_encrypt_data_type identified by sphereex_encrypt_data_type;

CREATE TABLE sphereex_encrypt_data_type.T_NUMBER_ORACLE (ID INT, A_NUMBER_CIPHER varchar2(100), A_NUMBER_P_CIPHER varchar2(100), A_NUMBER_PS_CIPHER varchar2(100), A_NUMBER_PS_LE_CIPHER varchar2(100), A_FLOAT_CIPHER varchar2(100), A_FLOAT_P_CIPHER varchar2(100), A_BINARY_FLOAT_CIPHER varchar2(100), A_BINARY_DOUBLE_CIPHER varchar2(100), A_NUMERIC_CIPHER varchar(100), A_NUMERIC_P_CIPHER varchar(100), A_NUMERIC_PS_CIPHER varchar(100), A_DECIMAL_CIPHER varchar(100), A_DECIMAL_P_CIPHER varchar(100), A_DECIMAL_PS_CIPHER varchar(100),  A_DEC_CIPHER varchar(100), A_DEC_P_CIPHER varchar(100), A_DEC_PS_CIPHER varchar(100), A_INTEGER_CIPHER varchar(100), A_INT_CIPHER varchar(100), A_SMALLINT_CIPHER varchar(100), A_DOUBLE_PRECISION_CIPHER varchar(100), A_REAL_CIPHER varchar(100));
CREATE TABLE sphereex_encrypt_data_type.T_INSERT_DATA_TYPE(ID INT primary key, NAME_CLOB CLOB, PASSWORD_PLAIN VARCHAR2(50), PASSWORD_CIPHER VARCHAR2(100));
CREATE TABLE sphereex_encrypt_data_type.T_SELECT_DATA_TYPE(ID INT primary key, NAME_CLOB CLOB, PASSWORD_PLAIN VARCHAR2(50), PASSWORD_CIPHER VARCHAR2(100));
--  SPEX ADDED: END
