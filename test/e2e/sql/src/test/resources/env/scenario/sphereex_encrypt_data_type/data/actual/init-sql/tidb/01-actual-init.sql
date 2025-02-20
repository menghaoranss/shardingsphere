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

DROP DATABASE IF EXISTS sphereex_encrypt_data_type;
CREATE DATABASE sphereex_encrypt_data_type;

CREATE TABLE sphereex_encrypt_data_type.t_user (user_id INT PRIMARY KEY, user_name VARCHAR(200), age_cipher VARCHAR(200) NOT NULL, age_plain int NOT NULL);

create table sphereex_encrypt_data_type.t_number (id int, asmallint_cipher varchar(200), aMEDIUMINT_cipher varchar(200), aintunsigned_cipher varchar(200), aint_cipher varchar(200), ainteger_cipher varchar(200), aintegerunsigned_cipher varchar(200), abigint_cipher varchar(200), abigintunsigned_cipher varchar(200), adecimal_cipher varchar(200), anumeric_cipher varchar(200), afixed_cipher varchar(200), afloat_cipher varchar(200), afloatbig_cipher varchar(200), afloutdouble_cipher varchar(200), adouble_cipher varchar(200), adoubleprecision_cipher varchar(200), areal_cipher varchar(200));
--  SPEX ADDED: END
