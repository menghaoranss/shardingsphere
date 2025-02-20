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

CREATE TABLE expected_dataset.T_USER (USER_ID INT PRIMARY KEY, USER_NAME VARCHAR(50) NOT NULL, PASSWORD VARCHAR(50) NOT NULL, EMAIL VARCHAR(50) NOT NULL, TELEPHONE CHAR(11) NOT NULL, CREATION_DATE DATE NOT NULL);
CREATE TABLE expected_dataset.T_ORDER_ITEM (ITEM_ID NUMBER(19,0) PRIMARY KEY, ORDER_ID NUMBER(19,0) NOT NULL, USER_ID INT NOT NULL, PRODUCT_ID INT NOT NULL, QUANTITY INT NOT NULL, CREATION_DATE DATE NOT NULL);
CREATE TABLE expected_dataset.T_PRODUCT_EXTEND (EXTEND_ID INT PRIMARY KEY, PRODUCT_ID INT NOT NULL, PRODUCT_DESC VARCHAR(50));
--  SPEX ADDED: END
