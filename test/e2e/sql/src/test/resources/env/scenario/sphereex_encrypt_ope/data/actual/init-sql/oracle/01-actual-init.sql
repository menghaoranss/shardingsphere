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
CREATE USER sphereex_encrypt_ope identified by sphereex_encrypt_ope;
GRANT ALL PRIVILEGES TO sphereex_encrypt_ope;
ALTER USER sphereex_encrypt_ope QUOTA UNLIMITED ON USERS;

CREATE TABLE sphereex_encrypt_ope.T_USER (USER_ID INT PRIMARY KEY, USER_NAME_PLAIN VARCHAR(50) NOT NULL, USER_NAME_CIPHER VARCHAR(100) NOT NULL, PASSWORD VARCHAR(50) NOT NULL, EMAIL VARCHAR(50) NOT NULL, TELEPHONE CHAR(11) NOT NULL, CREATION_DATE DATE NOT NULL);
CREATE TABLE sphereex_encrypt_ope.T_ORDER_ITEM (ITEM_ID NUMBER(19,0) PRIMARY KEY, ORDER_ID NUMBER(19,0) NOT NULL, USER_ID INT NOT NULL, PRODUCT_ID INT NOT NULL, QUANTITY_PLAIN INT NOT NULL, QUANTITY_CIPHER VARCHAR(100) NOT NULL, QUANTITY_ORDER VARCHAR(100) NOT NULL, CREATION_DATE DATE NOT NULL);
CREATE TABLE sphereex_encrypt_ope.T_PRODUCT (PRODUCT_ID INT PRIMARY KEY, PRODUCT_NAME VARCHAR(50) NOT NULL, CATEGORY_ID INT NOT NULL, PRICE_PLAIN DECIMAL(10, 0), PRICE_CIPHER VARCHAR(400), STATUS VARCHAR(50) NOT NULL, CREATION_DATE DATE NOT NULL);
CREATE TABLE sphereex_encrypt_ope.T_PRODUCT_EXTEND (EXTEND_ID INT PRIMARY KEY, PRODUCT_ID INT NOT NULL, PRODUCT_DESC_PLAIN VARCHAR(50), PRODUCT_DESC_CIPHER VARCHAR(400), PRODUCT_DESC_ORDER VARCHAR(400));
--  SPEX ADDED: END
