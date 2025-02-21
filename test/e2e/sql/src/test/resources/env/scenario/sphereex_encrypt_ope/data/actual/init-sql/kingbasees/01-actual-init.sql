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
CREATE DATABASE sphereex_encrypt_ope;

GRANT ALL PRIVILEGES ON DATABASE sphereex_encrypt_ope TO test_user;

DROP TABLE IF EXISTS t_user;
DROP TABLE IF EXISTS t_order_item;
DROP TABLE IF EXISTS t_product_extend;

CREATE TABLE t_user (user_id INT PRIMARY KEY, user_name_plain VARCHAR(50) NOT NULL, user_name_cipher VARCHAR(100) NOT NULL, password VARCHAR(50) NOT NULL, email VARCHAR(50) NOT NULL, telephone CHAR(11) NOT NULL, creation_date DATE NOT NULL);
CREATE TABLE t_order_item (item_id BIGINT PRIMARY KEY, order_id BIGINT NOT NULL, user_id INT NOT NULL, product_id INT NOT NULL, quantity_plain INT NOT NULL, quantity_cipher VARCHAR(100) NOT NULL, quantity_order VARCHAR(100) NOT NULL, creation_date DATE NOT NULL);
CREATE TABLE t_product_extend (extend_id INT PRIMARY KEY, product_id INT NOT NULL, product_desc_plain CHARACTER VARYING(50), product_desc_cipher CHARACTER VARYING(400), product_desc_order CHARACTER VARYING(400));
--  SPEX ADDED: END
