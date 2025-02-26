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

-- SPEX CHANGED: BEGIN
CREATE USER db_0 identified by db_0;
GRANT ALL PRIVILEGES TO db_0;
ALTER USER db_0 QUOTA UNLIMITED ON USERS;
CREATE USER db_1 identified by db_1;
GRANT ALL PRIVILEGES TO db_1;
ALTER USER db_1 QUOTA UNLIMITED ON USERS;
CREATE USER db_2 identified by db_2;
GRANT ALL PRIVILEGES TO db_2;
ALTER USER db_2 QUOTA UNLIMITED ON USERS;
CREATE USER db_3 identified by db_3;
GRANT ALL PRIVILEGES TO db_3;
ALTER USER db_3 QUOTA UNLIMITED ON USERS;
CREATE USER db_4 identified by db_4;
GRANT ALL PRIVILEGES TO db_4;
ALTER USER db_4 QUOTA UNLIMITED ON USERS;
CREATE USER db_5 identified by db_5;
GRANT ALL PRIVILEGES TO db_5;
ALTER USER db_5 QUOTA UNLIMITED ON USERS;
CREATE USER db_6 identified by db_6;
GRANT ALL PRIVILEGES TO db_6;
ALTER USER db_6 QUOTA UNLIMITED ON USERS;
CREATE USER db_7 identified by db_7;
GRANT ALL PRIVILEGES TO db_7;
ALTER USER db_7 QUOTA UNLIMITED ON USERS;
CREATE USER db_8 identified by db_8;
GRANT ALL PRIVILEGES TO db_8;
ALTER USER db_8 QUOTA UNLIMITED ON USERS;
CREATE USER db_9 identified by db_9;
GRANT ALL PRIVILEGES TO db_9;
ALTER USER db_9 QUOTA UNLIMITED ON USERS;

CREATE TABLE db_0.t_order (order_id NUMBER(19,0), user_id INT NOT NULL, status VARCHAR(50) NOT NULL, merchant_id INT, remark VARCHAR(50) NOT NULL, creation_date DATE NOT NULL);
CREATE TABLE db_0.t_order_item (item_id NUMBER(19,0) PRIMARY KEY, order_id NUMBER(19,0) NOT NULL, user_id INT NOT NULL, product_id INT NOT NULL, quantity INT NOT NULL, creation_date DATE NOT NULL);
CREATE TABLE db_0.t_user (user_id INT PRIMARY KEY, user_name VARCHAR(50) NOT NULL, password VARCHAR(50) NOT NULL, email VARCHAR(50) NOT NULL, telephone CHAR(11) NOT NULL, creation_date DATE NOT NULL);
CREATE TABLE db_0.t_product (product_id INT PRIMARY KEY, product_name VARCHAR(50) NOT NULL, category_id INT NOT NULL, price DECIMAL NOT NULL, status VARCHAR(50) NOT NULL, creation_date DATE NOT NULL);
CREATE TABLE db_0.t_product_category (category_id INT PRIMARY KEY, category_name VARCHAR(50) NOT NULL, parent_id INT NOT NULL, "LEVEL" NUMBER(3,0) NOT NULL, creation_date DATE NOT NULL);
CREATE TABLE db_0.t_country (country_id SMALLINT PRIMARY KEY, country_name VARCHAR(50), continent_name VARCHAR(50), creation_date DATE NOT NULL);
CREATE TABLE db_0.t_single_table (single_id INT NOT NULL, id INT NOT NULL, status VARCHAR(45) NULL, PRIMARY KEY (single_id));
CREATE TABLE db_0.t_broadcast_table (id INT NOT NULL, status VARCHAR(45) NULL, PRIMARY KEY (id));
CREATE INDEX db_0.order_index_t_order ON db_0.t_order (order_id);

CREATE TABLE db_1.t_order (order_id NUMBER(19,0), user_id INT NOT NULL, status VARCHAR(50) NOT NULL, merchant_id INT, remark VARCHAR(50) NOT NULL, creation_date DATE NOT NULL);
CREATE TABLE db_1.t_order_item (item_id NUMBER(19,0) PRIMARY KEY, order_id NUMBER(19,0) NOT NULL, user_id INT NOT NULL, product_id INT NOT NULL, quantity INT NOT NULL, creation_date DATE NOT NULL);
CREATE TABLE db_1.t_merchant (merchant_id INT PRIMARY KEY, country_id SMALLINT NOT NULL, merchant_name VARCHAR(50) NOT NULL, business_code VARCHAR(50) NOT NULL, telephone CHAR(11) NOT NULL, creation_date DATE NOT NULL);
CREATE TABLE db_1.t_product_detail (detail_id INT PRIMARY KEY, product_id INT NOT NULL, description VARCHAR(50) NOT NULL, creation_date DATE NOT NULL);
CREATE TABLE db_1.t_product_category (category_id INT PRIMARY KEY, category_name VARCHAR(50) NOT NULL, parent_id INT NOT NULL, "LEVEL" NUMBER(3,0) NOT NULL, creation_date DATE NOT NULL);
CREATE TABLE db_1.t_country (country_id SMALLINT PRIMARY KEY, country_name VARCHAR(50), continent_name VARCHAR(50), creation_date DATE NOT NULL);
CREATE TABLE db_1.t_broadcast_table (id INT NOT NULL, status VARCHAR(45) NULL, PRIMARY KEY (id));
CREATE INDEX db_1.order_index_t_order ON db_1.t_order (order_id);

CREATE TABLE db_2.t_order (order_id NUMBER(19,0), user_id INT NOT NULL, status VARCHAR(50) NOT NULL, merchant_id INT, remark VARCHAR(50) NOT NULL, creation_date DATE NOT NULL);
CREATE TABLE db_2.t_order_item (item_id NUMBER(19,0) PRIMARY KEY, order_id NUMBER(19,0) NOT NULL, user_id INT NOT NULL, product_id INT NOT NULL, quantity INT NOT NULL, creation_date DATE NOT NULL);
CREATE TABLE db_2.t_product_category (category_id INT PRIMARY KEY, category_name VARCHAR(50) NOT NULL, parent_id INT NOT NULL, "LEVEL" NUMBER(3,0) NOT NULL, creation_date DATE NOT NULL);
CREATE TABLE db_2.t_country (country_id SMALLINT PRIMARY KEY, country_name VARCHAR(50), continent_name VARCHAR(50), creation_date DATE NOT NULL);
CREATE TABLE db_2.t_broadcast_table (id INT NOT NULL, status VARCHAR(45) NULL, PRIMARY KEY (id));
CREATE INDEX db_2.order_index_t_order ON db_2.t_order (order_id);

CREATE TABLE db_3.t_order (order_id NUMBER(19,0), user_id INT NOT NULL, status VARCHAR(50) NOT NULL, merchant_id INT, remark VARCHAR(50) NOT NULL, creation_date DATE NOT NULL);
CREATE TABLE db_3.t_order_item (item_id NUMBER(19,0) PRIMARY KEY, order_id NUMBER(19,0) NOT NULL, user_id INT NOT NULL, product_id INT NOT NULL, quantity INT NOT NULL, creation_date DATE NOT NULL);
CREATE TABLE db_3.t_product_category (category_id INT PRIMARY KEY, category_name VARCHAR(50) NOT NULL, parent_id INT NOT NULL, "LEVEL" NUMBER(3,0) NOT NULL, creation_date DATE NOT NULL);
CREATE TABLE db_3.t_country (country_id SMALLINT PRIMARY KEY, country_name VARCHAR(50), continent_name VARCHAR(50), creation_date DATE NOT NULL);
CREATE TABLE db_3.t_broadcast_table (id INT NOT NULL, status VARCHAR(45) NULL, PRIMARY KEY (id));
CREATE INDEX db_3.order_index_t_order ON db_3.t_order (order_id);

CREATE TABLE db_4.t_order (order_id NUMBER(19,0), user_id INT NOT NULL, status VARCHAR(50) NOT NULL, merchant_id INT, remark VARCHAR(50) NOT NULL, creation_date DATE NOT NULL);
CREATE TABLE db_4.t_order_item (item_id NUMBER(19,0) PRIMARY KEY, order_id NUMBER(19,0) NOT NULL, user_id INT NOT NULL, product_id INT NOT NULL, quantity INT NOT NULL, creation_date DATE NOT NULL);
CREATE TABLE db_4.t_product_category (category_id INT PRIMARY KEY, category_name VARCHAR(50) NOT NULL, parent_id INT NOT NULL, "LEVEL" NUMBER(3,0) NOT NULL, creation_date DATE NOT NULL);
CREATE TABLE db_4.t_country (country_id SMALLINT PRIMARY KEY, country_name VARCHAR(50), continent_name VARCHAR(50), creation_date DATE NOT NULL);
CREATE TABLE db_4.t_broadcast_table (id INT NOT NULL, status VARCHAR(45) NULL, PRIMARY KEY (id));
CREATE INDEX db_4.order_index_t_order ON db_4.t_order (order_id);

CREATE TABLE db_5.t_order (order_id NUMBER(19,0), user_id INT NOT NULL, status VARCHAR(50) NOT NULL, merchant_id INT, remark VARCHAR(50) NOT NULL, creation_date DATE NOT NULL);
CREATE TABLE db_5.t_order_item (item_id NUMBER(19,0) PRIMARY KEY, order_id NUMBER(19,0) NOT NULL, user_id INT NOT NULL, product_id INT NOT NULL, quantity INT NOT NULL, creation_date DATE NOT NULL);
CREATE TABLE db_5.t_product_category (category_id INT PRIMARY KEY, category_name VARCHAR(50) NOT NULL, parent_id INT NOT NULL, "LEVEL" NUMBER(3,0) NOT NULL, creation_date DATE NOT NULL);
CREATE TABLE db_5.t_country (country_id SMALLINT PRIMARY KEY, country_name VARCHAR(50), continent_name VARCHAR(50), creation_date DATE NOT NULL);
CREATE TABLE db_5.t_broadcast_table (id INT NOT NULL, status VARCHAR(45) NULL, PRIMARY KEY (id));
CREATE INDEX db_5.order_index_t_order ON db_5.t_order (order_id);

CREATE TABLE db_6.t_order (order_id NUMBER(19,0), user_id INT NOT NULL, status VARCHAR(50) NOT NULL, merchant_id INT, remark VARCHAR(50) NOT NULL, creation_date DATE NOT NULL);
CREATE TABLE db_6.t_order_item (item_id NUMBER(19,0) PRIMARY KEY, order_id NUMBER(19,0) NOT NULL, user_id INT NOT NULL, product_id INT NOT NULL, quantity INT NOT NULL, creation_date DATE NOT NULL);
CREATE TABLE db_6.t_product_category (category_id INT PRIMARY KEY, category_name VARCHAR(50) NOT NULL, parent_id INT NOT NULL, "LEVEL" NUMBER(3,0) NOT NULL, creation_date DATE NOT NULL);
CREATE TABLE db_6.t_country (country_id SMALLINT PRIMARY KEY, country_name VARCHAR(50), continent_name VARCHAR(50), creation_date DATE NOT NULL);
CREATE TABLE db_6.t_broadcast_table (id INT NOT NULL, status VARCHAR(45) NULL, PRIMARY KEY (id));
CREATE INDEX db_6.order_index_t_order ON db_6.t_order (order_id);

CREATE TABLE db_7.t_order (order_id NUMBER(19,0), user_id INT NOT NULL, status VARCHAR(50) NOT NULL, merchant_id INT, remark VARCHAR(50) NOT NULL, creation_date DATE NOT NULL);
CREATE TABLE db_7.t_order_item (item_id NUMBER(19,0) PRIMARY KEY, order_id NUMBER(19,0) NOT NULL, user_id INT NOT NULL, product_id INT NOT NULL, quantity INT NOT NULL, creation_date DATE NOT NULL);
CREATE TABLE db_7.t_product_category (category_id INT PRIMARY KEY, category_name VARCHAR(50) NOT NULL, parent_id INT NOT NULL, "LEVEL" NUMBER(3,0) NOT NULL, creation_date DATE NOT NULL);
CREATE TABLE db_7.t_country (country_id SMALLINT PRIMARY KEY, country_name VARCHAR(50), continent_name VARCHAR(50), creation_date DATE NOT NULL);
CREATE TABLE db_7.t_broadcast_table (id INT NOT NULL, status VARCHAR(45) NULL, PRIMARY KEY (id));
CREATE INDEX db_7.order_index_t_order ON db_7.t_order (order_id);

CREATE TABLE db_8.t_order (order_id NUMBER(19,0), user_id INT NOT NULL, status VARCHAR(50) NOT NULL, merchant_id INT, remark VARCHAR(50) NOT NULL, creation_date DATE NOT NULL);
CREATE TABLE db_8.t_order_item (item_id NUMBER(19,0) PRIMARY KEY, order_id NUMBER(19,0) NOT NULL, user_id INT NOT NULL, product_id INT NOT NULL, quantity INT NOT NULL, creation_date DATE NOT NULL);
CREATE TABLE db_8.t_product_category (category_id INT PRIMARY KEY, category_name VARCHAR(50) NOT NULL, parent_id INT NOT NULL, "LEVEL" NUMBER(3,0) NOT NULL, creation_date DATE NOT NULL);
CREATE TABLE db_8.t_country (country_id SMALLINT PRIMARY KEY, country_name VARCHAR(50), continent_name VARCHAR(50), creation_date DATE NOT NULL);
CREATE TABLE db_8.t_broadcast_table (id INT NOT NULL, status VARCHAR(45) NULL, PRIMARY KEY (id));
CREATE INDEX db_8.order_index_t_order ON db_8.t_order (order_id);

CREATE TABLE db_9.t_order (order_id NUMBER(19,0), user_id INT NOT NULL, status VARCHAR(50) NOT NULL, merchant_id INT, remark VARCHAR(50) NOT NULL, creation_date DATE NOT NULL);
CREATE TABLE db_9.t_order_item (item_id NUMBER(19,0) PRIMARY KEY, order_id NUMBER(19,0) NOT NULL, user_id INT NOT NULL, product_id INT NOT NULL, quantity INT NOT NULL, creation_date DATE NOT NULL);
CREATE TABLE db_9.t_product_category (category_id INT PRIMARY KEY, category_name VARCHAR(50) NOT NULL, parent_id INT NOT NULL, "LEVEL" NUMBER(3,0) NOT NULL, creation_date DATE NOT NULL);
CREATE TABLE db_9.t_country (country_id SMALLINT PRIMARY KEY, country_name VARCHAR(50), continent_name VARCHAR(50), creation_date DATE NOT NULL);
CREATE TABLE db_9.t_broadcast_table (id INT NOT NULL, status VARCHAR(45) NULL, PRIMARY KEY (id));
CREATE INDEX db_9.order_index_t_order ON db_9.t_order (order_id);
-- SPEX CHANGED: END
