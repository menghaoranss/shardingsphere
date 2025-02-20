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
CREATE USER write_dataset identified by write_dataset;
GRANT ALL PRIVILEGES TO write_dataset;
ALTER USER write_dataset QUOTA UNLIMITED ON USERS;
-- SPEX CHANGED: END

CREATE TABLE write_dataset.t_single_table (single_id INT NOT NULL, id INT NOT NULL, status VARCHAR(45) NULL, PRIMARY KEY (single_id));
CREATE TABLE write_dataset.t_user (user_id INT NOT NULL, address_id INT NOT NULL, pwd VARCHAR(45) NULL, status VARCHAR(45) NULL, PRIMARY KEY (user_id));
CREATE TABLE write_dataset.t_user_item (item_id INT NOT NULL, user_id INT NOT NULL, status VARCHAR(45) NULL, creation_date DATE, PRIMARY KEY (item_id));
CREATE TABLE write_dataset.t_user_info (user_id INT NOT NULL, information VARCHAR(45) NULL, PRIMARY KEY (user_id));
CREATE TABLE write_dataset.t_merchant (merchant_id INT PRIMARY KEY, country_id SMALLINT NOT NULL, merchant_name VARCHAR(50) NOT NULL, business_code VARCHAR(50) NOT NULL, telephone CHAR(11) NOT NULL, creation_date DATE NOT NULL);

CREATE INDEX user_index_t_user ON write_dataset.t_user (user_id);


CREATE USER read_dataset identified by read_dataset;
GRANT ALL PRIVILEGES TO read_dataset;
ALTER USER read_dataset QUOTA UNLIMITED ON USERS;

CREATE TABLE read_dataset.t_single_table (single_id INT NOT NULL, id INT NOT NULL, status VARCHAR(45) NULL, PRIMARY KEY (single_id));
CREATE TABLE read_dataset.t_user (user_id INT NOT NULL, address_id INT NOT NULL, pwd VARCHAR(45) NULL, status VARCHAR(45) NULL, PRIMARY KEY (user_id));
CREATE TABLE read_dataset.t_user_item (item_id INT NOT NULL, user_id INT NOT NULL, status VARCHAR(45) NULL, creation_date DATE, PRIMARY KEY (item_id));
CREATE TABLE read_dataset.t_user_info (user_id INT NOT NULL, information VARCHAR(45) NULL, PRIMARY KEY (user_id));
CREATE TABLE read_dataset.t_merchant (merchant_id INT PRIMARY KEY, country_id SMALLINT NOT NULL, merchant_name VARCHAR(50) NOT NULL, business_code VARCHAR(50) NOT NULL, telephone CHAR(11) NOT NULL, creation_date DATE NOT NULL);

CREATE INDEX user_index_t_user ON read_dataset.t_user (user_id);
