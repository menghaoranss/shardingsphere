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
CREATE USER mask_encrypt_ds_0 identified by mask_encrypt_ds_0;
GRANT ALL PRIVILEGES TO mask_encrypt_ds_0;
ALTER USER mask_encrypt_ds_0 QUOTA UNLIMITED ON USERS;
CREATE USER mask_encrypt_ds_1 identified by mask_encrypt_ds_1;
GRANT ALL PRIVILEGES TO mask_encrypt_ds_1;
ALTER USER mask_encrypt_ds_1 QUOTA UNLIMITED ON USERS;
CREATE USER mask_encrypt_ds_2 identified by mask_encrypt_ds_2;
GRANT ALL PRIVILEGES TO mask_encrypt_ds_2;
ALTER USER mask_encrypt_ds_2 QUOTA UNLIMITED ON USERS;
CREATE USER mask_encrypt_ds_3 identified by mask_encrypt_ds_3;
GRANT ALL PRIVILEGES TO mask_encrypt_ds_3;
ALTER USER mask_encrypt_ds_3 QUOTA UNLIMITED ON USERS;
CREATE USER mask_encrypt_ds_4 identified by mask_encrypt_ds_4;
GRANT ALL PRIVILEGES TO mask_encrypt_ds_4;
ALTER USER mask_encrypt_ds_4 QUOTA UNLIMITED ON USERS;
CREATE USER mask_encrypt_ds_5 identified by mask_encrypt_ds_5;
GRANT ALL PRIVILEGES TO mask_encrypt_ds_5;
ALTER USER mask_encrypt_ds_5 QUOTA UNLIMITED ON USERS;
CREATE USER mask_encrypt_ds_6 identified by mask_encrypt_ds_6;
GRANT ALL PRIVILEGES TO mask_encrypt_ds_6;
ALTER USER mask_encrypt_ds_6 QUOTA UNLIMITED ON USERS;
CREATE USER mask_encrypt_ds_7 identified by mask_encrypt_ds_7;
GRANT ALL PRIVILEGES TO mask_encrypt_ds_7;
ALTER USER mask_encrypt_ds_7 QUOTA UNLIMITED ON USERS;
CREATE USER mask_encrypt_ds_8 identified by mask_encrypt_ds_8;
GRANT ALL PRIVILEGES TO mask_encrypt_ds_8;
ALTER USER mask_encrypt_ds_8 QUOTA UNLIMITED ON USERS;
CREATE USER mask_encrypt_ds_9 identified by mask_encrypt_ds_9;
GRANT ALL PRIVILEGES TO mask_encrypt_ds_9;
ALTER USER mask_encrypt_ds_9 QUOTA UNLIMITED ON USERS;
-- SPEX CHANGED: END

-- SPEX CHANGED: BEGIN
CREATE TABLE mask_encrypt_ds_0.t_user (user_id INT PRIMARY KEY, user_name_cipher VARCHAR(50) NOT NULL, user_name_plain VARCHAR(50) NOT NULL, password_cipher VARCHAR(50) NOT NULL, email_cipher VARCHAR(50) NOT NULL, telephone_cipher CHAR(50) NOT NULL, telephone_plain CHAR(11) NOT NULL, creation_date DATE NOT NULL);
CREATE TABLE mask_encrypt_ds_1.t_user (user_id INT PRIMARY KEY, user_name_cipher VARCHAR(50) NOT NULL, user_name_plain VARCHAR(50) NOT NULL, password_cipher VARCHAR(50) NOT NULL, email_cipher VARCHAR(50) NOT NULL, telephone_cipher CHAR(50) NOT NULL, telephone_plain CHAR(11) NOT NULL, creation_date DATE NOT NULL);
CREATE TABLE mask_encrypt_ds_2.t_user (user_id INT PRIMARY KEY, user_name_cipher VARCHAR(50) NOT NULL, user_name_plain VARCHAR(50) NOT NULL, password_cipher VARCHAR(50) NOT NULL, email_cipher VARCHAR(50) NOT NULL, telephone_cipher CHAR(50) NOT NULL, telephone_plain CHAR(11) NOT NULL, creation_date DATE NOT NULL);
CREATE TABLE mask_encrypt_ds_3.t_user (user_id INT PRIMARY KEY, user_name_cipher VARCHAR(50) NOT NULL, user_name_plain VARCHAR(50) NOT NULL, password_cipher VARCHAR(50) NOT NULL, email_cipher VARCHAR(50) NOT NULL, telephone_cipher CHAR(50) NOT NULL, telephone_plain CHAR(11) NOT NULL, creation_date DATE NOT NULL);
CREATE TABLE mask_encrypt_ds_4.t_user (user_id INT PRIMARY KEY, user_name_cipher VARCHAR(50) NOT NULL, user_name_plain VARCHAR(50) NOT NULL, password_cipher VARCHAR(50) NOT NULL, email_cipher VARCHAR(50) NOT NULL, telephone_cipher CHAR(50) NOT NULL, telephone_plain CHAR(11) NOT NULL, creation_date DATE NOT NULL);
CREATE TABLE mask_encrypt_ds_5.t_user (user_id INT PRIMARY KEY, user_name_cipher VARCHAR(50) NOT NULL, user_name_plain VARCHAR(50) NOT NULL, password_cipher VARCHAR(50) NOT NULL, email_cipher VARCHAR(50) NOT NULL, telephone_cipher CHAR(50) NOT NULL, telephone_plain CHAR(11) NOT NULL, creation_date DATE NOT NULL);
CREATE TABLE mask_encrypt_ds_6.t_user (user_id INT PRIMARY KEY, user_name_cipher VARCHAR(50) NOT NULL, user_name_plain VARCHAR(50) NOT NULL, password_cipher VARCHAR(50) NOT NULL, email_cipher VARCHAR(50) NOT NULL, telephone_cipher CHAR(50) NOT NULL, telephone_plain CHAR(11) NOT NULL, creation_date DATE NOT NULL);
CREATE TABLE mask_encrypt_ds_7.t_user (user_id INT PRIMARY KEY, user_name_cipher VARCHAR(50) NOT NULL, user_name_plain VARCHAR(50) NOT NULL, password_cipher VARCHAR(50) NOT NULL, email_cipher VARCHAR(50) NOT NULL, telephone_cipher CHAR(50) NOT NULL, telephone_plain CHAR(11) NOT NULL, creation_date DATE NOT NULL);
CREATE TABLE mask_encrypt_ds_8.t_user (user_id INT PRIMARY KEY, user_name_cipher VARCHAR(50) NOT NULL, user_name_plain VARCHAR(50) NOT NULL, password_cipher VARCHAR(50) NOT NULL, email_cipher VARCHAR(50) NOT NULL, telephone_cipher CHAR(50) NOT NULL, telephone_plain CHAR(11) NOT NULL, creation_date DATE NOT NULL);
CREATE TABLE mask_encrypt_ds_9.t_user (user_id INT PRIMARY KEY, user_name_cipher VARCHAR(50) NOT NULL, user_name_plain VARCHAR(50) NOT NULL, password_cipher VARCHAR(50) NOT NULL, email_cipher VARCHAR(50) NOT NULL, telephone_cipher CHAR(50) NOT NULL, telephone_plain CHAR(11) NOT NULL, creation_date DATE NOT NULL);
-- SPEX CHANGED: END
