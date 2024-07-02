from datetime import datetime, time
import time
import mysql.connector
from FastAPI.myAPI.myAPI.src.routers import castor_tools


# creating DB connection and cursor
def create_db_connector(db_name=str("none")):
    # db_host, db_port, db_user, db_pass = castor_tools.get_mysql_creds()
    db_host = "localhost"
    db_port = 3306
    db_user = "admin"
    db_pass = "adminoxilio"

    if db_name.lower() == "none":
        my_sql_conn = mysql.connector.connect(
            host=db_host,
            port=db_port,
            user=db_user,
            password=db_pass
        )
    else:
        my_sql_conn = mysql.connector.connect(
            host=db_host,
            port=db_port,
            user=db_user,
            password=db_pass,
            database=db_name
        )

    return my_sql_conn


# creating DB connection and cursor by client
def create_db_connector_by_client(db_name=str("none"), db_user=str, db_pass=str):
    # db_host, db_port, db_user, db_pass = castor_tools.get_mysql_creds()
    db_host = "localhost"
    db_port = 3306

    if db_name.lower() == "none":
        my_sql_conn = mysql.connector.connect(
            host=db_host,
            port=db_port,
            user=db_user,
            password=db_pass
        )
    else:
        my_sql_conn = mysql.connector.connect(
            host=db_host,
            port=db_port,
            user=db_user,
            password=db_pass,
            database=db_name
        )

    return my_sql_conn


#Retrieve username and password from Oxilio client db
def retrieve_client_credentials(client_id=int):
    try:
        conn = create_db_connector()
        c = conn.cursor()
        sql = f"SELECT user_name, aes_decrypt(password,'oxiliotakesgoodcareofitsemployees2024!') FROM oxilioclients.gen WHERE client_id={client_id}; "
        conn.commit()
        conn.close()
    except (mysql.connector.errors.ProgrammingError,
            mysql.connector.errors.DatabaseError) as e:
        print(f"Exception: {e} \n")


#Create user
def create_user_credentials(db_user=str, db_pass=str):
    records = ""
    try:
        conn = create_db_connector()
        c = conn.cursor()
        sql = f"CREATE USER '{db_user}' IDENTIFIED WITH mysql_native_password BY '{db_pass}'"
        c.execute(sql)
        records = c.fetchall()
        conn.close()
    except (mysql.connector.errors.ProgrammingError,
            mysql.connector.errors.DatabaseError) as e:
        print(f"Exception: {e} \n")
        print(f"Record about user do not exist.")

    return records


def check_db_exist(db_name=str):
    db_exist = False

    conn = create_db_connector()
    c = conn.cursor()
    sql = f"SHOW DATABASES"  # get a list of all databases
    c.execute(sql)
    sql_result = c.fetchall()
    num_items = len(sql_result)

    if num_items > 0:
        for db in sql_result:
            if db[0].lower() == db_name.lower():
                db_exist = True

    return db_exist


def list_existing_db():
    sql_result = 0
    try:
        conn = create_db_connector()
        c = conn.cursor()
        sql = f"SHOW DATABASES"  # get a list of all databases
        c.execute(sql)
        sql_result = c.fetchall()
    except (mysql.connector.errors.ProgrammingError, UnboundLocalError) as e:
        print(f"Exception: {e} \n")

    return sql_result


# Function to create the database
def create_database(db_name=str):
    try:
        conn = create_db_connector()
        c = conn.cursor()
        # creating the DB
        sql = f"CREATE DATABASE {db_name};"
        c.execute(sql)
        time.sleep(1)
    except (mysql.connector.errors.ProgrammingError,
            mysql.connector.errors.DatabaseError) as e:
        print(f"Exception: {e} \n")


#Create client database and grant access
def create_database_client(db_name=str, db_user=str):
    try:
        conn = create_db_connector()
        c = conn.cursor()
        # creating the DB
        sql_db = f"CREATE DATABASE {db_name};"
        sql_privileges = f"GRANT ALL PRIVILEGES ON {db_user}.* TO '{db_name}';"
        c.execute(sql_db)
        time.sleep(1)
        c.execute(sql_privileges)
        time.sleep(1)
    except (mysql.connector.errors.ProgrammingError,
            mysql.connector.errors.DatabaseError) as e:
        print(f"Exception: {e} \n")


# Function to delete the database
def delete_database(db_name=str):
    try:
        conn = create_db_connector()
        c = conn.cursor()
        # deleting the DB
        sql = f"DROP DATABASE {db_name}"
        c.execute(sql)
        time.sleep(1)
        conn.commit()
        conn.close()
    except mysql.connector.errors.DatabaseError as e:
        print(f"Exception: {e} \n")


# Function to create the database and table
def create_tables(db_name=str, db_user=str, db_pass=str):
    try:
        conn = create_db_connector_by_client(str(db_name), db_user, db_pass)
        c = conn.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS `gen` (
                      `id` int NOT NULL AUTO_INCREMENT,
                      `isActive` int NOT NULL,
                      `client_id` int DEFAULT NULL,
                      `client_name` varchar(255) DEFAULT NULL,
                      `system_api` varchar(255) DEFAULT NULL,
                      `last_modified` tinytext,
                      `key1` varbinary(255),
                      `secret1` varbinary(255),
                      `key2` varbinary(255),
                      `secret2` varbinary(255),
                      `injixo_api_id` int DEFAULT NULL,
                      `isHistDownload` int NOT NULL,
                      PRIMARY KEY (`id`),
                      CONSTRAINT `gen_chk_1` CHECK (((`isActive` >= 0) and (`isActive` <= 1))),
                      CONSTRAINT `gen_chk_2` CHECK (((`isHistDownload` >= 0) and (`isHistDownload` <= 1)))
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;''')
        # rta table
        c.execute('''CREATE TABLE IF NOT EXISTS `rta` (
                      `tstamp` bigint DEFAULT NULL,
                      `agentIdentifier` tinytext,
                      `activityIdentifier` text,
                      `startTime` tinytext,
                      `endTime` tinytext
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;''')
        # hist_rta table
        c.execute('''CREATE TABLE IF NOT EXISTS `hist_rta` (
                      `tstamp` bigint DEFAULT NULL,
                      `agentIdentifier` tinytext,
                      `activityIdentifier` text,
                      `startTime` tinytext,
                      `endTime` tinytext
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;''')
        c.execute('''CREATE TABLE IF NOT EXISTS `hist_contacts` (
                      `tstamp` bigint DEFAULT NULL,
                      `uniqueId` tinytext,
                      `queueName` text,
                      `eventtime` tinytext,
                      `chan` tinytext,
                      `offered` int DEFAULT NULL,
                      `handled` int DEFAULT NULL,
                      `duration` float DEFAULT NULL
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; ''')
        conn.commit()
        conn.close()

    except mysql.connector.errors.ProgrammingError as e:
        print(f"Exception: {e} \n")
    time.sleep(1)


#Insert new API user
def insert_user_api(db_name=str, username=str, hashed_password=str, full_name=str, email=str, create_time=datetime.now(),
                    disabled=int):
    try:
        conn = create_db_connector(str(db_name))
        c = conn.cursor()
        c.execute(f'''INSERT INTO `user`
                     (`username`,`hashed_password`,`full_name`,`email`,`create_time`,`disabled`) VALUES ("{username}", "{hashed_password}", "{full_name}", "{email}", "{create_time}", {disabled});''')
        conn.commit()
        conn.close()
    except (mysql.connector.errors.ProgrammingError,
            mysql.connector.errors.DatabaseError) as e:
        print(f"Exception: {e} \n")


#Insert client in Oxilio DB
def insert_client(db_name=str, is_active=bool, client_id=int, client_name=str, last_modified=datetime.now(),
                  user_name=str, password=str):
    try:
        conn = create_db_connector(str(db_name))
        c = conn.cursor()
        c.execute(f'''INSERT INTO `gen`
                     (`isActive`,`client_id`,`client_name`,`last_modified`,`user_name`,`password`) VALUES ({is_active}, {client_id}, "{client_name}", "{last_modified}, "{user_name}", aes_encrypt("{password}","oxiliotakesgoodcareofitsemployees2024"!)");''')
        conn.commit()
        conn.close()
        print(
            f" Values {is_active} , {client_id}, {client_name}, {last_modified} inserted in database: {db_name.upper()} table: oxilioclients.gen")
        create_user_credentials(db_user=user_name, db_pass=password)
        create_database_client(db_name, db_user=user_name)
    except (mysql.connector.errors.ProgrammingError,
            mysql.connector.errors.DatabaseError) as e:
        print(f"Exception: {e} \n")


def insert_default_gen_values(db_name=str, db_user=str, db_pass=str):
    try:
        conn = create_db_connector_by_client(str(db_name), db_user, db_pass)
        # insert into gen table default values
        c = conn.cursor()
        c.execute('''INSERT INTO `gen`
                     (`isActive`,`client_id`,`client_name`,`system_api`,`last_modified`,`key1`,`secret1`,`key2`,`secret2`,`injixo_api_id`,`isHistDownload`)
                       VALUES (0,123456,"client name","tbd","2024-04-25T00:06:14Z",aes_encrypt("key1Client1000","oxiliotakesgoodcareofitsemployees2024"!),aes_encrypt('secret1Client1000','oxiliotakesgoodcareofitsemployees2024!'),aes_encrypt('key2Client1000','oxiliotakesgoodcareofitsemployees2024!'),aes_encrypt('secret2Client1000','oxiliotakesgoodcareofitsemployees2024!'),"99999",0);''')
        conn.commit()
        conn.close()
    except mysql.connector.errors.ProgrammingError as e:
        print(f"Exception: {e} \n")
    time.sleep(1)


def delete_client(db_name=str, db_user=str, db_pass=str, client_id=int):
    try:
        conn = create_db_connector_by_client(str(db_name), db_user, db_pass)
        c = conn.cursor()
        c.execute(f'''UPDATE `gen` SET `isActive` = 0 WHERE (`client_id` = {client_id})''')
        conn.commit()
        conn.close()
    except (mysql.connector.errors.ProgrammingError,
            mysql.connector.errors.DatabaseError) as e:
        print(f"Exception: {e} \n")


def update_client(db_name=str, db_user=str, db_pass=str, is_active=bool, client_id=int, client_name=str,
                  system_api=str,
                  last_modified=datetime.now(), key1=str, secret1=str, key2=str, secret2=str, api_id=int, hist=bool):
    try:
        conn = create_db_connector_by_client(str(db_name), db_user, db_pass)
        # update gen table with values
        c = conn.cursor()
        c.execute(f'''UPDATE`gen` SET
                     `isActive` = {is_active},
                     `client_name` = "{client_name}",
                     `system_api` = "{system_api}",
                     `last_modified` = "{last_modified}",
                     `key1` = "aes_encrypt('{key1}','oxiliotakesgoodcareofitsemployees2024!')",
                     `secret1` = "aes_encrypt('{secret1}','oxiliotakesgoodcareofitsemployees2024!')",
                     `key2` = "aes_encrypt('{key2}','oxiliotakesgoodcareofitsemployees2024!')",
                     `secret2` = "aes_encrypt('{secret2}','oxiliotakesgoodcareofitsemployees2024!')",
                     `injixo_api_id` = {api_id},
                     `isHistDownload` = {hist}
                     WHERE (`client_id` = {client_id});''')
        print()
        conn.commit()
        conn.close()
    except mysql.connector.errors.ProgrammingError as e:
        print(f"Exception: {e} \n")
    time.sleep(1)


def truncate_rta_table(db_name=str, db_user=str, db_pass=str):
    db_exist = check_db_exist(db_name)

    if db_exist:
        conn = create_db_connector_by_client(str(db_name), db_user, db_pass)
        c = conn.cursor()
        c.execute("TRUNCATE TABLE rta")
        conn.commit()
        conn.close()
    else:
        print(f"Cannot truncate 'rta' table: '{db_name.upper()}' database does NOT exist.")


# Function to insert multiple records into the table
def insert_records(values, db_name=str, db_user=str, db_pass=str, table=str):
    try:
        conn = create_db_connector_by_client(str(db_name), db_user, db_pass)
        c = conn.cursor()
        sql = f'INSERT INTO {table} VALUES (%s, %s, %s, %s, %s)'
        c.executemany(sql, values)
        conn.commit()
        conn.close()
        print(c.rowcount, f" record(s) inserted in database: {db_name.upper()} table: {table.lower()}")
    except (mysql.connector.errors.ProgrammingError,
            mysql.connector.errors.DatabaseError) as e:
        print(f"Exception: {e} \n")


def get_records(db_name=str, db_user=str, db_pass=str, table=str, limit=int(500)):
    db_exist = check_db_exist(db_name)
    records = ""
    if db_exist:
        conn = create_db_connector_by_client(str(db_name), db_user, db_pass)
        c = conn.cursor()
        c.execute(f'SELECT * FROM {table} ORDER BY tstamp LIMIT {limit}')
        records = c.fetchall()
        conn.close()
    else:
        print(f"Cannot get records from database: '{db_name.upper()}'; does not exist.")

    return records


def get_records_by_tstamp(db_name=str, table=str, tstamp=str):
    db_exist = check_db_exist(db_name)
    records = ""
    if db_exist:
        conn = create_db_connector(str(db_name))
        c = conn.cursor()
        c.execute(f'SELECT * FROM {table} ORDER BY tstamp LIMIT 500')
        records = c.fetchall()
        conn.close()
    else:
        print(f"Cannot get records from database: '{db_name.upper()}'; does not exist.")

    return records


# Function to select all records
def get_all_records(db_name=str):
    db_exist = check_db_exist(db_name)
    records = ""
    if db_exist:
        conn = create_db_connector(str(db_name))
        c = conn.cursor()
        c.execute('SELECT * FROM rta ORDER BY tstamp LIMIT 500')
        records = c.fetchall()
        conn.close()
    else:
        print(f"Cannot get records from database: '{db_name.upper()}'; does not exist.")

    return records


# Function to delete records based on timestamp
def delete_records_by_timestamp(timestamp, db_name=str):
    db_exist = check_db_exist(db_name)

    if db_exist:
        conn = create_db_connector(str(db_name))
        c = conn.cursor()
        c.execute('DELETE FROM rta WHERE timestamp <= %s', (timestamp,))
        conn.commit()
        conn.close()
    else:
        print(f"Cannot delete records from database: '{db_name.upper()}'; does not exist.")


def query_mysql(query=str):
    query_result = ""
    try:
        conn = create_db_connector()
        c = conn.cursor()
        sql = f"{query}"
        c.execute(sql)
        query_result = c.fetchall()
        conn.close()
    except (mysql.connector.errors.ProgrammingError, UnboundLocalError) as e:
        print(f"Exception: {e} \n")

    return query_result


def query_client_db(db_name=str, query=str):
    db_exist = check_db_exist(db_name)
    query_result = ""
    if db_exist:
        try:
            conn = create_db_connector(str(db_name))
            c = conn.cursor()
            sql = f"{query}"
            c.execute(sql)
            query_result = c.fetchall()
            conn.close()
        except (mysql.connector.errors.ProgrammingError, UnboundLocalError) as e:
            print(f"Exception: {e} \n")
    return query_result


def query_client_db_protected(db_name=str, db_user=str, db_pass=str, query=str):
    db_exist = check_db_exist(db_name)
    query_result = ""
    if db_exist:
        try:
            conn = create_db_connector_by_client(str(db_name), db_user, db_pass)
            c = conn.cursor()
            sql = f"{query}"
            c.execute(sql)
            query_result = c.fetchall()
            conn.close()
        except (mysql.connector.errors.ProgrammingError, UnboundLocalError) as e:
            print(f"Exception: {e} \n")
    return query_result
