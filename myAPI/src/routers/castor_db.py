import time
import mysql.connector
from src.routers import castor_tools


# creating DB connection and cursor
def create_db_connector(db_name=str("none")):
    # db_host, db_port, db_user, db_pass = castor_tools.get_mysql_creds()
    db_host = "localhost"
    db_port = 3306
    db_user = "adm_castor"
    db_pass = "$Oxilio_01!"

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
def create_tables(db_name=str):
    try:
        conn = create_db_connector(str(db_name))
        c = conn.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS `gen` (
                      `id` int NOT NULL AUTO_INCREMENT,
                      `isActive` int NOT NULL,
                      `client_id` int DEFAULT NULL,
                      `client_name` varchar(255) DEFAULT NULL,
                      `system_api` varchar(255) DEFAULT NULL,
                      `last_modified` tinytext,
                      `key1` text,
                      `secret1` text,
                      `key2` text,
                      `secret2` text,
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


def insert_default_gen_values(db_name=str):
    try:
        conn = create_db_connector(str(db_name))
        # insert into gen table default values
        c = conn.cursor()
        c.execute('''INSERT INTO `gen`
                     (`isActive`,`client_id`,`client_name`,`system_api`,`last_modified`,`key1`,`secret1`,`key2`,`secret2`,`injixo_api_id`,`isHistDownload`)
                       VALUES (0,123456,"client name","tbd","2024-04-25T00:06:14Z","","","","","99999",0);''')
        conn.commit()
        conn.close()
    except mysql.connector.errors.ProgrammingError as e:
        print(f"Exception: {e} \n")
    time.sleep(1)


def truncate_rta_table(db_name=str):
    db_exist = check_db_exist(db_name)

    if db_exist:
        conn = create_db_connector(str(db_name))
        c = conn.cursor()
        c.execute("TRUNCATE TABLE rta")
        conn.commit()
        conn.close()
    else:
        print(f"Cannot truncate 'rta' table: '{db_name.upper()}' database does NOT exist.")


# Function to insert multiple records into the table
def insert_records(values, db_name=str, table=str):
    try:
        conn = create_db_connector(str(db_name))
        c = conn.cursor()
        sql = f'INSERT INTO {table} VALUES (%s, %s, %s, %s, %s)'
        c.executemany(sql, values)
        conn.commit()
        conn.close()
        print(c.rowcount, f" record(s) inserted in database: {db_name.upper()} table: {table.lower()}")
    except (mysql.connector.errors.ProgrammingError,
            mysql.connector.errors.DatabaseError) as e:
        print(f"Exception: {e} \n")


def get_records(db_name=str, table=str, limit=int(500)):
    db_exist = check_db_exist(db_name)
    records = ""
    if db_exist:
        conn = create_db_connector(str(db_name))
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


def db_test_main():
    pass


if __name__ == "__main__":
    db_test_main()
