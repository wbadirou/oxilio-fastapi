from datetime import datetime, time
import mysql.connector
import json
import psycopg2
import castor_db


def welcome():
    msg = "Welcome Castor APIs"
    return msg


def healthcheck(check_code):
    result = "Check code not valid"
    if check_code.lower() == "ping":
        result = "pong"
    return result


def list_existing_db():
    sql = f"SHOW DATABASES"
    q_result = castor_db.query_mysql(query=sql)
    db_list = []
    for r in q_result:
        print(r[0])
        db_list.append(r[0])

    return db_list


def get_rta_table(dbname, limit=int(500)):
    sql = (f"select JSON_OBJECT"
           f"('tstamp', tstamp,"
           f"'agentIdentifier', agentIdentifier,"
           f"'activityIdentifier', activityIdentifier,"
           f"'startTime', startTime,"
           f"'endTime', endTime)"
           f"from {dbname}.rta "
           f"limit {limit};")
    q_result = castor_db.query_client_db(db_name=dbname, query=sql)

    return q_result


def get_gen_table(dbname):
    sql = (f"select JSON_OBJECT("
           f"'isActive', isActive,"
           f"'client_id', client_id,"
           f"'client_name', client_name,"
           f"'last_modified', last_modified,"
           f"'system_api', system_api,"
           f"'injixo_api_id', injixo_api_id,"
           f"'isHistDownload', isHistDownload,"
           f"'key1', CAST(key1 as CHAR),"
           f"'secret1', CAST(secret1 as CHAR),"
           f"'key2', CAST(key2 as CHAR),"
           f"'secret2', CAST(secret2 as CHAR))"
           f"from {dbname}.gen "
           f"limit 1;")
    q_result = castor_db.query_client_db(db_name=dbname, query=sql)

    return q_result

def get_gen_table_decrypted(dbname):
    sql = (f"select JSON_OBJECT("
           f"'isActive', isActive,"
           f"'client_id', client_id,"
           f"'client_name', client_name,"
           f"'last_modified', last_modified,"
           f"'system_api', system_api,"
           f"'injixo_api_id', injixo_api_id,"
           f"'isHistDownload', isHistDownload,"
           f"'key1', CAST((aes_decrypt(key1,'oxiliotakesgoodcareofitsemployees2024!')) as CHAR),"
           f"'secret1', CAST((aes_decrypt(secret1,'oxiliotakesgoodcareofitsemployees2024!')) as CHAR),"
           f"'key2', CAST((aes_decrypt(key2,'oxiliotakesgoodcareofitsemployees2024!')) as CHAR),"
           f"'secret2', CAST((aes_decrypt(secret2,'oxiliotakesgoodcareofitsemployees2024!')) as CHAR))"
           f"from {dbname}.gen "
           f"limit 1;")
    q_result = castor_db.query_client_db(db_name=dbname, query=sql)

    return q_result

# Request to get the list of clients from Oxilio database
def get_oxilio_clients():
    dbname = "oxilioclients"
    sql = (f"select JSON_OBJECT("
           f"'isActive', isActive,"
           f"'client_id', client_id,"
           f"'client_name', client_name,"
           f"'last_modified', last_modified)"
           f"from oxilioclients.gen;")
    q_result = castor_db.query_client_db(db_name=dbname, query=sql)

    return q_result


def get_client_active(dbname):
    sql = f"select JSON_OBJECT('isActive', isActive) from {dbname}.gen limit 1;"
    q_result = castor_db.query_client_db(db_name=dbname, query=sql)

    return q_result


# Request to add a client into the Oxilio db
def create_client_db(isactive, clientid, clientname):
    dbname = "oxilioclients"
    q_result = castor_db.insert_client(db_name=dbname, is_active=isactive, client_id=clientid, client_name=clientname)
    return q_result


# Request to add a client into the Oxilio db
def update_client(dbname, isactive, clientid, clientname, systemapi, key1, secret1, key2, secret2, apiid, hist):
    q_result = castor_db.update_client(db_name=dbname, is_active=isactive, client_id=clientid, client_name=clientname,
                                       system_api=systemapi, key1=key1, secret1=secret1, key2=key2, secret2=secret2,
                                       api_id=apiid, hist=hist)
    return q_result


# Request to delete a client which is only putting the isActive at 0
def deactivate_client(dbname, clientid):
    q_result = castor_db.delete_client(db_name=dbname, client_id=clientid)
    return q_result


def format_result(q_result):
    json_result = []
    count_rows = len(q_result)

    if count_rows > 1:
        for r in q_result:
            json_result.append(json.loads(r[0]))
    else:
        json_result = json.loads(q_result[0][0])

    return json_result


if __name__ == '__main__':
    query_result = get_client_active("TEST1")
    # query_result = get_rta_table("test1", 1)
    # query_result = list_existing_db()
    print(query_result)

    print("\n")
    print(format_result(query_result))
