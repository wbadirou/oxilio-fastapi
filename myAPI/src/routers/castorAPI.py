import mysql.connector
import json
import psycopg2
from src.routers import castor_db


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
           f"'injixo_api_id', injixo_api_id,"
           f"'isHistDownload', isHistDownload)"
           f"from {dbname}.gen "
           f"limit 1;")
    q_result = castor_db.query_client_db(db_name=dbname, query=sql)

    return q_result


def get_client_active(dbname):
    sql = f"select JSON_OBJECT('isActive', isActive) from {dbname}.gen limit 1;"
    q_result = castor_db.query_client_db(db_name=dbname, query=sql)

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
