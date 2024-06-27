import os.path
import pycryp
import configparser
from os.path import exists
import datetime
import threading


class FileBackup(threading.Thread):
    def __init__(self, content, filename):
        threading.Thread.__init__(self)
        self.content = content
        self.filename = filename

    def run(self):
        with open(self.filename, "w") as f:
            f.write(self.content)
            f.close()


def get_current_timestamp(precision=str("sec")):
    timestamp = 0
    cur_dt = datetime.datetime.now(datetime.UTC)
    if precision.lower() == "sec":
        timestamp = int(cur_dt.timestamp())
    elif precision.lower() == "milliseconds":
        timestamp = int(cur_dt.timestamp() * 1000)

    return timestamp


def clear_screen():
    os.system('cls' if os.name == 'nt' else 'clear')


def set_db_pass(password):
    from src.routers.settings import pass_key
    encrypted_pass = pycryp.encrypt(password, pass_key)
    return encrypted_pass


def get_db_pass(encrypted_pass):
    from src.routers.settings import pass_key
    decrypted_pass = pycryp.decrypt(encrypted_pass, pass_key)
    decoded_pass = decrypted_pass.decode()
    return decoded_pass


# from INI file
def create_db_ini():
    print(f"Input db creds: ")
    db_host = input(f"    db host: ")
    db_port = input(f"    db port: ")
    db_user = input(f"    db_user: ")
    db_pass = input(f"    db_pass: ")
    config_file = ".\\castor.ini"
    config = configparser.RawConfigParser()
    encrypted_pass = set_db_pass(db_pass)
    config.add_section("settings")
    config["settings"]["db_host"] = str(db_host)
    config["settings"]["db_port"] = str(db_port)
    config["settings"]["db_user"] = str(db_user)
    config["settings"]["db_pass"] = str(encrypted_pass.decode())
    with open(config_file, "w") as f:
        config.write(f)
        f.close()


# from INI file
def get_mysql_creds():
    config_file = ".\\castor.ini"
    if not exists(config_file):
        create_db_ini()

    config = configparser.RawConfigParser()
    config.read(config_file)
    db_host = config.get("settings", "db_host")
    db_port = config.get("settings", "db_port")
    db_user = config.get("settings", "db_user")
    encrypted_db_pass = config.get("settings", "db_pass").encode()
    db_pass = get_db_pass(encrypted_db_pass)

    return db_host, db_port, db_user, db_pass


# ##################################################
# ##################################################
def castor_tools_main():
    # Code to run in this script go here
    # ##################################################
    print(f"Working in: {os.getcwd()}")
    print(f"Running current script >>> {os.path.basename(__file__)}\n")
    db_host, db_port, db_user, db_pass = get_mysql_creds()
    print(db_host, db_port, db_user, db_pass)


if __name__ == "__main__":
    castor_tools_main()
