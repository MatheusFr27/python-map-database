#
# Author: Matheus Eduardo França <matheusefranca1727@gmail.com>
# License: MIT
#
# #

from maps.map_mysql import MapMysql
import os


class Main:

    map_mysql = object()

    def __init__(self) -> None:
        self.map_mysql = MapMysql()

    def valid_or_create_folder(self):
        if not os.path.exists('tabelas_e_dados'):
            os.mkdir('./tabelas_e_dados')

        return self

    def set_credential_database(self):

        print('Informe as credenciais do seu banco Mysql')
        print('------------------------------------------------')

        host = input("Host: ")
        database = input("Database: ")
        user = input("User: ")
        password = input("Password: ")

        self.map_mysql.config_database(host, database, user, password)

        return self

    def define_current_database(self):
        current_path = os.path.dirname(os.path.realpath(__file__))

        self.map_mysql.set_current_path(current_path)

        return self

    def start(self):

        self.valid_or_create_folder().set_credential_database().define_current_database()

        self.map_mysql.connect_database().start_process()


if __name__ == "__main__":
    main = Main()
    main.start()
