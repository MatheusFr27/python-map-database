#
# Description: A class MapMysql mapeia, busca e salva os dados separados em arquivos csv.
# Author: Matheus Eduardo França <matheusefranca1727@gmail.com>
# License: MIT
#
# #

import mysql.connector
import csv


class MapMysql:

    config = {}
    current_path = ''
    database = mysql

    tables = list()
    columns = list()
    text_column = ''
    datas = list()

    count_tables = 0
    count_datas = 0

    def set_current_path(self, current_path: str):
        self.current_path = current_path

        return self

    # Exemplo de config { 'user': 'default', 'password': 'secret', 'host': '127.0.0.1', 'database': 'default' }
    def config_database(self, host: str, database: str, user: str, password: str):
        self.config = {'user': user, 'password': password,
                       'host': host, 'database': database}

        return self

    def connect_database(self):
        try:
            self.database = mysql.connector.connect(**self.config)
        except mysql.connector.Error as err:
            print(err)

        return self

    # Busca por todos os nomes das tabelas
    def get_tables(self):
        cursor = self.database.cursor()

        cursor.execute('SHOW TABLES')

        for tables in cursor.fetchall():
            self.tables.append([tables[0]])

        with open(self.current_path + '/tabelas_e_dados/' + 'tables.csv', 'w', newline='') as file:
            writer = csv.writer(file)

            writer.writerows([['tables'], *self.tables])

        cursor.close()

        self.count_tables = len(self.tables)  # Pega a quantidade de tabelas

        return self

    # Busca pelas colunas de uma tabela
    def get_column_name(self, table_name: str):
        cursor = self.database.cursor()

        cursor.execute(
            "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{}'".format(table_name))

        self.columns.clear()

        for column in cursor.fetchall():
            self.columns.append(column[0])

        cursor.close()

        return self

    # Converte array de colunas em uma string
    def convert_columns_string(self):
        self.text_column = ""

        for i, column in enumerate(self.columns):

            if i == 0:
                self.text_column = self.text_column + '{}'.format(column)
            else:
                self.text_column = self.text_column + ', {}'.format(column)

        return self

    # Captura todos os dados de uma tabela
    def get_datas(self, table_name: str):
        cursor = self.database.cursor()

        cursor.execute("SELECT {} FROM {}".format(
            self.text_column, table_name)
        )

        self.datas = cursor.fetchall()

        cursor.close()

        return self

    # Fluxo de salvamento de dados em arquivos csv
    def get_datas_tables(self):

        for table in self.tables:
            self.get_column_name(
                table[0]).convert_columns_string().get_datas(table[0])

            if len(self.datas) > 0:
                # Pega a quantidade de dados e soma com a existente
                self.count_datas = self.count_datas + len(self.datas)

            with open("{}/tabelas_e_dados/{}_data.csv".format(self.current_path, table[0]), 'w', newline='') as file:
                writer = csv.writer(file)

                writer.writerows(
                    [[table[0]], self.columns, *self.datas]
                )

        return self

    def print_result(self):
        print('---------------------------')
        print('Processo de extracao finalizado com sucesso')
        print('Quantidade de tabelas: {}'.format(self.count_tables))
        print('Quantidade de dados: {}'.format(self.count_datas))

        return self

    def start_process(self):

        self.get_tables().get_datas_tables().print_result()

        self.database.close()

        return self
