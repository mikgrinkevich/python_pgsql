import psycopg2
import json
from psycopg2.extras import Json


class DbConnector:

    def connect_to_db(self):
        self.conn = psycopg2.connect(
            database="postgres",
            user='postgres',
            password='postgres',
            host='0.0.0.0',
        )

    def create_tables(self):
        with self.conn.cursor() as cursor:
            cursor.execute(open("db_objects/create_obj.sql", "r").read())
            self.conn.commit()


class DbLoader(DbConnector):

    def parse_rooms(self):
        values_str = ""
        with open("json/rooms.json") as json_data:
            record_list = json.load(json_data)
            values = [list(x.values()) for x in record_list]

            for i, record in enumerate(values):
                val_list = []
                for v, val in enumerate(record):
                    if type(val) == str:
                        val = str(Json(val)).replace('"', '')
                    val_list += [str(val)]
                values_str += "(" + ', '.join(val_list) + "),\n"
            values_str = values_str[:-2] + ";"
        return values_str

    def insert_rooms(self):
        with self.conn.cursor() as cursor:
            values = self.parse_rooms()
            sql_string = "INSERT INTO rooms (id, name) VALUES %s" % (values)
            cursor.execute(sql_string)
            self.conn.commit()

    def parse_students(self):
        values_str = ""
        with open("json/students.json") as json_data:
            record_list = json.load(json_data)
            values = [list(x.values()) for x in record_list]

            for i, record in enumerate(values):
                val_list = []
                for v, val in enumerate(record):
                    if type(val) == str:
                        val = str(Json(val)).replace('"', '')
                    val_list += [str(val)]
                values_str += "(" + ', '.join(val_list) + "),\n"
            values_str = values_str[:-2] + ";"
        return values_str

    def insert_students(self):
        with self.conn.cursor() as cursor:
            values = self.parse_students()
            sql_string = 'INSERT INTO students (birthday, id, name, room, sex) VALUES %s' % (
                values)
            cursor.execute(sql_string)
            self.conn.commit()


if __name__ == '__main__':
    db_connector = DbConnector()
    db_connector.connect_to_db()
    db_connector.create_tables()

    db_loader = DbLoader()
    db_loader.connect_to_db()
    db_loader.insert_rooms()
    db_loader.insert_students()

    # db_loader = DbLoader()
    # print(db_loader.parse_students())
