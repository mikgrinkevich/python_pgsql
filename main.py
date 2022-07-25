import psycopg2
import json
from psycopg2.extras import Json, DictCursor
from dict2xml import dict2xml


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

    def parse_json(self, path: str):
        values_str = ""
        with open(path) as json_data:
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

    def insert_data(self, path: str):
        with self.conn.cursor() as cursor:
            values = self.parse_json(path)
            sql_string = ''
            if 'rooms' in path:
                sql_string = "INSERT INTO rooms (id, name) VALUES %s" % (
                    values)
            else:
                sql_string = "INSERT INTO students (birthday, id, name, room, sex) VALUES %s" % (
                    values)
            cursor.execute(sql_string)
            self.conn.commit()


class DbSelector(DbConnector):

    def get_dict(self, fetched_data):
        result = []
        for row in fetched_data:
            result.append(dict(row))
        return result

    def save_as_file(self, format, dictionary):

        with open(f"result_query.{format}", "w") as outfile:
            if format == 'json':
                outfile.write(json.dumps(dictionary, indent=4,
                              sort_keys=True, default=str))
            else:
                xml = dict2xml(dictionary)
                outfile.write(xml)

    def fetchall_data(self, query):
        cur = self.conn.cursor(cursor_factory=DictCursor)
        cur.execute(query)
        fetched_data = cur.fetchall()
        return fetched_data

    def get_students_by_room(self, format):
        query = '''select room, count(room) as roomers_count from students group by room order by roomers_count desc'''
        fetched_data = self.fetchall_data(query)
        res = self.get_dict(fetched_data)
        self.save_as_file(format, res)
        return print(res)

    def get_top_five_minimum_avg(self, format):
        query = '''select room, avg(age(now(),birthday)) as avg_age from students group by room order by avg_age asc limit(5)'''
        fetched_data = self.fetchall_data(query)
        res = self.get_dict(fetched_data)
        self.save_as_file(format, res)
        return print(res)

    def get_biggest_age_difference(self, format):
        query = '''select room, max( age(now(),birthday) ) - min( age(now(),birthday) ) as difference from students group by room order by difference desc limit(5)'''
        fetched_data = self.fetchall_data(query)
        res = self.get_dict(fetched_data)
        self.save_as_file(format, res)
        return print(res)


if __name__ == '__main__':
    db_connector = DbConnector()
    db_connector.connect_to_db()
    db_connector.create_tables()

    db_loader = DbLoader()
    db_loader.connect_to_db()
    db_loader.insert_data("json/rooms.json")
    db_loader.insert_data("json/students.json")

    db_selector = DbSelector()
    db_selector.connect_to_db()
    db_selector.get_top_five_minimum_avg('json')
    db_selector.get_top_five_minimum_avg('xml')
    db_selector.get_top_five_minimum_avg('xml')
