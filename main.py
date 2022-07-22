import psycopg2

class DbConnector:
    create_db = '''CREATE database students'''

    def connect_to_db(self):
        conn = psycopg2.connect(
            database="postgres",
            user='postgres',
            password='postgres',
            host='0.0.0.0',
        )
        return conn

    def db_create(self):
        cursor = self.connect_to_db()
        cursor.cursor()
        cursor.execute(self.create_db)

    def create_tables(self):
        with self.conn.cursor() as cursor:
            cursor.execute(open("db_objects/create_obj.sql", "r").read())

if __name__ == '__main__':
    db_connector = DbConnector()
    db_connector.connect_to_db()