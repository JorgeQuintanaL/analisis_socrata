from socrata import MariaDB_Connect
import sqlalchemy as db

db_connection = MariaDB_Connect(user="quantomm_dev", password="quantomm_dev", host="172.17.0.2", database="socrata")
db_connection.connect_db()
print(db_connection.search_by_id("2222-6rct"))
db_connection.update_dataset("2222-6rct", "2019-01-01", 0)
print(db_connection.search_by_id("2222-6rct"))