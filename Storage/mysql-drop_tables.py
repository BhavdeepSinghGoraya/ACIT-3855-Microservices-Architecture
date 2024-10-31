import mysql.connector
db_conn = mysql.connector.connect(host="acit-3855-mysql-kafka.francecentral.cloudapp.azure.com", user="user",
password="password", database="bookstore")
db_cursor = db_conn.cursor()
db_cursor.execute('''
DROP TABLE book_buy, book_sell
''')
db_conn.commit()
db_conn.close()