import mysql.connector
db_conn = mysql.connector.connect(host="acit-3855-mysql-kafka.francecentral.cloudapp.azure.com", user="user",
password="password", database="bookstore")
db_cursor = db_conn.cursor()
db_cursor.execute('''
CREATE TABLE book_buy
(id INT NOT NULL AUTO_INCREMENT,
 order_id VARCHAR(36) NOT NULL, 
 book_id INT NOT NULL,
 user_id VARCHAR(36) NOT NULL,
 name VARCHAR(100) NOT NULL,
 price FLOAT NOT NULL,
 sold BOOLEAN NOT NULL,
 date_created DATETIME NOT NULL,
 trace_id VARCHAR(36) NOT NULL,
 CONSTRAINT book_buy_pk PRIMARY KEY (id))
''')

db_cursor.execute('''
CREATE TABLE book_sell
(id INT NOT NULL AUTO_INCREMENT,
 book_id INT NOT NULL,
 user_id VARCHAR(36) NOT NULL,
 name VARCHAR(100) NOT NULL,
 listing_date VARCHAR(100) NOT NULL,
 price FLOAT NOT NULL,
 genre VARCHAR(100) NOT NULL,
 date_created DATETIME NOT NULL,
 trace_id VARCHAR(36) NOT NULL,
 CONSTRAINT book_sell_pk PRIMARY KEY (id))
''')
db_conn.commit()
db_conn.close()
