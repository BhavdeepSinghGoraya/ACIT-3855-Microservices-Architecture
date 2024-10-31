import sqlite3

conn = sqlite3.connect('bookstore.sqlite')

c = conn.cursor()
c.execute('''
          CREATE TABLE book_buy
          (id INTEGER PRIMARY KEY ASC, 
           order_id VARCHAR(36) NOT NULL, 
           book_id INTEGER NOT NULL,
           user_id VARCHAR(36) NOT NULL,
           name VARCHAR(100) NOT NULL,
           price FLOAT NOT NULL,
           sold BOOLEAN NOT NULL,
           date_created DATETIME NOT NULL,
           trace_id VARCHAR(36) NOT NULL)
          ''')

c.execute('''
          CREATE TABLE book_sell
          (id INTEGER PRIMARY KEY ASC, 
           book_id INTEGER NOT NULL,
           user_id VARCHAR(36) NOT NULL,
           name VARCHAR(100) NOT NULL,
           listing_date VARCHAR(100) NOT NULL,
           price FLOAT NOT NULL,
           genre VARCHAR(100) NOT NULL,
           date_created DATETIME NOT NULL,
           trace_id VARCHAR(36) NOT NULL)
          ''')

conn.commit()
conn.close()
