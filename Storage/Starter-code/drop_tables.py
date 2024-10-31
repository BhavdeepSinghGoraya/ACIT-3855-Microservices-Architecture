import sqlite3

conn = sqlite3.connect('point.sqlite')

c = conn.cursor()
c.execute('''
          DROP TABLE point
          ''')

conn.commit()
conn.close()
