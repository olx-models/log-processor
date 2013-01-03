#!/usr/bin/python

import sqlite3 as dbapi
from datetime import datetime


DBFILE = 'items_soa.db'

def get_db_conn(dbname):
    conn = dbapi.connect(dbname)
    return conn


def main(conn):
    cur = conn.cursor()
    cur.execute('SELECT * FROM itempageviews ORDER BY datetime(fecha) ASC')

    last_minute = 0
    counter = 0

    with open('hits_per_minute_sqlite.csv', 'w') as hits:
        while True:
            row = cur.fetchone()
            if row is None:
                break

            fecha = datetime.strptime(row[0], '%Y-%m-%d %H:%M:%S')
            domain = row[1]
            url = row[2]

            if fecha.minute != last_minute:
                hits.write('%s,%s\n' % (row[0], counter))
                last_minute = fecha.minute
                counter = 1
            else:
                counter += 1

    cur.close()


if __name__ == '__main__':
    conn = get_db_conn(DBFILE)
    main(conn)
    conn.close()
