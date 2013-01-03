#!/usr/bin/python

import MySQLdb as mysql


DBSETTINGS = {
        'host': 'dev-models.olx.com.ar',
        'user': 'root',
        'db':   'items_soa',
        }

def get_db_conn(dbsettings):
    conn = mysql.connect(**dbsettings)
    return conn


def main(conn):
    cur = conn.cursor()
    last_fecha = None
    counter = 0

    with open('hits_per_minute_mysql.csv', 'w') as hits:
        for i in range(24):
            cur.execute('''SELECT fecha FROM itempageviews
                           WHERE DAY(fecha)=25 AND HOUR(fecha)=%s
                           ORDER BY fecha ASC''', i)
            while True:
                row = cur.fetchone()
                if row is None:
                    break

                fecha = row[0]

                if last_fecha is None:
                    last_fecha = fecha

                if fecha.minute != last_fecha.minute:
                    hits.write('%s,%s\n' % (last_fecha, counter))
                    last_fecha = fecha
                    counter = 1
                else:
                    counter += 1

    cur.close()


if __name__ == '__main__':
    conn = get_db_conn(DBSETTINGS)
    main(conn)
    conn.close()
