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

    for i in range(24):
        ids = []
        last_fecha = None
        hour = str(i)
        if i < 10:
            hour = '0' + str(i)

        filename = 'freq_ids_%s.py' % hour
        with open(filename, 'w') as f:
            cur.execute('''SELECT fecha, id FROM itempageviews
                           WHERE DAY(fecha)=25 AND HOUR(fecha)=%s
                           ORDER BY fecha ASC''', i)

            f.write('ids_by_second = [\n')
            while True:
                row = cur.fetchone()
                if row is None:
                    break

                fecha = row[0]
                if last_fecha is None:
                    last_fecha = fecha

                if fecha.second != last_fecha.second:
                    f.write('\t[%s],\n' % ', '.join(ids))
                    last_fecha = fecha
                    ids = []

                ids.append(str(row[1]))

            f.write('\t[%s],\n' % ', '.join(ids))
            f.write(']\n')

    cur.close()


if __name__ == '__main__':
    conn = get_db_conn(DBSETTINGS)
    main(conn)
    conn.close()
