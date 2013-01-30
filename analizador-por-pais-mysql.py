import MySQLdb as mysql
import countries
from pprint import pprint


DBSETTINGS = {
        'host': 'dev-models.olx.com.ar',
        'user': 'root',
        'db':   'items_soa',
        }

unknowns = {}

def get_db_conn(dbsettings):
    conn = mysql.connect(**dbsettings)
    return conn

def get_counters():
    counters = {}
    for country in countries.ordered_countries:
        counters[country] = 0
    return counters

def get_country(domain):
    for country in countries.ordered_countries:
        if domain.endswith(country):
            return country
    unknowns[domain] = unknowns.get(domain, 0) + 1
    return 'unknown'

def get_titles():
    titles = ['Minute']
    for country in countries.ordered_countries:
        titles.append(countries.countries[country])
    return ','.join(titles)

def get_results(minute, counters):
    row = [str(minute)]
    for country in countries.ordered_countries:
        row.append(str(counters[country]))
    return ','.join(row)

def main(conn):
    cur = conn.cursor()
    last_fecha = None
    counters = get_counters()

    with open('hits_per_minute_mysql.csv', 'w') as hits:
        hits.write(get_titles() + '\n')

        for i in range(24):
            cur.execute('''SELECT fecha, domain FROM itempageviews
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
                    hits.write(get_results(last_fecha, counters) + '\n')
                    last_fecha = fecha
                    counters = get_counters()

                country = get_country(row[1])
                counters[country] += 1

        hits.write(get_results(last_fecha, counters) + '\n')

    cur.close()
    pprint(unknowns)


if __name__ == '__main__':
    conn = get_db_conn(DBSETTINGS)
    main(conn)
    conn.close()
