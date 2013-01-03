#!/usr/bin/python


import os
import re
import MySQLdb as mysql
from datetime import datetime


DBSETTINGS = {
        'host': 'dev-models.olx.com.ar',
        'user': 'root',
        'db':   'items_soa',
        }
LOGDIR = 'logs'

def get_db_conn(dbsettings):
    # create table itempageviews (
    #   fecha DATETIME,
    #   host VARCHAR(16),
    #   domain VARCHAR(128),
    #   id INT(11),
    #   url VARCHAR(512)
    # );
    conn = mysql.connect(**dbsettings)
    return conn


def get_file_list(dirname):
    flist = []
    for i in os.listdir(LOGDIR):
        fname = '%s/%s' % (dirname, i)
        flist.append(fname)
    return flist


def main(flist, conn):
    sql = "INSERT INTO itempageviews VALUES (%s, %s, %s, %s, %s);"
    cur = conn.cursor()
    with open('errors.log', 'w') as err:
        for fname in flist:
            print 'Procesando %s' % fname
            with open(fname, 'r') as f:
                for l in f:
                    fields = l.split(' ')
                    if len(fields) < 7:
                        err.write('[%s] Line too short: %s' % (fname, l))
                        continue

                    url = fields[6].strip('"')
                    re_result = re.search(r'iid-\d+', url)
                    if not re_result:
                        err.write('[%s] URL not valid: %s' % (fname, l))
                        continue

                    raw_date = fields[3].strip('[')
                    try:
                        date = datetime.strptime(raw_date, '%d/%b/%Y:%H:%M:%S')
                    except:
                        err.write('[%s] Datetime not valid: %s' % (fname, l))
                        continue

                    host = fname[5:10]
                    domain = fields[1]
                    id = re_result.group()[4:]
                    args = (date, host, domain, id, url)
                    try:
                        cur.execute(sql, args)
                    except Exception as e:
                        query = sql % args
                        err.write('[%s] %s %s\n' % (fname, e, query))

                    conn.commit()

    cur.close()


if __name__ == '__main__':
    flist = get_file_list(LOGDIR)
    conn = get_db_conn(DBSETTINGS)
    main(flist, conn)
    conn.close()
