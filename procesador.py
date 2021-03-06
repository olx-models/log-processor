#!/usr/bin/python

import os
import re
import sqlite3 as dbapi
from datetime import datetime


DBFILE = 'items_soa.db'
LOGDIR = 'logs'

def get_db_conn(dbname):
    conn = dbapi.connect(dbname)
    try:
        cur = conn.cursor()
        cur.execute('SELECT COUNT(*) FROM itempageviews')
    except dbapi.OperationalError:
        cur.execute('CREATE TABLE itempageviews (fecha text(100), domain text(100), url text(100))')
    cur.close()
    return conn


def get_file_list(dirname):
    flist = []
    for i in os.listdir(LOGDIR):
        fname = '%s/%s' % (dirname, i)
        flist.append(fname)
    return flist


def main(flist, conn):
    sql = "INSERT INTO itempageviews VALUES ('%s', '%s', '%s');"
    cur = conn.cursor()
    with open('error.sql', 'w') as err:
        for fname in flist:
            print 'Procesando %s' % fname
            with open(fname, 'r') as f:
                for l in f:
                    fields = l.split(' ')
                    if len(fields) < 7:
                        err.write('[%s] Line too short: %s\n' % (fname, l))
                        continue

                    url = fields[6].strip('"')
                    if not re.search(r'iid-\d+', url):
                        err.write('[%s] URL not valid: %s\n' % (fname, l))
                        continue

                    raw_date = fields[3].strip('[')
                    try:
                        date = datetime.strptime(raw_date, '%d/%b/%Y:%H:%M:%S')
                    except:
                        err.write('[%s] Datetime not valid: %s\n' % (fname, l))
                        continue

                    domain = fields[1]
                    args = (date.strftime('%Y-%m-%d %H:%M:%S'), domain, url)
                    try:
                        cur.execute(sql % args)
                    except Exception as e:
                        print str(e), ':', sql % args
                        query = sql % args
                        err.write('[%s] Query error: %s\n' % (fname, query))
                conn.commit()
    cur.close()


if __name__ == '__main__':
    flist = get_file_list(LOGDIR)
    #flist = ['logs/web15.log',]
    conn = get_db_conn(DBFILE)
    main(flist, conn)
    conn.close()
