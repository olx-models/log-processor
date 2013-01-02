#!/usr/bin/python

import os
import sqlite3 as dbapi


DBFILE = 'items_soa.db'
LOGDIR = 'items'

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
                    domain = fields[1]
                    date = fields[3].strip('[')
                    url = fields[6].strip('"')
                    args = (date, domain, url)
                    try:
                        cur.execute(sql % args)
                    except Exception as e:
                        print str(e)
                        line = sql % args
                        err.write(line + '\n')
                conn.commit()
    cur.close()


if __name__ == '__main__':
    flist = get_file_list(LOGDIR)
    flist = ['items/web13.log',]
    conn = get_db_conn(DBFILE)
    main(flist, conn)
    conn.close()
