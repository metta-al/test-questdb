'''
64 rows = 1032.0965032577515 18766957 5.499541045773971e-05
128 rows = 992.1185338497162 18766957 5.286517861418429e-05
512 rows = 972.1820328235626 18766957 5.1802859292721914e-05
1024 rows = 1013.0031986236572 18766957 5.3978020977170525e-05
'''

cmds = ['DROP TABLE IF EXISTS orders_l2;', 'DROP TABLE IF EXISTS trades;','''
CREATE TABLE IF NOT EXISTS orders_l2(
    timestamp TIMESTAMP(6),
    exch_time TIMESTAMP(6),
    exchsymb CHARACTER VARYING(64),
    property CHARACTER VARYING(64),
    prc REAL,
    qty REAL);
CREATE INDEX ON orders_l2(exchsymb, property, timestamp);
''','''
CREATE TABLE IF NOT EXISTS trades(
    timestamp TIMESTAMP(6),
    exch_time TIMESTAMP(6),
    exchsymb CHARACTER VARYING(64),
    side BOOLEAN,
    prc REAL,
    qty REAL);
CREATE INDEX ON trades(exchsymb, side, timestamp);
''']

import time
from datetime import datetime

import pandas
import psycopg2
import psycopg2.extras

from crypto.core import exch_symbol_from_name

from simulator.utils import get_data_file

DO_DOWNLOAD = True
DO_WRITEDB = True

DB_P_USER = 'postgres' 
DB_P_PASS = 'password'
DB_P_HOST = 'localhost'
DB_P_PORT = 5432

dbtime=0
dbrows=0

def _tstamp(ns):
    return datetime.utcfromtimestamp(int(ns)/10e9).isoformat()

BUFFER=64
def db_send(entries, query=''):
    try:
        cur = db_pg_get().cursor()
        stime = time.time()
        print(f'[LOG] {entries[0]} - {BUFFER} rows')
        psycopg2.extras.execute_batch(cur,query,entries,page_size=BUFFER)
        print(f'[LOG] wrote {len(entries)} rows')
        cur.close()

        global dbtime, dbrows
        dbtime += time.time()-stime
        dbrows += len(entries)
    except BaseException as e:
        raise e

def db_clean():
    cur = db_pg_get().cursor()
    for cmd in cmds:
        print(f'[LOG] running {cmd}')
        cur.execute(cmd)
    cur.close()

pgconn = None
def db_pg_get():
    global pgconn
    if pgconn is None:
        pgconn = (psycopg2.connect(dbname='postgres', host=DB_P_HOST, port=DB_P_PORT,
            user=DB_P_USER, password=DB_P_PASS))
    return pgconn

def db_pg_close():
    if pgconn is not None:
        pgconn.cursor().close()
        pgconn.close()

def db_rowsexist(header, entries):
    headers = header.split(',')
    table = headers[0]
    fields = [h.split('=') for h in headers[1:]]
    fields = [f'{a}=\'{b}\'' for a,b in fields]

    curr = len(entries)-1
    try:
        conn = db_pg_get()
        curs = conn.cursor()
        curs.execute((
            f'SELECT count() FROM {table} WHERE {" AND ".join(fields)} '
            + f'AND timestamp>=\'{_tstamp(entries[0].split(" ")[-1])}\' AND '
            + f'timestamp<=\'{_tstamp(entries[-1].split(" ")[-1])}\'' ))
        res = curs.fetchall()
        if res: curr = res[0][0]
    except BaseException as e:
        print(f'[ERR] postgres db fails - {e}')
    return curr == len(entries)

def process_fn(sym, dte, numlvl=1):
    exchsymb = exch_symbol_from_name(sym)
    prepend = f'exchsymb={exchsymb.tech_name()}'
    print(f'[LOG] processing {prepend} for {dte}')

    fname = (get_data_file(exchsymb, dte, 'book', allow_download=DO_DOWNLOAD))
    if fname:
        if not DO_WRITEDB:
            data = pandas.read_parquet(fname, columns=['exch_time'])
        else:
            fullcols = ['exch_time']

            level = [f'bid{i}' for i in range(numlvl)]
            level += [f'ask{i}' for i in range(numlvl)]
            for l in level:
                fullcols.append(f'{l}_prc')
                fullcols.append(f'{l}_qty')
            data0 = pandas.read_parquet(fname, columns=fullcols)

            for lvl in level:
                data = data0[['exch_time', f'{lvl}_prc',f'{lvl}_qty']]

                # some binance has exch_time = 0
                tstamps = data.index.tolist()
                q = 'INSERT INTO orders_l2(exchsymb,property,prc,qty,exch_time,timestamp) VALUES (%s,%s,%s,%s,%s,%s)'
                db_send([(exchsymb.tech_name(),lvl,p[1],p[2],_tstamp(p[0]*10e3),_tstamp(t*10e3))
                    for t,p in list(zip(tstamps,data.values.tolist()))], q)

    fname = (get_data_file(exchsymb, dte, 'trade', allow_download=DO_DOWNLOAD))
    if fname:
        if not DO_WRITEDB:
            data = pandas.read_parquet(fname, columns=['exch_time'])
        else:
            data = pandas.read_parquet(fname, columns=['exch_time','trd_prc','trd_qty','trd_side'])
            q = 'INSERT INTO trades(exchsymb,prc,qty,side,exch_time,timestamp) VALUES (%s,%s,%s,%s,%s,%s)'
            db_send([(exchsymb.tech_name(),p[1],p[2],p[3]==1,_tstamp(p[0]*10e3),_tstamp(t*10e3))
                for t,p in list(zip(tstamps,data.values.tolist()))], q)

def main():
    symbs = ([
        'BTC/USDT@Binance',
        'ETH/USDT@Binance',
        'ETH/BTC@Binance',
        'BTC/USDT_Perpetual@BinanceFut',
        'BTC/USDT@OKEx',
        'ETH/USDT@OKEx'
    ])
    dates = [20210901+i for i in range(5)]

    symbs = symbs[:2]
    dates = dates[:1]

    db_clean()
    for s in symbs:
        for d in dates:
            process_fn(s, d, 5)

    print(dbtime, dbrows, dbtime/dbrows)

if __name__ == '__main__':
    main()
