'''
CREATE TABLE IF NOT EXISTS orders_l2(
    timestamp timestamp,
    exchsymb SYMBOL,
    property SYMBOL,
    exch_time FLOAT,
    prc FLOAT,
    qty FLOAT) partition by DAY;
CREATE TABLE IF NOT EXISTS trades(
    timestamp timestamp,
    exch_time INT,
    exchsymb SYMBOL,
    side BOOLEAN,
    prc FLOAT,
    qty FLOAT) partition by DAY;

128  rows - 31.53143286705017 20363386 1.5484376157801148e-06 
1028 rows - 31.771186590194702 20363386 1.5602113808673421e-06
4096 rows - 32.36903643608093 20363386 1.58957043961554e-06
all  rows - 33.37786316871643 20363386 1.6391116471846298e-06
'''

import time
import socket
from datetime import datetime

import pandas
import psycopg2

from crypto.core import exch_symbol_from_name

from simulator.utils import get_data_file

DO_DOWNLOAD = True
DO_WRITEDB = True

DB_HOST = 'localhost'
DB_PORT = 9009

DB_P_HOST = '127.0.0.1'
DB_P_PORT = 8812
DB_P_USER = 'admin'
DB_P_PASS = 'quest'

dbtime=0
dbrows=0

BUFFER=128
def db_send(entries):
    header = entries[0].split(' ')[0]
    if db_rowsexist(header, entries):
        print(f'[LOG] skipping {header}')
        return

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((DB_HOST, DB_PORT))

    def _close():
        sock.shutdown(socket.SHUT_RDWR)
        sock.close()

    try:
        stime = time.time()
        print(f'[LOG] {header} - {BUFFER} rows')
        for i in range(int(len(entries)/BUFFER)+1):
            partial = entries[i*BUFFER:(i+1)*BUFFER]+['']
            sock.sendall(('\n'.join(partial)).encode())
        print(f'[LOG] wrote {len(entries)} rows')

        global dbtime, dbrows
        dbtime += time.time()-stime
        dbrows += len(entries)
    except BaseException as e:
        _close()
        raise e
    _close()

pgconn = None
def db_pg_get():
    global pgconn
    if pgconn is None:
        pgconn = (psycopg2.connect(dbname='qdb', host=DB_P_HOST, port=DB_P_PORT,
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

    def _tstamp(ns):
        return datetime.utcfromtimestamp(int(ns)/10e9).isoformat()

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

                # binance has exch_time = 0
                header = f'orders_l2,{prepend},property={lvl}'
                tstamps = data.index.tolist()
                db_send([f'{header} prc={p[1]},qty={p[2]},'
                    + f'exch_time={p[0]*1.0} {int(t*1000):d}'
                    for t,p in list(zip(tstamps,data.values.tolist()))])

    fname = (get_data_file(exchsymb, dte, 'trade', allow_download=DO_DOWNLOAD))
    if fname:
        if not DO_WRITEDB:
            data = pandas.read_parquet(fname, columns=['exch_time'])
        else:
            header=f'trades,{prepend}'
            data = pandas.read_parquet(fname, columns=['exch_time','trd_prc','trd_qty','trd_side'])
            db_send([f'{header} prc={p[1]},qty={p[2]},side={"true" if p[3]==1 else "false"},'
                + f'exch_time={p[0]*1.0} {int(t*1000):d}' for t,p in
                list(zip(data.index.tolist(),data.values.tolist()))])

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

    for s in symbs:
        for d in dates:
            process_fn(s, d, 5)
    db_pg_close()

    print(dbtime, dbrows, dbtime/dbrows)

if __name__ == '__main__':
    main()
