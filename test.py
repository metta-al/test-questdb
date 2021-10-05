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
'''

import pandas
import socket

from crypto.core import exch_symbol_from_name

from simulator.utils import get_data_file

DO_DOWNLOAD = True
DO_WRITEDB = False

DB_HOST = 'localhost'
DB_PORT = 9009

BUFFER=4096
def db_send(entries):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((DB_HOST, DB_PORT))

    def _close():
        sock.shutdown(socket.SHUT_RDWR)
        sock.close()

    try:
        for i in range(int(len(entries)/BUFFER)+1):
            partial = entries[i*BUFFER:(i+1)*BUFFER]
            sock.sendall(('\n'.join(partial)).encode())
            print(f'[LOG] {len(partial)} rows, starting from {i*BUFFER}')
    except BaseException as e:
        _close()
        raise e
    _close()

def process_fn(sym, dte, numlvl=1):
    exchsymb = exch_symbol_from_name(sym)
    prepend = f'exchsymb={exchsymb.tech_name()}'
    print(f'[LOG] processing {prepend} for {dte}')

    fname = (get_data_file(exchsymb, dte, 'book', allow_download=DO_DOWNLOAD))
    if DO_WRITEDB and fname:
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
            header=f'orders_l2,{prepend},property={lvl}'
            db_send([f'{header} prc={p[1]},qty={p[2]},exch_time={p[0]*1.0} {int(t*1000):d}'
                for t,p in list(zip(data.index.tolist(),data.values.tolist())) if p[0]>0])

    fname = (get_data_file(exchsymb, dte, 'trade', allow_download=DO_DOWNLOAD))
    if DO_WRITEDB and fname:
        pass
        header=f'trades,{prepend},property={lvl}'
        db_send([f'{header} prc={p[1]},qty={p[2]},exch_time={p[0]*1.0} {int(t*1000):d}'
            for t,p in list(zip(data.index.tolist(),data.values.tolist())) if p[0]>0])
        cols = ['exch_time','trd_prc','trd_qty','trd_side']
    data = pandas.read_parquet(fname)
    print(data.columns)
    print(data['trd_side'])

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

    for s in symbs:
        for d in dates:
            process_fn(s, d, 5)

if __name__ == '__main__':
    main()
