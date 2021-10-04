'''
# actually dunnid also can
CREATE TABLE IF NOT EXISTS orders_l2(
    timestamp timestamp,
    exchsymb SYMBOL,
    property SYMBOL,
    exch_time INT,
    prc FLOAT,
    qty FLOAT) partition by DAY;
CREATE TABLE IF NOT EXISTS trades(
    timestamp timestamp,
    exchsymb SYMBOL,
    exch_time DOUBLE,
    maker_is_ask BOOLEAN,
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

BUFFER=8192
def db_ordersl2(entries, prepend=''):
    """
        Binance got weird issue where exch_time=0
    """

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((DB_HOST, DB_PORT))

    def _close():
        sock.shutdown(socket.SHUT_RDWR)
        sock.close()

    try:
        sock.sendall(('\n'.join([
            f'orders_l2,{prepend} prc={p[1]},qty={p[2]},exch_time={p[0]*1.0} {int(t*1000):d}'
            for t,p in entries if p[0]>10])).encode())
        """
        for i in range(int(len(entries)/BUFFER)+1):
            partial = entries[i*BUFFER:(i+1)*BUFFER]
            sock.sendall(('\n'.join([
                f'orders_l2,{prepend} prc={p[1]},qty={p[2]},exch_time={p[0]*1.0} {int(t*1000):d}'
                for t,p in partial if p[0]>10])).encode())
            print(f'[LOG] {prepend} {len(partial)} rows ({i*BUFFER})')
        """
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
        data = pandas.read_parquet(fname, columns=fullcols)

        for lvl in level:
            cols = ['exch_time', f'{lvl}_prc',f'{lvl}_qty']
            db_ordersl2(list(zip(data.index.tolist(),
                data[cols].values.tolist())), f'{prepend},property={lvl}')

    fname = (get_data_file(exchsymb, dte, 'trade', allow_download=DO_DOWNLOAD))
    if DO_WRITEDB and fname:
        data = pandas.read_parquet(fname)
        print(data)

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
