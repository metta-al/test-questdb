'''
# actually dunnid also can
CREATE TABLE IF NOT EXISTS orders_l2(
    tstamp timestamp,
    exchsymb SYMBOL,
    property SYMBOL,
    prc DOUBLE,
    qty DOUBLE)
TIMESTAMP(tstamp);
'''

import pandas
import socket

from crypto.core import exch_symbol_from_name

from simulator.utils import get_data_file

DB_HOST = 'localhost'
DB_PORT = 9009

BUFFER=1024
def db_ordersl2(entries, prepend=''):
    """
        Binance got weird issue where exch_time=0
    """

    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((DB_HOST, DB_PORT))
        for i in range(int(len(entries)/BUFFER)+1):
            partial = entries[i*BUFFER:(i+1)*BUFFER]
            sock.sendall(('\n'.join([
                f'orders_l2,{prepend} prc={p[1]},qty={p[2]},exch_time={p[0]*1.0} {int(t*1000):d}'
                for t,p in partial if p[0]>10])).encode())
            print(f'[LOG] {prepend} {len(partial)} rows ({i*BUFFER})')
    except BaseException as e:
        sock.close()
        raise e

def process_fn(sym, dte, numlvl=1):
    exchsymb = exch_symbol_from_name(sym)
    prepend = f'exchsymb={exchsymb.tech_name()}'
    print(f'[LOG] processing {prepend} for {dte}')

    fname = (get_data_file(exchsymb, dte, 'book', allow_download=True))
    level = [f'bid{i}' for i in range(numlvl)]
    level += [f'ask{i}' for i in range(numlvl)]

    fullcols = ['exch_time']
    for l in level:
        fullcols.append(f'{l}_prc')
        fullcols.append(f'{l}_qty')
    data = pandas.read_parquet(fname, columns=fullcols)

    for lvl in level:
        cols = ['exch_time', f'{lvl}_prc',f'{lvl}_qty']
        db_ordersl2(list(zip(data.index.tolist(),
            data[cols].values.tolist())), f'{prepend},property={lvl}')

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
