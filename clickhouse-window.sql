-- window size of 3
SELECT *, moving_prc0/moving_prc1 as moving_ratio FROM (SELECT
    candles.tstamp as tstamp,
    avg(candles.prc0) OVER (ORDER BY candles.tstamp 
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as moving_prc0,
    avg(candles.prc1) OVER (ORDER BY candles.tstamp 
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as moving_prc1,
    candles.prc0 as _prc0,
    candles.prc0 as _prc1
FROM (SELECT
        filled.ts_sec as tstamp,
        avg(filled.prc0) as prc0,
        avg(filled.prc1) as prc1
    FROM (SELECT
            toStartOfSecond(res.1) as ts_sec,
            res.1 as timestamp,
            res.2 as prc0,
            res.3 as prc1
        FROM (SELECT arrayJoin(arrayZip(
                groupArray(raw.timestamp),
                arrayFill(x->x!=-1, groupArray(raw.prc0)),
                arrayFill(x->x!=-1, groupArray(raw.prc1))
            )) AS res
            FROM (SELECT timestamp,
                    IF(exchsymb='binance_btc_usdt',prc,-1) AS prc0,
                    IF(exchsymb='binance_eth_usdt',prc,-1) AS prc1
                FROM orders_l2
                WHERE (exchsymb='binance_btc_usdt' OR exchsymb='binance_eth_usdt')
                AND timestamp > '2021-09-01T00:10'
                AND timestamp < '2021-09-01T00:15'
                AND property = 'ask0'
                ORDER BY timestamp) AS raw
        ) WHERE prc0>-1 and prc1>-1) AS filled
    GROUP BY filled.ts_sec ORDER BY filled.ts_sec) candles);
