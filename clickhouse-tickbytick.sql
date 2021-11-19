SELECT
    res.1 as timestamp,
    res.2 as prc0,
    res.3 as prc1, 
    res.2/res.3 as ratio
FROM (SELECT arrayJoin(arrayZip(
        groupArray(raw.timestamp),
        arrayFill(x->x!=-1, groupArray(raw.prc0)),
        arrayFill(x->x!=-1, groupArray(raw.prc1))
    )) as res
    FROM (SELECT timestamp,
            IF(exchsymb='binance_btc_usdt',prc,-1) AS prc0,
            IF(exchsymb='binance_eth_usdt',prc,-1) AS prc1
        FROM orders_l2
        WHERE (exchsymb='binance_btc_usdt' OR exchsymb='binance_eth_usdt')
        AND timestamp > '2021-09-01T00:00:10'
        AND timestamp < '2021-09-01T00:00:15'
        AND property = 'ask0'
        ORDER BY timestamp) as raw
) WHERE prc0>-1 and prc1>-1;
