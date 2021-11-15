select a.prc prc0, b.prc prc1, a.prc/b.prc prc, a.timestamp from
(select avg(prc) prc, timestamp
    from orders_l2 
    where property='bid0'
    and exchsymb='binance_eth_usdt'
    sample by 1s fill(prev)
    align to calendar with offset '00:00') a
inner join
(select avg(prc) prc, timestamp
    from orders_l2 
    where property='bid0'
    and exchsymb='binance_btc_usdt'
    sample by 1s fill(prev)
    align to calendar with offset '00:00') b
on a.timestamp = b.timestamp
limit 10;
