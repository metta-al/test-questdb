select a.prc prc0, b.prc prc1, a.prc/b.prc prc, a.tstamp from
(select avg(prc) prc, toStartOfSecond(timestamp) tstamp
    from orders_l2 
    where property='bid0'
    and exchsymb='binance_eth_usdt'
    group by tstamp) a
inner join
(select avg(prc) prc, toStartOfSecond(timestamp) tstamp
    from orders_l2 
    where property='bid0'
    and exchsymb='binance_btc_usdt'
    group by tstamp) b
on a.tstamp = b.tstamp
order by tstamp
limit 10;

