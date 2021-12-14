
--finding the moving average price of NFTs

WITH raw_data AS 
(
SELECT usd_amount AS usd_amount, date_trunc('minute', block_time)AS block_time, tx_hash, nft_token_id
FROM nft.trades
LEFT JOIN
(
SELECT MINUTE, price AS eth_price
FROM prices.usd
WHERE symbol = 'WETH'
) AS prices ON date_trunc('minute', block_time) = MINUTE

WHERE nft_contract_address = CONCAT('\x', substring('{{address}}' FROM 3))::bytea
AND trade_type = 'Single Item Trade'
AND usd_amount > 0
)
, window_functions AS 
(
SELECT  date_trunc('day', block_time) AS DAY,
        SUM(usd_amount) AS usd_volume,
        COUNT(*) AS "number of trades"
        FROM raw_data
        GROUP BY 1
        ORDER BY 1
),

days AS 
(
SELECT generate_series(date_trunc('day',MIN(evt_block_time))::TIMESTAMP, date_trunc('day', NOW()), '1 day') AS DAY
FROM erc721."ERC721_evt_Transfer"
WHERE contract_address = CONCAT('\x', substring('{{address}}' FROM 3))::bytea  
)


SELECT days.day, coalesce(usd_volume, 0) AS usd_volume, coalesce("number of trades",0) AS "number of trades" 
FROM days
LEFT JOIN window_functions w ON days.day = w.day
WHERE days.day > DATE_TRUNC('day',now()) - INTERVAL '{{Last n TIME units}}'

