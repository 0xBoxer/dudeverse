
--finding the moving average price of NFTs

WITH raw_data AS 
(
SELECT usd_amount/eth_price AS eth_amount, date_trunc('minute', block_time)AS block_time, tx_hash, nft_token_id
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
SELECT  date_trunc('minute', block_time) AS MINUTE,
        AVG(eth_amount) OVER w1 AS "n_MA",
        AVG(eth_amount) OVER w2 AS "n*2_MA",
        AVG(eth_amount) OVER w3 AS "n*5_MA",
        eth_amount,
        nft_token_id
        
        FROM raw_data
        window  w1 AS (ORDER BY block_time ROWS BETWEEN ({{rolling_n_trades}}) preceding AND current ROW),
                w2 AS (ORDER BY block_time ROWS BETWEEN ({{rolling_n_trades}}*2) preceding AND current ROW),
                w3 AS (ORDER BY block_time ROWS BETWEEN ({{rolling_n_trades}}*5) preceding AND current ROW)
)


SELECT * FROM window_functions

WHERE MINUTE > DATE_TRUNC('minute',now()) - INTERVAL '{{Last n TIME units}}'
