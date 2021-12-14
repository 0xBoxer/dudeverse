
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
---and number_of_items = 1
--todo: find way to FILTER txs for nft_contract_address_array[n] = nft_contract_address_array[n]
)
, window_functions AS 
(
SELECT  date_trunc('day', block_time) AS DAY,
        SUM(eth_amount) over (ORDER BY date_trunc('day', block_time)) AS "cum_eth_volume",
        COUNT(eth_amount) over (ORDER BY date_trunc('day', block_time)) AS "cum_number of trades"
        FROM raw_data
        ORDER BY 1
) 
SELECT * FROM window_functions

WHERE DAY > DATE_TRUNC('minute',now()) - INTERVAL '{{Last n TIME units}}'

