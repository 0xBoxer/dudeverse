WITH raw_data AS 
(
SELECT usd_amount AS eth_amount, date_trunc('minute', block_time)AS block_time, tx_hash, nft_token_id
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



SELECT SUM(eth_amount) over (ORDER BY block_time ASC), block_time  FROM raw_data
ORDER BY 2 DESC
LIMIT 1

