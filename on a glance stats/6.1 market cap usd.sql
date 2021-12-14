--https://stackoverflow.com/questions/61420637/how-to-calculate-moving-median-with-percentile-disc-and-window
--implementing aggregate functions for median, q1, q3 IN other query

--finding the moving average price of NFTs

WITH raw_data AS 
(
SELECT usd_amount AS usd_amount, date_trunc('minute', block_time)AS block_time, tx_hash, nft_token_id
FROM nft.trades

WHERE nft_contract_address = CONCAT('\x', substring('{{address}}' FROM 3))::bytea
AND trade_type = 'Single Item Trade'
AND usd_amount > 0
)

, supply AS 

(SELECT SUM(minted) AS "circulating supply" FROM 

(

SELECT COUNT(*) AS minted FROM erc721."ERC721_evt_Transfer"

WHERE contract_address = CONCAT('\x', substring('{{address}}' FROM 3))::bytea
AND "from" = '\x0000000000000000000000000000000000000000'

UNION ALL

SELECT -count(*) AS burned FROM erc721."ERC721_evt_Transfer"

WHERE contract_address = CONCAT('\x', substring('{{address}}' FROM 3))::bytea
AND ("to" = '\x0000000000000000000000000000000000000000' OR "to" = '\x000000000000000000000000000000000000dead')
) s)

, window_functions AS 
(
SELECT  date_trunc('minute', block_time) AS MINUTE,
        AVG(usd_amount) OVER w1 AS "n_average"
        
        FROM raw_data
        window  w1 AS (ORDER BY block_time ROWS BETWEEN ({{rolling_n_trades}}) preceding AND current ROW)
) 
SELECT MINUTE, n_average*"circulating supply" AS mcap, "circulating supply"   FROM window_functions, supply

ORDER BY MINUTE DESC
LIMIT 1

