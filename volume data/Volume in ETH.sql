
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
),
--generating a DAY series to adjust for days WITH no sales
days AS 
(
SELECT generate_series(date_trunc('day',MIN(evt_block_time))::TIMESTAMP, date_trunc('day', NOW()), '1 day') AS DAY
FROM erc721."ERC721_evt_Transfer"
WHERE contract_address = CONCAT('\x', substring('{{address}}' FROM 3))::bytea  
)

, window_functions AS 
(
SELECT  date_trunc('day', block_time) AS DAY,
        CASE    WHEN eth_amount BETWEEN 0 AND 2 THEN '0-2 ETH Txns'
                WHEN eth_amount BETWEEN 2 AND 5 THEN '2-5 ETH Txns'
                WHEN eth_amount BETWEEN 5 AND 10 THEN '5-10 ETH Txns'
                WHEN eth_amount BETWEEN 10 AND 40 THEN '10-40 ETH Txns'
                ELSE '40+ ETH Txns' 
        END AS LABEL,
         CASE   WHEN eth_amount BETWEEN 0 AND 2 THEN 0
                WHEN eth_amount BETWEEN 2 AND 5 THEN 1
                WHEN eth_amount BETWEEN 5 AND 10 THEN 2
                WHEN eth_amount BETWEEN 10 AND 40 THEN 3
                ELSE 4 
        END AS label_order,
        SUM(eth_amount) AS eth_volume,
        COUNT(eth_amount) AS trd_cnt
        FROM raw_data
        GROUP BY 1,2,3
        ORDER BY 1,3
) 

SELECT 
days.day, 
CASE WHEN LABEL ISNULL THEN 'no trades' ELSE LABEL END AS LABEL,
coalesce(eth_volume,0) AS eth_volume, 
coalesce(trd_cnt,0) AS eth_amount
FROM days
LEFT JOIN window_functions w ON days.day = w.day

WHERE days.day > DATE_TRUNC('minute',now()) - INTERVAL '{{Last n TIME units}}'

