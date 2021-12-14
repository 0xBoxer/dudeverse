
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

days AS 
(
SELECT generate_series(date_trunc('day',MIN(evt_block_time))::TIMESTAMP, date_trunc('day', NOW()), '1 day') AS DAY --starts a DAY series after the FIRST mint
FROM erc721."ERC721_evt_Transfer"
WHERE contract_address = CONCAT('\x', substring('{{address}}' FROM 3))::bytea  
)

, window_functions AS 
(
SELECT  date_trunc('minute', block_time) AS MINUTE,
        AVG(eth_amount) OVER w AS average,
        MIN(eth_amount) OVER w AS minimum,
        MAX(eth_amount) OVER w AS maximum,
        COUNT(eth_amount) OVER w AS w_count,
        dune_user_generated.smallset_median_disc(eth_amount) over w AS median,
        dune_user_generated.smallset_q1_disc(eth_amount) over w AS "1st quartile",
        dune_user_generated.smallset_q3_disc(eth_amount) over w AS "3rd quartile",
        eth_amount
        
        FROM raw_data
        window w AS (ORDER BY block_time ROWS BETWEEN {{rolling_n_trades}} preceding AND current ROW)
        
) 

,labels AS 
(
SELECT date_trunc('day', MINUTE) AS DAY,
        CASE WHEN eth_amount BETWEEN 0 AND "1st quartile" THEN '1st quartile'
          WHEN eth_amount BETWEEN "1st quartile" AND median THEN '2nd quartile'
          WHEN eth_amount BETWEEN median AND "3rd quartile" THEN '3rd quartile'
         WHEN eth_amount > "3rd quartile" THEN '4th quartile'
         END AS LABEL,
       
        SUM(eth_amount) AS eth_volume
        
        FROM window_functions

WHERE MINUTE > DATE_TRUNC('day',now()) - INTERVAL '{{Last n TIME units}}'
GROUP BY 1,2
ORDER BY LABEL, DAY ASC
)

SELECT 
days.day,
CASE WHEN LABEL ISNULL THEN 'no trades' ELSE LABEL END AS LABEL,
coalesce(eth_volume,0) AS eth_volume

FROM days
LEFT JOIN labels l ON days.day = l.day