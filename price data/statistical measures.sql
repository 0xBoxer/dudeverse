
--building out statistical measures over n last trades

--selecting the data I want to work WITH
WITH raw_data AS 
(
SELECT usd_amount/eth_price AS eth_amount, date_trunc('minute', block_time)AS block_time, tx_hash, nft_token_id
FROM nft.trades
LEFT JOIN
--joining prices to get ETH price regardless of currency used
(
SELECT MINUTE, price AS eth_price
FROM prices.usd
WHERE symbol = 'WETH'
) AS prices ON date_trunc('minute', block_time) = MINUTE

WHERE nft_contract_address = CONCAT('\x', substring('{{address}}' FROM 3))::bytea
AND trade_type = 'Single Item Trade'
AND usd_amount > 0
--limiting to contract_address of NFT
--limiting to only INCLUDE non bundle transactions
--trying to correct for private sales/transfers, sometimes still doesn't work
)

-- calculating statistical measures USING pgsql windows functions
, window_functions AS 
(
SELECT  date_trunc('minute', block_time) AS MINUTE,
        AVG(eth_amount) OVER w AS average,
        MIN(eth_amount) OVER w AS minimum,
        MAX(eth_amount) OVER w AS maximum,
        COUNT(eth_amount) OVER w AS w_count,
        dune_user_generated.smallset_median_disc(eth_amount) over w AS median,
        dune_user_generated.smallset_q1_disc(eth_amount) over w AS "first quartile",
        dune_user_generated.smallset_q3_disc(eth_amount) over w AS "third quartile",
        eth_amount
        
        FROM raw_data
        window w AS (ORDER BY block_time ROWS BETWEEN {{rolling_n_trades}} preceding AND current ROW)
--window w = n rolling trades
) 
SELECT * FROM window_functions

WHERE MINUTE > DATE_TRUNC('minute',now()) - INTERVAL '{{Last n TIME units}}'

--selecting results AND limiting result dataset to certain timeframe
--only limiting timeframe here so statistical measures don't start at 0