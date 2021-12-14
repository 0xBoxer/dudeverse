
--finding the moving average price of NFTs

WITH raw_data AS 
(
SELECT usd_amount/eth_price AS eth_amount, date_trunc('day', block_time)AS DAY, tx_hash, nft_token_id 
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
),

days AS 
(
SELECT generate_series(date_trunc('day',MIN(evt_block_time))::TIMESTAMP, date_trunc('day', NOW()), '1 day') AS DAY
FROM erc721."ERC721_evt_Transfer"
WHERE contract_address = CONCAT('\x', substring('{{address}}' FROM 3))::bytea  
)


SELECT days.day, coalesce(eth_amount,0), CASE WHEN "nft_token_id" ISNULL THEN 'no_trades' ELSE nft_token_id END AS nft_token_id FROM days
LEFT JOIN raw_data r ON days.day = r.day

WHERE days.day > DATE_TRUNC('minute',now()) - INTERVAL '{{Last n TIME units}}'