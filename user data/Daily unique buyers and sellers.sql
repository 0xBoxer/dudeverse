--generating a series of DAY to make sure that EMPTY days ARE still displayed

WITH days AS
    (SELECT generate_series(date_trunc('day',MIN(evt_block_time))::TIMESTAMP, date_trunc('day', NOW()), '1 day') AS DAY
            FROM erc721."ERC721_evt_Transfer"
            WHERE contract_address = CONCAT('\x', substring('{{address}}' FROM 3))::bytea), --filtering for the correct NFT
            
--getting all trades AND UNIQUE buyers AND sellers per DAY
sales_data AS 
    (SELECT 
    date_trunc('day',block_time) AS DAY,
    COUNT(DISTINCT "seller") AS unique_sellers,
    COUNT(DISTINCT "buyer") AS unique_buyers
    FROM nft.trades
    WHERE nft_contract_address = CONCAT('\x', substring('{{address}}' FROM 3))::bytea --filtering for the correct NFT
    GROUP BY DAY)
    
SELECT 
days.day,
unique_sellers,
unique_buyers
FROM 
days 
LEFT JOIN sales_data ON days.day = sales_data.day
WHERE days.day > DATE_TRUNC('minute',now()) - INTERVAL '{{Last n TIME units}}'

--joining sales data ON TIME series
