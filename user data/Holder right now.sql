WITH 
last AS (
    SELECT 
    current_owner,
    MAX(evt_block_time) block_time_last_transfer,
    COUNT(*) AS cnt,
    ARRAY_AGG("tokenId") AS ids
    
    
    FROM (
        SELECT  REPLACE("to"::TEXT, '\', '0') AS current_owner,
                t."tokenId",
                t.evt_block_time
        FROM erc721."ERC721_evt_Transfer" t
        JOIN (
            SELECT "tokenId", MAX(evt_block_number) AS evt_block_number
            FROM erc721."ERC721_evt_Transfer" t
            WHERE t.contract_address = CONCAT('\x', substring('{{address}}' FROM 3))::bytea
            AND t.evt_block_time <= NOW() 
            GROUP BY 1 
        ) t2 ON t2."tokenId" = t."tokenId" AND t2.evt_block_number = t.evt_block_number
    ) t3
    GROUP BY 1
)

SELECT
current_owner AS "owner",
cnt AS "NFTs owned"
FROM last
ORDER BY cnt DESC







