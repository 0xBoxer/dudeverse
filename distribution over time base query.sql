WITH nfttransfers AS 
(
SELECT  "to" AS wallet,
        "from" AS fromwallet,
        contract_address,
        "tokenId" AS token_id,
        evt_block_time
        FROM erc721."ERC721_evt_Transfer"
        WHERE contract_address = CONCAT('\x', substring('{{address}}' FROM 3))::bytea
        ORDER BY evt_block_time ASC
)


,getting_state  AS 
(
SELECT  wallet,
        evt_block_time,
        contract_address,
        token_id,
        1 AS helper
FROM nfttransfers

UNION ALL

SELECT  fromwallet AS wallet,
        evt_block_time,
        contract_address,
        token_id,
        -1 AS helper
FROM nfttransfers    
)

,calc_state AS
(
SELECT  wallet,
        evt_block_time,
        token_id,
        contract_address,
        SUM(helper) over (PARTITION BY wallet, contract_address, token_id ORDER BY evt_block_time ASC) AS num_tokens
FROM getting_state
    WHERE wallet <> '\x0000000000000000000000000000000000000000'
    -- GROUP BY evt_block_time, wallet, token_id, contract_address
)

,calc_state_lead_data AS 
(
SELECT  wallet,
        evt_block_time,
        token_id,
        contract_address,
        num_tokens,
        lead(evt_block_time, 1, now()) OVER (PARTITION BY wallet, contract_address, token_id ORDER BY evt_block_time ASC) AS next_evt 
FROM calc_state
)

,filled_data AS
(
SELECT  dday.day, 
        a.contract_address,
        a.token_id,
        num_tokens,
        wallet

FROM    (
         SELECT generate_series(date_trunc('day',MIN(evt_block_time))::TIMESTAMP, date_trunc('day', NOW()), '1 day') AS day 
         FROM erc721."ERC721_evt_Transfer"
         WHERE contract_address = CONCAT('\x', substring('{{address}}' FROM 3))::bytea  
        ) AS dday --starts a DAY series after the FIRST mint
LEFT JOIN calc_state_lead_data a ON date_trunc('day', a.evt_block_time) <= dday.day AND dday.day < date_trunc('day', a.next_evt)
)

-----------------------END OF BASE QUERY-----------------------------------------------------------------

Select * from filled_data