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
        lead(evt_block_time, 1, now()) OVER (PARTITION BY wallet, contract_address, token_id ORDER BY evt_block_time ASC) AS next_evt --looks for the next event associated with this wallet, this NFT collection and this token_id
FROM calc_state
)

,filled_data AS
(
SELECT  dday.day, 
        a.contract_address,
        CASE WHEN num_tokens = 1 THEN a.token_id ELSE NULL END AS token_id, --drops the token_id from the table if item was transferred out
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

--aggregating data on wallet level

,wallet_data as
(
SELECT  SUM(num_tokens) AS nft_owned,
        array_agg(token_id),
        DAY,
        wallet

FROM filled_data
        where token_id is not null --to clean up the arrays, otherwise they will contain null values
        GROUP BY day, wallet
        HAVING SUM(num_tokens) >= 1 --only include Wallets that have a balance > 1
        ORDER BY day ASC, nft_owned DESC
)

--classifying wallets

Select  sum(nft_owned) as "items in wallet cohort",
        count(wallet) as "number of wallets in cohort",
        DAY,
        --the colorcodes are graded from green to red
        CASE    WHEN nft_owned between 0 and 1 THEN '0-1 NFTs owned' --#2CC601
                WHEN nft_owned between 2 and 5 THEN '2-5 NFTs owned' --#32E600
                WHEN nft_owned between 6 and 15 THEN '6-15 NFTs owned' --#E1F014
                WHEN nft_owned between 16 and 50 THEN '16-50 NFTs owned'--#DB8000
                WHEN nft_owned between 51 and 100 THEN '51-100 NFTs owned' --#F54747
                ELSE '100+ NFTs owned' --#E00000
        END AS label
from wallet_data
group by DAY, label
order by label desc
