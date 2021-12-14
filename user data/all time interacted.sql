WITH    first_nft_per_wallet AS 
        (
        SELECT DISTINCT ON ("to") * 
        FROM erc721."ERC721_evt_Transfer" t
        WHERE contract_address = CONCAT('\x',substring('{{address}}'from 3))::bytea
        ORDER BY "to", t.evt_block_time ASC
        )

        ,base AS 
        (
        SELECT  DATE_TRUNC('day', evt_block_time) dt,
                MAX(cnt) AS cnt
        FROM    (
                SELECT  *,
                        COUNT(*) OVER (ORDER BY evt_block_time ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cnt
                FROM first_nft_per_wallet fa
                ) t
                GROUP BY 1
        )

SELECT *, cnt - LAG(cnt) OVER (ORDER BY dt ASC) AS daily_change
FROM base
WHERE dt > DATE_TRUNC('minute', now()) - INTERVAL '{{Last n TIME units}}'