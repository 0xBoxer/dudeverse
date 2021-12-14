SELECT contract_address, first_mint_time + (INTERVAL '1 DAY' * n) AS day_n, n
    FROM (
    SELECT contract_address, MIN(evt_block_time)::DATE AS first_mint_time
    FROM erc721."ERC721_evt_Transfer"
    WHERE contract_address = CONCAT('\x', substring('{{address}}' FROM 3))::bytea
    GROUP BY 1
    ) b , generate_series(0, 100000 , 1) AS r(n)
    WHERE first_mint_time + (INTERVAL '1 DAY' * n) <= now()
    ORDER BY n DESC