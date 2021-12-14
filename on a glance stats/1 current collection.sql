WITH addresscte AS 
(
SELECT * FROM (VALUES
    (CONCAT('\x', substring('{{address}}' FROM 3))::bytea))t(address)
)
    SELECT address, CASE WHEN name IS NULL THEN 'n/a' ELSE name END AS name, contract_address
    FROM addresscte
    LEFT JOIN nft.tokens ON address = contract_address
