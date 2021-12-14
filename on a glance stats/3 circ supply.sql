

--same name due to UNION statement, read minted-burned

SELECT SUM(minted) AS "circulating supply" FROM 

(

SELECT COUNT(*) AS minted FROM erc721."ERC721_evt_Transfer"

WHERE contract_address = CONCAT('\x', substring('{{address}}' FROM 3))::bytea
AND "from" = '\x0000000000000000000000000000000000000000'

UNION ALL

SELECT -count(*) AS burned FROM erc721."ERC721_evt_Transfer"

WHERE contract_address = CONCAT('\x', substring('{{address}}' FROM 3))::bytea
AND ("to" = '\x0000000000000000000000000000000000000000' OR "to" = '\x000000000000000000000000000000000000dead')
) s


--to DO: implement erc1155 implementation

