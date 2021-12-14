WITH running_wallet_balances AS   
    (WITH base_data AS  
        (WITH days AS 
            (SELECT generate_series(date_trunc('day',MIN(evt_block_time))::TIMESTAMP, date_trunc('day', NOW()), '1 day') AS DAY
            FROM erc721."ERC721_evt_Transfer"
                               WHERE contract_address = CONCAT('\x', substring('{{address}}' FROM 3))::bytea  ),
        all_wallets AS
            (SELECT 
            DISTINCT wallet 
            FROM 
                (SELECT
                "from" AS wallet
                FROM erc721."ERC721_evt_Transfer"
                               WHERE contract_address = CONCAT('\x', substring('{{address}}' FROM 3))::bytea  
                                

                UNION all 
                SELECT
                "to" AS wallet 
                FROM erc721."ERC721_evt_Transfer"
                WHERE contract_address = CONCAT('\x', substring('{{address}}' FROM 3))::bytea  
                
)
            distinct_wallets)
        SELECT 
        DAY,
        wallet
        FROM 
        days
        full OUTER JOIN all_wallets ON TRUE),
        
    aggregated_transfers AS 
        (WITH transfers AS 
            ((SELECT 
            date_trunc('day',evt_block_time) AS DAY,
            "to" AS wallet,
            COUNT(evt_tx_hash) AS VALUE
           FROM erc721."ERC721_evt_Transfer"
                               WHERE contract_address = CONCAT('\x', substring('{{address}}' FROM 3))::bytea  
                              
           
            GROUP BY DAY, wallet)
            
            UNION all 
            
            (SELECT 
            date_trunc('day',evt_block_time) AS DAY,
            "from" AS wallet,
            COUNT(evt_tx_hash)*-1 AS VALUE
            FROM erc721."ERC721_evt_Transfer"
                              WHERE contract_address = CONCAT('\x', substring('{{address}}' FROM 3))::bytea  
                             
           
            GROUP BY DAY, wallet))
        SELECT
        DAY,
        wallet,
        SUM(VALUE) AS resulting
        FROM 
        transfers 
        GROUP BY DAY, wallet)
        
        SELECT 
        base_data.day,
        base_data.wallet,
        SUM(coalesce(resulting,0)) over (PARTITION BY base_data.wallet ORDER BY base_data.day) AS holding
        FROM 
        base_data
        LEFT JOIN aggregated_transfers ON base_data.day = aggregated_transfers.day AND base_data.wallet = aggregated_transfers.wallet 
        )
    
    SELECT 
   
 DAY::TIMESTAMP AS DAY,
-- DISTINCT wallet 
  COUNT(wallet) FILTER (WHERE holding > 0),
 (COUNT(wallet) FILTER (WHERE holding > 0) - LAG(COUNT(wallet) FILTER (WHERE holding > 0),1) OVER (ORDER BY DAY::TIMESTAMP DESC))*-1 AS daily_change
    FROM running_wallet_balances
WHERE DAY > DATE_TRUNC('minute',now()) - INTERVAL '{{Last n TIME units}}'
    GROUP BY 1
    ORDER BY DAY DESC
    
    
    

    