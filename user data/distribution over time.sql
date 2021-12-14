WITH agg AS 
(
WITH nfttransfers AS 
                    (
                    SELECT  
                    "to" AS wallet,
                    "from" AS fromwallet, 
                    date_trunc('day', evt_block_time) AS DAY 
                    FROM erc721."ERC721_evt_Transfer"
                    WHERE contract_address = CONCAT('\x', substring('{{address}}' FROM 3))::bytea
                    ORDER BY DAY ASC
                    ),
        
distributionbyperiod AS 
                        (
                        WITH transfercounter AS
                            (
                            WITH transactions AS 
                            (
                            
                                SELECT wallet, DAY, 1 AS VALUE
                                FROM nfttransfers
                        
                                UNION all
                                
                                (SELECT fromwallet AS wallet, DAY, -1 AS VALUE
                                FROM nfttransfers)
                                )
                        
                                SELECT
                                wallet, 
                                DAY,
                                SUM(VALUE) AS num_tokens_in_period
                                FROM transactions
                                WHERE wallet <> '\x0000000000000000000000000000000000000000'
                                GROUP BY DAY, wallet
                                ORDER BY DAY ASC, num_tokens_in_period DESC
                            )
     
        SELECT DAY, wallet, SUM(num_tokens_in_period) over (PARTITION BY wallet ORDER BY DAY) AS num_tokens FROM transfercounter ORDER BY DAY
                        )
        
    SELECT  dday.day,
            dwallet.wallet, 
            distrib.day AS dday, 
            distrib.num_tokens, 
            ROW_NUMBER() OVER (PARTITION BY dday.day, dwallet.wallet ORDER BY distrib.day DESC) AS no
    FROM            (
                    SELECT generate_series(date_trunc('day',MIN(evt_block_time))::TIMESTAMP, date_trunc('day', NOW()), '1 day') AS DAY 
                    FROM erc721."ERC721_evt_Transfer"
                    WHERE contract_address = CONCAT('\x', substring('{{address}}' FROM 3))::bytea  
                    ) AS dday --starts a DAY series after the FIRST mint
    CROSS JOIN      (SELECT DISTINCT wallet AS wallet FROM nfttransfers) AS dwallet --creating every wallet ON every DAY
    LEFT JOIN       distributionbyperiod distrib ON distrib.day <= dday.day AND distrib.wallet = dwallet.wallet --joining the distrubtion TABLE IN a way that fills gaps
    WHERE distrib.day IS NOT NULL)


SELECT DAY, COUNT(wallet) FILTER (WHERE num_tokens = 1) AS num_wallets_1,
    COUNT(wallet) FILTER (WHERE num_tokens > 1 AND num_tokens <=5) AS num_wallets_2, 
    COUNT(wallet) FILTER (WHERE num_tokens > 5 AND num_tokens <=25) AS num_wallets_3,  
    COUNT(wallet) FILTER (WHERE num_tokens > 25 AND num_tokens <=100) AS num_wallets_4, 
    COUNT(wallet) FILTER (WHERE num_tokens > 100 AND num_tokens <=250) AS num_wallets_5, 
    COUNT(wallet) FILTER (WHERE num_tokens > 250) AS num_wallets_6, 
    SUM(num_tokens) FILTER (WHERE num_tokens = 1) AS total_tokens_1,
    SUM(num_tokens) FILTER (WHERE num_tokens > 1 AND num_tokens <=5) AS total_tokens_2, 
    SUM(num_tokens) FILTER (WHERE num_tokens > 5 AND num_tokens <=25) AS total_tokens_3,  
    SUM(num_tokens) FILTER (WHERE num_tokens > 25 AND num_tokens <=100) AS total_tokens_4, 
    SUM(num_tokens) FILTER (WHERE num_tokens > 100 AND num_tokens <=250) AS total_tokens_5, 
    SUM(num_tokens) FILTER (WHERE num_tokens > 250) AS total_tokens_6
FROM agg
WHERE num_tokens >= 0 AND no = 1
AND DAY > DATE_TRUNC('minute',now()) - INTERVAL '{{Last n TIME units}}'
GROUP BY DAY
ORDER BY DAY ASC