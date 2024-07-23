WITH tb_transacao AS (

  SELECT DISTINCT idCliente,
        DATE(dtTransacao) AS dtRef

  FROM silver.upsell.transacoes

)

SELECT 
        string(t1.dtRef + INTERVAL 1 DAY) AS dtRef,
        t1.idCliente,
        max(case when t2.idCliente is not null then 1 else 0 end ) as flActivate
       
FROM tb_transacao as t1

LEFT JOIN tb_transacao AS t2
ON t1.idCliente = t2.idCliente
AND t1.dtRef < t2.dtRef
AND t1.dtRef > t2.dtRef - interval 3 day

WHERE day(t1.dtRef + INTERVAL 1 DAY) = 1

GROUP BY ALL