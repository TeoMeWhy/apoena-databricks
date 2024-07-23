SELECT 
       '{dt_ref}' AS dtRef,
       idCliente,
       count(idTransacao) AS nrQtdeTransacao,
       count(case when nrPontosTransacao > 0 then idTransacao end) AS nrQtdeTransacaoPos,
       count(case when nrPontosTransacao < 0 then idTransacao end) AS nrQtdeTransacaoNeg

FROM silver.upsell.transacoes

WHERE dtTransacao < '{dt_ref}'
AND dtTransacao >= '{dt_ref}' - INTERVAL 28 DAY

GROUP BY idCliente, dtRef