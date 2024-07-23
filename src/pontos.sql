SELECT 
       '{dt_ref}' AS dtRef,
       idCliente,
       sum(nrPontosTransacao) AS nrQtdePontos,
       sum(case when nrPontosTransacao > 0 then nrPontosTransacao else  0 end) AS nrQtdePontosPos,
       sum(case when nrPontosTransacao < 0 then nrPontosTransacao else  0 end) AS nrQtdePontosNeg

FROM silver.upsell.transacoes

WHERE dtTransacao < '{dt_ref}'
AND dtTransacao >= '{dt_ref}' - INTERVAL 28 DAY

GROUP BY idCliente, dtRef