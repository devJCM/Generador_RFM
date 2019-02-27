drop table if exists rfm_in,rfm_out,clv_out;

-- rfm_in --
CREATE TABLE IF NOT EXISTS rfm_in (Id INT(11) NOT NULL AUTO_INCREMENT PRIMARY KEY,
								   Ejecucion DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                                   Id_cliente VARCHAR(125),
                                   Nombre VARCHAR(125),
                                   Fecha datetime,
                                   Monto double(28,6));
--     
-- rfm_out --
CREATE TABLE IF NOT EXISTS rfm_out (Id INT(11) NOT NULL AUTO_INCREMENT PRIMARY KEY,
								   Ejecucion DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                                   Id_cliente VARCHAR(125),
                                   Nombre VARCHAR(125),
                                   Recencia_in char(20),
                                   Frecuencia_in int(5),
                                   Monto_in double(28,6),
                                   Recencia_out int(5),
                                   Frecuencia_out int(5),
                                   Monto_out int(5),
                                   Segmento char(30));
--  
-- clv_out --
CREATE TABLE IF NOT EXISTS clv_out (Id INT(11) NOT NULL AUTO_INCREMENT PRIMARY KEY,
								   Ejecucion DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                                   Id_cliente VARCHAR(125),
                                   Nombre VARCHAR(125),
                                   Frecuencia_in int(5),
                                   Monto_in double(28,6), 
                                   Valor_cliente double(28,6), 
                                   Vida_cliente double(16,4),
                                   CLV double(28,6));
--
select * from rfm_in;
select * from rfm_out;
select * from clv_out;

-- select fix
SET sql_mode=(SELECT REPLACE(@@sql_mode,'ONLY_FULL_GROUP_BY',''));
--
-- selects de clv
select distinct Id_cliente from rfm_in;
SELECT TIMESTAMPDIFF(MONTH,(select MAX(Fecha) FROM rfm_in WHERE Id_cliente='e45d4e71-22c6-421d-85ec-77ecaead04e9') , (select MIN(Fecha) FROM rfm_in WHERE Id_cliente='e45d4e71-22c6-421d-85ec-77ecaead04e9'));
--
-- select de RFM
SELECT Id_cliente,Nombre,max(Fecha) as 'Recencia',count(Monto) as 'Frecuencia',AVG(Monto) as 'Monto' FROM rfm_in group by Id_cliente;
--
-- llenar datos de rfm_in
insert into rfm_in(Id_cliente,Nombre,Fecha,Monto)
SELECT a.id,a.name,op.date_entered,op.amount FROM accounts a, opportunities op,accounts_opportunities ao WHERE a.id=ao.account_id AND op.id=ao.opportunity_id;                                   
--