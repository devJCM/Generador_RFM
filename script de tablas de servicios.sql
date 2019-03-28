drop table if exists rfm_out;
drop table if exists clv_out;
drop table if exists nbo_out;
drop table if exists acreedor_out;

-- rfm_in --
CREATE TABLE IF NOT EXISTS rfm_in (Id INT(11) NOT NULL AUTO_INCREMENT PRIMARY KEY,
								   Ejecucion DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                                   Id_cliente VARCHAR(125),
                                   Fecha datetime,
                                   Monto double(28,6));
--   
-- rfm_out --
CREATE TABLE IF NOT EXISTS rfm_out (Id INT(11) NOT NULL AUTO_INCREMENT PRIMARY KEY,
								   Ejecucion DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                                   Id_cliente VARCHAR(125),
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
                                   Frecuencia_in int(5),
                                   Monto_in double(28,6), 
                                   Valor_cliente double(28,6), 
                                   Vida_cliente double(16,4),
                                   CLV double(28,6));
--
-- nbo_model y nbo_in --
SET sql_mode = '';
CREATE TABLE IF NOT EXISTS nbo_model (Id INT(11) NOT NULL AUTO_INCREMENT PRIMARY KEY,
                                   Id_cliente VARCHAR(125),
                                   Macro_sector CHAR(5),
                                   Sector char(12),
                                   Subsector char(20),
                                   Actividad char(12),
                                   Ventas double(16,4),
                                   Empleados char(20),
                                   Activo_fijo double(16,4),
                                   Potencial double(16,4),
                                   Cheques double(16,4),
                                   Etapa CHAR(5),
                                   Subetapa CHAR(5),
                                   Monto double(28,6),
                                   Producto char(20)
                                   );

CREATE TABLE IF NOT EXISTS nbo_in select * from nbo_model;

alter table nbo_in drop column Etapa,drop column Subetapa,drop column Monto,drop column Producto;
--
-- nbo_out --
CREATE TABLE IF NOT EXISTS nbo_out (Id INT(11) NOT NULL AUTO_INCREMENT PRIMARY KEY,
								   Ejecucion DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                                   Id_cliente VARCHAR(125),
                                   Producto_Predict int(2),
                                   Producto_1 double(8,4),
                                   Producto_2 double(8,4),
                                   Producto_3 double(8,4),
                                   Producto_4 double(8,4),
                                   Producto_5 double(8,4)
                                   );
---
-- acreedor_out --
CREATE TABLE IF NOT EXISTS acreedor_out (Id INT(11) NOT NULL AUTO_INCREMENT PRIMARY KEY,
                                   Ejecucion DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                                   Id_cliente VARCHAR(125),
                                   Acreedor CHAR(5),
                                   Acreedor_prob double(8,4),
								   Monto_predict double(28,6)
                                   );
---
select * from rfm_in;
select * from rfm_out;
select * from clv_out;
select * from nbo_model;
select * from nbo_in;
select * from nbo_out;
select * from acreedor_out;
--
-- selects getcustomerinfo
select max(Recencia_out) from rfm_out;
select min(Recencia_out) from rfm_out;
select max(Frecuencia_out) from rfm_out;
select min(Frecuencia_out) from rfm_out;
select max(Monto_out) from rfm_out;
select min(Monto_out) from rfm_out;
select max(Ejecucion),Recencia_out,Frecuencia_out,Monto_out,Segmento from rfm_out where Id_cliente='b9d12fcc-5280-4a87-838f-672d00e70088';

select max(Valor_cliente) from clv_out;
select min(Valor_cliente) from clv_out;
select max(CLV) from clv_out;
select min(CLV) from clv_out;
select max(Ejecucion),Valor_cliente,Vida_cliente,CLV from clv_out where Id_cliente='b9d12fcc-5280-4a87-838f-672d00e70088';

select max(Ejecucion),Acreedor,Acreedor_prob from acreedor_out where Id_cliente='b9d12fcc-5280-4a87-838f-672d00e70088';

select max(Ejecucion),Producto_Predict,Producto_1,Producto_2,Producto_3,Producto_4,Producto_5 from nbo_out where Id_cliente='b9d12fcc-5280-4a87-838f-672d00e70088';
--
-- select fix
SET sql_mode=(SELECT REPLACE(@@sql_mode,'ONLY_FULL_GROUP_BY',''));
--
-- selects de clv
select distinct Id_cliente from rfm_in;
SELECT TIMESTAMPDIFF(MONTH,(select MAX(Fecha) FROM rfm_in WHERE Id_cliente='e45d4e71-22c6-421d-85ec-77ecaead04e9') , (select MIN(Fecha) FROM rfm_in WHERE Id_cliente='e45d4e71-22c6-421d-85ec-77ecaead04e9'));
--
-- select de RFM
SELECT Id_cliente,max(Fecha) as 'Recencia',count(Monto) as 'Frecuencia',AVG(Monto) as 'Monto' FROM rfm_in group by Id_cliente;
--
-- llenar datos de rfm_in
insert into rfm_in(Id_cliente,Fecha,Monto)
SELECT a.id,op.date_entered,op2.monto_c FROM accounts a, opportunities op,opportunities_cstm op2,accounts_opportunities ao WHERE a.id=ao.account_id AND op.id=ao.opportunity_id and op2.id_c=ao.opportunity_id and op.deleted=0;                                   
--

-- llenar datos de nbo_model
SET sql_mode = '';
insert into nbo_model(Id_cliente,Macro_sector,Sector,Subsector,Actividad,Ventas,Empleados,Activo_fijo,Potencial,Cheques,Etapa,Subetapa,Monto,Producto)
select a.id_c,a.tct_macro_sector_ddw_c,a.sectoreconomico_c,a.subsectoreconomico_c,a.actividadeconomica_c,a.ventas_anuales_c,
		a.empleados_c,a.activo_fijo_c,a.potencial_cuenta_c,a.tct_prom_cheques_cur_c,
        op.tct_etapa_ddw_c,op.estatus_c,op.monto_c,op.tipo_producto_c
FROM accounts_cstm a, opportunities_cstm op,accounts_opportunities ao WHERE a.id_c=ao.account_id AND op.id_c=ao.opportunity_id;