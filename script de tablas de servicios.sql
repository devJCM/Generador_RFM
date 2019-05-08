drop table if exists rmf_in;
drop table if exists nbo_model;
drop table if exists nbo_in;

drop table if exists rfm_out;
drop table if exists clv_out;
drop table if exists nbo_out;
drop table if exists acreedor_out;

create database CustomerInfo;
use CustomerInfo;

-- rfm_in --
CREATE TABLE IF NOT EXISTS rfm_in (Id INT(11) NOT NULL AUTO_INCREMENT PRIMARY KEY,
								   Ejecucion DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                                   Id_cliente VARCHAR(125),
                                   Nombre VARCHAR(125),
                                   Fecha datetime,
                                   Vigencia datetime,
                                   Last_call datetime,
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
								   Ejecucion DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
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
                                   Id_producto VARCHAR(125),
                                   Producto_prob double(8,4)
                                   );
---
-- acreedor_out --
CREATE TABLE IF NOT EXISTS acreedor_out (Id INT(11) NOT NULL AUTO_INCREMENT PRIMARY KEY,
                                   Ejecucion DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                                   Id_cliente VARCHAR(125),
                                   Acreedor CHAR(5),
                                   Acreedor_prob double(8,4),
								   Monto_predict double(28,6),
                                   Monto_seg double(28,6)
                                   );
---
-- agenda_out --
CREATE TABLE IF NOT EXISTS scheduler_out (Id INT(11) NOT NULL AUTO_INCREMENT PRIMARY KEY,
                                   Ejecucion DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                                   Id_cliente VARCHAR(125),
                                   Nombre VARCHAR(125),
                                   Date_predict DATETIME,
                                   Vigencia DATETIME,
                                   Last_call datetime,
                                   Contactado int(2)
                                   );
---
-- calls in --
CREATE TABLE IF NOT EXISTS calls_in (Id INT(11) NOT NULL AUTO_INCREMENT PRIMARY KEY,
                                   Ejecucion DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                                   Id_call VARCHAR(125),
                                   Nombre VARCHAR(125),
                                   Date_end datetime,	
                                   Id_cliente VARCHAR(125),
                                   Estado char(20),
                                   Venta int(2),
                                   Id_producto VARCHAR(125)
                                   );
---
-- items--
CREATE TABLE IF NOT EXISTS items (Id INT(11) NOT NULL AUTO_INCREMENT PRIMARY KEY,
                                   Ejecucion DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                                   Id_producto VARCHAR(125),
                                   Descripcion VARCHAR(125)
                                   );
                                   
insert into items(Id_producto,Descripcion) values(1,'Leasing'),(2,'Credito Simple'),(3,'Credito Automotriz'),(4,'Factoraje'),(5,'Linea Credito Simple');
select * from items;						
---
select * from rfm_in;
select * from rfm_out;
select * from clv_out;
select * from nbo_model;
select * from nbo_in;
select * from nbo_out;
select * from acreedor_out;
select * from scheduler_out;
select * from calls_in;
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
insert into rfm_in(Id_cliente,Nombre,Fecha,Vigencia,Last_call,Monto)
SELECT a.id,a.name,op.date_entered,vigencialinea_c,(select max(calls.date_end) from calls where calls.parent_id=a.id and calls.status='Held' and calls.deleted=0),op2.monto_c
FROM accounts a, opportunities op,opportunities_cstm op2,accounts_opportunities ao
WHERE a.id=ao.account_id AND op.id=ao.opportunity_id and op2.id_c=ao.opportunity_id and op.deleted=0; 
--

-- llenar datos de nbo_model
SET sql_mode = '';
insert into nbo_model(Id_cliente,Macro_sector,Sector,Subsector,Actividad,Ventas,Empleados,Activo_fijo,Potencial,Cheques,Etapa,Subetapa,Monto,Producto)
select a.id_c,a.tct_macro_sector_ddw_c,a.sectoreconomico_c,a.subsectoreconomico_c,a.actividadeconomica_c,a.ventas_anuales_c,
		a.empleados_c,a.activo_fijo_c,a.potencial_cuenta_c,a.tct_prom_cheques_cur_c,
        op.tct_etapa_ddw_c,op.estatus_c,op.monto_c,op.tipo_producto_c
FROM accounts_cstm a, opportunities_cstm op,accounts_opportunities ao WHERE a.id_c=ao.account_id AND op.id_c=ao.opportunity_id;

-- conocer numero de oportunidades por cuenta
select Frecuencia_in as 'Numero de Oportunidades',count(*) as 'Numero de Cuentas' from rfm_out group by Frecuencia_in; -- Frecuencia,Cuentas con esa frecuencia