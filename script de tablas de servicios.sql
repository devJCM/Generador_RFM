drop table if exists rmf_in;
drop table if exists nbo_model;
drop table if exists nbo_in;
drop table if exists calls_in;

drop table if exists rfm_out;
drop table if exists clv_out;
drop table if exists nbo_out;
drop table if exists creditor_out;

create database CustomerInfo;
use CustomerInfo;

-- rfm_in --
CREATE TABLE IF NOT EXISTS rfm_in (id INT(11) NOT NULL AUTO_INCREMENT PRIMARY KEY,
								   created_date datetime DEFAULT CURRENT_TIMESTAMP ON UPdate CURRENT_TIMESTAMP,
                                   id_client VARCHAR(125),
                                   name VARCHAR(125),
                                   transaction_date datetime,
                                   validity datetime,
                                   last_call datetime,
                                   amount double(28,6));
--   
-- rfm_out --
CREATE TABLE IF NOT EXISTS rfm_out (id INT(11) NOT NULL AUTO_INCREMENT PRIMARY KEY,
								   created_date datetime DEFAULT CURRENT_TIMESTAMP ON UPdate CURRENT_TIMESTAMP,
                                   id_client VARCHAR(125),
                                   recency_in char(20),
                                   frecuency_in int(5),
                                   amount_in double(28,6),
                                   recency_out int(5),
                                   frecuency_out int(5),
                                   amount_out int(5),
                                   segment char(30));
--  
-- clv_out --
CREATE TABLE IF NOT EXISTS clv_out (id INT(11) NOT NULL AUTO_INCREMENT PRIMARY KEY,
								   created_date datetime DEFAULT CURRENT_TIMESTAMP ON UPdate CURRENT_TIMESTAMP,
                                   id_client VARCHAR(125),
                                   frecuency_in int(5),
                                   amount_in double(28,6), 
                                   client_value double(28,6), 
                                   lifetime double(16,4),
                                   clv double(28,6));
--
-- nbo_model y nbo_in --
SET sql_mode = '';
CREATE TABLE IF NOT EXISTS nbo_model (id INT(11) NOT NULL AUTO_INCREMENT PRIMARY KEY,
								   created_date datetime DEFAULT CURRENT_TIMESTAMP ON UPdate CURRENT_TIMESTAMP,
                                   id_client VARCHAR(125),
                                   macrosector CHAR(5),
                                   sector char(12),
                                   subsector char(20),
                                   activity char(12),
                                   sales double(16,4),
                                   employees char(20),
                                   fixed_asset double(16,4),
                                   potential double(16,4),
                                   checks_avg double(16,4),
                                   phase CHAR(5),
                                   subphase CHAR(5),
                                   amount double(28,6),
                                   item char(20)
                                   );

CREATE TABLE IF NOT EXISTS nbo_in select * from nbo_model;

alter table nbo_in drop column phase,drop column subphase,drop column amount,drop column item;
--
-- nbo_out --
CREATE TABLE IF NOT EXISTS nbo_out (id INT(11) NOT NULL AUTO_INCREMENT PRIMARY KEY,
								   created_date datetime DEFAULT CURRENT_TIMESTAMP ON UPdate CURRENT_TIMESTAMP,
                                   id_client VARCHAR(125),
                                   id_item VARCHAR(125),
                                   item_prob double(8,4)
                                   );
---
-- creditor_out --
CREATE TABLE IF NOT EXISTS creditor_out (id INT(11) NOT NULL AUTO_INCREMENT PRIMARY KEY,
                                   created_date datetime DEFAULT CURRENT_TIMESTAMP ON UPdate CURRENT_TIMESTAMP,
                                   id_client VARCHAR(125),
                                   creditor CHAR(5),
                                   creditor_prob double(8,4),
								   amount_predict double(28,6),
                                   amount_seg double(28,6)
                                   );
---
-- agenda_out --
CREATE TABLE IF NOT EXISTS scheduler_out (id INT(11) NOT NULL AUTO_INCREMENT PRIMARY KEY,
                                   created_date datetime DEFAULT CURRENT_TIMESTAMP ON UPdate CURRENT_TIMESTAMP,
                                   id_client VARCHAR(125),
                                   name VARCHAR(125),
                                   date_predict datetime,
                                   validity datetime,
                                   last_call datetime,
                                   contacted int(2)
                                   );
---
-- calls in --
CREATE TABLE IF NOT EXISTS calls_in (id INT(11) NOT NULL AUTO_INCREMENT PRIMARY KEY,
                                   created_date datetime DEFAULT CURRENT_TIMESTAMP ON UPdate CURRENT_TIMESTAMP,
                                   id_call VARCHAR(125),
                                   name VARCHAR(125),
                                   date_end datetime,	
                                   id_client VARCHAR(125),
                                   status char(20),
                                   sale int(2),
                                   id_item VARCHAR(125)
                                   );
---
-- items--
CREATE TABLE IF NOT EXISTS items (id INT(11) NOT NULL AUTO_INCREMENT PRIMARY KEY,
                                   created_date datetime DEFAULT CURRENT_TIMESTAMP ON UPdate CURRENT_TIMESTAMP,
                                   id_item VARCHAR(125),
                                   description VARCHAR(125)
                                   );
                                   
drop table if exists items;
insert into items(id_item,description) values(1,'Leasing'),(2,'Credito Simple'),(3,'Credito Automotriz'),(4,'Factoraje'),(5,'Linea Credito Simple');
select * from items;		
---
select * from rfm_in;
select * from rfm_out;
select * from clv_out;
select * from nbo_model;
select * from nbo_in;
select * from nbo_out;
select * from creditor_out;
select * from scheduler_out;
select * from calls_in;
--
-- selects getcustomerinfo
select max(recency_out) from rfm_out;
select min(recency_out) from rfm_out;
select max(frecuency_out) from rfm_out;
select min(frecuency_out) from rfm_out;
select max(amount_out) from rfm_out;
select min(amount_out) from rfm_out;
select max(created_date),recency_out,frecuency_out,amount_out,segment from rfm_out where id_client='b9d12fcc-5280-4a87-838f-672d00e70088';

select max(client_value) from clv_out;
select min(client_value) from clv_out;
select max(clv) from clv_out;
select min(clv) from clv_out;
select max(created_date),client_value,lifetime,clv from clv_out where id_client='b9d12fcc-5280-4a87-838f-672d00e70088';

select max(created_date),creditor,creditor_prob from creditor_out where id_client='b9d12fcc-5280-4a87-838f-672d00e70088';

select max(created_date),item_Predict,item_1,item_2,item_3,item_4,item_5 from nbo_out where id_client='b9d12fcc-5280-4a87-838f-672d00e70088';
--
-- select fix
SET sql_mode=(SELECT REPLACE(@@sql_mode,'ONLY_FULL_GROUP_BY',''));
--
-- selects de clv
select distinct id_client from rfm_in;
SELECT TIMESTAMPDIFF(MONTH,(select MAX(transaction_date) FROM rfm_in WHERE id_client='e45d4e71-22c6-421d-85ec-77ecaead04e9') , (select MIN(transaction_date) FROM rfm_in WHERE id_client='e45d4e71-22c6-421d-85ec-77ecaead04e9'));
--
-- select de RFM
SELECT id_client,max(transaction_date) as 'recency',count(amount) as 'frecuency',AVG(amount) as 'amount' FROM rfm_in group by id_client;
--
-- llenar datos de rfm_in
insert into rfm_in(id_client,name,transaction_date,validity,last_call,amount)
SELECT a.id,a.name,op.date_entered,vigencialinea_c,(select max(calls.date_end) from calls where calls.parent_id=a.id and calls.status='Held' and calls.deleted=0),op2.monto_c
FROM accounts a, opportunities op,opportunities_cstm op2,accounts_opportunities ao
WHERE a.id=ao.account_id AND op.id=ao.opportunity_id and op2.id_c=ao.opportunity_id and op.deleted=0; 
--

-- llenar datos de nbo_model
SET sql_mode = '';
insert into nbo_model(id_client,macrosector,sector,subsector,activity,sales,employees,fixed_asset,potential,checks_avg,phase,Subphase,amount,item)
select a.id_c,a.tct_macro_sector_ddw_c,a.sectoreconomico_c,a.subsectoreconomico_c,a.actividadeconomica_c,a.ventas_anuales_c,
		a.empleados_c,a.activo_fijo_c,a.potencial_cuenta_c,a.tct_prom_cheques_cur_c,
        op.tct_etapa_ddw_c,op.estatus_c,op.monto_c,op.tipo_producto_c
FROM accounts_cstm a, opportunities_cstm op,accounts_opportunities ao WHERE a.id_c=ao.account_id AND op.id_c=ao.opportunity_id;

-- conocer numero de oportunidades por cuenta
select frecuency_in as 'Numero de Oportunidades',count(*) as 'Numero de Cuentas' from rfm_out group by frecuency_in; -- frecuency,Cuentas con esa frecuency