
"""API Generador-RFM"""

import hug
import pymysql
import pandas as pd
import numpy as np
import json
from sklearn.cluster import KMeans

host='localhost'
db='RFM_unifin'
user_db='root'
pass_db=''

id_RFM=''
target_R=''
target_F=''
target_M=''

@hug.get()

def setRFM():

	"""API que genera RFM para todas las cuentas"""

	post='{"ponderaciones":[{"recencia":25},{"frecuencia":25},{"monto":50}],"segmentos":[{"nombre":"bronce"},{"nombre":"plata"},{"nombre":"oro"}]}'
	params=json.loads(post)
	segmentos=params['segmentos']
	ponderaciones=params['ponderaciones']

	conn=pymysql.connect(host=host, user=user_db, passwd=pass_db, db=db)
	
	cur=conn.cursor()
	
	query="SELECT Cuenta,Last_date,Num_Tickets,Monto FROM Transacciones;"

	cur.execute(query),

	res = cur.fetchall()

	cur.close()

	conn.close()

	headers=['Id','Date','Frecuency','Money']

	global id_RFM
	global target_R
	global target_F
	global target_M

	id_RFM=headers[0]
	target_R=headers[1]
	target_F=headers[2]
	target_M=headers[3]
	
	dataset_dummy={}
	filas=0

	for h in headers:
		dataset_dummy[h]=[]
		
	if(len(res)>0):
		for r in res:
			filas+=1
			for i in range(len(headers)):
				dataset_dummy[headers[i]].append(r[i])

	print('El numero de filas de este dataset es de:'+str(filas))

	first_dataset=pd.DataFrame(dataset_dummy)

	first_dataset[target_R]=first_dataset[target_R].astype(str).str.replace('-', '')
	first_dataset[target_R]=first_dataset[target_R].astype(str).astype(int)

	#columns=first_dataset.columns.values.tolist()

	#Llamada a los 3 servicios
	dataset_R=setRecencia(first_dataset)
	dataset_F=setFrecuencia(first_dataset)
	dataset_M=setMonto(first_dataset)


	last_dataset=dataset_R.merge(dataset_F,on=headers).merge(dataset_M,on=headers)
	final_dataset=setCatego(last_dataset,segmentos,ponderaciones)

	return final_dataset


def setRecencia(first_dataset):
	dataset=first_dataset

	dataset=dataset.sort_values(by=target_R,ascending=True)
	dataset = dataset.reset_index(drop=True)

	model_r=KMeans(n_clusters=5).fit(dataset[[target_R]])
	clust_r=pd.Series(model_r.labels_)
	dataset['p_r']=clust_r


	lim_clusts=[]
	for i in range(0,5):
		x=dataset[dataset['p_r']==i]
		lim_clusts.append(x[target_R].max())
		
	lim_clusts.sort() 

	lim1=lim_clusts[0]
	lim2=lim_clusts[1]
	lim3=lim_clusts[2]
	lim4=lim_clusts[3]
	lim5=lim_clusts[4]

	dataset.loc[(dataset[target_R] <= lim1), 'R'] = 1
	dataset.loc[(dataset[target_R] <= lim2) & (dataset[target_R] > lim1), 'R'] = 2
	dataset.loc[(dataset[target_R] <= lim3) & (dataset[target_R] > lim2), 'R'] = 3
	dataset.loc[(dataset[target_R] <= lim4) & (dataset[target_R] > lim3), 'R'] = 4
	dataset.loc[(dataset[target_R] <= lim5) & (dataset[target_R] > lim4), 'R'] = 5

	dataset=dataset.drop(['p_r'], axis=1)


	conn=pymysql.connect(host=host, user=user_db, passwd=pass_db, db=db)
	cur=conn.cursor()

	for index,row in dataset.iterrows():
		query="Update Transacciones set R="+str(int(row['R']))+" where Cuenta="+str(int(row[id_RFM]))+";"
		cur.execute(query)

	conn.commit()
	cur.close()
	conn.close()

	return dataset


def setFrecuencia(first_dataset):
	dataset=first_dataset

	dataset=dataset.sort_values(by=target_F,ascending=True)
	dataset = dataset.reset_index(drop=True)

	model_f=KMeans(n_clusters=5).fit(dataset[[target_F]])
	clust_f=pd.Series(model_f.labels_)
	dataset['p_f']=clust_f

	lim_clusts=[]
	for i in range(0,5):
		x=dataset[dataset['p_f']==i]
		lim_clusts.append(x[target_F].max())
	lim_clusts.sort()   

	lim1=lim_clusts[0]
	lim2=lim_clusts[1]
	lim3=lim_clusts[2]
	lim4=lim_clusts[3]
	lim5=lim_clusts[4]

	dataset.loc[(dataset[target_F] <= lim1), 'F'] = 1
	dataset.loc[(dataset[target_F] <= lim2) & (dataset[target_F] > lim1), 'F'] = 2
	dataset.loc[(dataset[target_F] <= lim3) & (dataset[target_F] > lim2), 'F'] = 3
	dataset.loc[(dataset[target_F] <= lim4) & (dataset[target_F] > lim3), 'F'] = 4
	dataset.loc[(dataset[target_F] <= lim5) & (dataset[target_F] > lim4), 'F'] = 5

	dataset=dataset.drop(['p_f'], axis=1)

	conn=pymysql.connect(host=host, user=user_db, passwd=pass_db, db=db)

	cur=conn.cursor()
	for index,row in dataset.iterrows():
		query="Update Transacciones set F="+str(int(row['F']))+" where Cuenta="+str(int(row[id_RFM]))+";"
		cur.execute(query)

	conn.commit()
	cur.close()
	conn.close()

	return dataset	


def setMonto(first_dataset):
	dataset=first_dataset

	dataset=dataset.sort_values(by=target_M,ascending=True)
	dataset = dataset.reset_index(drop=True)

	model_m=KMeans(n_clusters=5).fit(dataset[[target_M]])
	clust_m=pd.Series(model_m.labels_)
	dataset['p_m']=clust_m

	lim_clusts=[]
	for i in range(0,5):
	    x=dataset[dataset['p_m']==i]
	    lim_clusts.append(x[target_M].max())
	lim_clusts.sort()   

	lim1=lim_clusts[0]
	lim2=lim_clusts[1]
	lim3=lim_clusts[2]
	lim4=lim_clusts[3]
	lim5=lim_clusts[4]

	dataset.loc[(dataset[target_M] <= lim1), 'M'] = 1
	dataset.loc[(dataset[target_M] <= lim2) & (dataset[target_M] > lim1), 'M'] = 2
	dataset.loc[(dataset[target_M] <= lim3) & (dataset[target_M] > lim2), 'M'] = 3
	dataset.loc[(dataset[target_M] <= lim4) & (dataset[target_M] > lim3), 'M'] = 4
	dataset.loc[(dataset[target_M] <= lim5) & (dataset[target_M] > lim4), 'M'] = 5

	dataset=dataset.drop(['p_m'], axis=1)

	conn=pymysql.connect(host=host, user=user_db, passwd=pass_db, db=db)

	cur=conn.cursor()
	for index,row in dataset.iterrows():
		query="Update Transacciones set M="+str(int(row['M']))+" where Cuenta="+str(int(row[id_RFM]))+";"
		cur.execute(query)

	conn.commit()
	cur.close()
	conn.close()

	return dataset		


def setCatego(last_dataset,segmentosx,ponderacionesx):
	dataset=last_dataset
	segmentos=segmentosx
	ponderaciones=ponderacionesx

	dataset['ponderacion']=dataset['ponderacion']=((dataset['R']*ponderaciones[0]['recencia'])+(dataset['F']*ponderaciones[1]['frecuencia'])+(dataset['M']*ponderaciones[2]['monto']))/100

	model_RFM=KMeans(n_clusters=len(segmentos)).fit(np.array(dataset[['ponderacion']]))
	clust_RFM=pd.Series(model_RFM.labels_)
	dataset['p_RFM']=clust_RFM

	lim_clusts=[]
	for i in range(0,len(segmentos)):
	    x=dataset[dataset['p_RFM']==i]
	    lim_clusts.append(x['ponderacion'].max())

	lim_clusts.sort()  		

	lim1=lim_clusts[0]
	lim2=lim_clusts[1]
	lim3=lim_clusts[2]

	dataset.loc[(dataset['ponderacion'] <= lim1), 'Categoria'] = segmentos[0]['nombre']
	dataset.loc[(dataset['ponderacion'] <= lim2) & (dataset['ponderacion'] > lim1), 'Categoria'] = segmentos[1]['nombre']
	dataset.loc[(dataset['ponderacion'] <= lim3) & (dataset['ponderacion'] > lim2), 'Categoria'] = segmentos[2]['nombre']

	dataset=dataset.drop(['p_RFM'], axis=1)

	conn=pymysql.connect(host=host, user=user_db, passwd=pass_db, db=db)
	cur=conn.cursor()
	for index,row in dataset.iterrows():
		query="Update Transacciones set Tag='"+row['Categoria']+"' where Cuenta="+str(int(row[id_RFM]))+";"
		cur.execute(query)

	conn.commit()
	cur.close()
	conn.close()		

	return dataset


@hug.get(examples="id_user=bb53237e-f9b1-11e8-8502-00155da06f04")

def predictRFM(id_user: hug.types.text):

	"""API que predice RFM para una cuenta en especifico"""

	conn=pymysql.connect(host=host, user=user_db, passwd=pass_db, db=db)
	
	cur=conn.cursor()
	
	query="SELECT * FROM accounts where id='"+id_user+"';"

	cur.execute(query)
	
	res = cur.fetchall()

	for row in res:
		print(row[0])
	
	cur.close()
	
	conn.close()
	
	return 'Conexion Exitosa'

	#return {'message': 'Happy {0} Birthday {1}!'.format(age, name),'took': float(hug_timer)}
