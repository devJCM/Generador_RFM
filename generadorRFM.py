
"""API Generador-RFM"""

import hug
import pymysql
import pandas as pd
import numpy as np
from sklearn.cluster import KMeans

host=None
db=None
user_db=None
pass_db=None
table=None

id_RFM=''
target_R=''
target_F=''
target_M=''

@hug.post()

def setRFM(body=None):

	"""API que genera RFM para todas las cuentas"""
	if body==None:
		return "No se enviaron parametros POST, por lo tanto el proceso se detuvo"
	else:
		for i in body:
			if(i=='segmentos'):
				segmentos=body['segmentos']
			elif(i=='ponderaciones'):
				ponderaciones=body['ponderaciones']
			elif(i=='db'):
				info_db=body['db']
			else:
				return "Key:"+i+" incorrecta,las keys deben ser lo siguientes, en el siguiente orden: \"segmentos\",\"ponderaciones\",\"db\""

		for i in segmentos:
			for key,val in i.items():
				if(key=='nombre'):
					if(type(val)!=str):
						return "Uno de los datos de \"Segmentos\" no es \"string\",favor de corregir"
				else:
					return "Key:"+key+" incorrecta en  \"segmentos\",debe ser Key:\"nombre\""

		for i in ponderaciones:
			for key,val in i.items():
				if(key=='recencia'):
					if(type(val)!=int):
						return "El dato de \"recencia\" debe ser \"int\" para que la sumatoria de recencia+frecuencia+monto=100,favor de corregir"
				elif(key=='frecuencia'):
					if(type(val)!=int):
						return "El dato de \"frecuencia\" debe ser \"int\" para que la sumatoria de recencia+frecuencia+monto=100,favor de corregir"
				elif(key=='monto'):
					if(type(val)!=int):
						return "El dato de \"monto\" debe ser \"int\" para que la sumatoria de recencia+frecuencia+monto=100,favor de corregir"
				else:
					return "Key:"+key+" incorrecta para \"ponderaciones\",las keys deben ser las siguientes, en el siguiente orden: \"recencia\",\"frecuencia\",\"monto\""

		for i in info_db:
			for key,val in i.items():
				if(key=='host'):
					if(type(val)!=str):
						return "El dato de \"host\" debe ser \"str\",favor de corregir"
				elif(key=='db'):
					if(type(val)!=str):
						return "El dato de \"db\" debe ser \"str\",favor de corregir"	
				elif(key=='table'):
					if(type(val)!=str):
						return "El dato de \"table\" debe ser \"str\",favor de corregir"	
				elif(key=='user'):
					if(type(val)!=str):
						return "El dato de \"user\" debe ser \"str\",favor de corregir"	
				elif(key=='password'):
					if(type(val)!=str):
						return "El dato de \"password\" debe ser \"str\",favor de corregir"	
				else:
					return "Key:"+key+" incorrecta para \"db\",las keys deben ser las siguientes, en el siguiente orden: \"host\",\"db\",\"table\",\"user\",\"password\""		
			

	global host
	global db
	global table
	global user_db
	global pass_db

	host=info_db[0]['host']
	db=info_db[1]['db']
	user_db=info_db[3]['user']
	pass_db=info_db[4]['password']
	table=info_db[2]['table']

	try:
		conn=pymysql.connect(host=host, user=user_db, passwd=pass_db, db=db)

		cur=conn.cursor()
		
		query="SELECT id,Recencia,Tickets,Monto FROM "+table+";"

		cur.execute(query)

		res = cur.fetchall()

		cur.close()

		conn.close()

	except pymysql.Error as e:
		return ("Error %d: %s" % (e.args[0], e.args[1]))

	else:
		headers=['id','Recencia','Tickets','Monto']

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
		else:
			return "No hay registros para iniciar el proceso."

		print('El numero de filas de este dataset es de:'+str(filas))

		first_dataset=pd.DataFrame(dataset_dummy)

		first_dataset[target_R]=first_dataset[target_R].astype(str).str.replace('\D', '')
		first_dataset[target_R]=first_dataset[target_R].astype(str).astype(int)


		#Llamada a los 3 servicios
		dataset_R=setRecencia(first_dataset)
		dataset_F=setFrecuencia(first_dataset)
		dataset_M=setMonto(first_dataset)


		last_dataset=dataset_R.merge(dataset_F,on=headers).merge(dataset_M,on=headers)
		final_dataset=setCatego(last_dataset,segmentos,ponderaciones)

		return 'Operacion concluida, numero de registros afectados:'+str(filas)


def setRecencia(first_dataset):
	print('Entro a setRecencia')
	dataset=first_dataset

	dataset=dataset.sort_values(by=target_R,ascending=True)
	dataset = dataset.reset_index(drop=True)
	dataset[target_R]=dataset[target_R].fillna(dataset[target_R].mean())

	model_r=KMeans(n_clusters=5).fit(dataset[[target_R]])
	clust_r=pd.Series(model_r.labels_)
	dataset['p_r']=clust_r
	print('Se genero cluster de setRecencia')

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

	return dataset


def setFrecuencia(first_dataset):
	print('Entro a setFrecuencia')
	dataset=first_dataset

	dataset=dataset.sort_values(by=target_F,ascending=True)
	dataset = dataset.reset_index(drop=True)
	dataset[target_F]=dataset[target_F].fillna(dataset[target_F].mean())

	model_f=KMeans(n_clusters=5).fit(dataset[[target_F]])
	clust_f=pd.Series(model_f.labels_)
	dataset['p_f']=clust_f
	print('Se genero cluster de setFrecuencia')

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

	return dataset	


def setMonto(first_dataset):
	print('Entro a setMonto')
	dataset=first_dataset

	dataset=dataset.sort_values(by=target_M,ascending=True)
	dataset = dataset.reset_index(drop=True)
	dataset[target_M]=dataset[target_M].fillna(dataset[target_M].mean())

	model_m=KMeans(n_clusters=5).fit(dataset[[target_M]])
	clust_m=pd.Series(model_m.labels_)
	dataset['p_m']=clust_m
	print('Se genero cluster de setMonto')

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

	return dataset		


def setCatego(last_dataset,segmentosx,ponderacionesx):
	print('Entro a setCatego')
	dataset=last_dataset
	segmentos=segmentosx
	ponderaciones=ponderacionesx

	dataset['ponderacion']=dataset['ponderacion']=((dataset['R']*ponderaciones[0]['recencia'])+(dataset['F']*ponderaciones[1]['frecuencia'])+(dataset['M']*ponderaciones[2]['monto']))/100

	model_RFM=KMeans(n_clusters=len(segmentos)).fit(np.array(dataset[['ponderacion']]))
	clust_RFM=pd.Series(model_RFM.labels_)
	dataset['p_RFM']=clust_RFM
	print('Se genero cluster de setCatego')

	lim_clusts=[]
	for i in range(0,len(segmentos)):
		x=dataset[dataset['p_RFM']==i]
		lim_clusts.append(x['ponderacion'].max())

	lim_clusts.sort()  		

	limits={}
	for index,i in enumerate(lim_clusts):
		key = 'limit'+str(index+1)
		value = i
		limits[key] = value

	for index,j in enumerate(limits):
		if j=='limit1':
			dataset.loc[(dataset['ponderacion'] <= int(limits[j])), 'Categoria'] = segmentos[index]['nombre']
			temp=limits[j]
		else:
			dataset.loc[(dataset['ponderacion'] <= int(limits[j])) & (dataset['ponderacion'] > int(temp)), 'Categoria'] = segmentos[index]['nombre']
			temp=limits[j]

	dataset=dataset.drop(['p_RFM'], axis=1)

	conn=pymysql.connect(host=host, user=user_db, passwd=pass_db, db=db)
	cur=conn.cursor()
	for index,row in dataset.iterrows():
		query="Update "+table+" set R="+str(int(row['R']))+",F="+str(int(row['F']))+",M="+str(int(row['M']))+",Categoria='"+str(row['Categoria'])+"' where "+id_RFM+"='"+str(row[id_RFM])+"';"
		cur.execute(query)

	conn.commit()
	cur.close()
	conn.close()		

	return dataset


@hug.post()

def predictRFM(body=None):

	"""API que predice RFM para una cuenta en especifico"""

	if body==None:
		return "No se enviaron parametros POST, por lo tanto el proceso se detuvo"
	else:
		for i in body:
			if(i=='id'):
				id_user=body['id']
			elif(i=='db'):
				info_db=body['db']
			else:
				return "Key:"+i+" incorrecta,las keys deben ser las siguientes, en el siguiente orden: \"id\",\"db\""

	global host
	global db
	global table
	global user_db
	global pass_db

	host=info_db[0]['host']
	db=info_db[1]['db']
	user_db=info_db[3]['user']
	pass_db=info_db[4]['password']
	table=info_db[2]['table']

	try:
		conn=pymysql.connect(host=host, user=user_db, passwd=pass_db, db=db)
		
		cur=conn.cursor()
		
		query="SELECT Recencia,Tickets,Monto,R,F,M,Categoria FROM "+table+";"

		cur.execute(query)
		
		res = cur.fetchall()

		cur.close()
		
		conn.close()

	except pymysql.Error as e:

		return ("Error %d: %s" % (e.args[0], e.args[1]))

	else:
		if(len(res)>0):
			headers=['Recencia','Tickets','Monto','R','F','M','Categoria']
			recencia=headers[0]
			
			dataset_dummy={}
			filas=0

			for h in headers:
				dataset_dummy[h]=[]
				
			for r in res:
				filas+=1
				for i in range(len(headers)):
					dataset_dummy[headers[i]].append(r[i])

			print('El numero de filas de este dataset es de:'+str(filas))

			first_dataset=pd.DataFrame(dataset_dummy)

			first_dataset[recencia]=first_dataset[recencia].astype(str).str.replace('\D', '')
			first_dataset[recencia]=first_dataset[recencia].astype(str).astype(int)

		else:
			return "No hay registros para iniciar el proceso."


	#return {'message': 'Happy {0} Birthday {1}!'.format(age, name),'took': float(hug_timer)}
