
"""APIs RFM-unifin"""

import hug
import pymysql
import pandas as pd

host='localhost'
db='RFM_unifin'
user_db='root'
pass_db=''


@hug.get()

def setRFM():

	"""API que genera RFM para todas las cuentas"""

	conn=pymysql.connect(host=host, user=user_db, passwd=pass_db, db=db)
	
	cur=conn.cursor()
	
	query="SELECT * FROM Transacciones;"

	cur.execute(query)

	res = cur.fetchall()
	
	cur.close()
	
	conn.close()

	if(len(res)>0):

		"""Creando Dataset"""








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
