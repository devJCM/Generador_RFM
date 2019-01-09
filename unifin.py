
"""APIs RFM-unifin"""

import hug
import pymysql
import pandas as pd

host='192.168.150.163'
db='unifin'

@hug.get(examples="user=usrambwen&password=acces0w3n")

def setRFM(user: hug.types.text, password: hug.types.text, hug_timer=3):

	"""API que genera RFM para todas las cuentas"""

	conn=pymysql.connect(host=host, user=user, passwd=password, db=db)
	
	cur=conn.cursor()
	
	query="SELECT * FROM accounts where id='bb53237e-f9b1-11e8-8502-00155da06f04';"

	cur.execute(query)
	
	for row in cur:
		print(row[0])
	
	cur.close()
	
	conn.close()
	
	return 'Conexion Exitosa'

	#return {'message': 'Happy {0} Birthday {1}!'.format(age, name),'took': float(hug_timer)}



@hug.get(examples="name=Jhon Doe&age=30")

def say_hello(name: hug.types.text, age: hug.types.number, hug_timer=3):
    """Decimos hola al usuario y calculamos su año de nacimiento"""
    year_of_birth = datetime.datetime.now().year - age
    return {
        'message': "Hola {0}, naciste el año {1}".format(name, year_of_birth),
        'took': float(hug_timer)
    }
