
"""API Generador-RFM"""

import pymysql
import json
import pandas as pd
import numpy as np
import pickle

from sklearn.preprocessing import MinMaxScaler
from flask import Flask,Response,request
from sklearn.cluster import KMeans
from sklearn.model_selection import train_test_split





app = Flask(__name__)

host=None
db=None
user_db=None
pass_db=None
query_fix="SET sql_mode=(SELECT REPLACE(@@sql_mode,'ONLY_FULL_GROUP_BY',''));"

id_RFM=None
target_R=None
target_F=None
target_M=None






@app.route("/setRFM",methods=['POST'])
def setRFM(body=None):

    """API que genera RFM para todas las cuentas"""
    body=request.get_json()
    if body==None:
        msj= "No se enviaron parametros POST, por lo tanto el proceso se detuvo"
        return Response(status=400,response=msj)
    else:
        check=checkBodysetRFM(body)
        if (check!='OK'):
            return check


    segmentos=body['segmentos']
    ponderaciones=body['ponderaciones']
    info_db=body['db']      

    global host
    global db
    global user_db
    global pass_db

    host=info_db[0]['host']
    db=info_db[1]['db']
    user_db=info_db[2]['user']
    pass_db=info_db[3]['password']


    try:
        conn=pymysql.connect(host=host, user=user_db, passwd=pass_db, db=db)

        cur=conn.cursor()

        #----Modify-----
        query_extract="SELECT Id_cliente,max(Fecha) as 'Recencia',count(Monto) as 'Frecuencia',AVG(Monto) as 'Monto' FROM rfm_in group by Id_cliente;"
        #----Modify-----
        
        cur.execute(query_fix)
        cur.execute(query_extract)

        res = cur.fetchall()

        cur.close()

        conn.close()

    except pymysql.Error as e:
        msj= ("Error %d: %s" % (e.args[0], e.args[1]))
        print(msj)
        return Response(status=400,response=msj)

    else:
        global id_RFM
        global target_R
        global target_F
        global target_M

        #----Modify-----
        headers=['id','Recencia','Frecuencia','Monto']

        id_RFM=headers[0]
        target_R=headers[1]
        target_F=headers[2]
        target_M=headers[3]
        #----Modify-----        
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
            msj= "No hay registros para iniciar el proceso."
            return Response(status=400,response=msj)

        print('El numero de filas de este dataset es de:'+str(filas))

        first_dataset=pd.DataFrame(dataset_dummy)


        first_dataset[target_R]=first_dataset[target_R].astype(str).str.replace('\D', '')
        first_dataset[target_R]=first_dataset[target_R].astype(str).astype(int)
        first_dataset[target_R]=first_dataset[target_R].fillna(first_dataset[target_R].mean())
        first_dataset[target_F]=first_dataset[target_F].fillna(0)
        first_dataset[target_M]=first_dataset[target_M].fillna(0)


        #Llamada a los 3 servicios
        dataset_R=setRecencia(first_dataset)
        dataset_F=setFrecuencia(first_dataset)
        dataset_M=setMonto(first_dataset)


        last_dataset=dataset_R.merge(dataset_F,on=headers).merge(dataset_M,on=headers)
        final_dataset=setCatego(last_dataset,segmentos,ponderaciones)



        msj= 'Operacion concluida, numero de registros afectados:'+str(filas)
        return Response(status=200,response=msj)
        #return last_dataset

@app.route("/setCLV",methods=['POST'])
def setCLV(body=None):
    body=request.get_json()
    if body==None:
        msj= "No se enviaron parametros POST, por lo tanto el proceso se detuvo"
        return Response(status=400,response=msj)
    else:
        check=checkBodysetCLV(body)
        if (check!='OK'):
            return check

    info_db=body['db']      

    global host
    global db
    global user_db
    global pass_db

    host=info_db[0]['host']
    db=info_db[1]['db']
    user_db=info_db[2]['user']
    pass_db=info_db[3]['password']

    if 'meanlife' in body:
        meanlife=body['meanlife']
    else:
        meanlife=getmeanlife()  

    print('Promedio de  vida:',meanlife)

    try:
        conn=pymysql.connect(host=host, user=user_db, passwd=pass_db, db=db)

        cur=conn.cursor()

        #----Modify-----
        query_extract="SELECT Id_cliente,count(Monto) as 'Frecuencia',AVG(Monto) as 'Monto' FROM rfm_in group by Id_cliente;"
        #----Modify-----
        
        cur.execute(query_fix)
        cur.execute(query_extract)

        res = cur.fetchall()

        cur.close()

        conn.close()

    except pymysql.Error as e:
        msj= ("Error %d: %s" % (e.args[0], e.args[1]))
        print(msj)
        return Response(status=400,response=msj)
    else:
        #----Modify-----
        headers=['id','Frecuencia','Monto']
        #----Modify-----        

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
            msj= "No hay registros para iniciar el proceso."
            return Response(status=400,response=msj)

        print('El numero de filas de este dataset es de:'+str(filas))

        first_dataset=pd.DataFrame(dataset_dummy)
        print(first_dataset.dtypes)
        
        first_dataset[headers[1]]=first_dataset[headers[1]].astype(float)
        first_dataset[headers[1]]=first_dataset[headers[1]].fillna(0)
        first_dataset[headers[2]]=first_dataset[headers[2]].fillna(0)

        first_dataset['Client_value']=first_dataset['Frecuencia']*first_dataset['Monto']
        first_dataset['CLV']=first_dataset['Client_value']*float(meanlife)


        conn=pymysql.connect(host=host, user=user_db, passwd=pass_db, db=db)
        cur=conn.cursor()

        cont=0
        val=[]
        for index,row in first_dataset.iterrows():
            #query="Update "+table+" set R="+str(int(row['R']))+",F="+str(int(row['F']))+",M="+str(int(row['M']))+",Categoria='"+str(row['Categoria'])+"' where "+id_RFM+"='"+str(row[id_RFM])+"';"
            val.append((row[headers[0]],row[headers[1]],row[headers[2]],row['Client_value'],meanlife,row['CLV']))
            cont=cont+1

        print('inserts:',cont)
        try:
            query="insert into clv_out (Id_cliente,Frecuencia_in,Monto_in,Valor_cliente,Vida_cliente,CLV) values (%s,%s,%s,%s,%s,%s)"
            cur.executemany(query,val)
            conn.commit()
            cur.close()
            conn.close()
        except pymysql.Error as e:
            msj= ("Error %d: %s" % (e.args[0], e.args[1]))
            print(msj)
            return Response(status=400,response=msj)            
        else:
            msj='Operacion concluida, se insertaron '+str(cont)+' regitros'
            return Response(msj,status=200)
        
@app.route("/setNBO",methods=['POST'])
def setNBO(body=None):
    body=request.get_json()
    if body==None:
        msj= "No se enviaron parametros POST, por lo tanto el proceso se detuvo"
        return Response(status=400,response=msj)
    else:
        check=checkBodysetCLV(body)
        if (check!='OK'):
            return check

    info_db=body['db']      

    global host
    global db
    global user_db
    global pass_db

    host=info_db[0]['host']
    db=info_db[1]['db']
    user_db=info_db[2]['user']
    pass_db=info_db[3]['password']

    try:
        conn=pymysql.connect(host=host, user=user_db, passwd=pass_db, db=db)

        cur=conn.cursor()

        #----Modify-----
        query_extract="SELECT Id_cliente,Macro_sector,Sector,Subsector,Actividad,Ventas,Activo_fijo,Potencial,Cheques from nbo_in;"
        #----Modify-----
        
        cur.execute(query_fix)
        cur.execute(query_extract)

        res = cur.fetchall()

        cur.close()

        conn.close()

    except pymysql.Error as e:
        msj= ("Error %d: %s" % (e.args[0], e.args[1]))
        print(msj)
        return Response(status=400,response=msj)
    else:
        headers=['id','Macro_sector','Sector','Subsector','Actividad','Ventas','Activo_fijo','Potencial','Cheques']
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

        df1=pd.DataFrame(dataset_dummy)

        #-------Analisis de Datos--------------

        X=df1[['Macro_sector','Sector','Subsector','Actividad','Ventas','Activo_fijo','Potencial']]

        indexes_empties=X[pd.isnull(X).any(axis=1)].index.tolist()

        X=X.dropna(axis=0,how="any")
        X= X.reset_index(drop=True)
        df1=df1.drop(indexes_empties)
        df1= df1.reset_index(drop=True)

        X_normalized=normalize_columns(X,['Ventas','Activo_fijo','Potencial'])
        X_normalized=create_dummies(X_normalized,['Macro_sector','Sector','Subsector','Actividad'])
        X_normalized=X_normalized[['Activo_fijo', 'Macro_sector_', 'Macro_sector_1', 'Macro_sector_2', 'Macro_sector_3', 'Macro_sector_4', 'Macro_sector_5', 'Sector_', 'Sector_1300003', 'Sector_3400009', 'Sector_4100004', 'Sector_6700000', 'Sector_6800002', 'Sector_6900006', 'Sector_7300007', 'Sector_9999999', 'Subsector_', 'Subsector_00000000', 'Subsector_00010000', 'Subsector_0200006-2', 'Subsector_1400001-1', 'Subsector_2300002-1', 'Subsector_2900000-2', 'Subsector_3000007-2', 'Subsector_3400009-2', 'Subsector_3500007-2', 'Subsector_3900009-11', 'Subsector_3900009-6', 'Subsector_6200000-3', 'Subsector_6600002-2', 'Subsector_6600002-5', 'Subsector_6600002-6', 'Subsector_6600002-7', 'Subsector_6800001-1', 'Subsector_6900006-1', 'Subsector_7100001-3', 'Subsector_8100000-3', 'Subsector_8100000-5', 'Subsector_8200008-1', 'Subsector_8200008-4', 'Subsector_8300006-1', 'Subsector_8400004-1', 'Subsector_8400004-2', 'Subsector_8400004-3', 'Subsector_8900004-2', 'Subsector_8900004-5', 'Subsector_9400003-1', 'Subsector_9999999-1', 'Actividad_', 'Actividad_0111063', 'Actividad_0112095', 'Actividad_0112128', 'Actividad_0232017', 'Actividad_0291013', 'Actividad_0312017', 'Actividad_1321017', 'Actividad_1411016', 'Actividad_2012037', 'Actividad_2025014', 'Actividad_2028026', 'Actividad_2049030', 'Actividad_2059013', 'Actividad_2061026', 'Actividad_2094019', 'Actividad_2096015', 'Actividad_2312031', 'Actividad_2321016', 'Actividad_2329010', 'Actividad_2394021', 'Actividad_2411015', 'Actividad_2413011', 'Actividad_2429018', 'Actividad_2431013', 'Actividad_2512011', 'Actividad_2529107', 'Actividad_2632017', 'Actividad_2912005', 'Actividad_3032018', 'Actividad_3092046', 'Actividad_3097054', 'Actividad_3112018', 'Actividad_3113016', 'Actividad_3122017', 'Actividad_3229045', 'Actividad_3323029', 'Actividad_3411022', 'Actividad_3421013', 'Actividad_3516038', 'Actividad_3532026', 'Actividad_3591022', 'Actividad_3596014', 'Actividad_3599018', 'Actividad_3694024', 'Actividad_3697010', 'Actividad_3723013', 'Actividad_3799014', 'Actividad_3812014', 'Actividad_3961027', 'Actividad_3999119', 'Actividad_4111027', 'Actividad_4111910', 'Actividad_4112017', 'Actividad_4121018', 'Actividad_4121026', 'Actividad_4121034', 'Actividad_4123014', 'Actividad_4129012', 'Actividad_4193017', 'Actividad_4222014', 'Actividad_4291019', 'Actividad_6135017', 'Actividad_6225024', 'Actividad_6311039', 'Actividad_6321012', 'Actividad_6329016', 'Actividad_6512017', 'Actividad_6514013', 'Actividad_6623020', 'Actividad_6625018', 'Actividad_6695011', 'Actividad_6699021', 'Actividad_6699039', 'Actividad_6711015', 'Actividad_6712013', 'Actividad_6714019', 'Actividad_6714027', 'Actividad_6732029', 'Actividad_6800002', 'Actividad_6811013', 'Actividad_6813027', 'Actividad_6814017', 'Actividad_6816013', 'Actividad_6993019', 'Actividad_6999041', 'Actividad_6999075', 'Actividad_6999108', 'Actividad_7111016', 'Actividad_7112014', 'Actividad_7114010', 'Actividad_7129019', 'Actividad_7212012', 'Actividad_7291016', 'Actividad_7299010', 'Actividad_7511018', 'Actividad_7512016', 'Actividad_7519020', 'Actividad_7611016', 'Actividad_7613004', 'Actividad_7613012', 'Actividad_7613905', 'Actividad_7614010', 'Actividad_8123010', 'Actividad_8123078', 'Actividad_8123094', 'Actividad_8211013', 'Actividad_8219017', 'Actividad_8219122', 'Actividad_8311011', 'Actividad_8312019', 'Actividad_8400002', 'Actividad_8412017', 'Actividad_8415011', 'Actividad_8419013', 'Actividad_8421018', 'Actividad_8424012', 'Actividad_8429038', 'Actividad_8511017', 'Actividad_8519011', 'Actividad_8522014', 'Actividad_8814015', 'Actividad_8824014', 'Actividad_8825012', 'Actividad_8839906', 'Actividad_8915011', 'Actividad_8934029', 'Actividad_8944028', 'Actividad_8944098', 'Actividad_8991011', 'Actividad_9119018', 'Actividad_9319014', 'Actividad_9321019', 'Actividad_9411018', 'Actividad_9471012', 'Actividad_9800100', 'Actividad_9900003', 'Actividad_9999999']]


        #-------Carga del modelo

        filename = 'productos2_forest_bvar.sav'

        model = pickle.load(open(filename, 'rb'))

        #---------Prediccion

        print('Pronosticando predicciones....')

        predict_producto=model.predict(X_normalized)

        probabilidades=model.predict_proba(X_normalized)

        df1['predict_producto']=predict_producto

        dfx=pd.DataFrame(probabilidades,columns=['P1','P2','P3','P4','P5'])

        df1=pd.concat([df1,dfx],axis=1)

        print('Se realizara insert')

        conn=pymysql.connect(host=host, user=user_db, passwd=pass_db, db=db)
        cur=conn.cursor()

        cont=0
        val=[]
        for index,row in df1.iterrows():
            #query="Update "+table+" set R="+str(int(row['R']))+",F="+str(int(row['F']))+",M="+str(int(row['M']))+",Categoria='"+str(row['Categoria'])+"' where "+id_RFM+"='"+str(row[id_RFM])+"';"
            val.append((row[headers[0]],row['predict_producto'],row['P1'],row['P2'],row['P3'],row['P4'],row['P5']))
            cont=cont+1

        print('inserts:',cont)

        try:
            query="insert into nbo_out (Id_cliente,Producto_Predict,Producto_1,Producto_2,Producto_3,Producto_4,Producto_5) values (%s,%s,%s,%s,%s,%s,%s)"
            cur.executemany(query,val)
            conn.commit()
            cur.close()
            conn.close()
        except pymysql.Error as e:
            msj= ("Error %d: %s" % (e.args[0], e.args[1]))
            print(msj)
            return Response(status=400,response=msj)            
        else:
            msj='Operacion concluida, se insertaron '+str(cont)+' regitros'
            return Response(msj,status=200)

@app.route("/setAcreedor",methods=['POST'])
def setAcreedor(body=None):
    body=request.get_json()
    if body==None:
        msj= "No se enviaron parametros POST, por lo tanto el proceso se detuvo"
        return Response(status=400,response=msj)
    else:
        check=checkBodysetCLV(body)
        if (check!='OK'):
            return check

    info_db=body['db']      

    global host
    global db
    global user_db
    global pass_db

    host=info_db[0]['host']
    db=info_db[1]['db']
    user_db=info_db[2]['user']
    pass_db=info_db[3]['password']

    try:
        conn=pymysql.connect(host=host, user=user_db, passwd=pass_db, db=db)

        cur=conn.cursor()

        #----Modify-----
        query_extract="SELECT Id_cliente,Macro_sector,Sector,Subsector,Actividad,Ventas,Activo_fijo,Potencial,Cheques from nbo_in;"
        #----Modify-----
        
        cur.execute(query_fix)
        cur.execute(query_extract)

        res = cur.fetchall()

        cur.close()

        conn.close()

    except pymysql.Error as e:
        msj= ("Error %d: %s" % (e.args[0], e.args[1]))
        print(msj)
        return Response(status=400,response=msj)
    else:
        headers=['id','Macro_sector','Sector','Subsector','Actividad','Ventas','Activo_fijo','Potencial','Cheques']
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

        df1=pd.DataFrame(dataset_dummy)

        #-------Analisis de Datos--------------

        X=df1[['Macro_sector','Sector','Subsector','Actividad','Ventas','Activo_fijo','Potencial']]

        indexes_empties=X[pd.isnull(X).any(axis=1)].index.tolist()

        X=X.dropna(axis=0,how="any")
        X= X.reset_index(drop=True)
        df1=df1.drop(indexes_empties)
        df1= df1.reset_index(drop=True)

        X_normalized=normalize_columns(X,['Ventas','Activo_fijo','Potencial'])
        X_normalized=create_dummies(X_normalized,['Macro_sector','Sector','Subsector','Actividad'])
        
        #-------Carga del modelo

        filename = 'acreedor2_knn_avar.sav'

        model = pickle.load(open(filename, 'rb'))

        #---------Prediccion

        print('Pronosticando predicciones....')

        predict_acreedor=model.predict(X_normalized)

        probabilidades=model.predict_proba(X_normalized)

        df1['predict_acreedor']=predict_acreedor
        df1['predict_acreedor']=np.where((df1['predict_acreedor']==1),'No','Si')
        df1['acreedor_prob']=probabilidades[:,0]

        print('Se realizara insert....')

        conn=pymysql.connect(host=host, user=user_db, passwd=pass_db, db=db)
        cur=conn.cursor()

        cont=0
        val=[]
        for index,row in df1.iterrows():
            #query="Update "+table+" set R="+str(int(row['R']))+",F="+str(int(row['F']))+",M="+str(int(row['M']))+",Categoria='"+str(row['Categoria'])+"' where "+id_RFM+"='"+str(row[id_RFM])+"';"
            val.append((row[headers[0]],row['predict_acreedor'],row['acreedor_prob']))
            cont=cont+1

        print('inserts:',cont)

        try:
            query="insert into acreedor_out (Id_cliente,Acreedor,Acreedor_prob) values (%s,%s,%s)"
            cur.executemany(query,val)
            conn.commit()
            cur.close()
            conn.close()
        except pymysql.Error as e:
            msj= ("Error %d: %s" % (e.args[0], e.args[1]))
            print(msj)
            return Response(status=400,response=msj)            
        else:
            msj='Operacion concluida, se insertaron '+str(cont)+' regitros'
            return Response(msj,status=200)         




def checkBodysetCLV(body):
        c=0
        for i in body:
            if(i!='id' and i!='db' and i!='meanlife'):
                msj= "Key:"+i+" incorrecta,las keys deben ser las siguientes:\"db\",\"meanlife\" (opcional)"
                return Response(status=400,response=msj)
            else:
                c=c+1
        if(c!=3 and c!=1):
                msj= "Falta alguna key,las keys deben ser las siguientes:\"db\",opcional(\"meanlife\")"
                return Response(status=400,response=msj)        

        if 'meanlife' in body:
            if(type(body['meanlife'])!=int):
                msj= "meanlife no es \"entero\",favor de corregir"
                return Response(status=400,response=msj)    

        c=0
        for i in body['db']:
            for key,val in i.items():
                if(key=='host'):
                    if(type(val)!=str):
                        msj= "El dato de \"host\" debe ser \"str\",favor de corregir"
                        return Response(status=400,response=msj)
                    else:
                        c=c+1   
                elif(key=='db'):
                    if(type(val)!=str):
                        msj= "El dato de \"db\" debe ser \"str\",favor de corregir"
                        return Response(status=400,response=msj)
                    else:
                        c=c+1   
                elif(key=='user'):
                    if(type(val)!=str):
                        msj= "El dato de \"user\" debe ser \"str\",favor de corregir"
                        return Response(status=400,response=msj)
                    else:
                        c=c+1   
                elif(key=='password'):
                    if(type(val)!=str):
                        msj= "El dato de \"password\" debe ser \"str\",favor de corregir"
                        return Response(status=400,response=msj)
                    else:
                        c=c+1   
                else:
                    msj= "Key:"+key+" incorrecta para \"db\",las keys deben ser las siguientes, en el siguiente orden: \"host\",\"db\",\"user\",\"password\""       
                    return Response(status=400,response=msj)    
        if(c!=4):
            msj= "Falta alguna key,las keys deben ser las siguientes:\"segmentos\",\"ponderaciones\",\"db\""
            return Response(status=400,response=msj)
        return 'OK'                                         

def checkBodysetRFM(body):
        c=0
        for i in body:
            if(i!='segmentos' and i!='ponderaciones' and i!='db'):
                msj= "Key:"+i+" incorrecta,las keys deben ser las siguientes:\"segmentos\",\"ponderaciones\",\"db\""
                return Response(status=400,response=msj)
            else:
                c=c+1
        if(c!=3):
                msj= "Falta alguna key,las keys deben ser las siguientes:\"segmentos\",\"ponderaciones\",\"db\""
                return Response(status=400,response=msj)        

        for i in body['segmentos']:
            for key,val in i.items():
                if(key=='nombre'):
                    if(type(val)!=str):
                        msj= "Uno de los datos de \"Segmentos\" no es \"string\",favor de corregir"
                        return Response(status=400,response=msj)
                else:
                    msj= "Key:"+key+" incorrecta en  \"segmentos\",debe ser Key:\"nombre\""
                    return Response(status=400,response=msj)

        total=0
        c=0
        for i in body['ponderaciones']:
            for key,val in i.items():
                if(key=='recencia'):
                    if(type(val)!=int):
                        msj= "El dato de \"recencia\" debe ser \"int\" para que la sumatoria de recencia+frecuencia+monto=100,favor de corregir"
                        return Response(status=400,response=msj)
                    else:
                        c=c+1
                        total=total+val 
                elif(key=='frecuencia'):
                    if(type(val)!=int):
                        msj= "El dato de \"frecuencia\" debe ser \"int\" para que la sumatoria de recencia+frecuencia+monto=100,favor de corregir"
                        return Response(status=400,response=msj)
                    else:
                        c=c+1
                        total=total+val 
                elif(key=='monto'):
                    if(type(val)!=int):
                        msj= "El dato de \"monto\" debe ser \"int\" para que la sumatoria de recencia+frecuencia+monto=100,favor de corregir"
                        return Response(status=400,response=msj)
                    else:
                        c=c+1
                        total=total+val 
                else:
                    msj= "Key:"+key+" incorrecta para \"ponderaciones\",las keys deben ser las siguientes, en el siguiente orden: \"recencia\",\"frecuencia\",\"monto\""
                    return Response(status=400,response=msj)
        if ((total>100) | (total<100)):
            msj= "La suma de las ponderaciones recencia+frecuencia+monto deber ser 100, no "+str(total)+", favor de corregir"
            return Response(status=400,response=msj)
        if(c!=3):
            msj= "Falta alguna key,las keys deben ser las siguientes:\"segmentos\",\"ponderaciones\",\"db\""
            return Response(status=400,response=msj)    

        c=0
        for i in body['db']:
            for key,val in i.items():
                if(key=='host'):
                    if(type(val)!=str):
                        msj= "El dato de \"host\" debe ser \"str\",favor de corregir"
                        return Response(status=400,response=msj)
                    else:
                        c=c+1   
                elif(key=='db'):
                    if(type(val)!=str):
                        msj= "El dato de \"db\" debe ser \"str\",favor de corregir"
                        return Response(status=400,response=msj)
                    else:
                        c=c+1   
                elif(key=='user'):
                    if(type(val)!=str):
                        msj= "El dato de \"user\" debe ser \"str\",favor de corregir"
                        return Response(status=400,response=msj)
                    else:
                        c=c+1   
                elif(key=='password'):
                    if(type(val)!=str):
                        msj= "El dato de \"password\" debe ser \"str\",favor de corregir"
                        return Response(status=400,response=msj)
                    else:
                        c=c+1   
                else:
                    msj= "Key:"+key+" incorrecta para \"db\",las keys deben ser las siguientes, en el siguiente orden: \"host\",\"db\",\"user\",\"password\""       
                    return Response(status=400,response=msj)    
        if(c!=4):
            msj= "Falta alguna key,las keys deben ser las siguientes:\"segmentos\",\"ponderaciones\",\"db\""
            return Response(status=400,response=msj)
        return 'OK'                     

def setRecencia(first_dataset):
    print('Entro a setRecencia')
    dataset=first_dataset

    dataset=dataset.sort_values(by=target_R,ascending=True)
    dataset = dataset.reset_index(drop=True)
    #dataset[target_R]=dataset[target_R].fillna(dataset[target_R].mean())

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
    #dataset[target_F]=dataset[target_F].fillna(dataset[target_F].mean())

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
    #dataset[target_M]=dataset[target_M].fillna(0)

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
            dataset.loc[(dataset['ponderacion'] <= limits[j]), 'Segmento'] = segmentos[index]['nombre']
            temp=limits[j]
        else:
            dataset.loc[(dataset['ponderacion']<=limits[j]) & (dataset['ponderacion']>temp), 'Segmento'] = segmentos[index]['nombre']
            temp=limits[j]

    dataset=dataset.drop(['p_RFM'], axis=1)
    dataset['Monto']=dataset['Monto'].astype(float)
    dataset=dataset.round({'Monto': 4})

    #db='RFM_Generator'
    conn=pymysql.connect(host=host, user=user_db, passwd=pass_db, db=db)
    cur=conn.cursor()

    cont=0
    val=[]
    for index,row in dataset.iterrows():
        #query="Update "+table+" set R="+str(int(row['R']))+",F="+str(int(row['F']))+",M="+str(int(row['M']))+",Categoria='"+str(row['Categoria'])+"' where "+id_RFM+"='"+str(row[id_RFM])+"';"
        val.append((row[id_RFM],row[target_R],row[target_F],row[target_M],row['R'],row['F'],row['M'],row['Segmento']))
        cont=cont+1

    print('inserts:',cont)
    try:
        query="insert into rfm_out (Id_cliente,Recencia_in,Frecuencia_in,Monto_in,Recencia_out,Frecuencia_out,Monto_out,Segmento) values (%s,%s,%s,%s,%s,%s,%s,%s)"
        cur.executemany(query,val)
        conn.commit()
        cur.close()
        conn.close()
    except pymysql.Error as e:
        msj= ("Error %d: %s" % (e.args[0], e.args[1]))
        print(msj)
        return Response(status=400,response=msj)            
    else:
        return dataset

def getmeanlife():
        print('Entro a getmeanlife')
        try:
            conn=pymysql.connect(host=host, user=user_db, passwd=pass_db, db=db)

            cur=conn.cursor()

            #----Modify-----
            query_accounts="select distinct Id_cliente from rfm_in;"
            #----Modify-----
        
            cur.execute(query_accounts)

            res = cur.fetchall()

            cur.close()

            conn.close()

        except pymysql.Error as e:
            msj= ("Error %d: %s" % (e.args[0], e.args[1]))
            print(msj)
            return Response(status=400,response=msj)
        else:
            filas=0
            ids=[]
            if(len(res)>0):
                for r in res:
                    ids.append(r[0])

            print('Total de cuentas:',len(ids))

            meanlife_count=0

            try:
                conn=pymysql.connect(host=host, user=user_db, passwd=pass_db, db=db)

                cur=conn.cursor()

                cont=0

                for id in ids:
                    #----Modify-----
                    query_meanlife="SELECT TIMESTAMPDIFF(MONTH,(select MAX(Fecha) FROM rfm_in WHERE Id_cliente='%s') , (select MIN(Fecha) FROM rfm_in WHERE Id_cliente='%s'));" %(id,id)
                    #----Modify-----
                    cur.execute(query_meanlife)

                    res = cur.fetchall()

                    #print('Resultado de query:',res[0][0])

                    if(res[0][0]==None):
                        continue

                    cont=cont+1
                    meanlife_count=meanlife_count+abs(res[0][0])

                #print('Total de meses',meanlife_count)
                print('Cuentas con al menos 1 mes:',cont)

                cur.close()

                conn.close()

            except pymysql.Error as e:
                msj= ("Error %d: %s" % (e.args[0], e.args[1]))
                print(msj)
                return Response(status=400,response=msj)            

            else:
                final_meanlife=meanlife_count/cont
                #print('Vida promedio en meses:',final_meanlife)
                return final_meanlife

def create_dummies(dataset,columns):
    
    for column in columns:
        dummy=pd.get_dummies(dataset[column],prefix=column)

        dataset=dataset.drop(column,axis=1)#axis=1-->para eliminar columna y no fila

        dataset=pd.concat([dataset,dummy],axis=1)#axis=1-->para agregar columna y no fila

    return dataset

def normalize_columns(dataset,columns):
    scaler = MinMaxScaler()
    scaled_X = scaler.fit_transform(dataset[columns])
    X_normalized=pd.DataFrame(scaled_X,columns=columns)
    dataset=dataset.drop(columns,axis=1)#axis=1-->para eliminar columna y no fila
    new_dataset=pd.concat([dataset,X_normalized],axis=1)#axis=1-->para agregar columna y no fila
    return new_dataset






if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)