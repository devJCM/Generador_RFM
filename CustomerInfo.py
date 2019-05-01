
"""API Generador-RFM"""

import pymysql
import json
import pandas as pd
import numpy as np
import pickle

from datetime import datetime
from sklearn.preprocessing import MinMaxScaler
from flask import Flask,Response,request
from sklearn.cluster import KMeans
from sklearn.model_selection import train_test_split





app = Flask(__name__)

host='localhost'
db='CustomerInfo'
user_db='root'
pass_db=''
query_fix="SET sql_mode=(SELECT REPLACE(@@sql_mode,'ONLY_FULL_GROUP_BY',''));" #para permitir group by sin declararlos
query_fix2="SET sql_mode = '';" #para permitir agregar nulos en fechas

id_RFM=None
target_R=None
target_F=None
target_M=None






@app.route("/setRFM",methods=['POST'])
def setRFM(body=None):

    """API que genera RFM para todas las cuentas"""
    print('Entro a setRFM')
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

@app.route("/setCLV",methods=['GET'])
@app.route("/setCLV/<meanlifein>",methods=['GET'])
def setCLV(meanlifein=None):

    print('Entro a setCLV')

    if (meanlifein!=None):
        meanlife=float(meanlifein)
        print('se recibio promedio de vida:',meanlife)
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
        #print(first_dataset.dtypes)
        
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
        
@app.route("/setNBO",methods=['GET'])
def setNBO():

    print('Entro a setNBO')

    try:
        conn=pymysql.connect(host=host, user=user_db, passwd=pass_db, db=db)

        cur=conn.cursor()

        #----Modify-----
        query_extract="SELECT Id_cliente,Macro_sector,Sector,Subsector,Actividad,Ventas,Activo_fijo,Potencial,Cheques from nbo_in;"
        #----Modify-----
        
        #cur.execute(query_fix)
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

        print('Despues de limpieza de vacios:',df1.shape)

        X_normalized=normalize_columns(X,['Ventas','Activo_fijo','Potencial'])
        X_normalized=create_dummies(X_normalized,['Macro_sector','Sector','Subsector','Actividad'])
        X_normalized=X_normalized[['Activo_fijo', 'Macro_sector_', 'Macro_sector_1', 'Macro_sector_2', 'Macro_sector_3', 'Macro_sector_4', 'Macro_sector_5', 'Sector_', 'Sector_1300003', 'Sector_3400009', 'Sector_4100004', 'Sector_6700000', 'Sector_6800002', 'Sector_6900006', 'Sector_7300007', 'Sector_9999999', 'Subsector_', 'Subsector_00000000', 'Subsector_00010000', 'Subsector_0200006-2', 'Subsector_1400001-1', 'Subsector_2300002-1', 'Subsector_2900000-2', 'Subsector_3000007-2', 'Subsector_3400009-2', 'Subsector_3500007-2', 'Subsector_3900009-11', 'Subsector_3900009-6', 'Subsector_6200000-3', 'Subsector_6600002-2', 'Subsector_6600002-5', 'Subsector_6600002-6', 'Subsector_6600002-7', 'Subsector_6800001-1', 'Subsector_6900006-1', 'Subsector_7100001-3', 'Subsector_8100000-3', 'Subsector_8100000-5', 'Subsector_8200008-1', 'Subsector_8200008-4', 'Subsector_8300006-1', 'Subsector_8400004-1', 'Subsector_8400004-2', 'Subsector_8400004-3', 'Subsector_8900004-2', 'Subsector_8900004-5', 'Subsector_9400003-1', 'Subsector_9999999-1', 'Actividad_', 'Actividad_0111063', 'Actividad_0112095', 'Actividad_0112128', 'Actividad_0232017', 'Actividad_0291013', 'Actividad_0312017', 'Actividad_1321017', 'Actividad_1411016', 'Actividad_2012037', 'Actividad_2025014', 'Actividad_2028026', 'Actividad_2049030', 'Actividad_2059013', 'Actividad_2061026', 'Actividad_2094019', 'Actividad_2096015', 'Actividad_2312031', 'Actividad_2321016', 'Actividad_2329010', 'Actividad_2394021', 'Actividad_2411015', 'Actividad_2413011', 'Actividad_2429018', 'Actividad_2431013', 'Actividad_2512011', 'Actividad_2529107', 'Actividad_2632017', 'Actividad_2912005', 'Actividad_3032018', 'Actividad_3092046', 'Actividad_3097054', 'Actividad_3112018', 'Actividad_3113016', 'Actividad_3122017', 'Actividad_3229045', 'Actividad_3323029', 'Actividad_3411022', 'Actividad_3421013', 'Actividad_3516038', 'Actividad_3532026', 'Actividad_3591022', 'Actividad_3596014', 'Actividad_3599018', 'Actividad_3694024', 'Actividad_3697010', 'Actividad_3723013', 'Actividad_3799014', 'Actividad_3812014', 'Actividad_3961027', 'Actividad_3999119', 'Actividad_4111027', 'Actividad_4111910', 'Actividad_4112017', 'Actividad_4121018', 'Actividad_4121026', 'Actividad_4121034', 'Actividad_4123014', 'Actividad_4129012', 'Actividad_4193017', 'Actividad_4222014', 'Actividad_4291019', 'Actividad_6135017', 'Actividad_6225024', 'Actividad_6311039', 'Actividad_6321012', 'Actividad_6329016', 'Actividad_6512017', 'Actividad_6514013', 'Actividad_6623020', 'Actividad_6625018', 'Actividad_6695011', 'Actividad_6699021', 'Actividad_6699039', 'Actividad_6711015', 'Actividad_6712013', 'Actividad_6714019', 'Actividad_6714027', 'Actividad_6732029', 'Actividad_6800002', 'Actividad_6811013', 'Actividad_6813027', 'Actividad_6814017', 'Actividad_6816013', 'Actividad_6993019', 'Actividad_6999041', 'Actividad_6999075', 'Actividad_6999108', 'Actividad_7111016', 'Actividad_7112014', 'Actividad_7114010', 'Actividad_7129019', 'Actividad_7212012', 'Actividad_7291016', 'Actividad_7299010', 'Actividad_7511018', 'Actividad_7512016', 'Actividad_7519020', 'Actividad_7611016', 'Actividad_7613004', 'Actividad_7613012', 'Actividad_7613905', 'Actividad_7614010', 'Actividad_8123010', 'Actividad_8123078', 'Actividad_8123094', 'Actividad_8211013', 'Actividad_8219017', 'Actividad_8219122', 'Actividad_8311011', 'Actividad_8312019', 'Actividad_8400002', 'Actividad_8412017', 'Actividad_8415011', 'Actividad_8419013', 'Actividad_8421018', 'Actividad_8424012', 'Actividad_8429038', 'Actividad_8511017', 'Actividad_8519011', 'Actividad_8522014', 'Actividad_8814015', 'Actividad_8824014', 'Actividad_8825012', 'Actividad_8839906', 'Actividad_8915011', 'Actividad_8934029', 'Actividad_8944028', 'Actividad_8944098', 'Actividad_8991011', 'Actividad_9119018', 'Actividad_9319014', 'Actividad_9321019', 'Actividad_9411018', 'Actividad_9471012', 'Actividad_9800100', 'Actividad_9900003', 'Actividad_9999999']]


        #-------Carga del modelo

        filename = 'productos2_forest_bvar.sav'

        model = pickle.load(open(filename, 'rb'))

        #---------Prediccion

        print('Pronosticando productos....')

        #predict_producto=model.predict(X_normalized)

        probabilidades=model.predict_proba(X_normalized)

        #df1['predict_producto']=predict_producto

        productos=get_items()

        dfx=pd.DataFrame(probabilidades,columns=list(productos.keys()))

        df2=pd.concat([df1['id'],dfx],axis=1)
        df2.head()
        dic=df2.to_dict(orient="records")

        num_productos=5

        dic2=[]
        for d in dic:
            temp_id=d['id']
            del d['id']
            sorted_x = sorted(d.items(), key=lambda kv: kv[1])
            sorted_x= sorted_x[::-1]
            probas=sorted_x[:num_productos]
            #print(probas)
            d={}
            d['id']=temp_id
            for p in probas:
                d[p[0]]=p[1]
            dic2.append(d)

        print('Se realizara insert')

        conn=pymysql.connect(host=host, user=user_db, passwd=pass_db, db=db)
        cur=conn.cursor()

        cont=0
        val=[]

        for d in dic2:
            list_idproductos=list(d.keys())
            for i in range(1,num_productos+1):
                val.append( (d['id'],list_idproductos[i],d[list_idproductos[i]]) ) 
                cont=cont+1   

        print('inserts:',cont)

        try:
            query="insert into nbo_out (Id_cliente,Id_producto,Producto_prob) values (%s,%s,%s)"
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

@app.route("/setAcreedor",methods=['GET'])
def setAcreedor():

    print('Entro a setAcreedor')

    try:
        conn=pymysql.connect(host=host, user=user_db, passwd=pass_db, db=db)

        cur=conn.cursor()

        #----Modify-----
        query_extract="SELECT Id_cliente,Macro_sector,Sector,Subsector,Actividad,Ventas,Activo_fijo,Potencial,Cheques from nbo_in;"
        #----Modify-----
        
        #cur.execute(query_fix)
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

        print('Despues de limpieza de vacios:',df1.shape)

        X_normalized=normalize_columns(X,['Ventas','Activo_fijo','Potencial'])
        X_normalized=create_dummies(X_normalized,['Macro_sector','Sector','Subsector','Actividad'])
        
        #-------Carga del modelo

        filename = 'acreedor2_knn_avar.sav'

        model = pickle.load(open(filename, 'rb'))

        #---------Prediccion

        print('Pronosticando acreedores....')

        predict_acreedor=model.predict(X_normalized)

        probabilidades=model.predict_proba(X_normalized)

        df1['predict_acreedor']=predict_acreedor
        df1['predict_acreedor']=np.where((df1['predict_acreedor']==1),'No','Si')
        df1['acreedor_prob']=probabilidades[:,0]

        #-------Carga del modelo2

        filename2 = 'credito1_forest_avar.sav'

        model2 = pickle.load(open(filename2, 'rb'))

        #---------Prediccion2

        print('Pronosticando montos de creditos....')

        Monto_predict=model2.predict(X_normalized)

        df1['Monto_predict']=Monto_predict

        conn=pymysql.connect(host=host, user=user_db, passwd=pass_db, db=db)
        cur=conn.cursor()

        cont=0
        val=[]

        query_monto_s="select Segmento,MAX(Monto_in) from rfm_out group by Segmento;"
        cur.execute(query_monto_s)
        res=cur.fetchall()
        montos={}
        monto_seg=0

        for i in res:
            montos[i[0]]=float(i[1])

        print('Se armara diccionario de valores....')

        for index,row in df1.iterrows():
            #query="Update "+table+" set R="+str(int(row['R']))+",F="+str(int(row['F']))+",M="+str(int(row['M']))+",Categoria='"+str(row['Categoria'])+"' where "+id_RFM+"='"+str(row[id_RFM])+"';"
            query_seg="select Segmento from rfm_out where Id_cliente='%s';" %(row[headers[0]])
            cur.execute(query_seg)
            res=cur.fetchall()
            monto_seg=0

            if(len(res)>0): 
                segmento=str(res[0][0])
                for i in montos:
                    if(i==segmento):
                        monto_seg=montos[i]
            else:
                print(row[headers[0]]," >>>NO TIENE SEGMENTO")

            val.append((row[headers[0]],row['predict_acreedor'],row['acreedor_prob'],monto_seg,row['Monto_predict']))
            cont=cont+1

        print('Se realizara insert....')
        print('inserts:',cont)

        try:
            query="insert into acreedor_out (Id_cliente,Acreedor,Acreedor_prob,Monto_seg,Monto_predict) values (%s,%s,%s,%s,%s)"
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

@app.route("/getCustomerInfo/<id>",methods=['GET'])
def getCustomerInfo(id):

    print('Entro a getCustomerInfo')

    if id==None:
        msj= "Es necesario el Id del cliente que quiere consultar, por lo tanto el proceso se detuvo"
        print(msj)
        return Response(status=400,response=msj)
    else:
        id_cliente=id

        qrfm="select max(Ejecucion),Recencia_out,Frecuencia_out,Monto_out,Segmento from rfm_out where Id_cliente='%s';" %(id_cliente)
        
        qclv="select max(Ejecucion),Valor_cliente,Vida_cliente,CLV from clv_out where Id_cliente='%s';" %(id_cliente)

        qacreedor="select max(Ejecucion),Acreedor,Acreedor_prob,Monto_predict,Monto_seg from acreedor_out where Id_cliente='%s';" %(id_cliente)

        qnbo="select max(Ejecucion),Id_producto,Producto_prob from nbo_out where Id_cliente='%s' group by Id_producto;" %(id_cliente)

        qcalls="select Id_call,Nombre,Date_end,Id_cliente,Estado,Venta,Id_producto from calls_in where Id_cliente='%s';" %(id_cliente)

        qcallspredict="select max(Ejecucion),Nombre,Date_predict from scheduler_out where Id_cliente='%s';" %(id_cliente)

        Data={'RFM':{},'CLV':{},'NBO':{},'Credito':{},'Calls':{}}

        limit_values=getlimitvalues()
        Data['limit_values']=limit_values

        try:
            conn=pymysql.connect(host=host, user=user_db, passwd=pass_db, db=db)

            cur=conn.cursor()
                   
            cur.execute(query_fix)

            cur.execute(qrfm)
            res=cur.fetchall()
            Data['RFM']['R']=res[0][1]
            Data['RFM']['F']=res[0][2]
            Data['RFM']['M']=res[0][3]
            Data['RFM']['Segmento']=res[0][4]

            cur.execute(qclv)
            res=cur.fetchall()
            Data['CLV']['CV']=res[0][1]
            Data['CLV']['LifeTime']=res[0][2]
            Data['CLV']['CLV']=res[0][3]

            cur.execute(qacreedor)
            res=cur.fetchall()
            Data['Credito']['Acreedor']=res[0][1]
            if(res[0][2]!=None):
                Data['Credito']['Acreedor_prob']=res[0][2]*100
            Data['Credito']['Monto_predict']=res[0][3]
            Data['Credito']['Monto_seg']=res[0][4]

            cur.execute(qnbo)
            res=cur.fetchall()
            productos=get_items()
            #Data['NBO']['Producto_Predict']=res[0][1]
            temp_prob=0
            producto_predict=''
            obj={}
            for r in res:
                if(len(r)>0):
                    if(r[2]>temp_prob):
                        temp_prob=r[2]
                        producto_predict=productos[r[1]]
                    #Data['NBO'][productos[r[1]]]=r[2]*100
                    obj[productos[r[1]]]=r[2]*100
            #---- Ordenar Resultados        
            sorted_x = sorted(obj.items(), key=lambda kv: kv[1])
            ordenado= sorted_x[::-1]
            for o in ordenado:
                Data['NBO'][o[0]]=o[1]
            #------        
            Data['NBO']['Producto_Predict']=producto_predict


            cur.execute(qcallspredict)
            res2=cur.fetchall()
            calls=[]
            call_pred={}
            if(res2[0][1]!=None):
                call_pred['Nombre']='Llamar a '+res2[0][1]+' para ofrecer producto: <b>'+producto_predict
            else:
                call_pred['Nombre']='Llamar para ofrecer producto: <b>'+producto_predict
            temp_date=res2[0][2]
            dt = datetime.today()
            x = datetime(dt.year, temp_date.month,temp_date.day)
            call_pred['Date_end']=x.strftime("%Y-%m-%d")
            call_pred['Id_cliente']=id_cliente
            call_pred['Estado']='Planned'
            call_pred['Venta']=0
            Data['Calls']['Predict']=[call_pred]

            cur.execute(qcalls)
            res=cur.fetchall()
            for i in range(len(res)):
                temp={}
                temp['Id_call']=res[i][0]
                temp['Nombre']=res[i][1]
                if(res[i][2]!=None):
                    temp['Date_end']=res[i][2].strftime("%Y-%m-%d %H:%M:%S")
                else:
                    temp['Date_end']=datetime(1,1,1).strftime("%Y-%m-%d %H:%M:%S")
                temp['Id_cliente']=res[i][3]
                temp['Estado']=res[i][4]

                if(res[i][5]!=None):
                    temp['Venta']=res[i][5]
                else:
                    #temp['Venta']=0
                    temp['Venta']=np.random.randint(2) #---!!!Temporal¡¡¡¡...........

                if(res[i][6]!=None):
                    temp['Producto_sold']=productos[res[i][6]]
                else:
                    #temp['Producto_sold']='-'
                    #---!!!Temporal¡¡¡¡...........
                    if(temp['Venta']==1):
                        temp['Producto_sold']=productos[str(np.random.randint(1,6))]
                    else:
                        temp['Producto_sold']='-'    
                    #---!!!Temporal¡¡¡¡...........

                calls.append(temp)

            Data['Calls']['CRM']=calls    
            

            cur.close()
            conn.close()

        except pymysql.Error as e:
            msj= ("Error %d: %s" % (e.args[0], e.args[1]))
            print(msj)
            return Response(status=400,response=msj)
        else:
            if(Data['RFM']['R']!=None and limit_values['rmin']!=None and limit_values['rmax']!=None):
                Data['RFM']['R_n']=(Data['RFM']['R']-limit_values['rmin'])/(limit_values['rmax']-limit_values['rmin'])
            if(Data['RFM']['F']!=None and limit_values['fmin']!=None and limit_values['fmax']!=None):
                Data['RFM']['F_n']=(Data['RFM']['F']-limit_values['fmin'])/(limit_values['fmax']-limit_values['fmin'])
            if(Data['RFM']['M']!=None and limit_values['mmin']!=None and limit_values['mmax']!=None):
                Data['RFM']['M_n']=(Data['RFM']['M']-limit_values['mmin'])/(limit_values['mmax']-limit_values['mmin'])
            if(Data['CLV']['CV']!=None and limit_values['cvmin']!=None and limit_values['cvmax']!=None):
                Data['CLV']['CV_n']=(Data['CLV']['CV']-limit_values['cvmin'])/(limit_values['cvmax']-limit_values['cvmin'])
            if(Data['CLV']['CLV']!=None and limit_values['clvmin']!=None and limit_values['clvmax']!=None):
                Data['CLV']['CLV_n']=(Data['CLV']['CLV']-limit_values['clvmin'])/(limit_values['clvmax']-limit_values['clvmin'])

            #msj= ("Todo correcto")
            #return Response(status=200,response=msj)
            msj=json.dumps(Data)
            return Response(msj,status=200)

@app.route("/addRFM",methods=['POST'])
def addRFM(body=None):

    print('Entro a addRFM')

    body=request.get_json()

    if body==None:
        msj= "No se enviaron parametros POST, por lo tanto el proceso se detuvo"
        print(msj)
        return Response(status=400,response=msj)
    else:
        if "deltas" in body:
            if(body['deltas']==0 or body['deltas']==1):
                deltas=body['deltas']
            else:
                msj= 'Solo se permite el valor 1 o 0 en la key "deltas"'
                print(msj)
                return Response(status=400,response=msj)    
        else:
            msj= 'No se envió el parametro "deltas", por lo tanto el proceso se detuvo'
            print(msj)
            return Response(status=400,response=msj)
        if "data" in body:
            if(len(body['data'])>0):        
                val=[]
                cont=0
                for row in body['data']:
                    temp=[]
                    ncol=0
                    for column in row:
                        temp.append(row[column])
                        ncol=ncol+1
                    if(ncol>6):
                        msj='Se estan enviando mas columnas de las debidas'
                        return Response(status=400,response=msj)   
                    val.append(temp)
                    cont=cont+1
            else:
                msj='El parametro "data" esta vacio, por lo tanto el proceso se detuvo'
                print(msj)
                return Response(status=400,response=msj)         
        else:
            msj= 'No se envió el parametro "data", por lo tanto el proceso se detuvo'
            print(msj)
            return Response(status=400,response=msj)

        try:
            conector=pymysql.connect(host=host,db=db,user=user_db,passwd=pass_db)
            cursor=conector.cursor()

            cursor.execute(query_fix2)

            if(deltas==0):
                print('se trunco la tabla')
                clear_table="truncate table rfm_in;"
                cursor.execute(clear_table)

            query='insert into rfm_in(Id_cliente,Nombre,Fecha,Vigencia,Last_call,Monto) values(%s,%s,%s,%s,%s,%s)'

            cursor.executemany(query,val)
            conector.commit()
            cursor.close()
            conector.close()
        except pymysql.Error as e:
            msj='Err0r %d: %s' %(e.args[0],e.args[1])
            print(msj)
            return Response(status=400,response=msj)
        else:
            msj='Operacion concluida, se insertaron '+str(cont)+' regitros'
            print(msj)
            return Response(msj,status=200)

@app.route("/addNBO_m",methods=['POST'])
def addNBO_m(body=None):

    print('Entro a addNBO_m')

    body=request.get_json()

    if body==None:
        msj= "No se enviaron parametros POST, por lo tanto el proceso se detuvo"
        print(msj)
        return Response(status=400,response=msj)
    else:
        if "deltas" in body:
            if(body['deltas']==0 or body['deltas']==1):
                deltas=body['deltas']
            else:
                msj= 'Solo se permite el valor 1 o 0 en la key "deltas"'
                print(msj)
                return Response(status=400,response=msj)    
        else:
            msj= 'No se envió el parametro "deltas", por lo tanto el proceso se detuvo'
            print(msj)
            return Response(status=400,response=msj)
        if "data" in body:
            if(len(body['data'])>0):        
                val=[]
                cont=0
                for row in body['data']:
                    temp=[]
                    ncol=0
                    for column in row:
                        temp.append(row[column])
                        ncol=ncol+1
                    if(ncol>14):
                        msj='Se estan enviando mas columnas de las debidas'
                        print(msj)
                        return Response(status=400,response=msj)   
                    val.append(temp)
                    cont=cont+1
            else:
                msj='El parametro "data" esta vacio, por lo tanto el proceso se detuvo'
                print(msj)
                return Response(status=400,response=msj)         
        else:
            msj= 'No se envió el parametro "data", por lo tanto el proceso se detuvo'
            print(msj)
            return Response(status=400,response=msj)

        try:
            conector=pymysql.connect(host=host,db=db,user=user_db,passwd=pass_db)
            cursor=conector.cursor()

            if(deltas==0):
                print('se trunco la tabla')
                clear_table="truncate table nbo_model;"
                cursor.execute(clear_table)

            query="insert into nbo_model(Id_cliente,Macro_sector,Sector,Subsector,Actividad,Ventas,Empleados,Activo_fijo,Potencial,Cheques,Etapa,Subetapa,Monto,Producto) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

            cursor.execute(query_fix2)
            cursor.executemany(query,val)
            conector.commit()
            cursor.close()
            cursor.close()
        except pymysql.Error as e:
            msj="Error %d: %s" %(e.args[0],e.args[1])
            print(msj)
            return Response(status=400,response=msj)
        else:
            msj='Operacion concluida, se insertaron '+str(cont)+' regitros'
            print(msj)
            return Response(status=200,response=msj)        

@app.route("/addNBO",methods=['POST'])
def addNBO(body=None):

    print('Entro a addNBO')

    body=request.get_json()

    if body==None:
        msj= "No se enviaron parametros POST, por lo tanto el proceso se detuvo"
        return Response(status=400,response=msj)
    else:
        if "deltas" in body:
            if(body['deltas']==0 or body['deltas']==1):
                deltas=body['deltas']
            else:
                msj= 'Solo se permite el valor 1 o 0 en la key "deltas"'
                return Response(status=400,response=msj)    
        else:
            msj= 'No se envió el parametro "deltas", por lo tanto el proceso se detuvo'
            return Response(status=400,response=msj)
        if "data" in body:
            if(len(body['data'])>0):        
                val=[]
                cont=0
                for row in body['data']:
                    temp=[]
                    ncol=0
                    for column in row:
                        temp.append(row[column])
                        ncol=ncol+1
                    if(ncol>10):
                        msj='Se estan enviando mas columnas de las debidas'
                        return Response(status=400,response=msj)   
                    val.append(temp)
                    cont=cont+1
            else:
                msj='El parametro "data" esta vacio, por lo tanto el proceso se detuvo'
                return Response(status=400,response=msj)         
        else:
            msj= 'No se envió el parametro "data", por lo tanto el proceso se detuvo'
            return Response(status=400,response=msj)

        try:
            conector=pymysql.connect(host=host,db=db,user=user_db,passwd=pass_db)
            cursor=conector.cursor()

            if(deltas==0):
                print('se trunco la tabla')
                clear_table="truncate table nbo_in;"
                cursor.execute(clear_table)

            query="insert into nbo_in(Id_cliente,Macro_sector,Sector,Subsector,Actividad,Ventas,Empleados,Activo_fijo,Potencial,Cheques) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

            cursor.execute(query_fix2)
            cursor.executemany(query,val)
            conector.commit()
            cursor.close()
            cursor.close()
        except pymysql.Error as e:
            msj="Error %d: %s" %(e.args[0],e.args[1])
            print(msj)
            return Response(status=400,response=msj)
        else:
            msj='Operacion concluida, se insertaron '+str(cont)+' regitros'
            print(msj)
            return Response(status=200,response=msj)        

@app.route("/getInfo",methods=['POST'])
def getInfo(body=None):

    print('Entro a getInfo')

    body=request.get_json()

    query_rfm="select Ejecucion,Id_cliente,Recencia_out,Frecuencia_out,Monto_out,Segmento from rfm_out"
    query_clv="select Ejecucion,Id_cliente,Valor_cliente,Vida_cliente,CLV from clv_out"
    query_nbo="select Ejecucion,Id_cliente,Id_producto,Producto_prob from nbo_out"
    query_acreedor="select Ejecucion,Id_cliente,Acreedor,Acreedor_prob,Monto_predict,Monto_seg from acreedor_out"


    if body!=None:

        if "id" in body:
            query_rfm=query_rfm+" where Id_cliente='%s'" %(body['id'])
            query_clv=query_clv+" where Id_cliente='%s'" %(body['id'])
            query_nbo=query_nbo+" where Id_cliente='%s'" %(body['id'])
            query_acreedor=query_acreedor+" where Id_cliente='%s'" %(body['id'])

            if (('date_start' in body) and ('date_end' in body)):
                query_rfm=query_rfm+" and Ejecucion between '%s' and '%s'" %(body['date_start'],body['date_end'])
                query_clv=query_clv+" and Ejecucion between '%s' and '%s'" %(body['date_start'],body['date_end'])
                query_nbo=query_nbo+" and Ejecucion between '%s' and '%s'" %(body['date_start'],body['date_end'])
                query_acreedor=query_acreedor+" and Ejecucion between '%s' and '%s'" %(body['date_start'],body['date_end'])
        else:
            if (('date_start' in body) and ('date_end' in body)):
                query_rfm=query_rfm+" where Ejecucion between '%s' and '%s'" %(body['date_start'],body['date_end'])
                query_clv=query_clv+" where Ejecucion between '%s' and '%s'" %(body['date_start'],body['date_end'])
                query_nbo=query_nbo+" where Ejecucion between '%s' and '%s'" %(body['date_start'],body['date_end'])
                query_acreedor=query_acreedor+" where Ejecucion between '%s' and '%s'" %(body['date_start'],body['date_end'])
    
    try:
        conector=pymysql.connect(host=host,db=db,user=user_db,passwd=pass_db)
        cursor=conector.cursor()

        cursor.execute(query_rfm)
        res_rfm=cursor.fetchall()

        cursor.execute(query_clv)
        res_clv=cursor.fetchall()

        cursor.execute(query_nbo)
        res_nbo=cursor.fetchall()

        cursor.execute(query_acreedor)
        res_acreedor=cursor.fetchall()

        print('Se realizaron consultas')

    except pymysql.Error as e:
        msj="Error %d: %s" %(e.args[0],e.args[1])
        print(msj)
        return Response(status=400,response=msj)
    else:

        headers_rfm=['Ejecucion','Id_cliente','Recencia_out','Frecuencia_out','Monto_out','Segmento']
        headers_clv=['Ejecucion','Id_cliente','Valor_cliente','Vida_cliente','CLV']
        headers_nbo=['Ejecucion','Id_cliente','Id_producto','Producto_prob']
        headers_acreedor=['Ejecucion','Id_cliente','Acreedor','Acreedor_prob','Monto_predict','Monto_seg']

        #-- TIENEN QUE ESTAR EN EL MISMO ORDEN PARA QUE LA ASIGNACION SEA CORRECTA

        Data={'RFM':{},'CLV':{},'NBO':{},'Credito':{}}
        res=[]
        headers=[]

        res.append(res_rfm)
        res.append(res_clv)
        res.append(res_nbo)
        res.append(res_acreedor)

        headers.append(headers_rfm)
        headers.append(headers_clv)
        headers.append(headers_nbo)
        headers.append(headers_acreedor)

        #-- TIENEN QUE ESTAR EN EL MISMO ORDEN PARA QUE LA ASIGNACION SEA CORRECTA

        for idx,key in enumerate(Data):
            filas=0

            for h in headers[idx]:
                Data[key][h]=[]
            
            if(len(res[idx])>0):
                for r in res[idx]:
                    filas+=1
                    for i in range(len(headers[idx])):
                        Data[key][headers[idx][i]].append(r[i])
            else:
                msj= "No hay registros en 'res %s'." %(str(idx))
                print(msj)

            print('El numero de filas de '+key+' es de:'+str(filas))

        print('Se creó objeto')

        msj=json.dumps(Data,default=str)
        return Response(msj,status=200)

@app.route("/setscheduler",methods=['GET'])
def setscheduler():

    print('Entro a setscheduler')

    try:
        conn=pymysql.connect(host=host, user=user_db, passwd=pass_db, db=db)

        cur=conn.cursor()

        #----Modify-----
        query_extract="select Id_cliente,Nombre,max(Fecha),max(Vigencia),max(Last_call) from rfm_in group by Id_cliente;"
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
        headers=['id','Nombre','Fecha','Vigencia','Last_call']
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

        df1['ts'] = df1.Fecha.values.astype(np.int64) // 10 ** 9
        X=df1[['ts']]

        indexes_empties=X[pd.isnull(X).any(axis=1)].index.tolist()

        X=X.dropna(axis=0,how="any")
        X= X.reset_index(drop=True)
        df1=df1.drop(indexes_empties)
        df1= df1.reset_index(drop=True)

        df1[headers[4]]=df1[headers[4]].fillna(pd.to_datetime(0, unit='s')) #convertir 0 a fecha

        #print(df1.dtypes)

        #-------Carga del modelo

        filename = 'agenda1_knn_1var.sav'

        model = pickle.load(open(filename, 'rb'))

        #---------Prediccion

        print('Pronosticando Fechas....')

        predict_fecha=model.predict(X)

        df1['predict_fecha']=predict_fecha

        df1['predict_fecha']=pd.to_datetime(df1['predict_fecha'], unit='s')

        #print(df1.head())

        print('Se realizara insert')

        conn=pymysql.connect(host=host, user=user_db, passwd=pass_db, db=db)
        cur=conn.cursor()

        cur.execute(query_fix2)

        cont=0
        val=[]
        for index,row in df1.iterrows():
            #query="Update "+table+" set R="+str(int(row['R']))+",F="+str(int(row['F']))+",M="+str(int(row['M']))+",Categoria='"+str(row['Categoria'])+"' where "+id_RFM+"='"+str(row[id_RFM])+"';"
            val.append((row[headers[0]],row[headers[1]],row['predict_fecha'].to_pydatetime(),row[headers[3]],row[headers[4]].to_pydatetime()))
            cont=cont+1

        print('inserts:',cont)

        try:
            query="insert into scheduler_out (Id_cliente,Nombre,Date_predict,Vigencia,Last_call) values (%s,%s,%s,%s,%s)"
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

@app.route("/getscheduler",methods=['GET'])
@app.route("/getscheduler/<producto_get>",methods=['GET'])
def getscheduler(producto_get=None):

    print('Entro a getscheduler')

    if producto_get!=None:
        print('>>Producto_get:',producto_get)

    try:
        conn=pymysql.connect(host=host, user=user_db, passwd=pass_db, db=db)

        cur=conn.cursor()

        #----Modify-----
        query_extract="SELECT max(Ejecucion),Id_cliente,Nombre,Date_predict,Vigencia,Last_call FROM scheduler_out WHERE MONTH(Date_predict) = MONTH(CURRENT_DATE()) and Vigencia < CURDATE() AND (SELECT TIMESTAMPDIFF(DAY,Last_call,CURRENT_DATE()))>15 group by Id_cliente;"
        
        #AND YEAR(Date_predict) = YEAR(CURRENT_DATE());

        query_nbo="select max(Ejecucion),Id_cliente,id_producto,max(Producto_prob) from nbo_out group by Id_cliente;"
        #----Modify-----
        
        cur.execute(query_fix)

        cur.execute(query_extract)

        res = cur.fetchall()

        cur.execute(query_nbo)

        res2 = cur.fetchall()

        cur.close()

        conn.close()

    except pymysql.Error as e:
        msj= ("Error %d: %s" % (e.args[0], e.args[1]))
        print(msj)
        return Response(status=400,response=msj)
    else:
        headers=['id','Nombre','Date_predict','Vigencia','Last_call']
        dataset_dummy={}
        filas=0

        for h in headers:
            dataset_dummy[h]=[]

        if(len(res)>0):
            for r in res:
                filas+=1
                for i in range(1,len(headers)+1):
                    dataset_dummy[headers[i-1]].append(r[i])

        print('El numero de filas de este dataset es de:'+str(filas))

        df1=pd.DataFrame(dataset_dummy)

        print('df1:',df1.shape)

        headers2=['id','Producto_Predict','Nombre_producto']
        dataset_dummy2={}
        filas=0

        productos=get_items()

        for h in headers2:
            dataset_dummy2[h]=[]

        if(len(res2)>0):
            for r in res2:
                filas+=1
                dataset_dummy2[headers2[0]].append(r[1])
                dataset_dummy2[headers2[1]].append(r[2])
                dataset_dummy2[headers2[2]].append(productos[r[2]])

        print('El numero de filas de este dataset2 es de:'+str(filas))

        df2=pd.DataFrame(dataset_dummy2)

        print('df2:',df2.shape)

        df3=df1.merge(df2,how='left',on=['id'])

        print('df3:',df3.shape)

        #print(df3.dtypes)
        if producto_get!=None:

            df4=df3[df3['Producto_Predict']==producto_get]

            print('df4:',df4.shape)

            df5=df4.drop(['Vigencia', 'Last_call'], axis=1)

        else:
            df5=df3.drop(['Vigencia', 'Last_call'], axis=1)

        print('df5:',df5.shape)
        #print(df5.head())   

        msj=df5.to_json(orient='records',date_format='iso')
        #msj=json.dumps(df4,default=str)
        return Response(msj,status=200)

@app.route("/addCalls",methods=['POST'])
def addCalls(body=None):

    print('Entro a addCalls')

    body=request.get_json()

    if body==None:
        msj= "No se enviaron parametros POST, por lo tanto el proceso se detuvo"
        print(msj)
        return Response(status=400,response=msj)
    else:
        if "deltas" in body:
            if(body['deltas']==0 or body['deltas']==1):
                deltas=body['deltas']
            else:
                msj= 'Solo se permite el valor 1 o 0 en la key "deltas"'
                print(msj)
                return Response(status=400,response=msj)    
        else:
            msj= 'No se envió el parametro "deltas", por lo tanto el proceso se detuvo'
            print(msj)
            return Response(status=400,response=msj)
        if "data" in body:
            if(len(body['data'])>0):        
                val=[]
                cont=0
                for row in body['data']:
                    temp=[]
                    ncol=0
                    for column in row:
                        temp.append(row[column])
                        ncol=ncol+1
                    if(ncol>5):
                        msj='Se estan enviando mas columnas de las debidas'
                        return Response(status=400,response=msj)   
                    val.append(temp)
                    cont=cont+1
            else:
                msj='El parametro "data" esta vacio, por lo tanto el proceso se detuvo'
                print(msj)
                return Response(status=400,response=msj)         
        else:
            msj= 'No se envió el parametro "data", por lo tanto el proceso se detuvo'
            print(msj)
            return Response(status=400,response=msj)

        try:
            conector=pymysql.connect(host=host,db=db,user=user_db,passwd=pass_db)
            cursor=conector.cursor()

            cursor.execute(query_fix2)

            if(deltas==0):
                print('se trunco la tabla')
                clear_table="truncate table rfm_in;"
                cursor.execute(clear_table)

            query='insert into calls_in(Id_call,Nombre,Date_end,Id_cliente,Estado) values(%s,%s,%s,%s,%s)'

            cursor.executemany(query,val)
            conector.commit()
            cursor.close()
            conector.close()
        except pymysql.Error as e:
            msj='Err0r %d: %s' %(e.args[0],e.args[1])
            print(msj)
            return Response(status=400,response=msj)
        else:
            msj='Operacion concluida, se insertaron '+str(cont)+' regitros'
            print(msj)
            return Response(msj,status=200)




def get_items():

    print('Entro a get_items')

    try:
        conn=pymysql.connect(host=host, user=user_db, passwd=pass_db, db=db)
        cur=conn.cursor()
        query_fix="SET sql_mode=(SELECT REPLACE(@@sql_mode,'ONLY_FULL_GROUP_BY',''));"
        cur.execute(query_fix)
        query="SELECT MAX(Ejecucion),Id_producto,Descripcion from items group by Id_producto;"
        cur.execute(query)
        res = cur.fetchall()
        res
        headers={}
        for r in res:
            headers[r[1]]=r[2]

        cur.close()

        conn.close()

    except pymysql.Error as e:
        msj= ("Error %d: %s" % (e.args[0], e.args[1]))
        print(msj)
        return Response(status=400,response=msj)
    else:
    
        return headers

def checkBodysetRFM(body):
        c=0
        for i in body:
            if(i!='segmentos' and i!='ponderaciones'):
                msj= "Key:"+i+" incorrecta,las keys deben ser las siguientes:\"segmentos\",\"ponderaciones\""
                print(msj)
                return Response(status=400,response=msj)
            else:
                c=c+1
        if(c!=2):
                msj= "Falta alguna key,las keys deben ser las siguientes:\"segmentos\",\"ponderaciones\""
                print(msj)
                return Response(status=400,response=msj)        

        for i in body['segmentos']:
            for key,val in i.items():
                if(key=='nombre'):
                    if(type(val)!=str):
                        msj= "Uno de los datos de \"Segmentos\" no es \"string\",favor de corregir"
                        print(msj)
                        return Response(status=400,response=msj)
                else:
                    msj= "Key:"+key+" incorrecta en  \"segmentos\",debe ser Key:\"nombre\""
                    print(msj)
                    return Response(status=400,response=msj)

        total=0
        c=0
        for i in body['ponderaciones']:
            for key,val in i.items():
                if(key=='recencia'):
                    if(type(val)!=int):
                        msj= "El dato de \"recencia\" debe ser \"int\" para que la sumatoria de recencia+frecuencia+monto=100,favor de corregir"
                        print(msj)
                        return Response(status=400,response=msj)
                    else:
                        c=c+1
                        total=total+val 
                elif(key=='frecuencia'):
                    if(type(val)!=int):
                        msj= "El dato de \"frecuencia\" debe ser \"int\" para que la sumatoria de recencia+frecuencia+monto=100,favor de corregir"
                        print(msj)
                        return Response(status=400,response=msj)
                    else:
                        c=c+1
                        total=total+val 
                elif(key=='monto'):
                    if(type(val)!=int):
                        msj= "El dato de \"monto\" debe ser \"int\" para que la sumatoria de recencia+frecuencia+monto=100,favor de corregir"
                        print(msj)
                        return Response(status=400,response=msj)
                    else:
                        c=c+1
                        total=total+val 
                else:
                    msj= "Key:"+key+" incorrecta para \"ponderaciones\",las keys deben ser las siguientes, en el siguiente orden: \"recencia\",\"frecuencia\",\"monto\""
                    print(msj)
                    return Response(status=400,response=msj)
        if ((total>100) | (total<100)):
            msj= "La suma de las ponderaciones recencia+frecuencia+monto deber ser 100, no "+str(total)+", favor de corregir"
            print(msj)
            return Response(status=400,response=msj)
        if(c!=3):
            msj= "Falta alguna key,las keys deben ser las siguientes:\"recencia\",\"frecuencia\",\"monto\""
            print(msj)
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

def getlimitvalues():

        qrmax='select max(Recencia_out) from rfm_out;'
        qrmin='select min(Recencia_out) from rfm_out;'
        qfmax='select max(Frecuencia_out) from rfm_out;'
        qfmin='select min(Frecuencia_out) from rfm_out;'
        qmmax='select max(Monto_out) from rfm_out;'
        qmmin='select min(Monto_out) from rfm_out;'
    
        qcvmax='select max(Valor_cliente) from clv_out;'
        qcvmin='select min(Valor_cliente) from clv_out;'
        qclvmax='select max(CLV) from clv_out;'
        qclvmin='select min(CLV) from clv_out;'

        obj={}

        try:
            conn=pymysql.connect(host=host, user=user_db, passwd=pass_db, db=db)

            cur=conn.cursor()

                   
            cur.execute(query_fix)

            cur.execute(qrmax)
            obj['rmax'] = cur.fetchall()[0][0]
            cur.execute(qrmin)
            obj['rmin'] = cur.fetchall()[0][0]

            cur.execute(qfmax)
            obj['fmax'] = cur.fetchall()[0][0]
            cur.execute(qfmin)
            obj['fmin'] = cur.fetchall()[0][0]

            cur.execute(qmmax)
            obj['mmax'] = cur.fetchall()[0][0]
            cur.execute(qmmin)
            obj['mmin'] = cur.fetchall()[0][0]

            cur.execute(qcvmax)
            obj['cvmax']=cur.fetchall()[0][0]
            cur.execute(qcvmin)
            obj['cvmin']=cur.fetchall()[0][0]

            cur.execute(qclvmax)
            obj['clvmax']=cur.fetchall()[0][0]
            cur.execute(qclvmin)
            obj['clvmin']=cur.fetchall()[0][0]

            cur.close()
            conn.close()

        except pymysql.Error as e:
            msj= ("Error %d: %s" % (e.args[0], e.args[1]))
            print(msj)
            return Response(status=400,response=msj)
        else:
            return obj




if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)