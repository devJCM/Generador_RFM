<?php

    class Conexion{

        public function get_conexion(){
            //$host=$_POST['db'][0]['host'];
            //$db=$_POST['db'][1]['db'];
            //$user=$_POST['db'][2]['user'];
            //$passbd=$_POST['db'][3]['password'];

            $host='localhost';
            $db='unifin';
            $user='root';
            $passbd='';
            $con=new PDO("mysql:host=$host;dbname=$db;",$user,$passbd);
            return $con;
        }
    }

    class Funciones{

        public function sendRFM(){
            $con=new Conexion();
            $conexion=$con->get_conexion();

            $query_fix="SET sql_mode=(SELECT REPLACE(@@sql_mode,'ONLY_FULL_GROUP_BY',''));";

            $query_rfm='SELECT a.id,op.date_entered,op2.monto_c FROM accounts a, opportunities op,opportunities_cstm op2,accounts_opportunities ao WHERE a.id=ao.account_id AND op.id=ao.opportunity_id and op2.id_c=ao.opportunity_id and op.deleted=0;';

            //$statement=$conexion->prepare($query_fix);
            //$statement->execute();
            $statement=$conexion->prepare($query_rfm);
            $statement->execute();
            while($renglon=$statement->fetch(PDO::FETCH_ASSOC)){
                $arr[]=$renglon; 
            }

            $obj = new  stdClass;
            $obj->deltas=0;
            $obj->data=$arr;

            $url = 'http://localhost:5000/addRFM';
             
            //inicializamos el objeto CUrl
            $ch = curl_init($url);

            $jsonDataEncoded = json_encode($obj);
             
            //Indicamos que nuestra petición sera Post
            curl_setopt($ch, CURLOPT_POST, 1);
             
            //para que la peticion no imprima el resultado como un echo comun, y podamos manipularlo
            curl_setopt($ch, CURLOPT_RETURNTRANSFER, 1);
             
            //Adjuntamos el json a nuestra petición
            curl_setopt($ch, CURLOPT_POSTFIELDS, $jsonDataEncoded);
             
            //Agregamos los encabezados del contenido
            curl_setopt($ch, CURLOPT_HTTPHEADER, array('Content-Type: application/json'));
             
            //ignorar el certificado, servidor de desarrollo
            //utilicen estas dos lineas si su petición es tipo https y estan en servidor de desarrollo
            //curl_setopt($ch, CURLOPT_SSL_VERIFYPEER, FALSE);
            //curl_setopt($process, CURLOPT_SSL_VERIFYHOST, FALSE);
             
            //Ejecutamos la petición
            $result = curl_exec($ch);
            curl_close($ch); 

            return $result;
        }

        public function sendNBO_m(){

            $con=new Conexion();
            $conexion=$con->get_conexion();

            $query_fix="SET sql_mode=(SELECT REPLACE(@@sql_mode,'ONLY_FULL_GROUP_BY',''));";

            $query_nbo_m="select a.id_c,a.tct_macro_sector_ddw_c,a.sectoreconomico_c,a.subsectoreconomico_c,a.actividadeconomica_c,a.ventas_anuales_c,
                        a.empleados_c,a.activo_fijo_c,a.potencial_cuenta_c,a.tct_prom_cheques_cur_c,
                        op.tct_etapa_ddw_c,op.estatus_c,op.monto_c,op.tipo_producto_c
                        FROM accounts_cstm a, opportunities_cstm op,accounts_opportunities ao WHERE a.id_c=ao.account_id AND op.id_c=ao.opportunity_id;";

            //$statement=$conexion->prepare($query_fix);
            //$statement->execute();
            $statement=$conexion->prepare($query_nbo_m);
            $statement->execute();
            while ($row=$statement->fetch(PDO::FETCH_ASSOC)) {
                $arr[]=$row;
            }

            $obj = new  stdClass;
            $obj->deltas=0;
            $obj->data=$arr;

            $url = 'http://localhost:5000/addNBO_m';
             
            //inicializamos el objeto CUrl
            $ch = curl_init($url);

            $jsonDataEncoded = json_encode($arr);
             
            //Indicamos que nuestra petición sera Post
            curl_setopt($ch, CURLOPT_POST, 1);
             
            //para que la peticion no imprima el resultado como un echo comun, y podamos manipularlo
            curl_setopt($ch, CURLOPT_RETURNTRANSFER, 1);
             
            //Adjuntamos el json a nuestra petición
            curl_setopt($ch, CURLOPT_POSTFIELDS, $jsonDataEncoded);
             
            //Agregamos los encabezados del contenido
            curl_setopt($ch, CURLOPT_HTTPHEADER, array('Content-Type: application/json'));
             
            //ignorar el certificado, servidor de desarrollo
            //utilicen estas dos lineas si su petición es tipo https y estan en servidor de desarrollo
            //curl_setopt($ch, CURLOPT_SSL_VERIFYPEER, FALSE);
            //curl_setopt($process, CURLOPT_SSL_VERIFYHOST, FALSE);
             
            //Ejecutamos la petición
            $result = curl_exec($ch);
            curl_close($ch); 

            return $result;
        }

        public function sendNBO(){

            $con=new Conexion();
            $conexion=$con->get_conexion();

            $query_fix="SET sql_mode=(SELECT REPLACE(@@sql_mode,'ONLY_FULL_GROUP_BY',''));";

            $query_nbo_m="select a.id_c,a.tct_macro_sector_ddw_c,a.sectoreconomico_c,a.subsectoreconomico_c,a.actividadeconomica_c,a.ventas_anuales_c,
                        a.empleados_c,a.activo_fijo_c,a.potencial_cuenta_c,a.tct_prom_cheques_cur_c
                        FROM accounts_cstm a, opportunities_cstm op,accounts_opportunities ao WHERE a.id_c=ao.account_id AND op.id_c=ao.opportunity_id;";

            //$statement=$conexion->prepare($query_fix);
            //$statement->execute();
            $statement=$conexion->prepare($query_nbo_m);
            $statement->execute();
            while ($row=$statement->fetch(PDO::FETCH_ASSOC)) {
                $arr[]=$row;
            }

            $obj = new  stdClass;
            $obj->deltas=0;
            $obj->data=$arr;

            $url = 'http://localhost:5000/addNBO';
             
            //inicializamos el objeto CUrl
            $ch = curl_init($url);

            $jsonDataEncoded = json_encode($arr);
             
            //Indicamos que nuestra petición sera Post
            curl_setopt($ch, CURLOPT_POST, 1);
             
            //para que la peticion no imprima el resultado como un echo comun, y podamos manipularlo
            curl_setopt($ch, CURLOPT_RETURNTRANSFER, 1);
             
            //Adjuntamos el json a nuestra petición
            curl_setopt($ch, CURLOPT_POSTFIELDS, $jsonDataEncoded);
             
            //Agregamos los encabezados del contenido
            curl_setopt($ch, CURLOPT_HTTPHEADER, array('Content-Type: application/json'));
             
            //ignorar el certificado, servidor de desarrollo
            //utilicen estas dos lineas si su petición es tipo https y estan en servidor de desarrollo
            //curl_setopt($ch, CURLOPT_SSL_VERIFYPEER, FALSE);
            //curl_setopt($process, CURLOPT_SSL_VERIFYHOST, FALSE);
             
            //Ejecutamos la petición
            $result = curl_exec($ch);
            curl_close($ch); 

            return $result;
        }

    }   

    $x=new Funciones();
    //$y=$x->sendRFM();
    //$y=$x->sendNBO_m();
    $y=$x->sendRFM();

    //echo json_encode($y);
    echo $y;
?>