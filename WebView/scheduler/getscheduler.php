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

        public function getlimitvalues(){
            $con=new Conexion();
            $conexion=$con->get_conexion();

            $obj = new  stdClass;

            $query_fix="SET sql_mode=(SELECT REPLACE(@@sql_mode,'ONLY_FULL_GROUP_BY',''));";

            $qrmax='select max(Recencia_out) from rfm_out;';
            $qrmin='select min(Recencia_out) from rfm_out;';
            $qfmax='select max(Frecuencia_out) from rfm_out;';
            $qfmin='select min(Frecuencia_out) from rfm_out;';
            $qmmax='select max(Monto_out) from rfm_out;';
            $qmmin='select min(Monto_out) from rfm_out;';
        
            $qcvmax='select max(Valor_cliente) from clv_out;';
            $qcvmin='select min(Valor_cliente) from clv_out;';
            $qclvmax='select max(CLV) from clv_out;';
            $qclvmin='select min(CLV) from clv_out;';

            $statement=$conexion->prepare($qrmax);
            $statement->execute();
            $obj->rmax=$statement->fetch()[0];
            $statement=$conexion->prepare($qrmin);
            $statement->execute();
            $obj->rmin=$statement->fetch()[0];

            $statement=$conexion->prepare($qfmax);
            $statement->execute();
            $obj->fmax=$statement->fetch()[0];
            $statement=$conexion->prepare($qfmin);
            $statement->execute();
            $obj->fmin=$statement->fetch()[0];

            $statement=$conexion->prepare($qmmax);
            $statement->execute();
            $obj->mmax=$statement->fetch()[0];
            $statement=$conexion->prepare($qmmin);
            $statement->execute();
            $obj->mmin=$statement->fetch()[0];

            $statement=$conexion->prepare($qcvmax);
            $statement->execute();
            $obj->cvmax=$statement->fetch()[0];
            $statement=$conexion->prepare($qcvmin);
            $statement->execute();
            $obj->cvmin=$statement->fetch()[0];

            $statement=$conexion->prepare($qclvmax);
            $statement->execute();
            $obj->clvmax=$statement->fetch()[0];
            $statement=$conexion->prepare($qclvmin);
            $statement->execute();
            $obj->clvmin=$statement->fetch()[0];
                 
            
            return $obj;
        }

        public function getscheduler($producto){
            if($producto!=null){
                $url = 'http://localhost:5000/getscheduler/'.$producto;
            }else{
                $url = 'http://localhost:5000/getscheduler';
            }    
             
            //inicializamos el objeto CUrl
            $ch = curl_init($url);

            curl_setopt($ch, CURLOPT_RETURNTRANSFER, 1);

            //Ejecutamos la petición
            $result = curl_exec($ch);
            curl_close($ch); 

            return $result;
        }
    }   

    if(isset($_GET['producto'])){
        $producto=$_GET['producto'];
    }else{
        $producto=null;
    }

    $x=new Funciones();
    $y=$x->getscheduler($producto);

    //echo json_encode($y);
    echo $y;
?>