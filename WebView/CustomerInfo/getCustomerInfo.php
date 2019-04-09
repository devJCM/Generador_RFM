<?php

    class Conexion{

        public function get_conexion(){
            //$host=$_POST['db'][0]['host'];
            //$db=$_POST['db'][1]['db'];
            //$user=$_POST['db'][2]['user'];
            //$passbd=$_POST['db'][3]['password'];

            $host='localhost';
            $db='CustomerInfo';
            $user='root';
            $passbd='';
            $con=new PDO("mysql:host=$host;dbname=$db;",$user,$passbd);
            return $con;
        }
    }

    class Funciones{

        public function getinfoclient($id){

            $url = 'http://localhost:5000/getCustomerInfo/'.$id;
             
            //inicializamos el objeto CUrl
            $ch = curl_init($url);

            curl_setopt($ch, CURLOPT_RETURNTRANSFER, 1);

            //Ejecutamos la petición
            $result = curl_exec($ch);
            curl_close($ch); 

            return $result;

        }
    }   

    if(isset($_GET['id'])){
        $id=$_GET['id'];
    }else{
        echo json_encode("No se envio parametro POST");
    }

    $x=new Funciones();
    $y=$x->getinfoclient($id);

    //echo json_encode($y);
    echo $y;
?>