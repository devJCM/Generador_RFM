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

        public function getinfoclient($id){
            $con=new Conexion();
            $conexion=$con->get_conexion();

            if(!isset($conexion)){
                return 'Hubo un problema en la conexion';
            }

            $obj = new  stdClass;

            $obj->limitvalues=$this->getlimitvalues();

            $query_fix="SET sql_mode=(SELECT REPLACE(@@sql_mode,'ONLY_FULL_GROUP_BY',''));";

            $qrfm="select max(Ejecucion),Recencia_out,Frecuencia_out,Monto_out,Segmento from rfm_out where Id_cliente='$id';";
            $qclv="select max(Ejecucion),Valor_cliente,Vida_cliente,CLV from clv_out where Id_cliente='".$id."';";
            $qacreedor="select max(Ejecucion),Acreedor,Acreedor_prob from acreedor_out where Id_cliente='".$id."';";
            $qnbo="select max(Ejecucion),Producto_Predict,Producto_1,Producto_2,Producto_3,Producto_4,Producto_5 from nbo_out where Id_cliente='".$id."';";

            $statement=$conexion->prepare($query_fix);
            if(!isset($statement)){
                return 'Hubo un problema en la consulta';
            }

            $statement->execute();

            $statement=$conexion->prepare($qrfm);
            $statement->execute();
            $x=$statement->fetch();
            $obj->RFM->R=$x[1];
            $obj->RFM->F=$x[2];
            $obj->RFM->M=$x[3];
            $obj->RFM->Segmento=$x[4];

            $statement=$conexion->prepare($query_fix);
            $statement->execute();

            $statement=$conexion->prepare($qclv);
            $statement->execute();
            $x=$statement->fetch();
            $obj->CLV->CV=$x[1];
            $obj->CLV->LifeTime=$x[2];
            $obj->CLV->CLV=$x[3];

            $statement=$conexion->prepare($qacreedor);
            $statement->execute();
            $x=$statement->fetch();
            $obj->Acreedor=$x[1];
            $obj->Acreedor_prob=$x[2]*100;

            $statement=$conexion->prepare($qnbo);
            $statement->execute();
            $x=$statement->fetch();

            $obj->NBO->Product_Predict=$x[1];
            $numidx=(count($x)-2)/2;
            for ($i=2; $i <=$numidx ; $i++) { 
                $temp='Producto_'.($i-1);
                $obj->NBO->$temp=$x[$i]*100;
            }
            
            /*$obj->NBO->Producto_1=$x[2]*100;
            $obj->NBO->Producto_2=$x[3]*100;
            $obj->NBO->Producto_3=$x[4]*100;
            $obj->NBO->Producto_4=$x[5]*100;
            $obj->NBO->Producto_5=$x[6]*100;*/


            $obj->RFM->R_n=(string)( ($obj->RFM->R /*- $obj->limitvalues->rmin*/ ) / ( $obj->limitvalues->rmax /*- $obj->limitvalues->rmin*/ ) );
            $obj->RFM->F_n=(string)( ($obj->RFM->F /*- $obj->limitvalues->fmin*/ ) / ( $obj->limitvalues->fmax /*- $obj->limitvalues->fmin*/ ) );
            $obj->RFM->M_n=(string)( ($obj->RFM->M /*- $obj->limitvalues->mmin*/ ) / ( $obj->limitvalues->mmax /*- $obj->limitvalues->mmin*/ ) );

            $obj->CLV->CV_n=(string)( ($obj->CLV->CV /*- $obj->limitvalues->cvmin*/ ) / ( $obj->limitvalues->cvmax /*- $obj->limitvalues->cvmin*/ ) );
            $obj->CLV->CLV_n=(string)( ($obj->CLV->CLV /*- $obj->limitvalues->clvmin*/ ) / ( $obj->limitvalues->clvmax /*- $obj->limitvalues->clvmin*/ ) );

            return $obj;

        }
    }   

    if(isset($_POST['id'])){
        $id=$_POST['id'];
    }else{
        echo json_encode("No se envio parametro POST");
    }

    $x=new Funciones();
    $y=$x->getinfoclient($id);

    echo json_encode($y);

?>