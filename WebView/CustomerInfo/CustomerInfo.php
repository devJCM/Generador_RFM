<!DOCTYPE html>
<html>
	<head>
        <title>Customer</title>
        <meta name="viewport" content="width=device-width, initial-scale=1, shrink-to-fit=no">
        <!-- Bootstrap -->
        <link rel="stylesheet" href="https://stackpath.bootstrapcdn.com/bootstrap/4.3.1/css/bootstrap.min.css" integrity="sha384-ggOyR0iXCbMQv3Xipma34MD+dH/1fQ784/j6cY/iJTQUOhcWr7x9JvoRxT2MZw1T" crossorigin="anonymous">
        <!-- Plotly.js -->
        <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
        <!-- Jquery -->
		<script src="https://ajax.googleapis.com/ajax/libs/jquery/3.3.1/jquery.min.js"></script>
        <!--fonts-->
        <!--<link href='http://fonts.googleapis.com/css?family=Oswald:400,300,700' rel='stylesheet' type='text/css'>-->
        <link rel="stylesheet" type="text/css" href="//fonts.googleapis.com/css?family=Open+Sans" />
        <style type="text/css">

            input {
                /* text-align: center; */
                color: #222 !important;
                background-color: rgba(0,0,0,0) !important;
                border-color: rgba(0,0,0,0) !important;
                font-size: 14px !important;
            }
            table, th, td {
                text-align: center;
            }    
            html {
              font-size: 12px;
            }
            body { 
                font-family: 'Open Sans',"Helvetica Neue" !important;
                color: #222;
            }
            h4{ 
                font-family: 'inherit'  !important; 
                margin-left: 10px;
            }
            label{
                color: #a5a5a5;
                margin-left: 10px;
                font-size: 12px !important;
            } 

        </style>
	</head>

	<body>
    	<div class="container">
            <h4>Snapshot de Cliente</h4>
            <div id="plot"><!-- Plotly chart will be drawn inside this DIV --></div>
            <h4>Credito</h4>
            <div class="row">
                <div class="col">
                    <label for="Acreedor">Viable para credito</label>
                      <input type="text" id="Acreedor" class="form-control" aria-describedby="basic-addon3" disabled="">
                </div>    
                <div class="col">    
                    <label for="Acreedor_prob">Nivel de Confianza</label>
                    <input type="text" id="Acreedor_prob" class="form-control" aria-describedby="basic-addon3" disabled>
                </div>    
            </div>
            <br> 
            <h4>RFM</h4>
			<div class="row">
                <div class="col-sm-3"> 
				    <label for="R">R</label>
				    <input type="text" id="R" class="form-control" aria-describedby="basic-addon3" disabled>
                </div>    
                <div class="col-sm-3"> 
                    <label for="F">F</label>
                    <input type="text" id="F" class="form-control" aria-describedby="basic-addon3" disabled>
                </div>    
                <div class="col-sm-3"> 
                    <label for="M">M</label>
                    <input type="text" id="M" class="form-control" aria-describedby="basic-addon3" disabled>
                </div>  
                <div class="col-sm-3"> 
                    <label for="Segmento">Segmento</label>
                    <input type="text" id="Segmento" class="form-control" aria-describedby="basic-addon3" disabled>
                </div>    
			</div>
            <br>
            <h4>CLV</h4>
            <div class="row">
                <div class="col-sm-4">
                    <label for="LifeTime">Tiempo de Vida Promedio de Cliente (Meses)</label>
                    <input type="text" id="LifeTime" class="form-control" aria-describedby="basic-addon3" disabled>
                </div>
                <div class="col-sm-4">   
                    <label for="CV">Valor de Cliente</label>
                    <input type="text" id="CV" class="form-control" aria-describedby="basic-addon3" disabled>
                </div>
                <div class="col-sm-4">  
                    <label for="CLV">Customer LifeTime Value</label>
                    <input type="text" id="CLV" class="form-control" aria-describedby="basic-addon3" disabled>
                </div>
            </div>
            <br>
            <h4>NBO</h4>
            <div class="row">  
                <div class="col-6">
                    <label for="Product_Predict">Producto Recomendado</label>
                    <input type="text" id="Product_Predict" class="form-control" aria-describedby="basic-addon3" disabled>
                </div>
            </div>
            <br>
            <div class="row">
                <table id='productos' class="table">
                    <thread>
                        <tr>
                            <th scope="col">#</th>
                            <th scope="col">Producto</th>
                            <th scope="col">Nivel de Confianza</th>
                        </tr>
                    </thread>
                    <tbody>
                        <tr></tr>
                    </tbody>    
                </table>
            </div>
 		</div>	
        <script>
			var id=getGetVariable("id");
			//var id="377a50ed-a657-db1c-c858-591b269b1e6c";
			var url='http://'+window.location.host+'/CustomerInfo/getCustomerInfo.php';
			var post={"id":id,};

			$.ajax({
			  type: "POST",
			  url: url,
			  data: post,
			  dataType:'json',
			  success:function(data){

                console.log(data);
				createplot(data);
            	fillfields(data);

			  },
			  error:function(){
				console.log('error');
			  },
			});

			function getGetVariable(variable){
				       var query = window.location.search.substring(1);
				       var vars = query.split("&");
				       for (var i=0;i<vars.length;i++) {
				               var pair = vars[i].split("=");
				               if(pair[0] == variable){
				               	return pair[1];
				               }
				       }
				       return(false);
			}

			function createplot(client_info){

				var CLV=parseFloat(client_info.CLV.CLV_n);
				var CV=parseFloat(client_info.CLV.CV_n);
				var R=parseFloat(client_info.RFM.R_n);
				var F=parseFloat(client_info.RFM.F_n);
				var M=parseFloat(client_info.RFM.M_n);

				data = [
				  {
				  type: 'scatterpolar',
                  theta: ['CLV', 'Client Value','Recencia','Frecuencia','Monto','CLV'],
				  r: [CLV, CV, R, F, M,CLV],
				  name: 'Group B',
				  hoverinfo: 'theta',
                  fill: 'toself',
                  fillcolor: '#rgba(22, 108, 227, 0.54)',
                  line : {color : '#166ce3'}       
				  }
				]
				layout = {
				  polar: {
				    radialaxis: {
				      visible: false,
				      range: [0, 1],
                      color: '#166ce3' //color de los ejes radiales
				    },
                    bgcolor: 'rgba(0,0,0,0)' //color del grafico
                  },
                  paper_bgcolor: 'rgba(0,0,0,0)', //color del layout
                  //font: {size:'20',color: '21ef8b'}
                }

				Plotly.plot("plot", data, layout,{responsive: true})	
            }

			function fillfields(client_info){
                var cv=parseFloat(client_info.CLV.CV);
                var clv=parseFloat(client_info.CLV.CLV);

                $('#Acreedor').val(client_info.Acreedor);
                $('#Acreedor_prob').val(client_info.Acreedor_prob+'%');
				$('#R').val(client_info.RFM.R);
                $('#F').val(client_info.RFM.F);
                $('#M').val(client_info.RFM.M);
                $('#Segmento').val(client_info.RFM.Segmento);
                $('#LifeTime').val(client_info.CLV.LifeTime);
                $('#CV').val('$ '+cv.toFixed(4));
                $('#CLV').val('$ '+clv.toFixed(4));
                $('#Product_Predict').val(client_info.NBO.Product_Predict);

                var numproductos=Object.keys(client_info.NBO).length-1;
                for (var i = 1; i <= numproductos; i++) {
                    var temp=parseFloat(client_info.NBO['Producto_'+i]);

                    $('#productos tr:last').after('<tr><th scope="row">'+i+'</th><td>Producto '+i+'</td><td>'+temp.toFixed(2)+'%</td></tr>');
                }
    		}

		</script>
	</body>
</html>


