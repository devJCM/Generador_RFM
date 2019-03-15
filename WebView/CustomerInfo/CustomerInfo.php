<!DOCTYPE html>
<html>
	<head>
		<script src="https://ajax.googleapis.com/ajax/libs/jquery/3.3.1/jquery.min.js"></script>
		<title>Customer</title>
		<!-- Plotly.js -->
		<script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
        <style type="text/css">
          input {
            display: : inline;
            text-align: center;
            background: #332d4f;
            color: #21ef8b;
            border-color: #21ef8b;
        }
          label{
            margin-left: 10px;
        }
          div.section{
            margin: 20px;
        }
          table, th, td {
            border: 1px solid #21ef8b;
            border-collapse: collapse;
            text-align: center;
        }
        body{
            background: #332d4f;
            color: #21ef8b;
        }      
        </style>
	</head>

	<body>

        <h2>Snapshot de Cliente</h2>

		<div id="plot"><!-- Plotly chart will be drawn inside this DIV --></div>

		<div id='cointainer'>
            <div class="section">
                <h2>Credito</h2>
                <label for="Acreedor">Viable para credito</label>
                <input type="text" id="Acreedor" disabled>
                <label for="Acreedor_prob">Nivel de Confianza</label>
                <input type="text" id="Acreedor_prob" disabled>
            </div> 
			<div class="section">
                <h2>RFM</h2>
				<label for="R">R</label>
				<input type="text" id="R" disabled>
                <label for="F">F</label>
                <input type="text" id="F" disabled>
                <label for="M">M</label>
                <input type="text" id="M" disabled>
                <label for="Segmento">Segmento</label>
                <input type="text" id="Segmento" class="rfm" disabled>
			</div>
            <div class="section">
                <h2>CLV</h2>
                <div>
                    <label for="LifeTime">Tiempo de Vida Promedio de Cliente (Meses)</label>
                    <input type="text" id="LifeTime" disabled>
                </div>
                <br>
                <div>    
                    <label for="CV">Valor de Cliente</label>
                    <input type="text" id="CV" disabled>
                </div>
                <br>
                <div>    
                    <label for="CLV">Customer LifeTime Value</label>
                    <input type="text" id="CLV" disabled>
                </div>
            </div>
            <div class="section">
                <h2>NBO</h2>
                <div>    
                    <label for="Product_Predict">Producto Recomendado</label>
                    <input type="text" id="Product_Predict" disabled>
                </div>
                <div class="section">
                    <table id='productos'>
                        <tr>
                            <th>Producto</th>
                            <th>Nivel de Confianza</th>
                        </tr>
                     </table>
                </div>
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
                  fillcolor: '#rgba(33,239,139,0.7)',
                  line : {color : '21ef8b'}       
				  }
				]
				layout = {
				  polar: {
				    radialaxis: {
				      visible: false,
				      range: [0, 1],
                      color: '#21ef8b' //color de los ejes radiales
				    },
                    bgcolor: 'rgba(0,0,0,0)' //color del grafico
                  },
                  paper_bgcolor: 'rgba(0,0,0,0)', //color del layout
                  font: {size:'20',color: '21ef8b'}
                }

				Plotly.plot("plot", data, layout, {showSendToCloud: false})	
            }

			function fillfields(client_info){

                $('#Acreedor').val(client_info.Acreedor);
                $('#Acreedor_prob').val(client_info.Acreedor_prob+'%');
				$('#R').val(client_info.RFM.R);
                $('#F').val(client_info.RFM.F);
                $('#M').val(client_info.RFM.M);
                $('#Segmento').val(client_info.RFM.Segmento);
                $('#LifeTime').val(client_info.CLV.LifeTime);
                $('#CV').val(client_info.CLV.CV);
                $('#CLV').val(client_info.CLV.CLV);
                $('#Product_Predict').val(client_info.NBO.Product_Predict);

                var numproductos=Object.keys(client_info.NBO).length-1;
                for (var i = 1; i <= numproductos; i++) {
                    $('#productos tr:last').after('<tr><td>Producto '+i+'</td><td>'+client_info.NBO['Producto_'+i]+'%</td></tr>');
                }
    		}

		</script>
	</body>
</html>


