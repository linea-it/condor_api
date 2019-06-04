$( document ).ready(function() {

	$.fn.dataTable.ext.errMode = 'none';
	var call = function(){ $.get( "http://loginicx.linea.gov.br:5000/jobs", function( data ) {

		$('#jobs_total').text(data.length);

		var status = 0
		var jobs_running = $.each(data, function(i, v) {
    		if (data[i].JobStatus == 2) {
         	status = status + 1
    		}
		})
        	$('#jobs_running').text(status);
	

        	var users = [];
        	$.each(data, function(i, v) {
        		if (!users.includes(data[i].Owner)) {
	   			users.push(data[i].Owner)
			}
        	});
        	$('#users').text(users.length);
  	
                if ( ! $.fn.DataTable.isDataTable( '#jobs_table' ) ) {
                        var table = $('#jobs_table').DataTable({
                                "data":data,

                               "aoColumns": [
                                        { "mData": "Process" },
                                        { "mData": "Owner" },
                                        { "mData": function(mdata){ 

						if (mdata.JobStatus == 2){
							return "Running"
						}else{
							return "Unknown"
						}

					 } },
                                        { "mData": "RemoteHost"},
                                ],
                                "bPaginate": true,  
                                "bLengthChange": false,  
                                "bFilter": true, 
                                "bSort": true, 
                                "bInfo": true,
				"lengthMenu": [[10, 25, 50, -1], [10, 25, 50, "All"]]

                        });

                }else{
			       $('#jobs_table').DataTable().clear();
        		       $('#jobs_table').DataTable().rows.add( data ).draw();
                }

	});

	$.get( "http://loginicx.linea.gov.br:5000/nodes", function( data ) {

		nodes = []
                $.each(data, function(i, v) {
                        if (!nodes.includes(data[i].UtsnameNodename)) {
                                nodes.push(data[i].UtsnameNodename)
                        }
                });
	
		$('#servers').text(nodes.length);	

		if ( ! $.fn.DataTable.isDataTable( '#nodes_table' ) ) {
			var table = $('#nodes_table').DataTable({
    				"data":data,

 			       "aoColumns": [
        				{ "mData": "Name" },
        				{ "mData": "Activity" },
					{ "mData": "LoadAvg" },
					{ "mData": "TotalCpus" },
					{ "mData": "RecentJobStarts" },
					{ "mData": "Memory" },
					{ "mData": "Disk" },
    				],
    				"bPaginate": true,
    				"bLengthChange": false, 
    				"bFilter": true, 
    				"bSort": true,
				"ordering": true, 
    				"bInfo": true,
    				"order": [[ 2, "desc" ]]	
			});

		}else{
                               $('#nodes_table').DataTable().clear();
                               $('#nodes_table').DataTable().rows.add( data ).draw();		
		}

	})
     }
	call();
	setInterval(call, 5000);
});



