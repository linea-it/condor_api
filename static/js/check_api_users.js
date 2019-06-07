$(document).ready(function () {

	//$.fn.dataTable.ext.errMode = 'none';

	var user_stats_url = "http://loginicx.linea.gov.br:5000/users_stats"
	var nodes_url = "http://loginicx.linea.gov.br:5000/nodes"

	var call = function () {
		var users_stats = $.ajax({
			dataType: "json",
			url: user_stats_url,
			async: true,
			success: function (result) { return result }	

		});
	
		var nodes = $.ajax({
			dataType: "json",
			url: nodes_url,
			async: true,
			success: function (result) { return result }
		});
	
	$.when(users_stats, nodes).done(function (u, n) {

                if ( ! $.fn.DataTable.isDataTable( '#users_table' ) ) {
                        var table = $('#users_table').DataTable({
                               "data":u[0],

                               "aoColumns": [
                                        { "mData": "Owner" },
                                        { "mData": "PortalProcesses" },
										{ "mData": "ManualJobs"},
										{ "mData": "Cluster"},
										{ "mData": "Running"},
										{ "mData": "Waiting"},
										{ "mData": function(mdata){return mdata.ClusterUtilization + "%"}}

                                ],
                                "bPaginate": true,  
                                "bLengthChange": false,  
                                "bFilter": true, 
                                "bSort": true, 
                                "bInfo": true,
								"lengthMenu": [[10, 25, 50, -1], [10, 25, 50, "All"]],
								"order": [[ 6, "desc" ]]

                        });

                }else{
			       $('#users_table').DataTable().clear();
        		   $('#users_table').DataTable().rows.add( u[0] ).draw();
                }

		});
		//$('#iframe').location.reload();

	}
	call();
	setInterval(call, 5000);
});



