$(document).ready(function () {

	$.fn.dataTable.ext.errMode = 'none';

	var jobs_url = "http://loginicx.linea.gov.br:5000/jobs"
	var nodes_url = "http://loginicx.linea.gov.br:5000/nodes"

	var call = function () {
		var jobs = $.ajax({
			dataType: "json",
			url: jobs_url,
			async: true,
			success: function (result) { return result }	

		});
	
		var nodes = $.ajax({
			dataType: "json",
			url: nodes_url,
			async: true,
			success: function (result) { return result }
		});
	
		$.when(jobs, nodes).done(function (j, n) {
			
			var Count = j[0].map( function(user) {

			       });

		});

	}
	call();
	setInterval(call, 5000);
});



