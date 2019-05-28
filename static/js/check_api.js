$( document ).ready(function() {

var evtSource = new EventSource("http://loginicx.linea.gov.br:/5000/jobs");
evtSource.onmessage = function(data) {
//$.get( "http://loginicx.linea.gov.br:5000/jobs", function( data ) {

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
  	
	$.each(users, function(i, prop) {
    		$('#users_table').append('<tr><td>'+users[i]+'</td></tr'); 
  	});

}
});



