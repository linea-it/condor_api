# condor_api


Run PEP8
```
pycodestyle app.py

autopep8 -i *.py
```

Run with gunicorn
```
gunicorn - -bind 186.232.60.33: 8000 - -reload wsgi
```


Exemplo de requisição para submeter um job. 

```
{
	"queues": 5,
	"submit_params": {
		"executable":"/bin/sleep",
		"arguments": "5m"
	}	
}
```