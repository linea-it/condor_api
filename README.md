# condor_api

# Retornando dados de JOBS


* **URL**
/jobs
* **Método:**
GET
* **Principais dados:**

`Args [str]` - Argumentos passados na execução do Job

`GlobalJobId [str]` - Identificador do Job

`JobStartDate [int]` - Data de início do job (Unix Timestamp)

`JobStatus [int]` - Status do Job

			1 - Idle
			2 - Running
			3 - Removed
			4 - Completed
			5 - Held
			6 - Transferring Output	

`Out [str]` - Arquivo de saída.

`Owner [str]` - Usuário que iniciou o job.

`Process [int]` - Número do processo iniciado pelo portal.

`Owner [str]` - Usuário que iniciou o job.
	
`RemoteHost [str]` - Em qual nó o job está rodando.

 `RequestCpus [int]` - CPUS destinadas ao JOB.
 
`RequiresWholeMachine [bool]` - Requer ou não toda a máquina.

`UserLog [str]` - Arquivo de log 

Uma lista completa com todos os parâmetros disponíveis pode ser encontrada em: https://research.cs.wisc.edu/htcondor/manual/v7.6/10_Appendix_A.html#sec:Job-ClassAd-Attributes


* **Requisição de exemplo:**

```
 $.ajax({
    url: "/jobs?Owner="adriano.pieres",
    dataType: "json",
    type : "GET",
    success : function(r) {
      console.log(r);
    }
  });
 ```
Resultado

```
[
  {
    "Args": "10034349 1.10 modstar_make_cats modstar_make_cats", 
    "GlobalJobId": "loginicx.ib0.cm.linea.gov.br#10526.0#1559145335", 
    "Job": "loginicx.ib0.cm.linea.gov.br#10526.0#1559145335", 
    "JobStartDate": "1559145354", 
    "JobStatus": "2", 
    "Out": "/mnt/scratch/users/adriano.pieres/master_des/000010034349/condor/modstar_make_cats_modstar_make_cats_1.10.out", 
    "Owner": "adriano.pieres", 
    "Process": "10034349", 
    "RemoteHost": "slot1@apl01.ib0.cm.linea.gov.br", 
    "RequestCpus": "1", 
    "RequiresWholeMachine": "True", 
    "ServerTime": "1559152454", 
    "UserLog": "/mnt/scratch/users/adriano.pieres/master_des/000010034349/condor/modstar_make_cats_modstar_make_cats_1.10.log"
  },
]


```

# Retornando dados dos nós


* **URL**
/nodes

* **Método:**
GET

* **Principais dados**

`Activity [str]` - Atividade

			Idle - Nenhum job rodando
			Busy - Rodando jobs
			Suspended - Job foi suspendido
			Killing - Matando job
			Benchmarking - Gravando informações de benchmark


`DetectedMemory [int]` - Memória detectada

`JobStarts [int]` - Número de jobs que já rodaram no nó

`LoadAvg [float]` - Load do nó

`Name [str]` - Nome do nó

`RecentJobStarts [int]` - Jobs iniciados recentemente

`State [str]` - Estado atual do nó


	Owner - Nó indisponível para o condor
	Unclaimed - Máquina disponível para rodar jobs
	Matched - Máquina com recursos disponíveis mas não escalada
	Claimed

`TotalCpus [float]` - Total de cores do nó

`UtsnameNodename [str]` - Nome do nó (físico)

* **Requisição de exemplo:**

```
 $.ajax({
    url: "/nodes",
    dataType: "json",
    type : "GET",
    success : function(r) {
      console.log(r);
    }
  });
 ```
Resultado

```
[
  {
    "Activity": "Idle", 
    "Disk": "16190476", 
    "JobStarts": "52", 
    "LoadAvg": "0.0", 
    "Memory": "2290", 
    "Name": "slot35@apl02.ib0.cm.linea.gov.br", 
    "RecentJobStarts": "0", 
    "State": "Unclaimed", 
    "TotalCpus": "56.0", 
    "UtsnameNodename": "apl02"
  }, 
  {
    "Activity": "Idle", 
    "Disk": "16191911", 
    "JobStarts": "42", 
    "LoadAvg": "0.0", 
    "Memory": "2290", 
    "Name": "slot14@apl07.ib0.cm.linea.gov.br", 
    "RecentJobStarts": "0", 
    "State": "Unclaimed", 
    "TotalCpus": "56.0", 
    "UtsnameNodename": "apl07"
  }, 
]
```




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
