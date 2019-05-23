import htcondor
import classad
import json


class Condor():

    def __init__(self):

        self.jobs = []

        try:
            for schedd_ad in htcondor.Collector().locateAll(
                    htcondor.DaemonTypes.Schedd):
                self.schedd = htcondor.Schedd(schedd_ad)
                self.jobs += self.schedd.xquery()

        except Exception as e:
            print("An exception occurred")
            raise e

    def list_parms(self):

        keys = []

        if len(self.jobs):
            for key in self.jobs[0].keys():

                keys.append(key)

            return ({'Parms': keys, 'Desc': 'Parameters list'})

        else:
            return False

    def get_jobs(self):

        self.job_procs = {}
        self.info = {}

        rows = list()

        for job in range(len(self.jobs)):

            del self.jobs[job]['Environment']

            process = str(self.jobs[job]).split('/0000')[1].split('/')[0]
            jobid = self.jobs[job]['GlobalJobId']
            self.info['owner'] = self.jobs[job]['Owner']
            print(self.jobs[job])

            row = dict({
                'process': process,
                'jobid': jobid,
            })

            for info in self.jobs[job]:

                row[info] = str(self.jobs[job][info])

            rows.append(row)

        return rows

    def get_nodes(self):
        self.rows = list()
        coll = htcondor.Collector()
        query = coll.query(htcondor.AdTypes.Startd)

        for node in range(len(query)):
            row = dict()

            for key in query[node].keys():
                row[key] = str(query[node].get(key))

                print(key)

            self.rows.append(row)

        return self.rows

    def submit_job(self, params):

        print("Params: ", params)

        n_queues = params.get("queues", 1)

        submit_param = params.get("submit_params", None)
        if submit_param is None:
            return dict({
                'success': False,
                'message': "NENHUM PARAMETRO DE SUBMISSAO FOI ENVIADO."
            })

        print(self.schedd)

        sub = htcondor.Submit(submit_param)

        with self.schedd.transaction() as txn:
            clusterId = sub.queue(txn, n_queues)

        print("clusterId:")
        print(clusterId)

        # Listar os jobs
        # TODO: pode usar a funcao get_jobs do valter.
        jobs = list()
        for job in self.schedd.xquery(
                projection=['ClusterId', 'ProcId', 'JobStatus'],
                requirements='ClusterId==%s' % clusterId):
            jobs.append(self.parse_job_to_dict(job))
            print(self.parse_job_to_dict(job))

        return dict({
            'success': True,
            'jobs': jobs
        })

    def parse_job_to_dict(self, job):
        j = dict()

        for key in job.keys():
            j[key] = str(job.get(key))

        return j
