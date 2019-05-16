import htcondor
import classad
import json


class Condor():

    def __init__(self):

        self.jobs = []

        try:
            for schedd_ad in htcondor.Collector().locateAll(htcondor.DaemonTypes.Schedd):
                self.schedd = htcondor.Schedd(schedd_ad)
                self.jobs += self.schedd.xquery()

        except:
            print("An exception occurred")

    def list_parms(self):

        keys = []

        if len(self.jobs):
            for key in self.jobs[0].keys():

                keys.append(key)

            return ({'Parms': keys, 'Desc': 'Parameters list'})

        else:

            return false

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

        nodes = list()
        coll = htcondor.Collector()
        query = coll.query(htcondor.AdTypes.Startd, projection=[
                           'UtsnameNodename', 'Name', 'CpuBusy', 'TotalLoadAvg', 'State', 'MyType'])

        for node in range(len(query)):

            for key in query[node].keys():

                nodes.append({key: query[node].get(key)})

        print(nodes)

    def submit_job(self, params):

        print"Params: ", params

        return dict({
            'success': True,
            'hello': "Teste"
        })
