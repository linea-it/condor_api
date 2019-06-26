import htcondor
import classad
import json
import urllib

class Condor():

    def get_jobs(self,args,cols):

      self.default_params = ['Args', 'GlobalJobId','JobStartDate','JobStatus','Out','Owner','RemoteHost','RequestCpus','RequiresWholeMachine', 'UserLog']
      self.params = self.default_params + str(cols).split(',')
      self.requirements = ''

      if len(args):

        t = 0
        for arg in args:
          self.requirements += arg + '==' + str(args[arg])
          if t <= len(args) - 2:
            self.requirements += '&&'
            t = t + 1
      else:
        self.requirements = None

      self.jobs = []

      try:
          for schedd_ad in htcondor.Collector().locateAll(
                  htcondor.DaemonTypes.Schedd):
              self.schedd = htcondor.Schedd(schedd_ad)
              self.jobs += self.schedd.xquery(projection=self.params,
                                              requirements=self.requirements)

      except Exception as e:
          print("An exception occurred") + str(e)
          raise e

      self.job_procs = {}
      self.info = {}

      rows = list()

      for job in range(len(self.jobs)):

          process = None
          process = self.jobs[job]['Args'].split(' ')[0] if self.jobs[job].get('Args') else ''
          jobid = self.jobs[job]['GlobalJobId']
          self.info['owner'] = self.jobs[job]['Owner']

          row = dict({
              'Process': process,
              'Job': jobid,
          })

          for info in self.jobs[job]:

              row[info] = str(self.jobs[job][info])

          rows.append(row)

      return rows

    def get_nodes(self, match, *args):

        self.default_params = [
            'UtsnameNodename', 'Name', 'State', 'Memory','Disk',
            'TotalCpus','RemoteOwner', 'LoadAvg', 'Activity', 'JobStarts', 'RecentJobStarts','DiskUsage']
        self.params = self.default_params + args[0]
        self.requirements = str(match).replace(
            "=", "==").replace(',', '&&') if match else None
        self.rows = list()

        coll = htcondor.Collector()
        query = coll.query(htcondor.AdTypes.Startd, projection=self.params)

        for node in range(len(query)):
            row = dict()

            for key in query[node].keys():
                row[key] = str(query[node].get(key))

            self.rows.append(row)

        return self.rows

    def submit_job(self, params):
        # TODO tratar execessao nesta funcao.
        print("Params: ", params)

        n_queues = params.get("queues", 1)

        submit_param = params.get("submit_params", None)
        if submit_param is None:
            return dict({
                'success': False,
                'message': "NENHUM PARAMETRO DE SUBMISSAO FOI ENVIADO."
            })

        schedd = htcondor.Schedd()
        sub = htcondor.Submit(submit_param)

        with schedd.transaction() as txn:
            clusterId = sub.queue(txn, n_queues)

        print("clusterId:")
        print(clusterId)

        # Listar os jobs
        jobs = list()
        for job in schedd.xquery(
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
