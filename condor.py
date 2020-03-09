import htcondor
import classad
import json
import urllib
import os
import configparser
import io
import time
import datetime
from database import *
from utils import Utils

class Condor():

    def __init__(self):

        try:
            config = configparser.ConfigParser()
            config.read('config.ini')

            self.cluster_name = config.get('condor', 'cluster_name')
            self.condor_scheduler = config.get('condor', 'scheduler')
            self.condor_version = config.get('condor', 'condor_version')

        except Exception as e:
            raise e
            raise SystemExit

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

      if self.condor_version >= '8.8.1':

        try:
            for schedd_ad in htcondor.Collector().locateAll(htcondor.DaemonTypes.Schedd):
                    self.schedd = htcondor.Schedd(schedd_ad)
                    self.jobs += self.schedd.xquery(projection=self.params,
                                              requirements=self.requirements)
        except Exception as e:
          print(str(e))
          raise e
      else:
          condor_q = os.popen("condor_q -l -global")
          ads = classad.parseOldAds(condor_q)
          for ad in ads:
            self.jobs.append(ad)

      self.job_procs = {}
      self.info = {}

      rows = list()

      for job in range(len(self.jobs)):

          process = None
          if 'Args' in self.jobs[job]:
            process = self.jobs[job]['Args'].split(' ')[0]
          else:
            process = ' '
          if 'GlobalJobId' in self.jobs[job]:
            jobid = self.jobs[job]['GlobalJobId']
          else:
            jobid = ' '
          if 'Owner' in self.jobs[job]:
            self.info['owner'] = self.jobs[job]['Owner']
          else:
            self.info['owner'] = ' '

          row = dict({
              'Process': process,
              'Job': jobid,
              'ClusterName': self.cluster_name
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



    def get_history(self, args, cols, limit):

        requirements = Utils.parse_requirements(args)

        if requirements is '':
            requirements = 'JobFinishedHookDone=!=""'

        if cols:
            projection = cols
        else:
            projection = ['Args', 'ClusterId','ProcId','QDate', \
                        'JobStartDate','CompletionDate','JobFinishedHookDone', \
                        'JobStatus','Out','Owner','RemoteHost','RequestCpus', \
                        'RequiresWholeMachine', 'UserLog', 'LastRemoteHost']
        if limit is '':
            limit = False

        rows = list()

        schedd = htcondor.Schedd()
        for job in schedd.history(
            requirements,
            projection,
            limit):

            if len(job):
                rows.append(self.parse_job_to_dict(job))

        return rows


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
            print(self.parse_job_to_dict(job))
            jobs.append(self.parse_job_to_dict(job))


        return dict({
            'success': True,
            'jobs': jobs
        })

    def remove_job(self, clusterId, procId):
        print("Removing Job ClusterId: [%s] ProcId: [%s]" % (clusterId, procId))

        schedd = htcondor.Schedd()

        try:
            schedd.act(htcondor.JobAction.Remove, 'ClusterId==%s && ProcId==%s' % (clusterId, procId))


            job = self.get_job(clusterId, procId, ["ClusterId", "ProcId", "JobStatus"])

            return dict({
                'success': True,
                'job': job
            })
        except:
            return dict({
                'success': False
            })

    def get_job(self, clusterId, procId, projection=[]):

        schedd = htcondor.Schedd()

        requirements = 'ClusterId==%s && ProcId==%s' % (clusterId, procId)

        jobs = list()
        for job in schedd.xquery(requirements=requirements, projection=projection):
            jobs.append(self.parse_job_to_dict(job))

        if len(jobs) == 0:
            # Tenta recuperar o job do historico
            for job in schedd.history(
                requirements,
                projection,
                1):

                if len(job) > 0:
                    return self.parse_job_to_dict(job)
                else:
                    return None

        elif len(jobs) == 1:
            return jobs[0]
        else:
            return jobs

    def parse_job_to_dict(self, job):
        j = dict()

        for key in job.keys():
            j[key] = str(job.get(key))
        try:
            j['JobId'] = j['ClusterId'] + '.' + j['ProcId']
        except:
            j['JobId'] = None

        j['Args'] = j['Args'] if ('Args' in j) else None
        j['ClusterName'] = self.cluster_name
        j['GlobalJobId'] = j['GlobalJobId'] if ('GlobalJobId' in j) else None
        try:
           date = j['JobStartDate']
           j['JobStartDate'] = datetime.datetime.fromtimestamp(int(date)).strftime('%Y-%m-%d %H:%M:%S')
        except:
            pass
        try:
           date = j['QDate']
           j['QDate'] = datetime.datetime.fromtimestamp(int(date)).strftime('%Y-%m-%d %H:%M:%S')
        except:
            pass
        try:
           date = j['CompletionDate']
           if date:
            j['CompletionDate'] = datetime.datetime.fromtimestamp(int(date)).strftime('%Y-%m-%d %H:%M:%S')
        except:
            pass
        try:
           date = j['JobFinishedHookDone']
           j['JobFinishedHookDone'] = datetime.datetime.fromtimestamp(int(date)).strftime('%Y-%m-%d %H:%M:%S')
        except:
            pass
        j['JobStatus'] = j['JobStatus'] if ('JobStatus' in j) else None
        j['Out'] = j['Out'] if ('Out' in j) else None
        j['Owner'] = j['Owner'] if ('Owner' in j) else None
        j['Process'] = j['Process'] if ('Process' in j) else None
        j['ServerTime'] = j['ServerTime'] if ('ServerTime' in j) else None
        j['UserLog'] = j['UserLog'] if ('UserLog' in j) else None
        j['RequiresWholeMachine'] = j['RequiresWholeMachine'] if ('RequiresWholeMachine' in j) else None
        j['LastRemoteHost'] = j['LastRemoteHost'] if ('LastRemoteHost' in j) else None
        return j

    def job_history(self,args,cols,limit,offset):

        search_fields = ['Job', 'ClusterName', 'JobFinishedHookDone', 'JobStartDate', 'Owner']

        if not cols:
            cols = '*'
        else:
            cols = ','.join(map(str, cols))

        Parser = Utils()
        requirements = Parser.parse_requirements(search_fields, **args)

        requirements_sql = requirements.replace('&', ' ' + 'AND' + ' ').replace('|', ' ' + 'OR' + ' ')

        sql = ''

        if requirements:
            sql = 'select {} from condor_history where {} ORDER BY JobFinishedHookDone desc'.format(cols,requirements_sql)

        else:
            sql = 'select {} from condor_history ORDER BY JobFinishedHookDone desc'.format(cols)

        if limit:
            sql += ' limit {}'.format(limit)

        if limit and offset:
            sql += ' offset {}'.format(offset)

        sql_count = 'select count(*) from condor_history'

        cur = get_db().cursor()
        query = dict({
            "data": query_dict(sql),
            "total_count": query_one(sql_count),
        })

        return query

    def update_db(self):
        print ("Updating database")
        cols = list()
        limit = ''
        days_ago = datetime.datetime.now() - datetime.timedelta(days=30)
        args = {'JobFinishedHookDone__gt': str(days_ago.timestamp())}

        jobs = self.get_history(args,cols,limit)
        cur = get_db().cursor()

        for job in jobs:
            djob = self.parse_job_to_dict(job)

            query_insert('INSERT OR REPLACE INTO condor_history (JobId,Args,ClusterName,GlobalJobId,\
            Job,QDate,JobStartDate,CompletionDate,JobFinishedHookDone,\
            JobStatus,Out,Owner,Process,RequestCpus,ServerTime,UserLog, \
            RequiresWholeMachine,LastRemoteHost) \
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)', \
            (djob['JobId'],djob['Args'], \
            djob['ClusterName'],djob['GlobalJobId'],djob['JobId'],djob['QDate'], \
            djob['QDate'],djob['CompletionDate'], djob['JobFinishedHookDone'], \
            djob['JobStatus'],djob['Out'],djob['Owner'],djob['Process'],djob['RequestCpus'], \
            djob['ServerTime'],djob['UserLog'],djob['RequiresWholeMachine'],djob['LastRemoteHost']))

        print ("Done")