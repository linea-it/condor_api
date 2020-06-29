import htcondor
import classad
import json
import urllib
import os
import configparser
import io
import time
import datetime
import subprocess
from database import *
from utils import Utils


class Condor():
    """ responsible for managing actions with HTCondor """

    def __init__(self):
        try:
            self.config = configparser.ConfigParser()
            self.config.read('config.ini')

            self.cluster_name = self.config.get('condor', 'cluster_name')
            self.condor_scheduler = self.config.get('condor', 'scheduler')
            self.condor_version = self.config.get('condor', 'condor_version')

        except Exception as e:
            raise e

    def get_jobs(self, args, cols):
        """ gets jobs """

        self.default_params = [
            'Cmd', 'Args', 'ClusterId', 'GlobalJobId', 'Job', 'JobStartDate',
            'JobStatus', 'Out', 'Owner', 'RemoteHost', 'RequestCpus',
            'RequiresWholeMachine', 'UserLog'
        ]

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

        self.jobs = list()

        if self.condor_version >= '8.8.1':
            try:
                for schedd_ad in htcondor.Collector().locateAll(htcondor.DaemonTypes.Schedd):
                    self.schedd = htcondor.Schedd(schedd_ad)
                    self.jobs += self.schedd.xquery(projection=self.params, requirements=self.requirements)
            except Exception as e:
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
            process = self.jobs[job].get('Args', '').split(' ').pop()
            jobid = self.jobs[job].get('GlobalJobId', '')
            self.info['owner'] = self.jobs[job].get('Owner', '')

            portal = None
            if self.jobs[job].get('Cmd', '').find('pypeline/bin/run.py') > -1:
                portal = self.jobs[job].get('Owner', '')

            row = dict({
                'Process': process,
                'Job': jobid,
                'Portal': portal,
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
        search_fields = ['Job', 'ClusterName', 'JobFinishedHookDone', 'JobStartDate', 'Owner']

        Parser = Utils()
        requirements = Parser.parse_requirements(search_fields, **args)

        if requirements is '':
            requirements = 'JobFinishedHookDone=!=""'

        if cols:
            projection = cols
        else:
            projection = ['Cmd', 'Args', 'ClusterId','ProcId','QDate', \
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

    def get_old_history(self, args, cols, limit):
        search_fields = ['Job', 'ClusterName', 'JobFinishedHookDone', 'JobStartDate', 'Owner']

        Parser = Utils()
        requirements = Parser.parse_requirements(search_fields, **args)

        if requirements is '':
            requirements = 'JobFinishedHookDone=!=""'

        if cols:
            projection = cols
        else:
            projection = ['Cmd', 'Args', 'ClusterId','ProcId','QDate', \
                        'JobStartDate','CompletionDate','JobFinishedHookDone', \
                        'JobStatus','Out','Owner','RemoteHost','RequestCpus', \
                        'RequiresWholeMachine', 'UserLog', 'LastRemoteHost']
        if limit is '':
            limit = False

        rows = list()

        command = 'condor_history -l -constraint \'({})\''.format(requirements)

        p = subprocess.Popen(command,shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding='utf-8')

        condor_history, err = p.communicate()

        ads = classad.parseOldAds(condor_history)

        for ad in ads:
            rows.append(ad)

        return rows

    def get_remote_history(self, args, cols, limit):
        search_fields = ['Job', 'ClusterName', 'JobFinishedHookDone', 'JobStartDate', 'Owner']

        Parser = Utils()
        requirements = Parser.parse_requirements(search_fields, **args)

        if cols:
            projection = cols
        else:
            projection = ['Cmd', 'Args', 'ClusterId','ProcId','QDate', \
                        'JobStartDate','CompletionDate','JobFinishedHookDone', \
                        'JobStatus','Out','Owner','RemoteHost','RequestCpus', \
                        'RequiresWholeMachine', 'UserLog', 'LastRemoteHost']
        if limit is '':
            limit = False

        rows = list()

        for submitter in self.config.sections():
            if submitter != 'condor' and self.config[submitter]['Remote'] == 'Yes' :
                scheduler = self.config[submitter]['Scheduler']
                user = self.config[submitter]['user']
                key = self.config[submitter]['Key']
                port = self.config[submitter]['Port']

                command = 'ssh {} -p {} -l {} -i {} \"condor_history -l -constraint \'({})\'\"'.format(scheduler,port,user,key,requirements)
                #command = 'ssh {} -p {} -l {} -i {} \"condor_history -l -match 100"'.format(scheduler,port,user,key,requirements)

                p = subprocess.Popen(command,shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding='utf-8')

                condor_history, err = p.communicate()

                ads = classad.parseOldAds(condor_history)

                for ad in ads:
                    rows.append(ad)

        return rows


    def get_cluster_history(self, args, cols, limit):
        search_fields = ['Job', 'ClusterName', 'JobFinishedHookDone', 'JobStartDate', 'Owner']

        Parser = Utils()
        requirements = Parser.parse_requirements(search_fields, **args)

        if cols:
            projection = cols
        else:
            projection = [
                'Cmd', 'Args', 'ClusterId','ProcId', 'QDate', 'JobStartDate',
                'CompletionDate', 'JobFinishedHookDone', 'JobStatus', 'Out',
                'Owner', 'RemoteHost', 'RequestCpus', 'RequiresWholeMachine',
                'UserLog', 'LastRemoteHost'
            ]

        if limit is '':
            limit = False

        rows = list()

        if self.condor_version >= '8.8.1':
            history = self.get_history(args,cols,limit)
        else:
            history = self.get_old_history(args,cols,limit)

        for job in history:
            rows.append(job)

        remote_history = self.get_remote_history(args,cols,limit)

        for job in remote_history:
            rows.append(job)

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

        parent_id = None

        if j['JobId'] and j['JobId'].find('.0') < 0:
            parent_id = j['ClusterId']

        j['ParentId'] = parent_id

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

        j['Portal'] = None
        if j['Cmd'] and j['Cmd'].find('pypeline/bin/run.py') > -1:
            j['Portal'] = j['Owner']

        return j

    def job_history(self, args, cols, limit, offset):

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
            sql = 'select {} from condor_history where {}'.format(cols,requirements_sql)
            sql_count = 'select count(*) from condor_history where {}'.format(requirements_sql)
        else:
            sql = 'select {} from condor_history'.format(cols)
            sql_count = 'select count(*) from condor_history'

        print("---->")
        print(sql)

        if('ordering' in args):
            if(args['ordering'][0] == '-'):

                sql = sql + ' ORDER BY "' + args['ordering'][1:] + '" DESC'
            else:
                sql = sql + ' ORDER BY "%' + args['ordering'] + '%" ASC'
        else:
            sql = sql + ' ORDER BY JobFinishedHookDone DESC'


        if limit:
            sql += ' limit {}'.format(limit)

        if limit and offset:
            sql += ' offset {}'.format(offset)


        cur = get_db().cursor()
        query = dict({
            "data": query_dict(sql),
            "total_count": query_one(sql_count),
        })

        return query


    def job_parent_history(self, args, cols, limit, offset):

        search_fields = ['Job', 'ClusterName', 'JobFinishedHookDone', 'JobStartDate', 'Owner']

        if not cols:
            cols = '*'
        else:
            cols = ','.join(map(str, cols))

        Parser = Utils()
        requirements = Parser.parse_requirements(search_fields, **args)

        if requirements:
            requirements = "AND {}".format(requirements.replace('&', ' AND ').replace('|', ' OR '))

        sql = 'select {} from condor_history where ParentId is null {}'.format(cols, requirements)

        if('ordering' in args):
            if(args['ordering'][0] == '-'):
                sql = sql + ' ORDER BY "' + args['ordering'][1:] + '" DESC'
            else:
                sql = sql + ' ORDER BY "%' + args['ordering'] + '%" ASC'
        else:
            sql = sql + ' ORDER BY JobFinishedHookDone DESC'

        if limit:
            sql += ' limit {}'.format(limit)

        if limit and offset:
            sql += ' offset {}'.format(offset)

        sql_count = 'select count(*) from condor_history where ParentId is null {}'.format(requirements)

        return dict({
            "data": query_dict(sql),
            "total_count": query_one(sql_count),
        })


    def update_execution_time(self):

        try:
            jobs = self.job_history({}, None, None, None)

            for job in jobs['data']:

                djob = dict(job)

                start_date = datetime.datetime.strptime(djob['JobStartDate'], '%Y-%m-%d %H:%M:%S')
                end_date = datetime.datetime.strptime(djob['JobFinishedHookDone'], '%Y-%m-%d %H:%M:%S')

                execution_time = end_date - start_date

                print('Execution time [%s]: %s' % (type(execution_time), execution_time))

                execution_time = execution_time.total_seconds()

                if djob['JobID']:
                    query_insert('UPDATE condor_history SET ExecutionTime = %f WHERE JobId = %f' % (execution_time, djob['JobID']))
        except:
            query_insert('ALTER TABLE condor_history ADD ExecutionTime REAL')
            self.update_execution_time()

        return 'Done'


    def top_users_history(self, args, limit):

        Parser = Utils()
        requirements = Parser.parse_requirements([], **args)

        requirements_sql = requirements.replace('&', ' ' + 'AND' + ' ').replace('|', ' ' + 'OR' + ' ')

        sql = ''

        if requirements:
            sql = 'SELECT Owner, SUM(ExecutionTime) as TotalExecutionTime from condor_history WHERE {} GROUP BY Owner ORDER BY SUM(ExecutionTime) DESC'.format(requirements_sql)
        else:
            sql = 'SELECT Owner, SUM(ExecutionTime) as TotalExecutionTime from condor_history GROUP BY Owner ORDER BY SUM(ExecutionTime) DESC'

        if limit:
            sql += ' limit {}'.format(limit)


        # Descomente a linha abaixo na primeira vez que ligar e for chamar o endpoint "top_users".
        # Ira, reatroativamente, preencher todas as colunas execution time de jobs passados.
        # self.update_execution_time()

        return query_dict(sql)


    def update_db(self):
        print ("Updating database")
        cols = list()
        limit = ''
        days_ago = datetime.datetime.now() - datetime.timedelta(days=10)
        args = {'JobFinishedHookDone__gt': str(days_ago.timestamp())}

        jobs = self.get_cluster_history(args,cols,limit)
        cur = get_db().cursor()

        for job in jobs:
            djob = self.parse_job_to_dict(job)

            start_date = datetime.datetime.strptime(djob['QDate'], '%Y-%m-%d %H:%M:%S')
            end_date = datetime.datetime.strptime(djob['JobFinishedHookDone'], '%Y-%m-%d %H:%M:%S')

            execution_time = end_date - start_date

            execution_time = execution_time.total_seconds()

            query_insert('INSERT OR REPLACE INTO condor_history (JobId, Args, ClusterId, ClusterName, ParentId, GlobalJobId,\
            Job,QDate,JobStartDate,CompletionDate,JobFinishedHookDone,\
            JobStatus,Out,Owner,Process,RequestCpus,ServerTime,UserLog, \
            RequiresWholeMachine,LastRemoteHost,ExecutionTime, Portal) \
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)', \
            (djob['JobId'],djob['Args'], djob['ClusterId'], \
            djob['ClusterName'], djob['ParentId'], djob['GlobalJobId'],djob['JobId'],djob['QDate'], \
            djob['QDate'],djob['CompletionDate'], djob['JobFinishedHookDone'], \
            djob['JobStatus'],djob['Out'],djob['Owner'],djob['Process'],djob['RequestCpus'], \
            djob['ServerTime'],djob['UserLog'],djob['RequiresWholeMachine'],djob['LastRemoteHost'], execution_time, djob['Portal']))

        print("Done")
