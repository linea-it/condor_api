import htcondor
import classad
import os
import datetime
import configparser

JOB_STATUS = {
    "0": "Unexpanded", "1": "Idle",
    "2": "Running", "3": "Removed",
    "4": "Completed", "5": "Held",
    "6": "Submission_err"
}

CONVERT_DATE = [
    'JobStartDate', 'JobCurrentStartDate',
    'QDate', 'JobFinishedHookDone' ]


class Condor():
    """ Responsible for managing actions with HTCondor """


    def __init__(self):
        self.config = configparser.ConfigParser()
        self.config.read('condor.ini')

        self.required_columns = [
            'Cmd', 'Args', 'ClusterId', 'GlobalJobId', 'JobStatus', 'RemoteHost',
            'JobStartDate', 'ProcId', 'LastRemoteHost', 'JobFinishedHookDone',
            'Owner', 'AppType', 'AppUser', 'AppId', 'AppName', 'AppModule',
            'RequiresWholeMachine'
        ]

        self.cluster_info = self.__get_cluster_info()


    def get_sections(self):
        """ Gets sessions info by config.ini """

        sections = dict()

        for section in self.config.sections():
            scheduler = self.config.get(section, "scheduler").split(".")
            scheduler.reverse()
            scheduler = scheduler.pop()
            sections[section] = {
                "ClusterName": self.config.get(section, "cluster_name"),
                "Scheduler": scheduler
            }

        return sections


    def get_history_by_section(self, section, cols=list()):
        """ Gets history by section """

        cols = list(set(self.required_columns + cols))

        if section == 'main':
            match = self.config.get('main', 'max_entries')

            jobs = dict()

            if self.config.get('main', 'condor_version') >= '8.8.0':
                schedd = htcondor.Schedd()
                ads = schedd.history(
                    requirements='JobFinishedHookDone=!=""',
                    projection=cols,
                    match=int(match)
                )
            else:
                cmd_initial = "condor_history -backwards -match {}".format(match)
                cmd = self.__get_cmd(cmd_initial, cols)
                ads = self.__run(cmd)
        else:
            # get remote history
            cmd_initial = "condor_history -backwards"
            cmd = self.__get_remote_cmd(cmd_initial, section, cols, history=True)
            ads = self.__run(cmd)

        return self.__group_jobs(ads, section)


    def group_by_key(self, sections, key):
        """ Group jobs by key """

        data = dict()

        for section in sections:
            jobs = sections[section].get('data', [])
            for job in jobs:
                value = job.get(key, "-")
                if not value in data:
                    data[value] = list()

                job_list = data[value]
                job_list.append(job)

        return data


    def get_history_jobs_by_cluster_id(self, section, cluster_id, cols=[]):
        """ Gets history jobs by Cluster Id """

        cols = list(set(self.required_columns + cols))

        if section == 'main':
            if self.config.get('main', 'condor_version') >= '8.8.0':
                schedd = htcondor.Schedd()
                requirements = 'ClusterId=={}'.format(cluster_id)
                ads = schedd.history(requirements, cols)
            else:
                cmd_initial = "condor_history {}".format(cluster_id)
                cmd = self.__get_cmd(cmd_initial, cols)
                ads = self.__run(cmd)
        else:
            # get remote history
            cmd_initial = "condor_history {}".format(cluster_id)
            cmd = self.__get_remote_cmd(cmd_initial, section, cols, history=True)
            ads = self.__run(cmd)

        return self.__group_jobs(ads, section)


    def get_running_by_section(self, section, cols=list()):
        """ Gets jobs running by section """

        cols = list(set(self.required_columns + cols))
        cmd_initial = "condor_q -l"

        if section == 'main':
            ads = list()
            if self.config.get('main', 'condor_version') >= '8.8.0':
                for schedd_ad in htcondor.Collector().locateAll(htcondor.DaemonTypes.Schedd):
                    schedd = htcondor.Schedd(schedd_ad)
                    ads += schedd.xquery(projection=cols)
            else:
                cmd = self.__get_cmd(cmd_initial, cols)
                ads = self.__run(cmd)
        else:
            # get remote
            cmd = self.__get_remote_cmd(cmd_initial, section, cols, history=False)
            ads = self.__run(cmd)

        return self.__group_jobs(ads, section)


    def submit_job(self, params):
        """ Submit job """

        # TODO tratar excessao nesta funcao
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
            jobs.append(self.__parser_job(dict(job)))

        return dict({
            'success': True,
            'jobs': jobs
        })


    def remove_job(self, clusterId, procId):
        """ Removes job by ClusterId and ProcId """

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
        """ Gets job """

        schedd = htcondor.Schedd()
        requirements = 'ClusterId==%s && ProcId==%s' % (clusterId, procId)

        jobs = list()
        for job in schedd.xquery(requirements=requirements, projection=projection):
            jobs.append(self.__parser_job(job))

        if len(jobs) == 0:
            # Tenta recuperar o job do historico
            for job in schedd.history(requirements, projection, 1):
                if len(job) > 0:
                    return self.__parser_job(job)
                else:
                    return None

        elif len(jobs) == 1:
            return jobs[0]
        else:
            return jobs


    def get_nodes(self, match, *args):
        """ Gets nodes info """

        params = [
            'UtsnameNodename', 'Name', 'State', 'Memory','Disk',
            'TotalCpus','RemoteOwner', 'LoadAvg', 'Activity',
            'JobStarts', 'RecentJobStarts','DiskUsage'
        ]

        if args:
            params += args[0]

        requirements = str(match).replace(
            "=", "==").replace(',', '&&') if match else None
        rows = list()

        coll = htcondor.Collector()
        query = coll.query(htcondor.AdTypes.Startd, projection=params)

        for node in range(len(query)):
            row = dict()

            for key in query[node].keys():
                row[key] = str(query[node].get(key))

            rows.append(row)

        return rows


    def top_users_history(self, sections):
        """ Gets top users info from history """

        clusters = dict()

        for section in sections:
            parents_job = sections[section]
            for parent_job in parents_job.get("data", []):
                cluster = parent_job.get("ClusterName")
                section = parent_job.get("Section")
                if not cluster in clusters:
                    clusters[cluster] = {
                        "MaxEntries": self.config.get(section, "max_entries"),
                        "Users": list()
                    }
                users = clusters[cluster]["Users"]

                for job in parent_job.get("Jobs"):
                    username = job.get("User")
                    has_user = list(filter(lambda x: x.get("User", "") == username, users))

                    if not has_user:
                        user = {"User":username, "TotalExecutionTime": 0}
                        users.append(user)
                    else:
                        user = has_user.pop()

                    user["TotalExecutionTime"] += job.get("ExecutionTime")

        return clusters


    def users_running(self, sections):
        """ Gets users info from jobs running """

        clusters = dict()

        for section in sections:
            parents_job = sections[section]
            nodes = self.cluster_info.get(section)
            for parent_job in parents_job.get("data", []):
                cluster = parent_job.get("ClusterName")
                section = parent_job.get("Section")
                if not cluster in clusters:
                    clusters[cluster] = {
                        "Cluster": cluster,
                        "Users": list()
                    }
                users = clusters[cluster]["Users"]
                for job in parent_job.get("Jobs"):
                    username = job.get("User")
                    has_user = list(filter(lambda x: x.get("User", "") == username, users))
                    if not has_user:
                        user = {
                            'User': username, 'PortalJobs': 0, 'Processes': list(),
                            'ManualJobs': 0, 'Cluster': job.get('ClusterName'),
                            'Waiting': 0, 'Running': 0, 'Submitter': job.get('Submitter'),
                            'Cores': 0, 'CoresPercentage': 0
                        }
                        users.append(user)
                    else:
                        user = has_user.pop()

                    appid = job.get('ProcessId', job.get('ClusterId'))
                    if not appid in user['Processes']:
                        user['Processes'].append(appid)

                    if job.get("JobStatus") == "Running":
                        user["Running"] += 1
                        if 'RemoteHost' in job and 'RequiresWholeMachine' in job:
                            host = job.get("RemoteHost")
                            user["Cores"] += int(nodes.get(host).get('TotalCpus'))
                        else:
                            user["Cores"] += 1

                        div = len(nodes) / 100
                        user['CoresPercentage'] = div * user['Cores'] / 100

                    elif job.get("JobStatus") == "Idle":
                        user["Waiting"] += 1
                    else:
                        print("Warning: unmonitored {} status".format(job.get("JobStatus")))

                    if job.get("ProcessId", None):
                        user["PortalJobs"] += 1
                    else:
                        user["ManualJobs"] += 1

        return clusters


    def __group_jobs(self, jobs, section):
        """ Groups jobs by ClusterId and DES Science portal processes """

        cluster_name = self.config.get(section, 'cluster_name')
        cluster_dict, process_dict = dict(), dict()

        for job in jobs:
            job = self.__parser_job(dict(job))

            # filtering identical attributes by group
            job_attrs = {
                "ClusterName": cluster_name,
                "Section": section,
                "Submitter": job.get("Submitter"),
                "AppType": job.get("AppType"),
                "AppName": job.get("AppName"),
                "User": job.get("User"),
                "Cmd": job.get("Cmd")
            }

            job.update(job_attrs)
            job_attrs["Jobs"] = list()

            if job.get('ProcessId'):
                group = job.get('ProcessId')
                if not group in process_dict:
                    job_attrs.update({
                        "Portal": job.get('Owner', ''),
                        "ProcessId": group
                    })
                    process_dict[group] = job_attrs
                process_dict[group]["Jobs"].append(job)
            else:
                group = job.get("ClusterId")
                if not group in cluster_dict:
                    job_attrs.update({
                        "ClusterId": group
                    })
                    cluster_dict[group] = job_attrs
                cluster_dict[group]["Jobs"].append(job)

        job_list = list()

        for key in cluster_dict.keys():
            job_list.append(cluster_dict[key])

        for key in process_dict.keys():
            job_list.append(process_dict[key])

        return job_list


    def __get_cmd(self, cmd_initial, cols):
        """ Gets HTCondor command """

        count = len(cols)
        cols_str = str()

        for idx, arg in enumerate(cols):
            if count == idx + 1:
                cols_str += "-format '{}=\"%s\"\n\n' {} ".format(arg, arg)
            else:
                cols_str += "-format '{}=\"%s\"\n' {} ".format(arg, arg)

        return '{} {}'.format(cmd_initial, cols_str)


    def __get_remote_jobs(self, cmd_initial, cols, history=False):
        """ Get remote jobs """

        sections = list(filter(lambda x: x.find("-node") > 0, self.config.sections()))
        jobs = dict()

        for section in sections:
            cmd = self.__get_remote_cmd(cmd_initial, section, cols, history)
            ads = self.__run(cmd)
            jobs[section] = self.__group_jobs(ads, section)

        return jobs


    def __get_remote_cmd(self, cmd_initial, section, cols, history=False):
        """ Gets HTCondor remote command """

        scheduler = self.config.get(section, "scheduler")
        user = self.config.get(section, "user")
        port = self.config.get(section, "port")
        key = self.config.get(section, "key")

        cmd_ini = cmd_initial
        if history:
            match = self.config.get(section, 'max_entries')
            cmd_ini = "{} -match {}".format(cmd_initial, match)

        cmdh = self.__get_cmd(cmd_ini, cols)

        cmd = 'ssh {} -p {} -o "StrictHostKeyChecking no" -l {} -i {} "{}"'.format(scheduler, port, user, key, cmdh)
        return cmd.replace('"%s"', '\\"%s\\"')


    def __run(self, cmd):
        """ Runs HTCondor command """

        outputs = os.popen(cmd)
        return classad.parseOldAds(outputs)


    def __parser_job(self, job):
        """ Handles job info """

        job["AppType"] = job.get("AppType", "-")
        job["AppName"] = job.get("AppName", "-")
        job["AppModule"] = job.get("AppModule", "-")
        job["ProcessId"] = job.get("AppId", None)
        job["User"] = job.get("AppUser", job.get("Owner", None))

        if job.get('Cmd', '').find('pypeline/bin/run.py') > -1:
            job["AppType"] = "DES Science"

            if not job.get("ProcessId"):
                proc_args = job.get('Args', '').split(' ')
                proc_args.reverse()
                job["ProcessId"] = proc_args.pop()

        submitter = job.get('GlobalJobId', '').split('.')
        submitter.reverse()
        submitter = submitter.pop()
        job["Submitter"] = submitter

        job["JobStatus"] = JOB_STATUS.get(str(job.get("JobStatus", "")), "-")

        for _date in CONVERT_DATE:
            if _date in job and job.get(_date, None):
                job[_date] = datetime.datetime.fromtimestamp(
                    int(job.get(_date))
                ).strftime('%Y-%m-%d %H:%M:%S')

        job['ExecutionTime'] = 0

        if 'JobStartDate' in job and 'JobFinishedHookDone' in job:
            start_date = datetime.datetime.strptime(job['JobStartDate'], '%Y-%m-%d %H:%M:%S')
            end_date = datetime.datetime.strptime(job['JobFinishedHookDone'], '%Y-%m-%d %H:%M:%S')
            execution_time = end_date - start_date
            job['ExecutionTime'] = execution_time.total_seconds()

        return job


    def __get_cluster_info(self):
        """ """

        sections = dict()

        for section in self.config.sections():
            sections[section] = self.__get_condor_status(section)

        return sections


    def __get_condor_status(self, section):
        """ """

        cols = ['Name', 'Memory','Disk', 'TotalCpus']

        cmd_initial = "condor_status"

        if section == 'main':
            ads = list()
            if self.config.get('main', 'condor_version') >= '8.8.0':
                coll = htcondor.Collector()
                ads = coll.query(htcondor.AdTypes.Startd, projection=cols)
            else:
                cmd = self.__get_cmd(cmd_initial, cols)
                ads = self.__run(cmd)
        else:
            # get remote
            cmd = self.__get_remote_cmd(cmd_initial, section, cols, history=False)
            ads = self.__run(cmd)

        nodes_dict = dict()
        for item in ads:
            node = dict(item)
            name = node.pop("Name")
            nodes_dict[name] = node

        return nodes_dict
