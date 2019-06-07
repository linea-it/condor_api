from condor import Condor
from flask import Flask
from flask import render_template
from flask import request
from flask import jsonify

application = Flask(__name__)


@application.route('/')
def index():

    condor_m = Condor()

    return render_template('index.html')


@application.route('/user')
def user():

    condor_m = Condor()

    return render_template('user.html')


@application.route('/submit_job', methods=['POST'])
def submit_job():

    condor_m = Condor()
    result = condor_m.submit_job(request.json)

    response = jsonify(result)

    return response


@application.route('/jobs', methods=['GET'])
def jobs():
    cols = list()
    args = dict()

    if len(request.args):

        args = request.args.to_dict()
        if args.has_key('cols'):
		args.pop('cols')

    if request.args.get('cols'):

        cols = request.args.get('cols')

    condor_m = Condor()

    response = jsonify(condor_m.get_jobs(args,cols))

    return response

    
@application.route('/users_stats', methods=['GET'])
def get_users_stats():

    condor_m = Condor()

    jobs = condor_m.get_jobs({},[])

    users = list()
    rows = list()

    for j in jobs:

        if j['Owner'] not in users:

            users.append(j['Owner'])


    for user in users:

        userjobs = condor_m.get_jobs({'Owner': '"'+user+'"'},[])
        nodes = condor_m.get_nodes('',[])

        total_nodes = len(nodes)

        owner = user
        processes = list()
        portal_jobs = 0
        manual_jobs = 0
        cluster = 'ICEX'
        user_jobs_running = 0
        user_jobs_idle = 0
        cluster_utilization = 0
        cores = int()
        userslots = list()

        for j in userjobs:
        
            job  = j['Process'].split('100')

            if job[0]:

               manual_jobs += 1

            else:
                
                portal_jobs += 1
                
                if j['Process'] not in processes:

                    processes.append(j['Process'])

            if j['JobStatus'] == "1":

                user_jobs_idle += 1

            if j['JobStatus'] == "2":

                user_jobs_running += 1
                
            for n in nodes:

                if j['JobStatus'] == "2":

                    if j.has_key('RemoteHost') and j['RemoteHost'] == n['Name']:

                        if j.has_key('RequiresWholeMachine'):

                            cores += int(round(float(n['TotalCpus'])))
                        else:
                            cores += 1

        div = total_nodes / 100
        total = div * cores / 100
        
        rows.append({'Owner': owner, 'PortalProcesses': len(processes), 
        'ManualJobs': manual_jobs, 
        'Cluster': cluster, 
        'Waiting': user_jobs_idle,
        'Running': user_jobs_running,'ClusterUtilization': total})


    return jsonify(rows)
    
    
@application.route('/nodes', methods=['GET'])
def nodes():

    args = []
    match = myString = ",".join(request.args.getlist('match'))

    if len(request.args):
        args = request.args.keys()
        if 'match' in args:
            args.remove('match')

    condor_m = Condor()

    response = jsonify(condor_m.get_nodes(match, args))

    return response


if __name__ == '__main__':
    application.run(host='186.232.60.33', port=5000, debug=True)
