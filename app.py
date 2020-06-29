from condor import Condor
from flask import Flask, render_template, request, jsonify
from flask_cors import CORS
import threading
from apscheduler.schedulers.background import BackgroundScheduler
import time

application = Flask(__name__)
cors = CORS(application, resources={r"/*": {"origins": "*"}})


@application.route('/')
def index():
    condor_m = Condor()
    return render_template('index.html')


@application.route('/users')
def users():
    condor_m = Condor()
    return render_template('users.html')


@application.route('/submit_job', methods=['POST'])
def submit_job():
    """ submit job """

    condor_m = Condor()
    result = condor_m.submit_job(request.json)
    return jsonify(result)


@application.route('/jobs', methods=['GET'])
def jobs():
    """ gets jobs """

    cols, args = list(), dict()

    if len(request.args):
            args = request.args.to_dict()

            if 'cols' in args:
                args.pop('cols')

    if request.args.get('cols'):
        cols = request.args.get('cols')

    condor_m = Condor()
    return jsonify(condor_m.get_jobs(args, cols))


@application.route('/users_stats', methods=['GET'])
def get_users_stats():
    """ gets users stats """

    condor_m = Condor()
    jobs = condor_m.get_jobs({}, [])
    users = list()
    rows = list()

    for j in jobs:
        if 'Owner' in j:
            if j['Owner'] not in users:
                users.append(j['Owner'])
        else:
            j['Owner'] = ''

    for user in users:
        userjobs = condor_m.get_jobs({'Owner': '"'+user+'"'}, [])
        nodes = condor_m.get_nodes('', [])
        total_nodes = len(nodes)
        owner = user
        portal_jobs, manual_jobs, user_jobs_running, user_jobs_idle, cluster_utilization = 0, 0, 0, 0, 0
        cluster = ''
        cores = int()
        processes, userslots = list(), list()

        for j in userjobs:
            job  = j['Process'].split('100')
            cluster = j['ClusterName']

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
                    if 'RemoteHost' in j and j['RemoteHost'] == n['Name']:
                        if 'RequiresWholeMachine' in j:
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
    """ gets info per nodes """

    args = list()
    match = myString = ",".join(request.args.getlist('match'))

    if len(request.args):
        args = request.args.keys()
        if 'match' in args:
            args.remove('match')

    condor_m = Condor()

    return jsonify(condor_m.get_nodes(match, args))


@application.route('/parent_history', methods=['GET'])
def parent_history():
    """ gets history info per parents """

    cols, args, limit, offset = list(), dict(), False, False

    if len(request.args):
        args = request.args.to_dict()

    if request.args.get('cols'):
        cols = request.args.get('cols').split(',')
        args.pop('cols')

    if request.args.get('limit'):
        limit = int(request.args.get('limit'))
        args.pop('limit')

    if request.args.get('offset'):
        offset = int(request.args.get('offset'))
        args.pop('offset')

    condor_m = Condor()
    return jsonify(condor_m.job_parent_history(args, cols, limit, offset))


@application.route('/history', methods=['GET'])
def history():
    """ gets history info """

    cols, args, limit, offset = list(), dict(), False, False

    if len(request.args):
        args = request.args.to_dict()

    if request.args.get('cols'):
        cols = request.args.get('cols').split(',')
        args.pop('cols')

    # if request.args.get('search'):
    #   search = request.args.get('search')
    #   args.pop('search')

    if request.args.get('limit'):
        limit = int(request.args.get('limit'))
        args.pop('limit')

    if request.args.get('offset'):
        offset = int(request.args.get('offset'))
        args.pop('offset')

    condor_m = Condor()

    response = jsonify(condor_m.job_history(args, cols, limit, offset))

    return response

@application.route('/old_history', methods=['GET'])
def old_history():
    """ """
    cols, args, limit, offset = list(), dict(), False, False

    if len(request.args):
        args = request.args.to_dict()

    if request.args.get('cols'):
        cols = request.args.get('cols').split(',')
        args.pop('cols')

    # if request.args.get('search'):
    #   search = request.args.get('search')
    #   args.pop('search')

    if request.args.get('limit'):
        limit = int(request.args.get('limit'))
        args.pop('limit')

    if request.args.get('offset'):
        offset = int(request.args.get('offset'))
        args.pop('offset')

    condor_m = Condor()

    return jsonify(condor_m.get_cluster_history(args, cols, limit))


@application.route('/remove', methods=['GET'])
def remove():
    """ removes job by ClusterId and ProcId """

    args = request.args.to_dict()

    if 'ClusterId' not in args or 'ProcId' not in args:
        raise Exception("Parameter ClusterId and ProcId are required")

    condor_m = Condor()

    return jsonify(condor_m.remove_job(args['ClusterId'], args['ProcId']))


@application.route('/get_job', methods=['GET'])
def get_job():
    """ gets job by ClusterId and ProcId """

    args = request.args.to_dict()

    if 'ClusterId' not in args or 'ProcId' not in args:
        raise Exception("Parameter ClusterId and ProcId are required")

    condor_m = Condor()

    return jsonify(condor_m.get_job(args['ClusterId'], args['ProcId']))


@application.route('/top_users', methods=['GET'])
def get_top_users():
    """ gets cluter's top users """
    cols = list()
    args = dict()
    limit = False
    offset = False

    if len(request.args):
        args = request.args.to_dict()

    if request.args.get('limit'):
        limit = int(request.args.get('limit'))
        args.pop('limit')

    condor_m = Condor()

    return jsonify(condor_m.top_users_history(args, limit))


@application.route('/test_endpoint', methods=['GET'])
def test_endpoint():
    cols = list()
    args = dict()
    limit = 100

    if len(request.args):
        args = request.args.to_dict()

    if request.args.get('cols'):
        cols = request.args.get('cols').split(',')
        args.pop('cols')

    if request.args.get('limit'):
        limit = int(request.args.get('limit'))
        args.pop('limit')

    condor_m = Condor()
    return jsonify(condor_m.parse_requirements(args))


# @application.route('/update_db', methods=['GET'])
# def update_db():
#   condor_m = Condor()
#   response = jsonify(condor_m.update_db())
#   return response

def update_db():
    with application.app_context():
        condor_m = Condor()
        condor_m.update_db()


if __name__ == '__main__':
    scheduler = BackgroundScheduler()
    if scheduler.get_jobs():
        print ("Job ainda rodando")
    else:
        scheduler.add_job(update_db, 'interval', minutes=10, max_instances=1)

    scheduler.start()

    #application.run(host='localhost', port=5000, debug=True)
    application.run(host='186.232.60.33', port=5000, debug=True)
