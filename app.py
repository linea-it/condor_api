from condor import Condor
from database import db, clear_jsondb
from flask import Flask, request, jsonify
from flask_cors import CORS
import time
import json
import os
from apscheduler.schedulers.background import BackgroundScheduler
from filelock import FileLock

application = Flask(__name__)
cors = CORS(application, resources={r"/*": {"origins": "*"}})

CONDOR = Condor()
JSONDB = "jsondb"


def get_jobs_by_section(prefix, section, func, expire=None, cols=list(), force=False):
    """ Retrieves job information per section using memcached """

    key = '{}-{}'.format(prefix, section)
    jsonpath = os.path.join(JSONDB,'{}.json'.format(key))
    data = db.get(key)

    if not data or not os.path.isfile(jsonpath):
        data = func(section, cols)
        db.set(key, jsonpath, expire=expire)
        with FileLock("{}.lock".format(jsonpath)):
            with open(jsonpath, 'w') as outfile:
                json.dump(data, outfile)
    elif force:
        data = func(section, cols)
        db.replace(key, jsonpath, expire=expire)
        with FileLock("{}.lock".format(jsonpath)):
            with open(jsonpath, 'w') as outfile:
                json.dump(data, outfile)
    else:
        with open(jsonpath) as jsonfile:
            data = json.load(jsonfile)

    return data


@application.route('/sections', methods=['GET'])
def sections():
    """ Gets sections by config.ini """

    sections = CONDOR.get_sections()
    section_list = list()

    for key in sections.keys():
        section = sections[key]
        section["Section"] = key
        section_list.append(section)

    return jsonify(section_list)


@application.route('/jobs_by_key', methods=['GET'])
def jobs_by_key():
    """ Gets jobs by applications """

    force, cols, section, history = False, list(), None, False

    args = request.args.to_dict()

    if not 'key' in args:
        return jsonify({"error": "It is necessary to inform a key to group the jobs (/jobs_by_key?key=<key>)"})

    key = args.get('key')

    if 'cols' in args:
        cols = args.get('cols').split(',')

    if 'force' in args and args.get('force').lower() == 'true':
        force = True

    if 'section' in args:
        section = args.get('section')

    if 'history' in args and args.get('history').lower() == 'true':
            history = True

    sections = CONDOR.get_sections()

    def _get_jobs(data):
        if history:
            data['data'] = get_jobs_by_section(
                "history", section, CONDOR.get_history_by_section,
                600, cols, force
            )
        else:
            data['data'] = get_jobs_by_section(
                "running", section, CONDOR.get_running_by_section,
                60, cols, force
            )
        data['count'] = len(data.get('data'))
        return data

    if section:
        data = sections[section]
        data = _get_jobs(data)
        return jsonify(CONDOR.group_by_key(dict(section=data), key))

    for section in sections:
        data = sections.get(section)
        data = _get_jobs(data)
        sections[section] = data

    return jsonify(CONDOR.group_by_key(sections, key))


@application.route('/history_jobs_by_cluster_id', methods=['GET'])
def history_jobs_by_cluster_id():
    """ Gets history jobs by Cluster Id """

    force, cols, section = False, list(), "main"
    args = request.args.to_dict()

    if not 'id' in args:
        return jsonify({"error": "It is necessary to inform a Cluster Id (/history_jobs_by_cluster_id?id=<cluster_id>)"})

    cluster_id = args.get('id')

    if 'cols' in args:
        cols = args.get('cols').split(',')

    if 'force' in args and args.get('force').lower() == 'true':
        force = True

    if 'section' in args:
        section = args.get('section')

    key = 'clusterid-{}-{}'.format(cluster_id, section)
    jsonpath = os.path.join(JSONDB,'{}.json'.format(key))
    data = db.get(key)

    if not data or not os.path.isfile(jsonpath):
        data = CONDOR.get_history_jobs_by_cluster_id(section, cluster_id, cols)
        db.set(key, jsonpath)
        with FileLock("{}.lock".format(jsonpath)):
            with open(jsonpath, 'w') as outfile:
                json.dump(data, outfile)
    elif force:
        data = CONDOR.get_history_jobs_by_cluster_id(section, cluster_id, cols)
        db.replace(key, jsonpath)
        with FileLock("{}.lock".format(jsonpath)):
            with open(jsonpath, 'w') as outfile:
                json.dump(data, outfile)
    else:
        with open(jsonpath) as jsonfile:
            data = json.load(jsonfile)

    return jsonify(data)


@application.route('/submit_job', methods=['POST'])
def submit_job():
    """ Submit job """

    result = CONDOR.submit_job(request.json)
    return jsonify(result)


@application.route('/history', methods=['GET'])
def history():
    """ Gets history  """

    force, cols, section = False, list(), None

    if len(request.args):
        args = request.args.to_dict()

        if 'cols' in args:
            cols = args.get('cols').split(',')

        if 'force' in args and args.get('force') == 'True':
            force = True

        if 'section' in args:
            section = args.get('section')

    sections = CONDOR.get_sections()

    if section:
        data = sections[section]
        data['data'] = get_jobs_by_section(
            "history", section, CONDOR.get_history_by_section,
            600, cols, force
        )
        data['count'] = len(data.get('data'))
        return jsonify(data)

    for section in sections:
        sections[section]['data'] = get_jobs_by_section(
            "history", section, CONDOR.get_history_by_section,
            600, cols, force
        )
        sections[section]['count'] = len(sections[section].get('data'))

    return jsonify(sections)


@application.route('/jobs', methods=['GET'])
def jobs():
    """ Gets jobs running """

    force, cols, section = False, list(), None

    if len(request.args):
        args = request.args.to_dict()

        if 'cols' in args:
            cols = args.get('cols').split(',')
            args.pop('cols')

        if 'force' in args and args.get('force') == 'True':
            force = True

        if 'section' in args:
            section = args.get('section')

    sections = CONDOR.get_sections()

    if section:
        data = sections[section]
        data['data'] = get_jobs_by_section(
            "running", section, CONDOR.get_running_by_section,
            60, cols, force
        )
        data['count'] = len(data.get('data'))
        return jsonify(data)

    for section in sections:
        sections[section]['data'] = get_jobs_by_section(
            "running", section, CONDOR.get_running_by_section,
            60, cols, force
        )
        sections[section]['count'] = len(sections[section].get('data'))

    return jsonify(sections)


@application.route('/users_stats', methods=['GET'])
def get_users_stats():
    """ Gets users stats """

    sections = CONDOR.get_sections()

    for section in sections:
        sections[section]['data'] = get_jobs_by_section(
            "running", section, CONDOR.get_running_by_section, 60
        )

    return jsonify(CONDOR.users_running(sections))


@application.route('/top_users', methods=['GET'])
def get_top_users():
    """ Gets cluster's top users """

    sections = CONDOR.get_sections()

    for section in sections:
        sections[section]['data'] = get_jobs_by_section(
            "history", section, CONDOR.get_history_by_section, 600
        )

    return jsonify(CONDOR.top_users_history(sections))


@application.route('/nodes', methods=['GET'])
def nodes():
    """ Gets info per nodes """

    args = list()
    match = myString = ",".join(request.args.getlist('match'))

    if len(request.args):
        args = request.args.keys()
        if 'match' in args:
            args.remove('match')

    return jsonify(CONDOR.get_nodes(match, args))


@application.route('/remove', methods=['GET'])
def remove():
    """ Removes job by ClusterId and ProcId """

    args = request.args.to_dict()

    if 'ClusterId' not in args or 'ProcId' not in args:
        raise Exception("Parameter ClusterId and ProcId are required")

    return jsonify(CONDOR.remove_job(args['ClusterId'], args['ProcId']))


@application.route('/get_job', methods=['GET'])
def get_job():
    """ Gets job by ClusterId and ProcId """

    args = request.args.to_dict()

    if 'ClusterId' not in args or 'ProcId' not in args:
        raise Exception("Parameter ClusterId and ProcId are required")

    return jsonify(CONDOR.get_job(args['ClusterId'], args['ProcId']))


def clear_filedir():
    """ Clears db file directory """
    with application.app_context():
        clear_jsondb(JSONDB)

if __name__ == '__main__':
    scheduler = BackgroundScheduler()
    if not scheduler.get_jobs():
        scheduler.add_job(clear_filedir, 'interval', minutes=10, max_instances=1)

    scheduler.start()

    os.makedirs(JSONDB, exist_ok=True)
    application.run(host='186.232.60.33', port=5011, debug=True)
    #application.run(host='localhost', port=5000, debug=True
