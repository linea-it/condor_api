from condor import Condor
#from pycondor import Job, Dagman
from flask import Flask
from flask import render_template
from flask import request
from flask import jsonify

application = Flask(__name__)


@application.route('/')
def index():

    condor_m = Condor()

    response = jsonify(condor_m.list_parms())

    return response
#    return render_template('index.html')


@application.route('/submit_job', methods=['POST'])
def submit_job():

    condor_m = Condor()
    result = condor_m.submit_job(request.json)

    response = jsonify(result)

    return response


@application.route('/jobs', methods=['GET'])
def jobs():

    args = []
    match = myString = ",".join(request.args.getlist('match'))
    
    if len(request.args):
        args = request.args.keys()
	if 'match' in args:
		args.remove('match')
    condor_m = Condor()

    response = jsonify(condor_m.get_jobs(match,args))

    return response


@application.route('/nodes', methods=['GET'])
def nodes():

    args = []
    match = myString = ",".join(request.args.getlist('match'))

    if len(request.args):
        args = request.args.keys()
        if 'match' in args:
                args.remove('match')

    condor_m = Condor()

    response = jsonify(condor_m.get_nodes(match,args))

    return response


if __name__ == '__main__':
    application.run(host='186.232.60.33', port=5000, debug=True)
