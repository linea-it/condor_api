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
