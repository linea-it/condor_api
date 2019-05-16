from flask import Flask
from flask import render_template
from flask import jsonify
from flask import request
from pycondor import Job, Dagman
from condor import Condor

application = Flask(__name__)


@application.route('/')
def index():

    condor_m = Condor()

    response = jsonify(condor_m.get_jobs())

    return response
#    return render_template('index.html')


@application.route('/submit_job', methods=['POST'])
def submit_job():

    condor_m = Condor()
    result = condor_m.submit_job(request.json)

    response = jsonify(result)

    return response


if __name__ == '__main__':
    application.run(host='186.232.60.33', port=5001, debug=True)
