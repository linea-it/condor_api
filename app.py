from flask import Flask
from flask import render_template
from flask import request
from flask import jsonify
#from pycondor import Job, Dagman
from condor import Condor

app = Flask(__name__)

@app.route('/')
def index():

     condor_m = Condor()

     response=jsonify(condor_m.list_parms())

     return response
#    return render_template('index.html')


@app.route('/jobs')

def jobs():

    condor_m = Condor()

    response=jsonify(condor_m.get_jobs())

    return response


@app.route('/nodes')
def nodes():

    condor_m = Condor()

    response=jsonify(condor_m.get_nodes())

    return response

    

if __name__ == '__main__':
    app.run(host='186.232.60.33',port=5000,debug=True)
