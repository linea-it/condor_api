from flask import Flask
from flask import render_template
from flask import jsonify
from pycondor import Job, Dagman
from condor import Condor

app = Flask(__name__)

@app.route('/')
def index():

     condor_m = Condor()

     response=jsonify(condor_m.get_jobs())

     return response
#    return render_template('index.html')

if __name__ == '__main__':
    app.run(host='186.232.60.33',port=5000,debug=True)
