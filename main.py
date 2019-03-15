from flask import Flask
from flask import request
import weight_setting as ws
import json

app = Flask(__name__)


@app.route('/weight_update', methods=['GET', 'POST'])
def ga_maker():
    try:
        if request.method == 'GET':
            response = app.response_class(
                response=json.dumps(ws.wsmain()),
                status=200,
                mimetype='application/json'
            )
            return response
        return ""
    except Exception as e:
        print(str(e))
        return ""


app.run('0.0.0.0', 15555)
