from app import app, auth, session
from app.models import User, Sensor
from flask import request, abort, jsonify, g, send_from_directory
from cassandra.query  import SimpleStatement
from flask_httpauth import HTTPBasicAuth
from datetime import datetime
import os, tarfile

@app.route('/api/token/v1.0/getauthtoken', methods=['POST'])
@auth.login_required
def getauthtoken():
    token = g.user.generate_auth_token()
    return jsonify({'token': token.decode('ascii')})
