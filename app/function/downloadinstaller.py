from app import app, auth, session
from app.models import User, Sensor
from flask import request, abort, jsonify, g, send_from_directory
from cassandra.query  import SimpleStatement
from flask_httpauth import HTTPBasicAuth
from datetime import datetime
import os, tarfile

def downloadinstaller():
    filename = 'installer.sh'
    filedir = app.config['BASEDIR'] + '/app/static/'

    return send_from_directory(filedir, filename, as_attachment=True)
