from app import app, auth, session
from app.models import User, Sensor
from flask import request, abort, jsonify, g, send_from_directory
from cassandra.query  import SimpleStatement
from flask_httpauth import HTTPBasicAuth
from datetime import datetime
import os, tarfile

def listsensors():
    company = g.user['company']
    if company is None:
        abort(400)
    
    obj={
        "company" : g.user['company'],
        "count" : 0,
        "sensors" : []
    }
    for sensor in Sensor.objects.filter(company=company):
        sensor_obj = {
            "device_id" : sensor['device_id'],
            "device_name" : sensor['device_name'],
            "hostname" : sensor['hostname'],
            "ip_address" : sensor['ip_address'],
            "location" : sensor['location'],
            "protected_subnet" : sensor['protected_subnet'],
            "external_subnet" : sensor['external_subnet'],
            "oinkcode" : sensor['oinkcode'],
            "topic_global" : sensor['topic_global'],
            "topic_cmd" : sensor['topic_cmd'],
            "topic_resp" : sensor['topic_resp'],
            "sensor_key" : sensor['sensor_key'],
            "time_created" : sensor['time_created']
        }
        obj['sensors'].append(sensor_obj)
        obj['count'] = obj['count'] + 1
    
    return jsonify(obj)