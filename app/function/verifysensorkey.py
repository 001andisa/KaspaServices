from app import app, auth, session
from app.models import User, Sensor
from flask import request, abort, jsonify, g, send_from_directory
from cassandra.query  import SimpleStatement
from flask_httpauth import HTTPBasicAuth
from datetime import datetime
import os, tarfile

def verifysensorkey():
    device_id = request.json.get('device_id')
    sensor_key = request.json.get('sensor_key')
    netint = request.json.get('netint')
    print(device_id)
    print(sensor_key)
    print(netint)
    if device_id is None or sensor_key is None or netint is None:
        abort(400)
    q = Sensor.objects.filter(company = g.user['company'])
    q = q.filter(device_id = device_id)
    sensor = q.first()

    if sensor is None:
        abort(400)

    #create tarball

    buildfile = 'build_snoqtt.sh'
    conffile = 'env-conf.conf'
    removefile = 'remove_snoqtt.sh'
    startfile = 'start_snoqtt.sh'
    stopfile = 'stop_snoqtt.sh'
    
    filedirtemplate = app.config['BASEDIR'] + '/app/static/template/'
    
    if not os.path.exists(app.config['BASEDIR'] + '/app/static/generated/{}/'.format(sensor_key)):
        os.makedirs(app.config['BASEDIR'] + '/app/static/generated/{}/'.format(sensor_key))
    
    filediroutput = app.config['BASEDIR'] + '/app/static/generated/{}/'.format(sensor_key)

    with open(filedirtemplate + buildfile) as build_template:
        templatebuild = build_template.read()
    with open(filediroutput + buildfile, "w") as current:
        current.write(templatebuild.format(protected_subnet=sensor['protected_subnet'],
                                            external_subnet="'{}'".format(sensor['external_subnet']),
                                            oinkcode=sensor['oinkcode']))
    
    with open(filedirtemplate + conffile) as conf_template:
        templateconf = conf_template.read()
    with open(filediroutput + conffile, "w") as current:
        current.write(templateconf.format(global_topic=sensor['topic_global'],
                                            global_server='103.24.56.244',
                                            global_port='1883',
                                            device_id=sensor['device_id'],
                                            oinkcode=sensor['oinkcode'],
                                            protected_subnet=sensor['protected_subnet'],
                                            external_subnet=sensor['external_subnet'],
                                            netint=netint,
                                            company=g.user['company']))
    
    with open(filedirtemplate + removefile) as remove_template:
        templateremove = remove_template.read()
    with open(filediroutput + removefile, "w") as current:
        current.write(templateremove)

    with open(filedirtemplate + startfile) as start_template:
        templatestart = start_template.read()
    with open(filediroutput + startfile, "w") as current:
        current.write(templatestart)

    with open(filedirtemplate + stopfile) as stop_template:
        templatestop = stop_template.read()
    with open(filediroutput + stopfile, "w") as current:
        current.write(templatestop)

    filetarname='snoqtt-{}.tar.gz'.format(sensor_key)
    if os.path.exists(filediroutput + filetarname):
        os.remove(filediroutput + filetarname)

    tar = tarfile.open((filediroutput + filetarname), "w:gz")
    tar.add(filediroutput + buildfile, arcname=buildfile)
    tar.add(filediroutput + conffile, arcname=conffile)
    tar.add(filediroutput + removefile, arcname=removefile)
    tar.add(filediroutput + startfile, arcname=startfile)
    tar.add(filediroutput + stopfile, arcname=stopfile)
    tar.close()

    os.remove(filediroutput + buildfile)
    os.remove(filediroutput + conffile)
    os.remove(filediroutput + removefile)
    os.remove(filediroutput + startfile)
    os.remove(filediroutput + stopfile)

    return send_from_directory(filediroutput, filetarname, as_attachment=True)
