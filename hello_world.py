# coding: utf-8
"""
    mta-api-sanity
    ~~~~~~

    Expose the MTA's real-time subway feed as a json api

    :copyright: (c) 2014 by Jon Thornton.
    :license: BSD, see LICENSE for more details.
"""

import mta_realtime
import flask
import calendar
from flask import Flask, request, jsonify, render_template, abort
from flask.json import JSONEncoder
from datetime import datetime
from functools import wraps
import logging
import requests

app = Flask(__name__)
app.config.update(
    MAX_TRAINS=10,
    MAX_MINUTES=30,
    CACHE_SECONDS=60,
    MTA_KEY = '29276f90f0b0658736ed8be60442c9d5',
    STATIONS_FILE = './stations.json',
    THREADED=True,
    DEBUG = True
)

mta = mta_realtime.MtaSanitizer(
    app.config['MTA_KEY'],
    app.config['STATIONS_FILE'],
    max_trains=app.config['MAX_TRAINS'],
    max_minutes=app.config['MAX_MINUTES'],
    expires_seconds=app.config['CACHE_SECONDS'],
    threaded=app.config['THREADED'])

def print_updates():    
    import time

    def compress_dict_destructively(dict):
        # handle if these fields don't exist??
        return ({
            'r':int(dict["route"]),
            't':int(str(int(time.mktime(dict["time"].timetuple())))[-6:]) # only keeping last 6 digits of the strtime to save message space
        })

    def compress_dict_destructively_to_array(dict):
        # handle if these fields don't exist??
        return (
            int(dict["route"]),
            int(str(int(time.mktime(dict["time"].timetuple())))) # only keeping last 6 digits of the strtime to save message space
        )

    while True:

        borough_hall_info = mta.get_by_id([93])
        mta_time_tz = borough_hall_info[0]["N"][0]["time"].tzinfo
        
        nb_manhattan_bound_four_or_five_trains = filter(lambda x: (x["route"] == u'4' or x["route"] == u'5') and x["time"] > datetime.now(mta_time_tz), borough_hall_info[0]["N"])
        sb_manhattan_bound_four_or_five_trains = filter(lambda x: (x["route"] == u'4' or x["route"] == u'5') and x["time"] > datetime.now(mta_time_tz), borough_hall_info[0]["S"])

        #' Northbound train extractor functions, but I'm going to send the whole JSON.
        next_four_or_five_train_time = nb_manhattan_bound_four_or_five_trains[0]["time"]
        next_time = str(next_four_or_five_train_time - datetime.now(mta_time_tz)).split(".")[0]

        compressed_dict = map(compress_dict_destructively_to_array, nb_manhattan_bound_four_or_five_trains)
        sendthis=flask.json.dumps(compressed_dict[0:4]).replace(" ","")
        
        print "Sending the following request:"
        print sendthis
        print "Request length is (over 63 characters will cause problems with the Particle Photon):"
        print len(sendthis)
        
        r = requests.post(
            "https://api.particle.io/v1/devices/2c0037000347343337373738/led?access_token=3f08c91af9753dd0a6d66697899a3a919580b071",
            data={'value': sendthis}
        )
        
        print r.status_code            
        
        time.sleep(30)

def sync_time_with_photon():    
    import time
    print "sending time"

    sendthis = flask.json.dumps([{'synctime': int(time.mktime(datetime.now().timetuple()))}])
    print sendthis
    
    r = requests.post(
        "https://api.particle.io/v1/devices/2c0037000347343337373738/synctime?access_token=3f08c91af9753dd0a6d66697899a3a919580b071",
        data={'value': sendthis}
    )

    print r.status_code            

sync_time_with_photon()    
print_updates()                 
