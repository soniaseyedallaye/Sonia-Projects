import json
import pickle
import datetime
import dateutil
import joblib
import pandas as pd
from flask import Flask, jsonify, request
from peewee import (
    SqliteDatabase, Model, IntegerField,
    FloatField, TextField, IntegrityError)
import re
import os
from playhouse.db_url import connect
#from playhouse.shortcuts import model_to_dict
########################################
# Begin database stuff
DB = SqliteDatabase('predictions.db')
#DB = connect(os.environ.get('DATABASE_URL') or 'sqlite:///predictions.db')


class Prediction(Model):
    observation_id = IntegerField(unique=True)
    observation = TextField()
    #proba = FloatField()
    prediction = TextField()
    outcome = IntegerField(null=True)

    class Meta:
        database = DB


DB.create_tables([Prediction], safe=True)
# End database stuff
########################################
########################################
# Unpickle the previously-trained model
with open('columns.json') as fh:
    columns = json.load(fh)

with open('pipeline.pickle', 'rb') as fh:
    pipeline = joblib.load(fh)

with open('dtypes.pickle', 'rb') as fh:
    dtypes = pickle.load(fh)


# End model un-pickling
########################################
########################################
# Input validation functions


def check_valid_column(observation):
    """
        Validates that our observation only has valid columns

        Returns:
        - assertion value: True if all provided columns are valid, False otherwise
        - error message: empty if all provided columns are valid, False otherwise
    """

    valid_columns = {"observation_id","Type", "Date","Part of a policing operation", "Latitude", "Longitude", "Gender", "Age range", "Officer-defined ethnicity","Legislation", "Object of search", "station"}

    keys = set(observation.keys())

    if len(valid_columns - keys) > 0:
        missing = valid_columns - keys
        error = "Missing columns: {}".format(missing)
        return False, error

    if len(keys - valid_columns) > 0:
        extra = keys - valid_columns
        error = "Unrecognized columns provided: {}".format(extra)
        return False, error

    if len(keys - valid_columns) == 0:
        return True, ""

def check_column_types(observation):
    column_types = {
        "observation_id": str,
        "Part of a policing operation": str,
    }

    for col, type_ in column_types.items():
        if not isinstance(observation[col], type_):
            error = "Field {} is {}, while it should be {}".format(col, type(observation[col]), type_)
            return False, error
    return True, ""


regex = r'^(-?(?:[1-9][0-9]*)?[0-9]{4})-(1[0-2]|0[1-9])-(3[01]|0[1-9]|[12][0-9])T(2[0-3]|[01][0-9]):([0-5][0-9]):([0-5][0-9])(\.[0-9]+)?(Z|[+-](?:2[0-3]|[01][0-9]):[0-5][0-9])?$'
match_iso8601 = re.compile(regex).match
def check_date(observation):
    date = observation.get("Date")
    try:
        if match_iso8601(date) is not None:
            return True,""
    except ValueError:
        pass
        error= "ERROR: Date '{}' is not in correct ISO8601String format".format(date)
        return False,error
def check_latitude(observation):
    latitude = observation.get("Latitude")
    if not latitude:
        error = "Field `Latitude` missing"
        return False, error

    x = isinstance(latitude, int)
    y = isinstance(latitude, float)
    if x or y:
        return True, ""
    else:
        error = "{} must be a number".format(latitude)
        return False,


def check_longitude(observation):
    longitude = observation.get("Longitude")
    if not longitude:
        error = "Field `Longitude` missing"
        return False, error

    x = isinstance(longitude, int)
    y = isinstance(longitude, float)
    if x or y:
        return True, ""
    else:
        error = "{} must be a number".format(longitude)
        return False,




# End input validation functions
########################################
########################################
# Begin webserver stuff
app = Flask(__name__)


@app.route('/should_search/', methods=['POST'])
def should_search():
    obs_dict = request.get_json()
    # verification routines
    valid_columns_ok, error = check_valid_column(obs_dict)
    if not valid_columns_ok:
        response = {'error': error}
        return jsonify(response)

    valid_types_ok, error = check_column_types(obs_dict)
    if not valid_types_ok:
        response = {'error': error}
        return jsonify(response)

    valid_date_ok, error = check_date(obs_dict)
    if not valid_date_ok:
        response = {'error': error}
        return jsonify(response)

    valid_latitude_ok, error = check_latitude(obs_dict)
    if not valid_latitude_ok:
        response = {'error': error}
        return jsonify(response)

    valid_longitude_ok, error = check_longitude(obs_dict)
    if not valid_longitude_ok:
        response = {'error': error}
        return jsonify(response)

    # read data
    _id = obs_dict['observation_id']
    obs_dict.pop('observation_id')
    _iso = obs_dict['Date']
    obs_dict.pop('Date')
    date_iso = dateutil.parser.isoparse(_iso)
    hour = date_iso.hour
    day = date_iso.day
    month = date_iso.month
    obs_dict['hour'] = hour
    obs_dict['month'] = month
    obs_dict['day'] = day

    obs = pd.DataFrame([obs_dict], columns=columns).astype(dtypes)

    # compute prediction
    #proba = pipeline.predict_proba(obs)[0,1]
    prediction = pipeline.predict(obs)[0]
    response = {'outcome': bool(prediction)}

    p = Prediction(
        observation_id=_id,
        observation=request.data,
        #proba=proba,
        prediction=prediction,)
    try:
       p.save()
       return jsonify(response)
    except IntegrityError:
        error_msg = "ERROR: Observation ID: '{}' already exists".format(_id)
        #response["error"] = error_msg
        #print(error_msg)
        DB.rollback()
        #{'error': error_msg}
        return jsonify({'error': error_msg})



@app.route('/search_result/', methods=['POST'])
def search_result():
    obs_dict = request.get_json()

    try:
        p = Prediction.get(Prediction.observation_id == obs_dict['observation_id'])
        p.outcome = obs_dict['outcome']
        #p.outcome = Prediction.prediction
        #response = obs_dict
        p.save()
        #obs_dict['outcome'] =p
        obs_dict['predicted_outcome'] = p.prediction
        return jsonify(obs_dict)
    except Prediction.DoesNotExist:
        error_msg = 'Observation ID: "{}" does not exist'.format(obs_dict['observation_id'])
        return jsonify({'error': error_msg})
if __name__ == "__main__":
    app.run()
