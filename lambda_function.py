
import requests
import json
import datetime
import boto3
from config import *



def main_function(event, context):
    is_incremental = event.get('is_incremental', '')
    refresh_from = event.get('refresh_from', '')
    params_ios = get_params(is_incremental, refresh_from, 'ios')
    params_android = get_params(is_incremental, refresh_from, 'android')
    response_ios, response_android = get_raw_data(header, params_ios, params_android) 
    try:
        data_full = format_data(response_ios, 'ios') + format_data(response_android, 'android')
    except Exception as e:
        print(str(e))
        return
    bucket_content = data_full
    write_to_bucket(s3_obj, json.dumps(bucket_content))
    return 


def get_params(is_incremental, refresh_from, source):
    print('Getting parameters for start and end dates')
    if is_incremental:
        # start_date = datetime.datetime.now().strftime('%Y-%m-%d')
        start_date = get_date_from_bucket(s3_obj, source)
        end_date = start_date
        print('start date: ', start_date)
        print('end_date: ', end_date)
    elif refresh_from:
        start_date = refresh_from
        end_date = datetime.datetime.now().strftime('%Y-%m-%d')
        print('start date: ', start_date)
        print('end date: ', end_date)
    else:
        start_date = '2021-03-01'
        # end_date = datetime.datetime.now().strftime('%Y-%m-%d')
        end_date = '2021-03-15'
        print('start date: ', start_date)
        print('end date: ', end_date)
    return {'start_date': start_date, 'end_date': end_date}



def get_raw_data(header, params_ios, params_android):
    print('Getting data from apptweak')
    response_ios = requests.get(url_ios, headers=header, params=params_ios)
    response_android = requests.get(url_android, headers=header, params=params_android)
    if response_ios.status_code == 200:
        print('ios response was of status: 200')
        response_ios = json.loads(response_ios._content)
    else:
        print('could not get ios response 200. Returning empty json')
        response_ios = json.loads("{}")
    if response_android.status_code == 200:
        print('android response was of status: 200')
        response_android = json.loads(response_android._content)
    else:
        print('could not get android response 200. Returning empty json')
        response_android = json.loads("{}")
    return (response_ios, response_android)


def format_data(data, source):
    print('Formatting data from: ', source)
    rows = []
    try:
        data = data['content']
    except:
        raise Exception('No content found. Exiting')
    end_date = data['end_date'][:10]
    end_date = datetime.datetime.strptime(end_date, '%Y-%m-%d')
    start_date = data['start_date'][:10]
    start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d')
    interval = end_date - start_date
    interval = interval.days
    if interval == 0:
        #when start date and end date is the same
        interval = interval + 1
    current_date = start_date
    for i in range(0, interval):
        row = data['ratings'][i]
        row = format_row(row, current_date, source)
        rows.append(row)
        current_date = current_date + datetime.timedelta(days=1)
    return rows


def format_row(row, current_date, source):
    formatted_row = {}
    columns = {
        '1': 'one',
        '2': 'two',
        '3': 'three',
        '4': 'four',
        '5': 'five',
        'total': 'total',
        'avg': 'average'
    }
    for key, value in row.items():
        formatted_row[columns[key]] = value
    formatted_row['as_of_date'] = current_date.strftime('%Y-%m-%d')
    formatted_row['source'] = source
    return formatted_row



def read_bucket(s3_obj):
    content = s3_obj.get()['Body'].read()
    content = content.decode('utf-8')
    return content


def write_to_bucket(s3_obj, data):
    print('Writing to the s3 bucket')
    s3_obj.put(Body=data)


def get_date_from_bucket(s3_obj, source):
    content = read_bucket(s3_obj)
    try:
        content = json.loads(content)
    except:
        content = []
    dates = [x['as_of_date'] for x in content if x['source'] == source]
    dates = [datetime.datetime.strptime(x,'%Y-%m-%d') for x in dates]
    try:
        max_date = sorted(dates, reverse=True)[0]
    except:
        raise Exception('No content in the bucket. Try running a historical reset first')
    return (max_date + datetime.timedelta(days=1)).strftime('%Y-%m-%d')
    



# event = {
#   "is_incremental": True,
#   "refresh_from": False
# }
# res = main_function(event, [])
