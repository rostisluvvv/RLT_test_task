import json
from datetime import datetime, timedelta
from pymongo import MongoClient
import calendar


GROUP_TYPES_FORMAT = {
    'hour': '%Y-%m-%dT%H',
    'day': '%Y-%m-%d',
    'month': '%Y-%m'
}


def validate_request_data(data):

    if not isinstance(data, dict):
        raise TypeError(f'Invalid data type: {type(data)} '
                        f'expected dict')
    if not all(key in data for key in ['dt_from', 'dt_upto', 'group_type']):
        raise ValueError('Missing required keys')
    group_type = data['group_type']
    if group_type not in GROUP_TYPES_FORMAT:
        raise ValueError(f'Invalid value of "group_type" key: {group_type}')


def execute_query(dt_from, dt_upto, dt_format):

    client = MongoClient('mongodb://127.0.0.1:27017/')
    db = client['sampleDB']
    collection = db['sample_collection']

    query = [
        {
            "$match": {"dt": {"$gte": dt_from, "$lte": dt_upto}}
        },
        {
            "$group": {
                "_id": {
                    "$dateToString": {"format": dt_format, "date": "$dt"}},
                "totalValue": {"$sum": "$value"},
            }
        },
        {"$sort": {"_id": 1}},
    ]
    return collection.aggregate(query)


def get_aggregated_values(data):

    validate_request_data(data)
    dt_from = datetime.strptime(data['dt_from'], '%Y-%m-%dT%H:%M:%S')
    dt_upto = datetime.strptime(data['dt_upto'], '%Y-%m-%dT%H:%M:%S')
    group_type = data['group_type']
    dt_format = GROUP_TYPES_FORMAT[group_type]
    result = execute_query(dt_from, dt_upto, dt_format)
    result = {it['_id']: it['totalValue'] for it in result}
    dataset = []
    labels = []
    for dt in daterange(dt_from, dt_upto, group_type):
        dt_str = dt.strftime(dt_format)
        if dt_str in result:
            dataset.append(result[dt_str])
        else:
            dataset.append(0)
        labels.append(dt.isoformat()[:19])
    return json.dumps({'dataset': dataset, 'labels': labels})


def daterange(dt_from, dt_upto, group_type):

    if group_type == 'hour':
        delta = timedelta(hours=1)
    elif group_type == 'day':
        delta = timedelta(days=1)
    elif group_type != 'month':
        raise ValueError(f"Invalid group type: {group_type}")
    current_date = dt_from
    while current_date <= dt_upto:
        yield current_date
        if group_type == 'month':
            _, days_in_month = calendar.monthrange(current_date.year,
                                                   current_date.month)
            delta = timedelta(days=days_in_month)
        current_date += delta
