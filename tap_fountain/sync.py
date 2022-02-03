import datetime
import json
import os

import singer
from singer import Transformer, utils

from tap_fountain.streams import STREAMS

LOGGER = singer.get_logger()

RAW_DATA_FIELDS = (
    "id",
    "created_at",
    "updated_at"
)


def convert_to_utc(dtime: str) -> datetime.datetime:
    return utils.strptime_to_utc(dtime)


def get_activity_start(key, state, config):
    if key in state:
        return convert_to_utc(state[key])

    if "user_activity_start_date" in config:
        return convert_to_utc(config["user_activity_start_date"])

    start = datetime.datetime.combine(datetime.datetime.utcnow(), datetime.time.min) + datetime.timedelta(days=-1)
    return start


def get_activity_stop(start_datetime, days=30):
    end_time = utils.now()
    return min(start_datetime + datetime.timedelta(days=days), end_time)


def get_applicants_start(key, state, config):
    if key in state:
        return convert_to_utc(state[key])

    if "applicants_start_date" in config:
        return convert_to_utc(config["applicants_start_date"])

    start = datetime.datetime.utcfromtimestamp(0).replace(tzinfo=datetime.timezone.utc)
    return start


def sync_applicants(streams, client, config, state):
    stream_obj = STREAMS.get("applicants")
    stream_name = stream_obj.stream_name
    if stream_name not in streams.keys():
        return

    schema = streams.get(stream_name).schema.to_dict()
    singer.write_schema(stream_name, schema, stream_obj.key_properties)

    from_datetime = get_applicants_start(stream_name, state, config)
    to_datetime = utils.now()

    if to_datetime < from_datetime:
        LOGGER.error("to_datetime (%s) is less than from_endtime (%s).", to_datetime, from_datetime)
        return

    # time format - https://developer.fountain.com/docs/get-apiv2applicants-list-applicants
    params = {
        "updated_at[gt]": from_datetime.strftime("%Y-%m-%dT%H:%M:%S"),
        "updated_at[lt]": to_datetime.strftime("%Y-%m-%dT%H:%M:%S")
    }

    pages = client.request_pages(url=stream_obj.endpoint, params=params)

    bookmark = from_datetime
    applicants_ids = []

    with singer.metrics.record_counter(stream_name) as counter:
        with Transformer() as transformer:
            for data in pages:
                for row in data.get('applicants'):
                    record = {}
                    for header in RAW_DATA_FIELDS:
                        record[header] = row.get(header)
                    record['raw_json'] = row
                    applicants_ids.append(record['id'])
                    records_tf = transformer.transform(data=record, schema=schema)
                    singer.write_record(stream_name, records_tf)
                    try:
                        if convert_to_utc(row["updated_at"]) > bookmark:
                            bookmark = convert_to_utc(row["updated_at"])
                    except:
                        LOGGER.error("failed to get updated_at")
                        raise
                    counter.increment()
        LOGGER.info('FINISHED Syncing stream: Stream Name:[{}], Total Records:[{}], Total Pages:[{}]'
                    .format(stream_name, counter.value, len(pages)))
    utils.update_state(state, stream_name, bookmark)
    sync_applicant_info(streams, client, applicants_ids)
    sync_transitions(streams, client, applicants_ids)
    singer.write_state(state)


# https://developer.fountain.com/docs/get-applicant-info
def sync_applicant_info(streams, client, ids):
    stream_obj = STREAMS.get("applicant_info")
    stream_name = stream_obj.stream_name
    if stream_name not in streams.keys():
        return

    schema = streams.get(stream_name).schema.to_dict()
    singer.write_schema(stream_name, schema, stream_obj.key_properties)

    with singer.metrics.record_counter(stream_name) as counter:
        with Transformer() as transformer:
            for id in ids:
                # url ='{}/{}'.format(stream_obj.endpoint, id)
                url = os.path.join(stream_obj.endpoint, id)
                data = client.get_request(url)

                record = {}
                for header in RAW_DATA_FIELDS:
                    record[header] = data.get(header)
                record['raw_json'] = data
                records_tf = transformer.transform(data=record, schema=schema)
                singer.write_record(stream_name, records_tf)
                counter.increment()
        LOGGER.info('FINISHED Syncing stream: Stream Name:[{}], Total Records:[{}]'
                    .format(stream_name, counter.value))


# https://developer.fountain.com/docs/transition-history-for-multiple-applicants
def sync_transitions(streams, client, ids):
    stream_obj = STREAMS.get("transitions")
    stream_name = stream_obj.stream_name
    if stream_name not in streams.keys():
        return

    schema = streams.get(stream_name).schema.to_dict()
    singer.write_schema(stream_name, schema, stream_obj.key_properties)

    chunk_size = 1000
    with singer.metrics.record_counter(stream_name) as counter:
        with Transformer() as transformer:
            for chunk in utils.chunk(ids, chunk_size):
                data = {
                    "ids": chunk
                }
                data = json.dumps(data)
                response = client.post_request(stream_obj.endpoint, data)
                for applicant in response:
                    record = {
                        "applicant_id": applicant.get("applicant_id"),
                        "raw_json": {"transitions": applicant.get("transitions", [])}
                    }
                    records_tf = transformer.transform(data=record, schema=schema)
                    singer.write_record(stream_name, records_tf)
                    counter.increment()
        LOGGER.info('FINISHED Syncing stream: Stream Name:[{}], Total Records:[{}]'
                    .format(stream_name, counter.value))


# https://developer.fountain.com/docs/list-all-positions
def sync_funnels(streams, client):
    stream_obj = STREAMS.get("funnels")
    stream_name = stream_obj.stream_name
    if stream_name not in streams.keys():
        return

    schema = streams.get(stream_name).schema.to_dict()
    singer.write_schema(stream_name, schema, stream_obj.key_properties)

    funnels = []
    response = client.get_request(stream_obj.endpoint)
    funnels.extend(response.get('funnels'))
    next = response.get('next')

    while next is not None:
        params = {"page": next}
        response = client.get_request(stream_obj.endpoint, params=params)
        funnels.extend(response.get('funnels'))
        next = response.get('next')

    with singer.metrics.record_counter(stream_name) as counter:
        with Transformer() as transformer:
            for funnel in funnels:
                record = {
                    "id": funnel.get('id'),
                    "created_at": funnel.get('created_at'),
                    "raw_json": funnel
                }
                records_tf = transformer.transform(data=record, schema=schema)
                singer.write_record(stream_name, records_tf)
                counter.increment()
        LOGGER.info('FINISHED Syncing stream: Stream Name:[{}], Total Records:[{}]'
                    .format(stream_name, counter.value))


# https://developer.fountain.com/docs/list-all-users
def sync_users(streams, client, config, state):
    stream_obj = STREAMS.get("users")
    stream_name = stream_obj.stream_name
    if stream_name not in streams.keys():
        return

    schema = streams.get(stream_name).schema.to_dict()
    singer.write_schema(stream_name, schema, stream_obj.key_properties)

    user_ids = []
    response = client.get_request(stream_obj.endpoint)
    with singer.metrics.record_counter(stream_name) as counter:
        with Transformer() as transformer:
            for user in response.get('users', []):
                record = user
                records_tf = transformer.transform(data=record, schema=schema)
                singer.write_record(stream_name, records_tf)
                user_ids.append(record.get("id"))
                counter.increment()
        LOGGER.info('FINISHED Syncing stream: Stream Name:[{}], Total Records:[{}]'
                    .format(stream_name, counter.value))
    sync_user_activities(streams, client, config, state, user_ids)


# https://developer.fountain.com/docs/list-user-activities
def sync_user_activities(streams, client, config, state, ids):
    stream_obj = STREAMS.get("user_activities")
    stream_name = stream_obj.stream_name
    if stream_name not in streams.keys():
        return

    schema = streams.get(stream_name).schema.to_dict()
    singer.write_schema(stream_name, schema, stream_obj.key_properties)

    start_time = get_activity_start(stream_name, state, config)
    end_time = get_activity_stop(start_time)

    bookmark = start_time
    with singer.metrics.record_counter(stream_name) as counter:
        for id in ids:
            activities = []
            page = 1
            params = {
                "start_time": start_time.strftime("%Y-%m-%dT%H:%M:%S"),
                "end_time": end_time.strftime("%Y-%m-%dT%H:%M:%S"),
                "per_page": 100,
                "page": page
            }
            url = os.path.join(stream_obj.endpoint, id)
            response = client.get_request(url, params=params)
            data = response.get('activities', [])
            activities.extend(data)
            while data:
                page = page + 1
                params['page'] = page
                response = client.get_request(url, params=params)
                data = response.get('activities', [])
                activities.extend(data)

            with Transformer() as transformer:
                for activity in activities:
                    record = {
                        "user_id": id,
                        "id": activity.get('id'),
                        "created_at": activity.get('created_at'),
                        "raw_json": activity
                    }
                    records_tf = transformer.transform(data=record, schema=schema)
                    singer.write_record(stream_name, records_tf)
                    try:
                        if convert_to_utc(record["created_at"]) > bookmark:
                            bookmark = convert_to_utc(record["created_at"])
                    except:
                        LOGGER.error("failed to get created_at")
                        raise
                    counter.increment()
        LOGGER.info('FINISHED Syncing stream: Stream Name:[{}], Total Records:[{}]'
                    .format(stream_name, counter.value))
        utils.update_state(state, stream_name, bookmark)
        singer.write_state(state)


def do_sync(config, state, catalog, client):
    current_state = state
    selected_streams = {stream.stream: stream for stream in catalog.get_selected_streams(state)}
    LOGGER.info('Starting sync. Will sync these streams: %s', selected_streams.keys())
    # for stream in streams:
        # LOGGER.info('Syncing %s', stream.name)
        # STATE["this_stream"] = stream.name
        # stream.sync()  # pylint: disable=not-callable
    # STATE["this_stream"] = None
    sync_applicants(selected_streams, client, config, current_state)
    sync_funnels(selected_streams, client)
    sync_users(selected_streams, client, config, current_state)
    LOGGER.info("Sync completed")