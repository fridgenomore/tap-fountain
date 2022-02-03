import datetime

import attr
import singer

LOGGER = singer.get_logger()


# class Stream:
#     def __init__(self, client, config, state):
#         self.client = client
#         self.config = config
#         self.state = state
#
#
# class Applicants(Stream):
#     stream_id = 'applicants'
#     stream_name = 'applicants'
#     endpoint = 'https://api.fountain.com/v2/applicants'
#     key_properties = ["id"]
#     replication_method = "INCREMENTAL"
#     replication_keys = ["updated_at"]
#
#     applicants_ids = []
#
#     def get_applicants_ids(self):
#         if Applicants.applicants_ids:
#             for id in Applicants.applicants_ids:
#                 yield id
#         else:
#             records = self.client.get(self.endpoint)
#             for rec in records.get('results'):
#                 Applicants.applicants_ids.append(rec['eid'])
#                 yield rec['eid']
#
#     def sync(self):
#         records = self.client.get(self.endpoint)
#         for rec in records.get('results'):
#             yield rec


@attr.s
class Stream(object):
    stream_name = attr.ib()
    key_properties = attr.ib()
    replication_method = attr.ib()
    replication_keys = attr.ib()
    endpoint = attr.ib()


STREAMS = {
    "applicants": Stream("applicants", ["id"], "INCREMENTAL", ["updated_at"], "https://api.fountain.com/v2/applicants"),
    "applicant_info": Stream("applicant_info", ["id"], "INCREMENTAL", [], "https://api.fountain.com/v2/applicants"),
    "transitions": Stream("transitions", ["applicant_id"], "INCREMENTAL", [], "https://api.fountain.com/v2/applicants/transitions"),
    "funnels": Stream("funnels", ["id"], "FULL_TABLE", [], "https://api.fountain.com/v2/funnels"),
    "users": Stream("users", ["id"], "FULL_TABLE", [], "https://api.fountain.com/v2/users"),
    "user_activities": Stream("user_activities", ["id"], "INCREMENTAL", [], "https://api.fountain.com/v2/user_activities")
}
