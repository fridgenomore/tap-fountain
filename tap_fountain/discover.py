import os
import json

from singer import Schema, metadata
from singer.catalog import Catalog, CatalogEntry
from tap_fountain.streams import STREAMS


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


# Load schemas from schemas folder
def load_schemas():
    schemas = {}

    for filename in os.listdir(get_abs_path("schemas")):
        path = get_abs_path("schemas") + "/" + filename
        file_raw = filename.replace(".json", "")
        with open(path) as file:
            schemas[file_raw] = json.load(file)

    return schemas


def discover():
    raw_schemas = load_schemas()
    catalog = Catalog([])

    for stream_name, schema_dict in raw_schemas.items():
        stream = STREAMS.get(stream_name)
        mdata = metadata.get_standard_metadata(
            schema=schema_dict,
            key_properties=stream.key_properties,
            valid_replication_keys=stream.replication_keys,
            replication_method=stream.replication_method
        )
        schema_dict['selected'] = True
        # schema = Schema.from_dict(schema_dict)
        # print(schema_obj)
        # stream_metadata = []
        # catalog_entries.append(
        #     CatalogEntry(
        #         tap_stream_id=stream.stream_name,
        #         stream=stream.stream_name,
        #         schema=schema,
        #         key_properties=stream.key_properties,
        #         metadata=stream_metadata,
        #         replication_key=stream.replication_keys
        #     )
        # )

        catalog.streams.append(CatalogEntry(
            stream=stream.stream_name,
            tap_stream_id=stream.stream_name,
            key_properties=stream.key_properties,
            schema=Schema.from_dict(schema_dict),
            metadata=mdata
        ))
    return catalog
