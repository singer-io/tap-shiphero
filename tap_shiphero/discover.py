import os
import json
from copy import deepcopy

from singer.catalog import Catalog, CatalogEntry, Schema
from singer import metadata

SCHEMAS = None
FIELD_METADATA = None

PKS = {
    'orders': ['id'],
    'products': ['id'],
    'vendors': ['vendor_id'],
    'shipments': ['shipment_id']
}
REPLICATION_KEYS = {
    'products': ['updated_at']
}

def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)

def get_schemas():
    global SCHEMAS, FIELD_METADATA

    if SCHEMAS:
        return SCHEMAS, FIELD_METADATA

    SCHEMAS = {}
    FIELD_METADATA = {}

    schemas_path = get_abs_path('schemas')

    file_names = [f for f in os.listdir(schemas_path)
                  if os.path.isfile(os.path.join(schemas_path, f))]

    file_names = sorted(file_names)

    for file_name in file_names:
        if file_name.endswith('.json'):
            stream_name = file_name[:-5]
            with open(os.path.join(schemas_path, file_name)) as data_file:
                schema = json.load(data_file)

            SCHEMAS[stream_name] = schema
            pk = PKS[stream_name]
            replication_key = REPLICATION_KEYS.get(stream_name, [])
            schema_metadata = metadata.new()

            for prop, json_schema in schema['properties'].items():
                if prop in pk:
                    inclusion = 'automatic'
                elif prop in replication_key:
                    inclusion = 'automatic'
                else:
                    inclusion = 'available'
                schema_metadata = metadata.write(
                    schema_metadata,
                    ('properties', prop),
                    'inclusion',
                    inclusion
                )

            FIELD_METADATA[stream_name] = metadata.to_list(schema_metadata)

    return SCHEMAS, FIELD_METADATA

def discover():
    schemas, field_metadata = get_schemas()
    catalog = Catalog([])

    for stream_name, schema_dict in schemas.items():
        schema = Schema.from_dict(schema_dict)
        metadata = field_metadata[stream_name]
        pk = PKS[stream_name]

        catalog.streams.append(CatalogEntry(
            stream=stream_name,
            tap_stream_id=stream_name,
            key_properties=pk,
            schema=schema,
            metadata=metadata
        ))

    return catalog
