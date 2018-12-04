import pendulum
import singer
from singer import metrics, metadata, Transformer

from tap_shiphero.discover import PKS

LOGGER = singer.get_logger()

def write_schema(catalog, stream_id):
    stream = catalog.get_stream(stream_id)
    schema = stream.schema.to_dict()
    key_properties = PKS[stream_id]
    singer.write_schema(stream_id, schema, key_properties)

def persist_records(catalog, stream_id, records):
    if records: # check for empty array
        stream = catalog.get_stream(stream_id)
        schema = stream.schema.to_dict()
        stream_metadata = metadata.to_map(stream.metadata)
        with metrics.record_counter(stream_id) as counter:
            for record in records:
                with Transformer() as transformer:
                    record = transformer.transform(record,
                                                   schema,
                                                   stream_metadata)
                singer.write_record(stream_id, record)
                counter.increment()

def get_bookmark(state, stream_id, default):
    return (
        state
        .get('bookmarks', {})
        .get(stream_id, default)
    )

def set_bookmark(state, stream_id, value):
    if 'bookmarks' not in state:
        state['bookmarks'] = {}
    state['bookmarks'][stream_id] = value
    singer.write_state(state)

def get_selected_streams(catalog):
    selected_streams = set()
    for stream in catalog.streams:
        mdata = metadata.to_map(stream.metadata)
        root_metadata = mdata.get(())
        if root_metadata and root_metadata.get('selected') is True:
            selected_streams.add(stream.tap_stream_id)
    return list(selected_streams)

def is_next_page(limit, num_results):
    return num_results is None or num_results == limit

def sync_products(client, catalog, state, start_date):
    stream_id = 'products'

    write_schema(catalog, stream_id)

    last_date = get_bookmark(state, stream_id, start_date)

    page = 1
    limit = 200
    num_results = None
    max_datetime = last_date
    while is_next_page(limit, num_results):
        data = client.get(
            '/get-product/',
            params={
                'updated_at_min': pendulum.parse(last_date).to_datetime_string(),
                'page': page,
                'count': limit
            },
            endpoint=stream_id)
        page += 1

        records = data['products']

        if records:
            num_results = len(records)

            max_page_datetime = max(map(lambda x: x['updated_at'], records))
            if max_page_datetime > max_datetime:
                max_datetime = max_page_datetime

            persist_records(catalog, stream_id, records)
        else:
            num_results = 0

        set_bookmark(state, stream_id, max_datetime)

def sync_orders(client, catalog, state, start_date):
    stream_id = 'orders'

    write_schema(catalog, stream_id)

    last_date = get_bookmark(state, stream_id, start_date)

    page = 1
    limit = 100
    num_results = None
    max_datetime = last_date
    while is_next_page(limit, num_results):
        data = client.get(
            '/get-orders/',
            params={
                'updated_from': pendulum.parse(last_date).to_date_string(),
                'updated_to': pendulum.now('UTC').to_date_string(),
                'page': page,
                'sort': 'updated',
                'sort_direction': 'asc',
                'all_orders': 1
            },
            endpoint=stream_id)
        page += 1

        records = data['results']

        if records:
            num_results = len(records)

            max_page_datetime = max(map(lambda x: x['updated_at'], records))
            if max_page_datetime > max_datetime:
                max_datetime = max_page_datetime

            persist_records(catalog, stream_id, records)
        else:
            num_results = 0

        set_bookmark(state, stream_id, max_datetime)

def sync_vendors(client, catalog):
    stream_id = 'vendors'

    write_schema(catalog, stream_id)

    data = client.get(
        '/list-vendors/',
        endpoint=stream_id)

    records = data['vendors']

    persist_records(catalog, stream_id, records)

def sync_shipments(client, catalog, state, start_date):
    stream_id = 'shipments'

    # write_schema(catalog, stream_id)

    last_date_raw = get_bookmark(state, stream_id, start_date)
    last_date = pendulum.parse(last_date_raw).to_date_string()

    page = 1
    limit = 100
    num_results = None
    max_datetime = last_date
    while is_next_page(limit, num_results):
        data = client.get(
            '/get-shipments/',
            params={
                'from': last_date,
                'to': pendulum.now('UTC').to_date_string(),
                'page': page,
                'filter_on': 'shipment',
                'all_orders': 1
            },
            endpoint=stream_id)
        page += 1

        raw_shipments = data.get('orders', {}).get('results', {})
        records = []
        for shipment_id, shipment in raw_shipments.items():
            records.append({'shipment_id': shipment_id, **shipment})
            if shipment['date'] > max_datetime:
                max_datetime = shipment['date']

        num_results = len(records)

        persist_records(catalog, stream_id, records)

    set_bookmark(state, stream_id, max_datetime)

def sync(client, catalog, state, start_date):
    selected_streams = get_selected_streams(catalog)

    ## TODO: Do not include customs_value, value_currency, price, value in the product object schema but keep them in the warehouse object schema.

    if 'products' in selected_streams:
        sync_products(client, catalog, state, start_date)

    if 'orders' in selected_streams:
        sync_orders(client, catalog, state, start_date)

    if 'vendors' in selected_streams:
        sync_vendors(client, catalog)

    if 'shipments' in selected_streams:
        sync_shipments(client, catalog, state, start_date)
