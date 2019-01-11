from datetime import datetime, timedelta

import singer
from singer import metrics, metadata, Transformer
from singer.utils import strptime_to_utc, strftime, now

from tap_shiphero.discover import PKS

LOGGER = singer.get_logger()

DEPRECATED_PRODUCT_FIELDS = ['customs_value', 'value_currency', 'price', 'value']

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

def sync_products(client, catalog, state, start_date, end_date, stream_id, stream_config):
    stream_id = 'products'

    write_schema(catalog, stream_id)

    last_date = get_bookmark(state, stream_id, start_date)

    def products_transform(record):
        out = {}
        for key, value in record.items():
            if key not in DEPRECATED_PRODUCT_FIELDS:
                out[key] = value
        return out

    page = 1
    limit = 200
    num_results = None
    max_datetime = last_date
    while is_next_page(limit, num_results):
        LOGGER.info('Sycing products - page {}'.format(page))

        updated_min = strptime_to_utc(last_date).strftime('%Y-%m-%d')
        data = client.get(
            '/get-product/',
            params={
                'updated_at_min': updated_min,
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

            persist_records(catalog, stream_id, map(products_transform, records))
        else:
            num_results = 0

        set_bookmark(state, stream_id, max_datetime)

def sync_vendors(client, catalog, state, start_date, end_date, stream_id, stream_config):
    stream_id = 'vendors'

    LOGGER.info('Syncing all vendors')

    write_schema(catalog, stream_id)

    data = client.get(
        '/list-vendors/',
        endpoint=stream_id)

    records = data['vendors']

    persist_records(catalog, stream_id, records)

def sync_daily(client, catalog, state, start_date, end_date, stream_id, stream_config):
    write_schema(catalog, stream_id)

    last_date = get_bookmark(state, stream_id, start_date)
    path = stream_config['path']
    params = stream_config.get('params', {})

    if end_date:
        end_date = strptime_to_utc(end_date)
    else:
        end_date = now()

    updated_from = strptime_to_utc(last_date)

    if updated_from > end_date:
        raise Exception('{} start_date is greater than end_date'.format(stream_id))

    limit = 100
    while updated_from < end_date:
        page = 1
        num_results = None
        updated_to = updated_from + timedelta(days=1)
        updated_from_str = updated_from.strftime('%Y-%m-%d')
        updated_to_str = updated_to.strftime('%Y-%m-%d')
        while is_next_page(limit, num_results):
            LOGGER.info('Syncing {} from: {} to: {} - page {}'.format(
                stream_id,
                updated_from_str,
                updated_to_str,
                page))

            params[stream_config['from_col']] = updated_from_str
            params[stream_config['to_col']] = updated_to_str

            params['page'] = page

            data = client.get(
                path,
                params=params,
                endpoint=stream_id)

            records = stream_config['get_records'](data)

            num_results = len(records)

            if num_results > 0:
                if num_results == limit:
                    page += 1

                persist_records(catalog, stream_id, records)

        bookmark_date = updated_to
        if bookmark_date > now():
            bookmark_date = now()
        set_bookmark(state, stream_id, bookmark_date.isoformat())

        updated_from = updated_to

def order_get_records(data):
    records = data.get('results', [])
    if records == 'Order not found':
        return []
    return records

def shipments_get_records(data):
    raw_shipments = data.get('orders', {}).get('results', {})
    records = []
    for shipment_id, shipment in raw_shipments.items():
        records.append({'shipment_id': shipment_id, **shipment})
    return records

def should_sync_stream(last_stream, selected_streams, stream_name):
    if last_stream == stream_name or \
       (last_stream is None and stream_name in selected_streams):
        return True
    return False

def set_current_stream(state, stream_name):
    state['current_stream'] = stream_name
    singer.write_state(state)

def sync(client, catalog, state, start_date, end_date):
    selected_streams = get_selected_streams(catalog)

    streams = {
        'products': {
            'sync_fn': sync_products
        },
        'vendors': {
            'sync_fn': sync_vendors
        },
        'orders': {
            'sync_fn': sync_daily,
            'path': '/get-orders/',
            'params': {
                'sort': 'updated',
                'sort_direction': 'asc',
                'all_orders': 1
            },
            'from_col': 'updated_from',
            'to_col': 'updated_to',
            'get_records': order_get_records
        },
        'shipments': {
            'sync_fn': sync_daily,
            'path': '/get-shipments/',
            'params': {
                'filter_on': 'shipment',
                'all_orders': 1
            },
            'from_col': 'from',
            'to_col': 'to',
            'get_records': shipments_get_records
        }
    }

    last_stream = state.get('current_stream')

    for stream_id, stream_config in streams.items():
        if should_sync_stream(last_stream, selected_streams, stream_id):
            last_stream = None
            set_current_stream(state, stream_id)
            stream_config['sync_fn'](client,
                                     catalog,
                                     state,
                                     start_date,
                                     end_date,
                                     stream_id,
                                     stream_config)

    set_current_stream(state, None)
