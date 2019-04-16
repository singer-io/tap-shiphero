from datetime import datetime, timedelta

import singer
from singer import metrics, metadata, Transformer
from singer.utils import strptime_to_utc, strftime, now
from singer import bookmarks
from singer import utils

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

    # Rip this out once all bookmarks are converted
    if isinstance(state.get('bookmarks',{}).get(stream_id), str):
        # Old style bookmark found. Use it and delete it
        last_date = state['bookmarks'].pop(stream_id)

        # Write this bookmark in the new style
        bookmarks.write_bookmark(state, stream_id, 'datetime', last_date)
        singer.write_state(state)

    last_date = bookmarks.get_bookmark(state, stream_id, 'datetime', start_date)

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

        bookmarks.write_bookmark(state, stream_id, 'datetime', max_datetime)
        singer.write_state(state)

def sync_vendors(client, catalog, state, start_date, end_date, stream_id, stream_config):
    stream_id = 'vendors'

    LOGGER.info('Syncing all vendors')

    write_schema(catalog, stream_id)

    data = client.get(
        '/list-vendors/',
        endpoint=stream_id)

    records = data['vendors']

    persist_records(catalog, stream_id, records)

def sync_a_day(stream_id, path, params, start_ymd, end_ymd,
               func_get_records, client, catalog, state, window_end):
    """Sync a single day's worth of data, paginating through as many times as
    necessary.

    Loop Guide
    1. Make the API call
    2. Process the response to get a list of records
    3. Persist the records
    4. Update the call parameters

    When we get back an empty result set, log a message that we are done,
    reset the page bookmark to page 1, and update the datetime bookmark to
    the day we just finished syncing.

    The length of the response can vary, but once we get back an empty
    response, it seems that we have requested all objects. Do not trust
    the `total` field on the response, it has varied from the actual total
    by O(10). The path to that field is response.json()['orders']['total'],
    where the response is in the request function in `client.py`.

    """
    # Page until we're done
    while True:
        page_bookmark = bookmarks.get_bookmark(state, stream_id, 'page', 1)
        # Set the page to start paginating on
        params['page'] = page_bookmark
        LOGGER.info('Syncing %s from: %s to: %s - page %s',
                    stream_id,
                    start_ymd,
                    end_ymd,
                    params['page'])
        data = client.get(path, params=params, endpoint=stream_id)
        records = func_get_records(data)
        if not records:
            LOGGER.info(
                'Done daily pagination for endpoint %s at page %d',
                stream_id,
                params['page'])
            bookmarks.write_bookmark(state, stream_id, 'page', 1)
            bookmarks.write_bookmark(state, stream_id, 'datetime', utils.strftime(window_end))
            singer.write_state(state)
            break
        else:
            persist_records(catalog, stream_id, records)
            singer.write_state(state)
            bookmarks.write_bookmark(state, stream_id, 'page', params['page'] + 1)

def sync_daily(client, catalog, state, start_date, end_date, stream_id, stream_config):
    """Syncs a given date range, bookmarking after each day.

    Argument Types:
      client: [ShipHeroClient]
      catalog: [Dictionary]
      state: [Dictionary]
      start_date: [String, UTC datetime]
      end_date: Optional, non-inclusive day [String, UTC datetime]
      stream_id: [String]
      stream_config: [Dictionary]
    """
    write_schema(catalog, stream_id)

    #######################################################################
    ### Set up datetime versions of the start_date, end_date
    #######################################################################

    # Rip this out once all bookmarks are converted
    if isinstance(state.get('bookmarks',{}).get(stream_id), str):
        # Old style bookmark found. Use it and delete it
        old_style_bookmark = state['bookmarks'].pop(stream_id)

        # Write this bookmark in the new style
        bookmarks.write_bookmark(state,
                                 stream_id,
                                 'datetime',
                                 old_style_bookmark)

    start_date_bookmark = bookmarks.get_bookmark(state,
                                                 stream_id,
                                                 'datetime',
                                                 start_date)

    start_date_dt = strptime_to_utc(start_date_bookmark)

    # Since end_date is optional in the top level sync
    if end_date:
        end_date_dt = strptime_to_utc(end_date)
    else:
        end_date_dt = utils.now()

    if start_date_dt > end_date_dt:
        raise Exception('{} start_date is greater than end_date'.format(stream_id))

    #######################################################################
    ### Sync data by day
    #######################################################################

    # Extract params from config
    path = stream_config['path']
    params = stream_config.get('params', {})
    from_col = stream_config['from_col']
    to_col = stream_config['to_col']
    records_fn = stream_config['get_records']

    page_bookmark = bookmarks.get_bookmark(state, stream_id, 'page', 1)

    # Set the page to start paginating on
    params['page'] = page_bookmark

    # Loop over all the days
    while start_date_dt != end_date_dt:
        window_end = start_date_dt + timedelta(days=1)
        if window_end > now() or window_end > end_date_dt:
            window_end = end_date_dt

        # The API expects the dates in %Y-%m-%d
        start_ymd = start_date_dt.strftime('%Y-%m-%d')
        end_ymd = window_end.strftime('%Y-%m-%d')

        params.update({from_col: start_ymd,
                       to_col: end_ymd})

        sync_a_day(stream_id, path, params, start_ymd, end_ymd,
                   records_fn, client, catalog, state, window_end)

        start_date_dt = window_end

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
