# tap-shiphero
This is a Singer tap that produces JSON-formatted data following the Singer
spec. It pulls raw data from the ShipHero API, extracting:
- Orders
- Products
- Shipments
- Vendors

## Pagination
The API returns up to 100 objects at a time. In order to get everything, we
paginate until we get an empty response back.

## Bookmark Structure
This is what the `state.json` looks like:
```
{
  "bookmarks": {
    "stream_id_1" : {
      "datetime": "2019-01-01T00:00:00Z",
      "page": 75
    },
    "stream_id_2" : {
      "datetime": "2019-01-01T00:00:00Z",
      "page": 75
    },
    ...
  }
  ...
}
```

`datetime` is for the last timestamp we saw. `page` is for the last page we
requested and completed.

---

Copyright &copy; 2018 Stitch
