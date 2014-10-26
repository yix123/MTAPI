# MTA Realtime API JSON Proxy

MtaSanitizer is a small HTTP server that converts the [MTA's realtime subway feed](http://datamine.mta.info/feed-documentation) from [Protocol Buffers/GTFS](https://developers.google.com/transit/gtfs/) to JSON. The app also adds caching and makes it possible to retreive information by location and train line. 

## Active Development

This project is under active development and may contain bugs. Feedback is very welcome.

## Endpoints

Most calls return one or more `station` objects, which look like this:

```javascript
{
    "realtime": {
        "N": [
            {
                "route": "6",
                "time": "2014-08-29T14:00:55-04:00"
            },
            {
                "route": "6X",
                "time": "2014-08-29T14:10:30-04:00"
            },
            ...
        ],
        "S": [
            {
                "route": "6",
                "time": "2014-08-29T14:04:14-04:00"
            },
            {
                "route": "6",
                "time": "2014-08-29T14:11:07-04:00"
            },
            ...
        ],
    },
    "schedule_estimates": {
        "B": 1200,      // estimate of train frequency, in seconds
        "D": 1140,      // estimates based on MTA timetables
        "F": 570
    }
    "id": 123,
    "location": [
        40.725606,
        -73.9954315
    ],
    "name": "Broadway-Lafayette St / Bleecker St",
    "routes": [
        "6",
        "B",
        "D",
        "F",
        "M"
    ],
    "stops": {
        "637": [
            40.725915,
            -73.994659
        ],
        "D21": [
            40.725297,
            -73.996204
        ]
    }
}
```

- **/by-location?lat=[latitude]&lon=[longitude]**  
Returns the 5 stations nearest the provided lat/lon pair.
```javascript
{
    "data": [
        station,
        station,
        ...
    ],
    "updated": "2014-08-29T15:27:27-04:00"
}
```

- **/by-route/[route]**  
Returns all stations on the provided train route.  
```javascript
{
    "data": [
        station,
        station,
        ...
    ],
    "updated": "2014-08-29T15:25:27-04:00"
}
```

- **/by-id/[id],[id],[id]...**  
Returns the stations with the provided IDs, in the order provided. IDs should be comma separated with no space characters.
```javascript
{
    "data": [
        station,
        station,
        ...
    ],
    "updated": "2014-08-29T15:25:27-04:00"
}
```

- **/routes**  
Lists available routes.  
```javascript
{
    "data": [
        "S",
        "L",
        "1",
        "3",
        "2",
        "5",
        "4",
        "6",
        ...
    ],
    "updated": "2014-08-29T15:09:57-04:00"
}
```

## Running the server

MtaSanitizer is a Flask app designed to run under Python 2.7.

1. Create a settings file. A sample is provided as `settings.cfg.sample`.
2. Set up your environment and install dependencies.  
`$ virtualenv .venv`  
`$ source .venv/bin/activate`  
`$ pip install -r requirements.txt`
3. Run the server  
`$ export MTA_SETTINGS=your_settings_file.cfg; python app.py`

This app makes use of Python threads. If running under uWSGI include the --enable-threads flag.

## Settings

- **MTA_KEY** (required)  
The API key provided at http://datamine.mta.info/user/register  
*default: None*

- **STATIONS_FILE** (required)  
Path to the JSON file containing station information. See [Generating a Stations File](#generating-a-stations-file) for more info.  
*default: None*

- **CROSS_ORIGIN**    
Add [CORS](http://enable-cors.org/) headers to the HTTP output.  
*default: "&#42;" when in debug mode, None otherwise*

- **MAX_TRAINS**  
Limits the number of trains that will be listed for each station.  
*default: 10*

- **MAX_MINUTES**  
Limits how far in advance train information will be listed.  
*default: 30*

- **CACHE_SECONDS**  
How frequently the app will request fresh data from the MTA API.  
*default: 60*

- **THREADED**  
Enable background data refresh. This will prevent requests from hanging while new data is retreived from the MTA API.  
*default: True*

- **DEBUG**  
Standard Flask option. Will enabled enhanced logging and wildcard CORS headers.  
*default: False*

## Generating a Stations File

The MTA provides several static data files about the subway system but none include canonical information about each station. MtaSanitizer includes a script that will parse the `stops.txt` dataset provided by the MTA and attempt to group the different train stops into subway stations. MtaSanitizer will use this JSON file for station names and locations. The grouping is not perfect and editing the resulting JSON file is encouraged.

Usage: `$ python generate_stations_file.py stops.txt stations.json --threshold=0.0025`

## Help

Submit a [GitHub Issues request](https://github.com/jonthornton/MtaSanitizer/issues). 

## License

The project is made available under the MIT license.
