import gtfs_realtime_pb2, nyct_subway_pb2
import urllib2, contextlib, datetime, copy
from operator import itemgetter
from pytz import timezone
import threading, time
import csv, math, json
import logging
import google.protobuf.message
from collections import defaultdict
import bisect

def distance(p1, p2):
    return math.sqrt((p2[0] - p1[0])**2 + (p2[1] - p1[1])**2)

def timestring2seconds(timestring):
    parts = timestring.split(':')
    return int(parts[0]) * 3600 + int(parts[1]) * 60 + int(float(parts[2]))

def seconds2timestring(secs):
    return str(secs)
    mins, secs = divmod(secs, 60)
    hours, mins = divmod(mins, 60)
    return '%02d:%02d:%02d' % (hours, mins, secs)

class StaticDataset(object):
    DOWS = ['SAT', 'SUN', 'WKD']
    INDEXED_DOWS = ['WKD', 'WKD', 'WKD', 'WKD', 'WKD', 'SAT', 'SUN']
    ONE_DAY = 86400

    def __init__(self):
        self._sorted = True
        self._data = {
            'SAT': {
                'N':[],
                'S':[]
            },
            'SUN': {
                'N':[],
                'S':[]
            },
            'WKD': {
                'N':[],
                'S':[]
            }
        }

    def add_train(self, direction, dow, time):
        self._sorted = False
        if isinstance(time, str):
            time = timestring2seconds(time)

        if time > self.ONE_DAY:
            time = time % self.ONE_DAY
            idx = (self.DOWS.index(dow) + 1) % len(self.DOWS)
            dow = self.DOWS[idx]

        self._data[dow][direction].append(time)

    def get_frequency(self, direction, time=datetime.datetime.now()):
        if not self._sorted:
            self._sort()

        dow = self.INDEXED_DOWS[time.weekday()]
        start_seconds = timestring2seconds(str(time.time()));
        start_idx = bisect.bisect_left(self._data[dow][direction], start_seconds)

        if start_idx >= len(self._data[dow][direction]):
            start_idx = 0
            dow = self.INDEXED_DOWS[(time.weekday() + 1) % 7]

        if not self._data[dow][direction]:
            return None

        start = self._data[dow][direction][start_idx]

        try:
            end = self._data[dow][direction][start_idx + 1]
        except IndexError as e:
            # look at tomorrow's schedule
            next_dow = self.INDEXED_DOWS[(time.weekday() + 1) % 7]
            end = self._data[next_dow][direction][0] + self.ONE_DAY

        return end - start

    def _sort(self):
        for day in self.DOWS:
            for direction in ['N', 'S']:
                self._data[day][direction].sort()


class MtApi(object):
    _LOCK_TIMEOUT = 300
    _tz = timezone('US/Eastern')

    def __init__(self, key, stations_file, stop_times_file, expires_seconds=None, max_trains=10, max_minutes=30, threaded=False):
        self._KEY = key
        self._MAX_TRAINS = max_trains
        self._MAX_MINUTES = max_minutes
        self._EXPIRES_SECONDS = expires_seconds
        self._THREADED = threaded
        self._read_lock = threading.RLock()
        self._update_lock = threading.Lock()
        self._logger = logging.getLogger(__name__)

        # initialize the stations database
        try:
            with open(stations_file, 'rb') as f:
                self._stations = json.load(f)
                for idx, station in enumerate(self._stations):
                    station['id'] = idx
                    station['routes'] = set()

        except IOError as e:
            print 'Couldn\'t load stations file '+stations_file
            exit()

        self._stops = self.__class__._build_stops_index(self._stations)
        self._load_static_data(stop_times_file)
        self._update_realtime_data()

        if self._THREADED:
            self._start_timer()

    def _load_static_data(self, stop_times_file):
        self._logger.info('loading static train data')

        self._routes = defaultdict(set)
        self._static_data = defaultdict(lambda:
                defaultdict(StaticDataset)
            )

        with open(stop_times_file, 'rb') as f:
            reader = csv.DictReader(f)
            for row in reader:
                stop_id = row['stop_id'][:3]
                route_id = row['trip_id'][20]
                dow = row['trip_id'][9:12].upper() # MTA data is inconsistent on capitalization
                direction = row['stop_id'][3]
                self._static_data[stop_id][route_id].add_train(direction, dow, row['departure_time'])
                self._routes[route_id].add(stop_id)

        for stop_id in self._static_data:
            routes = self._static_data[stop_id].keys()
            station = self._stops[stop_id]
            station['routes'].update(routes)


    def _start_timer(self):
        self._logger.info('Starting update thread...')
        self._timer_thread = threading.Thread(target=self._update_timer)
        self._timer_thread.daemon = True
        self._timer_thread.start()

    def _update_timer(self):
        while True:
            time.sleep(self._EXPIRES_SECONDS)
            self._update_thread = threading.Thread(target=self._update_realtime_data)
            self._update_thread.start()

    @staticmethod
    def _build_stops_index(stations):
        stops = {}
        for station in stations:
            for stop_id in station['stops'].keys():
                stops[stop_id] = station

        return stops

    def _update_realtime_data(self):
        if not self._update_lock.acquire(False):

            self._logger.info('Update locked!')

            lock_age = datetime.datetime.now() - self._update_lock_time
            if lock_age.total_seconds() > self._LOCK_TIMEOUT:
                self._update_lock = threading.Lock()
                self._logger.info('Cleared expired update lock')

            return

        self._update_lock_time = datetime.datetime.now()
        self._logger.info('updating...')

        realtime_data = {}

        feed_urls = [
            'http://datamine.mta.info/mta_esi.php?feed_id=1&key='+self._KEY,
            'http://datamine.mta.info/mta_esi.php?feed_id=2&key='+self._KEY
        ]

        for feed_url in feed_urls:
            mta_data = gtfs_realtime_pb2.FeedMessage()
            try:
                with contextlib.closing(urllib2.urlopen(feed_url)) as r:
                    data = r.read()
                    mta_data.ParseFromString(data)

            except (urllib2.URLError, google.protobuf.message.DecodeError) as e:
                self._logger.error('Couldn\'t connect to MTA server: ' + str(e))
                self._update_lock.release()
                return

            self.last_update = datetime.datetime.fromtimestamp(mta_data.header.timestamp, self._tz)
            self._MAX_TIME = self.last_update + datetime.timedelta(minutes = self._MAX_MINUTES)

            for entity in mta_data.entity:
                if entity.trip_update:
                    for update in entity.trip_update.stop_time_update:
                        time = update.arrival.time
                        if time == 0:
                            time = update.departure.time

                        time = datetime.datetime.fromtimestamp(time, self._tz)
                        if time < self.last_update or time > self._MAX_TIME:
                            continue

                        route_id = entity.trip_update.trip.route_id
                        if route_id == 'GS':
                            route_id = 'S'

                        stop_id = str(update.stop_id[:3])

                        stop = realtime_data.setdefault(stop_id, {
                                'N': [],
                                'S': [],
                                'routes': set()
                            })

                        stop['routes'].add(route_id)

                        direction = update.stop_id[3]
                        stop[direction].append({
                            'route': route_id,
                            'time': time
                        })

        # update station objects
        # create working copy for thread safety
        stations = copy.deepcopy(self._stations)
        stops = self.__class__._build_stops_index(stations)

        for station in stations:
            station['schedule_estimates'] = {}
            for stop_id in station['stops']:
                for route_id in self._static_data[stop_id]:
                    freq = self._static_data[stop_id][route_id].get_frequency('N')
                    if freq:
                        station['schedule_estimates'][route_id] = freq

            station['realtime'] = {
                    'N': [],
                    'S': [],
                    'routes': set()
                }
            for direction in ['N', 'S']:
                trains = []
                for stop_id in station['stops']:
                    if stop_id in realtime_data:
                        trains.extend(realtime_data[stop_id][direction])
                        station['realtime']['routes'].update(realtime_data[stop_id]['routes'])

                if trains:
                    station['realtime'][direction] = sorted(trains, key=itemgetter('time'))[:self._MAX_TRAINS]

        with self._read_lock:
            self._stops = stops
            self._stations = stations

        self._update_lock.release()

    def last_update(self):
        return self.last_update

    def get_by_point(self, point, limit=5):
        if self.is_expired():
            self.update_realtime_data()

        with self._read_lock:
            sortable_stations = copy.deepcopy(self._stations)

        sortable_stations.sort(key=lambda x: distance(x['location'], point))
        return sortable_stations[:limit]

    def get_routes(self):
        return self._routes.keys()

    def get_by_route(self, route):
        if self.is_expired():
            self.update_realtime_data()

        with self._read_lock:
            out = [ self._stops[k] for k in self._routes[route] ]

        out.sort(key=lambda x: x['name'])

        return out

    def get_by_id(self, ids):
        if self.is_expired():
            self.update_realtime_data()

        with self._read_lock:
            out = [ self._stations[k] for k in ids ]

        return out

    def is_expired(self):
        if self._THREADED:
            # check that the update thread is still running
            if not self._timer_thread.is_alive():
                self._start_timer()
                return False

        elif self._EXPIRES_SECONDS:
            age = datetime.datetime.now(self._tz) - self.last_update
            return age.total_seconds() > self._EXPIRES_SECONDS
        else:
            return False
