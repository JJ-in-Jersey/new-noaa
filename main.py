import time
from datetime import datetime as dt
from dateutil.relativedelta import relativedelta
from scipy.interpolate import CubicSpline
from pathlib import Path
import pandas as pd
import requests
from io import StringIO
from os import mkdir
from shutil import copyfile
from math import isnan

from tt_file_tools.file_tools import SoupFromXMLResponse, SoupFromXMLFile, write_df, read_df, print_file_exists
from tt_job_manager.job_manager import JobManager, Job


class CurrentWaypoint:

    template_path = Path('/users/jason/Documents/OpenCPN/mooring.gpx')

    def __init__(self, my_row: pd.Series, my_folder: Path):

        self.DataNotAvailable = False
        self.NotMonotonic = False

        self.soup = None
        self.id = my_row['id']
        self.bin = int(my_row['min_bin']) if not isnan(my_row['min_bin']) else None
        self.name = my_row['name'].strip()
        self.type = my_row['type']
        self.folder = my_folder

        tree = SoupFromXMLFile(CurrentWaypoint.template_path).tree
        tree.find('name').string = my_row['name'].strip()
        tree.find('wpt')['lat'] = my_row['lat']
        tree.find('wpt')['lon'] = my_row['lng']

        if my_row['type'] == 'S':  # subordinate
            tree.find('sym').string = 'Symbol-Pin-Green'
        elif my_row['type'] == 'H':  # harmonic
            tree.find('sym').string = 'Symbol-Spot-Green'
        elif my_row['type'] == 'W':  # weak & variable
            tree.find('sym').string = 'Symbol-Pin-Yellow'
        else:
            print(self.id, my_row['type'])

        id_tag = tree.new_tag('id')
        id_tag.string = self.id
        tree.find('name').insert_before(id_tag)

        desc_tag = tree.new_tag('desc')
        desc_tag.string = self.id + ' - ' + str(int(self.bin)) + ' - ' + my_row['type'] if bool(pd.isna(my_row)['min_bin']) is False else self.id + ' - ' + my_row['type']
        tree.find('name').insert_after(desc_tag)

        self.soup = tree


class OneMonth:

    def __init__(self, month: int, year: int, station_id: str, station_bin: int = None, interval_time: int = 1):

        self.frame = None
        if month < 1 or month > 12:
            raise ValueError

        start = dt(year, month, 1)
        end = start + relativedelta(months=1) - relativedelta(days=1)

        bin_no = ""
        header = "https://api.tidesandcurrents.noaa.gov/api/prod/datagetter?"
        begin_date = "&begin_date=" + start.strftime("%Y%m%d")  # yyyymmdd
        end_date = "&end_date=" + end.strftime("%Y%m%d")  # yyyymmdd
        station = "&station=" + station_id  # station id string
        interval = "&interval=" + str(interval_time)
        if station_bin is not None:
            bin_no = "&bin=" + str(station_bin)
        footer = "&product=currents_predictions&time_zone=lst_ldt" + interval + "&units=english&format=csv" + bin_no
        my_request = header + begin_date + end_date + station + footer

        for _ in range(3):
            try:
                my_response = requests.get(my_request)
                my_response.raise_for_status()
                if 'predictions are not available' in my_response.content.decode():
                    raise DataNotAvailable(station_id)
                frame = pd.read_csv(StringIO(my_response.content.decode()))
                if frame.empty or frame.isna().all().all():
                    raise EmptyDataframe(station_id)
                self.frame = frame.rename(columns={heading: heading.strip() for heading in frame.columns.tolist()})
                self.frame.rename(columns={'Velocity_Major': 'velocity'}, inplace=True)
                break
            except requests.exceptions.RequestException as req_err:
                print(str(req_err))
                time.sleep(1)
            except DataNotAvailable as req_err:
                print(str(req_err))
                time.sleep(1)
            except EmptyDataframe as req_err:
                print(str(req_err))
                time.sleep(1)


class SixteenMonths:
    def __init__(self, year: int, station_code: str, station_bin: int = None):
        self.frame = None
        months = ([OneMonth(month, year - 1, station_code, station_bin) for month in range(11, 13)] +
                  [OneMonth(month, year, station_code, station_bin) for month in range(1, 13)] +
                  [OneMonth(month, year + 1, station_code, station_bin) for month in range(1, 3)])
        frames = [m.frame for m in months]
        self.frame = pd.concat(frames, axis=0, ignore_index=True)

        for f in range(len(frames)):
            del f
        for m in range(len(months)):
            del m


class RequestVelocityCSV:

    def __init__(self, year: int, waypoint: CurrentWaypoint):

        self.csv = waypoint.folder.joinpath('downloaded_frame.csv')
        waypoint_csv = waypoint.folder.joinpath('waypoint_velocity_frame.csv')

        if not print_file_exists(self.csv):
            sixteen_months = SixteenMonths(year, waypoint.id, waypoint.bin)
            downloaded_frame = sixteen_months.frame
            self.csv = print_file_exists(write_df(downloaded_frame, self.csv))
            if waypoint.type == "H":
                copyfile(self.csv, waypoint_csv)

            del downloaded_frame, sixteen_months


class RequestVelocityJob(Job):  # super -> job name, result key, function/object, arguments

    def execute(self): return super().execute()
    def execute_callback(self, result): return super().execute_callback(result)
    def error_callback(self, result): return super().error_callback(result)

    def __init__(self, year, waypoint: CurrentWaypoint):
        result_key = id(waypoint.id)
        arguments = tuple([year, waypoint])
        super().__init__(wp.id + ' ' + wp.name, result_key, RequestVelocityCSV, arguments)


class SplineCSV:

    def __init__(self, waypoint: CurrentWaypoint):

        downloaded_csv = waypoint.folder.joinpath('downloaded_frame.csv')
        waypoint_csv = waypoint.folder.joinpath('waypoint_velocity_frame.csv')
        downloaded_velocity = read_df(downloaded_csv)
        self.csv = waypoint.folder.joinpath('cubic_spline_frame.csv')

        try:
            if not downloaded_velocity['Time'].is_monotonic_increasing:
                raise NonMonotonic(row['id'])
            elif row['type'] == 'H':
                copyfile(downloaded_csv, waypoint_csv)
            elif row['type'] == 'S':
                if not print_file_exists(self.csv):
                    print_file_exists(write_df(CubicSplineVelocityFrame(downloaded_velocity).frame, self.csv))
                    copyfile(self.csv, waypoint_csv)
        except NonMonotonic as e:
            print(str(e))
            waypoint.NotMonotonic = True
            # for r in range(0, len(downloaded_velocity) - 1):
            #     if downloaded_velocity.loc[r]['timestamp'] >= downloaded_velocity.loc[r + 1]['timestamp']:
            #         print(f'Check value in row {r}')


class SplineJob(Job):  # super -> job name, result key, function/object, arguments

    def execute(self): return super().execute()
    def execute_callback(self, result): return super().execute_callback(result)
    def error_callback(self, result): return super().error_callback(result)

    def __init__(self, waypoint: CurrentWaypoint):
        result_key = id(waypoint.id)
        arguments = tuple([waypoint])
        super().__init__(wp.id + ' ' + wp.name, result_key, RequestVelocityCSV, arguments)


class CubicSplineVelocityFrame:

    def __init__(self, frame: pd.DataFrame):

        self.frame = None

        frame['datetime'] = pd.to_datetime(frame['Time'])
        frame['timestamp'] = frame['datetime'].apply(dt.timestamp).astype('int')

        if frame['timestamp'].is_monotonic_increasing:
            cs = CubicSpline(frame['timestamp'], frame['velocity'])
            start_date = frame['datetime'].iloc[0].date()
            start_date = dt.combine(start_date, dt.min.time())
            end_date = frame['datetime'].iloc[-1].date() + relativedelta(days=1)
            end_date = dt.combine(end_date, dt.min.time())
            minutes = int((end_date - start_date).total_seconds()/60)

            self.frame = pd.DataFrame({'datetime': [start_date + relativedelta(minutes=m) for m in range(0, minutes)]})
            self.frame['timestamp'] = self.frame['datetime'].apply(dt.timestamp).astype('int')
            self.frame['velocity'] = self.frame['timestamp'].apply(cs)
            self.frame['velocity'] = self.frame['velocity'].round(2)
        else:
            raise SystemExit(f'frame is not monotonically increasing')


class NonMonotonic(Exception):
    def __init__(self, station_id: str):
        self.message = 'Timestamps for ' + station_id + ' are not monotonically increasing'
        super().__init__(self.message)


class DataNotAvailable(Exception):
    def __init__(self, station_id: str):
        self.message = 'Currents predictions are not available from ' + station_id
        super().__init__(self.message)


class EmptyDataframe(Exception):
    def __init__(self, station_id: str):
        self.message = station_id + " dataframe is empty or all NaN"
        super().__init__(self.message)


def currents_fetch_stations():

    rows = None
    my_request = "https://api.tidesandcurrents.noaa.gov/mdapi/prod/webapi/stations.xml?type=currentpredictions&units=english"
    for _ in range(3):
        try:
            my_response = requests.get(my_request)
            my_response.raise_for_status()
            print(f'Requesting list of stations')
            stations_tree = SoupFromXMLResponse(StringIO(my_response.content.decode())).tree
            rows = [{'id': station.find_next('id').text, 'name': station.find_next('name').text, 'lat': float(station.find_next('lat').text),
                     'lng': float(station.find_next('lng').text), 'type': station.find_next('type').text} for station in stations_tree.find_all('Station')]
            rows = pd.DataFrame(rows).drop_duplicates().to_dict('records')

            for wp_row in rows:
                my_request = "https://api.tidesandcurrents.noaa.gov/mdapi/prod/webapi/stations/" + wp_row['id'] + "/bins.xml?units=english"
                for _ in range(3):
                    try:
                        my_response = requests.get(my_request)
                        my_response.raise_for_status()
                        print(f'Requesting bin number for {wp_row['id']}')
                        bins_tree = SoupFromXMLResponse(StringIO(my_response.content.decode())).tree
                        bin_count = int(bins_tree.find("nbr_of_bins").text)
                        if bin_count and bins_tree.find('Bin').find('depth') is not None:
                            bin_dict = {int(tag.num.text): float(tag.depth.text) for tag in bins_tree.find_all('Bin')}
                            wp_row['min_bin'] = min(bin_dict, key=bin_dict.get)
                        break
                    except requests.exceptions.RequestException as req_err:
                        print(str(req_err))
                        time.sleep(1)
            break
        except requests.exceptions.RequestException as req_err:
            print(str(req_err))
            time.sleep(1)

    return pd.DataFrame(rows).drop_duplicates()


if __name__ == '__main__':

    # fire up the job manager
    job_manager = JobManager()
    waypoint_dict = {}

    # set up folder structure
    folder = Path('/users/jason/Developer Workspace')
    GPX_folder = folder.joinpath('GPX')
    stations_folder = GPX_folder.joinpath('stations')
    OpenCPN_folder = GPX_folder.joinpath('Open_CPN')

    if not GPX_folder.exists():
        mkdir(GPX_folder)
    if not stations_folder.exists():
        mkdir(stations_folder)
    if not OpenCPN_folder.exists():
        mkdir(OpenCPN_folder)

    # request all the current stations from NOAA
    print(f'Requesting all NOAA current stations')
    stations_file = stations_folder.joinpath('stations.csv')
    if print_file_exists(stations_file):
        station_frame = read_df(stations_file)
    else:
        station_frame = currents_fetch_stations()
        print_file_exists(write_df(station_frame, stations_file))

    print(f'Creating all the NOAA waypoint folders and gpx files')
    for index, row in station_frame.iterrows():
        if '#' not in row['id']:
            # print(f'{row['id']}')
            wp_folder = stations_folder.joinpath(row['id'])
            if not wp_folder.exists():
                mkdir(wp_folder)

            wp = CurrentWaypoint(row, wp_folder)
            waypoint_dict[wp.id] = wp
            with open(wp_folder.joinpath(wp.id + '.gpx'), "w") as file:
                file.write(str(wp.soup))
            with open(OpenCPN_folder.joinpath(wp.id + '.gpx'), "w") as file:
                file.write(str(wp.soup))

    print(f'Requesting velocity data for each waypoint')
    non_weak_waypoints = [wp for wp in waypoint_dict.values() if wp.type != 'W']
    keys = [job_manager.put(RequestVelocityJob(2024, wp)) for wp in non_weak_waypoints]
    # for wp in waypoint_dict.values():
    #     job = RequestVelocityJob(2024, wp)
    #     result = job.execute()
    job_manager.wait()
    for path in [job_manager.get(key).csv for key in keys]:
        print_file_exists(path)

    print(f'Spline fitting subordinate waypoints')
    subordinate_waypoints = [wp for wp in waypoint_dict.values() if wp.type == 'S']
    keys = [job_manager.put(SplineJob(wp)) for wp in subordinate_waypoints]
    # for wp in subordinate_waypoints:
    #     job = SplineJob(wp)
    #     result = job.execute()
    job_manager.wait()
    for path in [job_manager.get(key).csv for key in keys]:
        print_file_exists(path)
