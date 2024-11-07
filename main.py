import time
from datetime import datetime as dt

from dateutil.relativedelta import relativedelta
from scipy.interpolate import CubicSpline
from pathlib import Path
import pandas as pd
import requests
from io import StringIO
import os
import json

from tt_file_tools.file_tools import SoupFromXMLResponse, SoupFromXMLFile, write_df, read_df, print_file_exists
from tt_job_manager.job_manager import JobManager, Job


class NOAAFolders:

    base = Path('/users/jason/Developer Workspace')
    GPX = base.joinpath('GPX')
    stations_folder = GPX.joinpath('stations')
    Open_CPN = GPX.joinpath('open_cpn')

    stations_filepath = stations_folder.joinpath('stations.json')
    template_path = Path('/users/jason/Documents/OpenCPN/mooring.gpx')

    @staticmethod
    def build_folders():
        if not NOAAFolders.GPX.exists():
            os.makedirs(NOAAFolders.GPX)
        if not NOAAFolders.stations_folder.exists():
            os.mkdir(NOAAFolders.stations_folder)
        if not NOAAFolders.Open_CPN.exists():
            os.mkdir(NOAAFolders.Open_CPN)

    def __init__(self):
        pass


class CurrentWaypoint:

    download_csv = 'downloaded_frame.csv'
    velocity_csv = 'waypoint_velocity_frame.csv'
    spline_csv = 'cubic_spline_frame.csv'

    def write_gpx(self):
        with open(self.folder.joinpath(self.id + '.gpx'), 'w') as a_file:
            a_file.write(str(self.soup))

    def write_open_cpn(self):
        with open(NOAAFolders.Open_CPN.joinpath(self.id + '.gpx'), "w") as a_file:
            a_file.write(str(self.soup))

    def __init__(self, station_id: str):

        dash = ' - '

        self.soup = None
        self.id = station_id
        self.bins = station_dict[station_id]['bins'] if 'bins' in station_dict[station_id].keys() else None
        self.bin = None if self.bins is None else int(list(self.bins.values())[0])
        self.name = station_dict[station_id]['name'].strip()
        self.type = station_dict[station_id]['type']
        self.folder = NOAAFolders.stations_folder.joinpath(station_id)
        self.download_file = self.folder.joinpath(CurrentWaypoint.download_csv)
        self.velocity_file = self.folder.joinpath(CurrentWaypoint.velocity_csv)
        self.spline_file = self.folder.joinpath(CurrentWaypoint.spline_csv)
        if not self.folder.exists():
            os.mkdir(self.folder)

        tree = SoupFromXMLFile(NOAAFolders.template_path).tree
        tree.find('name').string = self.name
        tree.find('wpt')['lat'] = station_dict[station_id]['lat']
        tree.find('wpt')['lon'] = station_dict[station_id]['lng']

        if self.type == 'S':  # subordinate
            tree.find('sym').string = 'Symbol-Pin-Green'
        elif self.type == 'H':  # harmonic
            tree.find('sym').string = 'Symbol-Spot-Green'
        elif self.type == 'W':  # weak & variable
            tree.find('sym').string = 'Symbol-Pin-Yellow'
        # else:
        #     print(self.id, my_row['type'])

        id_tag = tree.new_tag('id')
        id_tag.string = self.id
        tree.find('name').insert_before(id_tag)

        desc_tag = tree.new_tag('desc')
        desc_tag.string = self.id + dash + self.type if self.bin is None else self.id + dash + str(int(self.bin)) + dash + self. type
        tree.find('name').insert_after(desc_tag)

        self.soup = tree
        self.write_gpx()
        self.write_open_cpn()


class OneMonth:

    def __init__(self, month: int, year: int, waypoint: CurrentWaypoint, interval_time: int = 1):

        self.frame = None

        if month < 1 or month > 12:
            raise ValueError

        start = dt(year, month, 1)
        end = start + relativedelta(months=1) - relativedelta(days=1)

        header = "https://api.tidesandcurrents.noaa.gov/api/prod/datagetter?"
        begin_date_field = "&begin_date=" + start.strftime("%Y%m%d")  # yyyymmdd
        end_date_field = "&end_date=" + end.strftime("%Y%m%d")  # yyyymmdd
        station_field = "&station=" + waypoint.id  # station id string
        interval_field = "&interval=" + str(interval_time)
        footer_wo_bin = "&product=currents_predictions&time_zone=lst_ldt" + interval_field + "&units=english&format=csv"
        # footer_w_bin = footer_wo_bin + "&bin=" + str(waypoint.bin)
        # footer = footer_wo_bin if waypoint.bin is None else footer_w_bin
        footer = footer_wo_bin  # requests wo bin seem to return shallowest predictions
        my_request = header + begin_date_field + end_date_field + station_field + footer

        self.error = False
        for _ in range(5):
            try:
                self.error = False
                my_response = requests.get(my_request)
                my_response.raise_for_status()
                if 'predictions are not available' in my_response.content.decode():
                    raise DataNotAvailable('<!> ' + waypoint.id + ' Current predictions are not available')
                frame = pd.read_csv(StringIO(my_response.content.decode()))
                if frame.empty or frame.isna().all().all():
                    raise EmptyDataframe('<!> ' + waypoint.id + 'Dataframe is empty or all NaN')
                self.frame = frame.rename(columns={heading: heading.strip() for heading in frame.columns.tolist()})
                self.frame.rename(columns={'Velocity_Major': 'velocity'}, inplace=True)
                break
            except requests.exceptions.RequestException:
                self.error = True
                time.sleep(1)
            except DataNotAvailable:
                self.error = True
                time.sleep(1)
            except EmptyDataframe:
                self.error = True
                time.sleep(1)


class SixteenMonths:

    def __init__(self, year: int, waypoint: CurrentWaypoint):

        months = []
        failure_message = '<!> ' + waypoint.id + ' CSV Request failed'

        for m in range(11, 13):
            month = OneMonth(m, year - 1, waypoint)
            if month.error:
                raise CSVRequestFailed(failure_message)
            months.append(month)
        for m in range(1, 13):
            month = OneMonth(m, year, waypoint)
            if month.error:
                raise CSVRequestFailed(failure_message)
            months.append(month)
        for m in range(1, 3):
            month = OneMonth(m, year + 1, waypoint)
            if month.error:
                raise CSVRequestFailed('<!> ' + waypoint.id + ' CSV Request failed')
            months.append(month)

        self.frame = pd.concat([m.frame for m in months], axis=0, ignore_index=True)
        for m in range(len(months)):
            del m


class RequestVelocityCSV:

    def __init__(self, year: int, waypoint: CurrentWaypoint):

        self.id = waypoint.id
        self.csv = waypoint.folder.joinpath('downloaded_frame.csv')
        waypoint_csv = waypoint.folder.joinpath('waypoint_velocity_frame.csv')

        if not self.csv.exists():
            sixteen_months = SixteenMonths(year, waypoint)
            downloaded_frame = sixteen_months.frame
            self.csv = print_file_exists(write_df(downloaded_frame, self.csv))
            if waypoint.type == "H":
                write_df(downloaded_frame, waypoint_csv)
                del downloaded_frame, sixteen_months


class RequestVelocityJob(Job):  # super -> job name, result key, function/object, arguments

    def execute(self): return super().execute()
    def execute_callback(self, result): return super().execute_callback(result)
    def error_callback(self, result): return super().error_callback(result)

    def __init__(self, year, waypoint: CurrentWaypoint):
        result_key = id(waypoint)
        arguments = tuple([year, waypoint])
        super().__init__(waypoint.id + ' ' + waypoint.name, result_key, RequestVelocityCSV, arguments)


class SplineCSV:

    def __init__(self, waypoint: CurrentWaypoint):

        self.csv = None

        self.id = waypoint.id
        velocity_frame = read_df(waypoint.download_file)

        frame = CubicSplineVelocityFrame(velocity_frame).frame
        if print_file_exists(write_df(frame, waypoint.spline_file)):
            write_df(frame, waypoint.velocity_file)
        self.csv = waypoint.spline_file


class SplineJob(Job):  # super -> job name, result key, function/object, arguments

    def execute(self): return super().execute()
    def execute_callback(self, result): return super().execute_callback(result)
    def error_callback(self, result): return super().error_callback(result)

    def __init__(self, waypoint: CurrentWaypoint):
        result_key = id(waypoint.id)
        arguments = tuple([waypoint])
        super().__init__(waypoint.id + ' ' + waypoint.name, result_key, SplineCSV, arguments)


class CubicSplineVelocityFrame:

    def __init__(self, frame: pd.DataFrame):

        self.frame = None

        frame['datetime'] = pd.to_datetime(frame['Time'])
        frame['timestamp'] = frame['datetime'].apply(dt.timestamp).astype('int')

        if not (frame['timestamp'].is_monotonic_increasing and frame['timestamp'].is_unique):
            raise NonMonotonic('<!> Timestamp column is not strictly monotonically increasing')

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


class NonMonotonic(Exception):
    def __init__(self, message: str):
        self.message = message
        super().__init__(self.message)


class DataNotAvailable(Exception):
    def __init__(self, message: str):
        self.message = message
        super().__init__(self.message)


class EmptyDataframe(Exception):
    def __init__(self, message: str):
        self.message = message
        super().__init__(self.message)


class CSVRequestFailed(Exception):
    def __init__(self, message: str):
        self.message = message
        super().__init__(message)


class SplineCSVFailed(Exception):
    def __init__(self, message: str):
        self.message = message
        super().__init__(message)


class StationDict:

    @staticmethod
    def write_dict(dictionary: dict):
        with open(NOAAFolders.stations_filepath, 'w') as a_file:
            # noinspection PyTypeChecker
            json.dump(dictionary, a_file)
        return NOAAFolders.stations_filepath

    @staticmethod
    def read_dict():
        if not NOAAFolders.stations_filepath.exists():
            raise FileExistsError(NOAAFolders.stations_filepath)
        with open(NOAAFolders.stations_filepath, 'r') as a_file:
            return json.load(a_file)

    def __init__(self):

        self.dict = None

        if print_file_exists(NOAAFolders.stations_filepath):
            self.dict = self.read_dict()
        else:
            self.dict = {}
            my_request = "https://api.tidesandcurrents.noaa.gov/mdapi/prod/webapi/stations.xml?type=currentpredictions&units=english"
            for _ in range(3):
                try:
                    print(f'Requesting list of stations')
                    my_response = requests.get(my_request)
                    my_response.raise_for_status()
                    stations_tree = SoupFromXMLResponse(StringIO(my_response.content.decode())).tree
                    rows = [{'id': station_tag.find_next('id').text, 'name': station_tag.find_next('name').text,
                             'lat': float(station_tag.find_next('lat').text),
                             'lng': float(station_tag.find_next('lng').text), 'type': station_tag.find_next('type').text}
                            for station_tag in stations_tree.find_all('Station')]
                    print_file_exists(write_df(pd.DataFrame(rows).drop_duplicates(), NOAAFolders.stations_folder.joinpath('stations.csv')))
                    rows = pd.DataFrame(rows).drop_duplicates().to_dict('records')
                    self.dict = {r['id']: r for r in rows}
                    for value in self.dict.values():
                        del value['id']
                    print_file_exists(self.write_dict(self.dict))

                    print(f'Requesting bins for each station')
                    for station_id in self.dict.keys():
                        print(station_id)
                        my_request = "https://api.tidesandcurrents.noaa.gov/mdapi/prod/webapi/stations/" + station_id + "/bins.xml?units=english"
                        for _ in range(3):
                            try:
                                my_response = requests.get(my_request)
                                my_response.raise_for_status()
                                bins_tree = SoupFromXMLResponse(StringIO(my_response.content.decode())).tree
                                bin_count = int(bins_tree.find("nbr_of_bins").text)
                                if bin_count and bins_tree.find('Bin').find('depth') is not None:
                                    bin_dict = {int(tag.num.text): float(tag.depth.text) for tag in bins_tree.find_all('Bin')}
                                    bin_dict = dict(sorted(bin_dict.items(), key=lambda item: item[1]))
                                    self.dict[station_id]['bins'] = bin_dict
                                break
                            except requests.exceptions.RequestException:
                                time.sleep(1)
                    break
                except requests.exceptions.RequestException:
                    time.sleep(1)


if __name__ == '__main__':

    this_year = 2024

    print(f'Creating all the NOAA waypoint folders and gpx files')
    station_dict = StationDict().dict
    waypoint_dict = { station: CurrentWaypoint(station) for station in station_dict.keys() if '# not in station'}

    spline_dict = {}

    # fire up the job manager
    job_manager = JobManager()
    NOAAFolders.build_folders()

    print(f'Requesting velocity data for each waypoint')
    waypoints = [wp for wp in waypoint_dict.values() if not (wp.type == 'W' or wp.download_file.exists() or '#' in wp.id)]
    while len(waypoints):
        print(f'Length of list: {len(waypoints)}')
        if len(waypoints) < 11:
            for wp in waypoints:
                print(f'{wp.id} is missing downloaded velocity data')

        v_keys = [job_manager.submit_job(RequestVelocityJob(this_year, wp)) for wp in waypoints]
        job_manager.wait()
        for result in [job_manager.get_result(key) for key in v_keys]:
            if result is not None:
                print_file_exists(waypoint_dict[result.id].download_file)
        waypoints = [wp for wp in waypoint_dict.values() if not (wp.type == 'W' or wp.download_file.exists())]

    print(f'Spline fitting subordinate waypoints')
    subordinate_waypoints = [wp for wp in waypoint_dict.values() if wp.type == 'S' and not wp.spline_file.exists()]
    keys = [job_manager.submit_job(SplineJob(wp)) for wp in subordinate_waypoints]
    job_manager.wait()
    for result in [job_manager.get_result(key) for key in keys]:
        if result is not None:
            spline_dict[result.id] = print_file_exists(waypoint_dict[result.id].spline_file)

    job_manager.stop_queue()