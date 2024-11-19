from argparse import ArgumentParser as argParser
from datetime import datetime as dt
from dateutil.relativedelta import relativedelta
from scipy.interpolate import CubicSpline
import pandas as pd

from tt_file_tools.file_tools import write_df, read_df, print_file_exists
from tt_job_manager.job_manager import JobManager, Job
from tt_noaa_data.noaa_data import StationDict, SixteenMonths, NonMonotonic
from tt_gpx.gpx import Waypoint


class RequestVelocityCSV:
    def __init__(self, year: int, waypoint: Waypoint):
        self.id = waypoint.id
        downloaded_frame = None
        if not waypoint.download_csv_path.exists():
            sixteen_months = SixteenMonths(year, waypoint)
            downloaded_frame = sixteen_months.frame
            print_file_exists(write_df(downloaded_frame, waypoint.download_csv_path))
        if waypoint.type == 'H' and not waypoint.velocity_csv_path.exists():
            downloaded_frame['datetime'] = pd.to_datetime(downloaded_frame['Time'])
            downloaded_frame['timestamp'] = downloaded_frame['datetime'].apply(dt.timestamp).astype('int')
            if not (downloaded_frame['timestamp'].is_monotonic_increasing and downloaded_frame['timestamp'].is_unique):
                raise NonMonotonic('<!> Timestamp column is not strictly monotonically increasing')
            write_df(downloaded_frame, waypoint.velocity_csv_path)


# noinspection PyShadowingNames
class RequestVelocityJob(Job):  # super -> job name, result key, function/object, arguments
    def execute(self): return super().execute()
    def execute_callback(self, result): return super().execute_callback(result)
    def error_callback(self, result): return super().error_callback(result)

    def __init__(self, year, waypoint: Waypoint):
        result_key = id(waypoint)
        arguments = tuple([year, waypoint])
        super().__init__(waypoint.id + ' ' + waypoint.name, result_key, RequestVelocityCSV, arguments)


class SplineCSV:
    def __init__(self, waypoint: Waypoint):
        self.id = waypoint.id
        frame = CubicSplineVelocityFrame(read_df(waypoint.download_csv_path)).frame
        if print_file_exists(write_df(frame, waypoint.spline_csv_path)):
            write_df(frame, waypoint.velocity_csv_path)


# noinspection PyShadowingNames
class SplineJob(Job):  # super -> job name, result key, function/object, arguments
    def execute(self): return super().execute()
    def execute_callback(self, result): return super().execute_callback(result)
    def error_callback(self, result): return super().error_callback(result)

    def __init__(self, waypoint: Waypoint):
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


class SplineCSVFailed(Exception):
    def __init__(self, message: str):
        self.message = message
        super().__init__(message)


if __name__ == '__main__':
    ap = argParser()
    ap.add_argument('year', type=int)
    args = vars(ap.parse_args())

    print(f'Creating all the NOAA waypoint folders and gpx files')
    station_dict = StationDict().dict
    waypoint_dict = {key: Waypoint(station_dict[key]) for key in station_dict.keys() if '#' not in key}
    for wp in waypoint_dict.values():
        wp.write_gpx()

    # fire up the job manager
    job_manager = JobManager()

    print(f'Requesting velocity data for each waypoint')
    waypoints = [wp for wp in waypoint_dict.values() if not (wp.type == 'W' or wp.download_csv_path.exists() or '#' in wp.id)]
    while len(waypoints):
        print(f'Length of list: {len(waypoints)}')
        if len(waypoints) < 11:
            for wp in waypoints:
                print(f'{wp.id} is missing downloaded velocity data')

        v_keys = [job_manager.submit_job(RequestVelocityJob(args['year'], wp)) for wp in waypoints]
        job_manager.wait()
        for result in [job_manager.get_result(key) for key in v_keys]:
            if result is not None:
                print_file_exists(waypoint_dict[result.id].download_csv_path)
        waypoints = [wp for wp in waypoint_dict.values() if not (wp.type == 'W' or wp.download_csv_path.exists())]

    print(f'Spline fitting subordinate waypoints')
    spline_dict = {}
    subordinate_waypoints = [wp for wp in waypoint_dict.values() if wp.type == 'S' and not wp.spline_csv_path.exists()]
    s_keys = [job_manager.submit_job(SplineJob(wp)) for wp in subordinate_waypoints]
    job_manager.wait()
    for result in [job_manager.get_result(key) for key in s_keys]:
        if result is not None:
            spline_dict[result.id] = print_file_exists(waypoint_dict[result.id].spline_csv_path)

    job_manager.stop_queue()
