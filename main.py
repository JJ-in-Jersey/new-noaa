from argparse import ArgumentParser as argParser
from datetime import datetime as dt

from dateutil.relativedelta import relativedelta
from scipy.interpolate import CubicSpline
import pandas as pd

from tt_globals.globals import PresetGlobals
from tt_file_tools.file_tools import write_df, read_df, print_file_exists
from tt_job_manager.job_manager import JobManager, Job
from tt_noaa_data.noaa_data import StationDict, SixteenMonths, NonMonotonic, OneMonth
from tt_gpx.gpx import Waypoint


class RequestVelocityCSV:
    def __init__(self, year: int, waypoint: Waypoint):
        self.id = waypoint.id

        if not waypoint.download_csv_path.exists():
            sixteen_months = SixteenMonths(year, waypoint)
            if not sixteen_months.error:
                self.frame = sixteen_months.adj_frame
                write_df(sixteen_months.raw_frame, waypoint.raw_csv_path)
                write_df(sixteen_months.adj_frame, waypoint.download_csv_path)
            else:
                raise sixteen_months.error


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

        if not (frame['stamp'].is_monotonic_increasing and frame['stamp'].is_unique):
            raise NonMonotonic('<!> Data not strictly monotonic')
        cs = CubicSpline(frame['stamp'], frame['Velocity_Major'])

        start_date = dt.combine(pd.to_datetime(frame['Time'].iloc[0], utc=True).date(), dt.min.time())
        end_date = dt.combine(pd.to_datetime(frame['Time'].iloc[-1], utc=True).date() + relativedelta(days=1), dt.min.time())
        minutes = int((end_date - start_date).total_seconds()/60)
        self.frame = pd.DataFrame({'datetime': [start_date + relativedelta(minutes=m) for m in range(0, minutes)]})
        self.frame['stamp'] = self.frame['datetime'].apply(dt.timestamp).astype('int')
        self.frame['Velocity_Major'] = self.frame['stamp'].apply(cs).round(2)


class SplineCSVFailed(Exception):
    def __init__(self, message: str):
        self.message = message
        super().__init__(message)


if __name__ == '__main__':
    ap = argParser()
    ap.add_argument('year', type=int)
    args = vars(ap.parse_args())

    PresetGlobals.make_folders()

    print(f'Creating all the NOAA waypoint folders and gpx files')
    station_dict = StationDict().dict
    waypoint_dict = {key: Waypoint(station_dict[key]) for key in station_dict.keys() if '#' not in key}
    for wp in waypoint_dict.values():
        wp.write_gpx()

    # fire up the job manager
    job_manager = JobManager()
    # job_manager = None

    print(f'Requesting velocity data for each waypoint')
    waypoints = [wp for wp in waypoint_dict.values() if not (wp.type == 'W' or wp.download_csv_path.exists() or
                                                             '#' in wp.id or OneMonth.content_error(wp.folder))]
    while len(waypoints):
        print(f'Length of list: {len(waypoints)}')
        if len(waypoints) < 11:
            for wp in waypoints:
                print(f'{wp.id} is missing downloaded velocity data')

        v_keys = [job_manager.submit_job(RequestVelocityJob(args['year'], wp)) for wp in waypoints]
        # for wp in waypoints:
        #     job = RequestVelocityJob(args['year'], wp)
        #     result = job.execute()
        job_manager.wait()
        for result in [job_manager.get_result(key) for key in v_keys]:
            if result is not None:
                print_file_exists(waypoint_dict[result.id].download_csv_path)
        waypoints = [wp for wp in waypoint_dict.values()
                     if not (wp.type == 'W' or wp.download_csv_path.exists() or OneMonth.content_error(wp.folder))]

    print(f'Spline fitting subordinate waypoints')
    spline_dict = {}
    subordinate_waypoints = [wp for wp in waypoint_dict.values() if wp.type == 'S' and not wp.spline_csv_path.exists()
                             and not OneMonth.content_error(wp.folder)]
    s_keys = [job_manager.submit_job(SplineJob(wp)) for wp in subordinate_waypoints]
    # for wp in subordinate_waypoints:
    #     job = SplineJob(wp)
    #     result = job.execute()
    job_manager.wait()
    for result in [job_manager.get_result(key) for key in s_keys]:
        if result is not None:
            spline_dict[result.id] = print_file_exists(waypoint_dict[result.id].spline_csv_path)

    job_manager.stop_queue()
