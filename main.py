from argparse import ArgumentParser as argParser
from datetime import datetime as dt

from dateutil.relativedelta import relativedelta
from scipy.interpolate import CubicSpline
import pandas as pd

from tt_globals.globals import PresetGlobals
from tt_file_tools.file_tools import write_df, read_df, print_file_exists
from tt_job_manager.job_manager import JobManager, Job
from tt_noaa_data.noaa_data import StationDict, SixteenMonths, NonMonotonic, DuplicateTimestamps, OneMonth
from tt_gpx.gpx import Waypoint


class RequestVelocityCSV:
    def __init__(self, year: int, waypoint: Waypoint):
        self.id = waypoint.id
        self.path = None

        if not waypoint.adjusted_csv_path.exists():
            sixteen_months = SixteenMonths(year, waypoint)
            if not sixteen_months.error:
                write_df(sixteen_months.adj_frame, waypoint.adjusted_csv_path)
                if waypoint.type == 'H':
                    self.path = write_df(sixteen_months.adj_frame[['Time', 'stamp', 'Velocity_Major']].copy(), waypoint.velocity_csv_path)
                del sixteen_months
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
        self.path = None
        frame = CubicSplineVelocityFrame(read_df(waypoint.adjusted_csv_path)).frame
        if print_file_exists(write_df(frame, waypoint.spline_csv_path)):
            self.path = write_df(frame[['Time', 'stamp', 'Velocity_Major']].copy(), waypoint.velocity_csv_path)


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

        if not frame.Time.is_unique:
            raise DuplicateTimestamps(f'<!> Duplicate times')
        if not frame.stamp.is_monotonic_increasing:
            raise NonMonotonic(f'<!> Data not monotonic')
        cs = CubicSpline(frame.stamp, frame.Velocity_Major)

        start_date = dt.combine(pd.to_datetime(frame.Time.iloc[0], utc=True).date(), dt.min.time())
        end_date = dt.combine(pd.to_datetime(frame.Time.iloc[-1], utc=True).date() + relativedelta(days=1), dt.min.time())
        minutes = int((end_date - start_date).total_seconds()/60)
        self.frame = pd.DataFrame({'Time': [start_date + relativedelta(minutes=m) for m in range(0, minutes)]})
        self.frame['Time'] = self.frame.Time.apply(lambda x: pd.to_datetime(x, utc=True))
        self.frame['stamp'] = self.frame.Time.apply(dt.timestamp).astype('int')
        self.frame['Velocity_Major'] = self.frame.stamp.apply(cs).round(2)


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
    waypoint_dict = {key: Waypoint(station_dict[key]) for key in station_dict.keys() if not ('#' in key or station_dict[key]['type'] == 'W')}
    for wp in waypoint_dict.values():
        wp.write_gpx()

    # fire up the job manager
    job_manager = JobManager()
    # job_manager = None

    print(f'Requesting velocity data for each waypoint')
    for wp in [w for w in waypoint_dict.values() if OneMonth.connection_error(w.folder)]:
        wp.empty_folder()

    waypoints = [w for w in waypoint_dict.values() if not (w.velocity_csv_path.exists() or w.adjusted_csv_path.exists() or OneMonth.content_error(w.folder))]
    while len(waypoints):
        print(f'Length of list: {len(waypoints)}')
        if len(waypoints) < 11:
            for wp in waypoints:
                print(f'{wp.id} is missing downloaded velocity data')

        keys = [job_manager.submit_job(RequestVelocityJob(args['year'], wp)) for wp in waypoints]
        # for wp in waypoints:
        #     job = RequestVelocityJob(args['year'], wp)
        #     result = job.execute()
        job_manager.wait()
        # for result in [job_manager.get_result(key) for key in keys]:
        #     if result is not None and result.path is not None:
        #         print_file_exists(result.path)
        #         del result
        for wp in [w for w in waypoint_dict.values() if OneMonth.connection_error(w.folder)]:
            wp.empty_folder()
        waypoints = [w for w in waypoint_dict.values() if not (w.velocity_csv_path.exists() or w.adjusted_csv_path.exists() or OneMonth.content_error(w.folder))]

    print(f'Spline fitting subordinate waypoints')
    subordinate_waypoints = [wp for wp in waypoint_dict.values() if wp.type == 'S' and not (wp.velocity_csv_path.exists()
                             or wp.spline_csv_path.exists() or OneMonth.content_error(wp.folder))]
    keys = [job_manager.submit_job(SplineJob(wp)) for wp in subordinate_waypoints]
    # for wp in subordinate_waypoints:
    # job = SplineJob(wp)
    # result = job.execute()
    job_manager.wait()
    for result in [job_manager.get_result(key) for key in keys]:
        if result is not None:
            print_file_exists(result.path)
            del result

    job_manager.stop_queue()
