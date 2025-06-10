from argparse import ArgumentParser as argParser
from pandas import to_datetime
from os import remove

from tt_dataframe.dataframe import DataFrame
from tt_globals.globals import PresetGlobals
from tt_file_tools.file_tools import print_file_exists
from tt_job_manager.job_manager import JobManager, Job
from tt_noaa_data.noaa_data import StationDict, SixteenMonths, OneMonth
from tt_gpx.gpx import Waypoint
from tt_interpolation.interpolation import CubicSplineFrame

class RequestVelocityCSV:
    def __init__(self, year: int, waypoint: Waypoint):
        self.id = waypoint.id
        self.path = None

        if not waypoint.adjusted_csv_path.exists():
            sixteen_months = SixteenMonths(year, waypoint)
            if not sixteen_months.error:
                if print_file_exists(sixteen_months.adj_frame.write(waypoint.adjusted_csv_path)) and waypoint.type == 'H':
                    self.path = sixteen_months.adj_frame[['Time', 'stamp', 'Velocity_Major']].copy().write(waypoint.velocity_csv_path)
                    if print_file_exists(self.path):
                        remove(waypoint.adjusted_csv_path)
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

        stamp_step = 60  # timestamps in seconds so steps of one minute is 60
        input_frame = DataFrame(csv_source=waypoint.adjusted_csv_path)
        cs_frame = CubicSplineFrame(input_frame.stamp, input_frame.Velocity_Major, stamp_step)
        cs_frame['Time'] = to_datetime(cs_frame.stamp, unit='s').dt.tz_localize('UTC')
        cs_frame['Velocity_Major'] = cs_frame.Velocity_Major.round(2)

        if print_file_exists(cs_frame.write(waypoint.spline_csv_path)):
            if print_file_exists(cs_frame.write(waypoint.velocity_csv_path)):
                remove(waypoint.adjusted_csv_path)
                remove(waypoint.spline_csv_path)


# noinspection PyShadowingNames
class SplineJob(Job):  # super -> job name, result key, function/object, arguments
    def execute(self): return super().execute()
    def execute_callback(self, result): return super().execute_callback(result)
    def error_callback(self, result): return super().error_callback(result)

    def __init__(self, waypoint: Waypoint):
        result_key = id(waypoint.id)
        arguments = tuple([waypoint])
        super().__init__(waypoint.id + ' ' + waypoint.name, result_key, SplineCSV, arguments)


if __name__ == '__main__':
    ap = argParser()
    ap.add_argument('year', type=int)
    args = vars(ap.parse_args())

    PresetGlobals.make_folders()

    print(f'Creating all the NOAA waypoint folders and gpx files')
    station_dict = StationDict()
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
        for wp in [w for w in waypoint_dict.values() if OneMonth.connection_error(w.folder)]:
            wp.empty_folder()
        waypoints = [w for w in waypoint_dict.values() if not (w.velocity_csv_path.exists() or w.adjusted_csv_path.exists() or OneMonth.content_error(w.folder))]

        for result in [job_manager.get_result(key) for key in keys]:
            if result is not None:
                print_file_exists(result.path)

    print(f'Spline fitting subordinate waypoints')
    subordinate_waypoints = [wp for wp in waypoint_dict.values() if wp.type == 'S' and not (wp.velocity_csv_path.exists()
                             or wp.spline_csv_path.exists() or OneMonth.content_error(wp.folder))]
    keys = [job_manager.submit_job(SplineJob(wp)) for wp in subordinate_waypoints]
    # for wp in subordinate_waypoints:
    #     job = SplineJob(wp)
    #     result = job.execute()
    job_manager.wait()

    for result in [job_manager.get_result(key) for key in keys]:
        if result is not None:
            print_file_exists(result.path)

    job_manager.stop_queue()
