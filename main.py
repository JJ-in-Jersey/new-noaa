from argparse import ArgumentParser as argParser
from pandas import to_datetime, Timestamp
from os import remove
from time import sleep

from tt_dataframe.dataframe import DataFrame
from tt_dictionary.dictionary import Dictionary
from tt_globals.globals import PresetGlobals as pg
from tt_job_manager.job_manager import JobManager, Job
from tt_noaa_data.noaa_data import StationDict, SixteenMonths
from tt_gpx.gpx import Waypoint
from tt_interpolation.interpolation import CubicSplineFrame

class RequestVelocityCSV:
    def __init__(self, year: int, waypoint: Waypoint):
        self.id = waypoint.id

        if waypoint.type == "H":
            path = waypoint.velocity_csv_path
        elif waypoint.type == 'S':
            path = waypoint.adjusted_csv_path
        else:
            raise TypeError

        if not path.exists():
            try:
                sixteen_months = SixteenMonths(year, waypoint)
                sixteen_months.write(path)
                self.success = True
                self.failure_message = None
            except Exception as e:
                self.success = False
                self.failure_message = f'<!> {waypoint.id} {type(e).__name__}'
                print(self.failure_message)


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
    def __init__(self, year: int, waypoint: Waypoint):
        self.id = waypoint.id

        try:
            stamp_step = 60  # timestamps in seconds so steps of one minute is 60
            start_stamp = int(Timestamp(year=year - 1, month=11, day=1).timestamp())
            end_stamp = int(Timestamp(year=year + 1, month=3, day=1).timestamp())
            stamps = [start_stamp + i * stamp_step for i in range(int((end_stamp - start_stamp)/stamp_step))]

            input_frame = DataFrame(csv_source=waypoint.adjusted_csv_path)
            cs_frame = CubicSplineFrame(input_frame.stamp, input_frame.Velocity_Major, stamps)
            cs_frame['Time'] = to_datetime(cs_frame.stamp, unit='s').dt.tz_localize('UTC')
            cs_frame['Velocity_Major'] = cs_frame.Velocity_Major.round(2)
            cs_frame.write(waypoint.velocity_csv_path)
            remove(waypoint.adjusted_csv_path)
            self.success = True
            self.failure_message = None
        except Exception as e:
            self.success = False
            self.failure_message = f'<!> {waypoint.id} {type(e).__name__}'
            print(self.failure_message)


# noinspection PyShadowingNames
class SplineJob(Job):  # super -> job name, result key, function/object, arguments
    def execute(self): return super().execute()
    def execute_callback(self, result): return super().execute_callback(result)
    def error_callback(self, result): return super().error_callback(result)

    def __init__(self, year: int, waypoint: Waypoint):
        result_key = id(waypoint.id)
        arguments = tuple([year, waypoint])
        super().__init__(waypoint.id + ' ' + waypoint.name, result_key, SplineCSV, arguments)


if __name__ == '__main__':
    ap = argParser()
    ap.add_argument('year', type=int)
    args = vars(ap.parse_args())

    pg.make_folders()

    print(f'Creating all the NOAA waypoint folders and gpx files')
    station_dict = StationDict()
    waypoint_dict = Dictionary({key: Waypoint(station_dict[key]) for key in station_dict.keys() if not ('#' in key or station_dict[key]['type'] == 'W')})
    for wp in [wp for wp in waypoint_dict.values() if not pg.gpx_folder.joinpath(wp.id + '.gpx').exists()]:
        wp.write_gpx()

    # fire up the job manager
    job_manager = JobManager()
    # job_manager = None

    print(f'Requesting velocity data for each waypoint')
    waypoints = [w for w in waypoint_dict.values()
                 if not (w.velocity_csv_path.exists() or w.adjusted_csv_path.exists())
                 and (w.type == 'H' or w.type == 'S')]

    while len(waypoints):
        print(f'\nLength of list: {len(waypoints)}')
        sleep(5)

        keys = [job_manager.submit_job(RequestVelocityJob(args['year'], wp)) for wp in waypoints]
        # for wp in waypoints:
        #     job = RequestVelocityJob(args['year'], wp)
        #     result = job.execute()
        job_manager.wait()

        print(f'\nProcessing velocity data results')
        for result in [r for r in [job_manager.get_result(key) for key in keys] if not r.success]:
            yn = input(f'Exclude {result.failure_message} from StationDict processing? (y/n): ').lower()
            if yn == 'y' or yn == 'yes':
                station_dict.comment_waypoint(result.id)
                waypoint_dict.pop(result.id)

        waypoints = [w for w in waypoint_dict.values()
                     if not (w.velocity_csv_path.exists() or w.adjusted_csv_path.exists())
                     and (w.type == 'H' or w.type == 'S')]

    print(f'\nSpline fitting subordinate waypoints')
    subordinate_waypoints = [wp for wp in waypoint_dict.values() if wp.type == 'S' and not wp.velocity_csv_path.exists()]
    keys = [job_manager.submit_job(SplineJob(args['year'], wp)) for wp in subordinate_waypoints]
    # for wp in subordinate_waypoints:
    #     job = SplineJob(args['year'], wp)
    #     result = job.execute()
    job_manager.wait()

    print(f'\nProcessing spline fitting results')
    for result in [r for r in [job_manager.get_result(key) for key in keys] if not r.success]:
        yn = input(f'Exclude {result.failure_message} from StationDict processing? (y/n): ').lower()
        if yn == 'y' or yn == 'yes':
            station_dict.comment_waypoint(result.id)
            waypoint_dict.pop(result.id)

    job_manager.stop_queue()
