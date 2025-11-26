from argparse import ArgumentParser as argParser
import shutil

from tt_dictionary.dictionary import Dictionary
import tt_globals.globals as Globals
from tt_job_manager.job_manager import JobManager
from tt_jobs.jobs import RequestVelocityJob, SplineJob, RequestVelocityFrame, SplineFrame
from tt_noaa_data.noaa_data import StationDict
from tt_gpx.gpx import Waypoint

if __name__ == '__main__':
    ap = argParser()
    ap.add_argument('year', type=int)
    args = vars(ap.parse_args())

    waypoint_template = Globals.TEMPLATES_FOLDER.joinpath('waypoint_template.gpx')

    if not waypoint_template.exists():
        print(f'\n**   {waypoint_template} not found.\n')
        exit(1)

    Globals.make_project_folders()

    print(f'Creating all the NOAA waypoint folders and gpx files')
    if Globals.STATIONS_FILE.exists():
        station_dict = StationDict(json_source=Globals.STATIONS_FILE)
    else:
        station_dict = StationDict()

    waypoint_dict = Dictionary({key: Waypoint(station_dict[key]) for key in station_dict.keys() if not ('#' in key or station_dict[key]['type'] == 'W')})
    for wp in [wp for wp in waypoint_dict.values() if not Globals.GPX_FOLDER.joinpath(wp.id + '.gpx').exists()]:
        wp.write_gpx()

    # fire up the job manager
    job_manager = JobManager()
    # job_manager = None

    print(f'Requesting velocity data for each waypoint')
    waypoints = [w for w in waypoint_dict.values() if not w.raw_csv_path.exists() and (w.type == 'H' or w.type == 'S')]

    while len(waypoints):
        print(f'\nLength of list: {len(waypoints)}')

        keys = [job_manager.submit_job(RequestVelocityJob(args['year'], wp)) for wp in waypoints]
        # for wp in waypoints:
        #     job = RequestVelocityJob(args['year'], wp)
        #     result = job.execute()
        job_manager.wait()

        print(f'\nProcessing velocity data results')
        results_dict = {wp_id: job_manager.get_result(wp_id) for wp_id in keys}
        success_dict = {k: v for k, v in results_dict.items() if isinstance(v, RequestVelocityFrame)}
        error_dict = {k: v for k, v in results_dict.items() if isinstance(v, Exception)}
        for wp_id, result in error_dict.items():
            yn = input(f'Exclude {result.__class__.__name__} {wp_id} from StationDict processing? (y/n): ').lower()
            if yn == 'y' or yn == 'yes':
                station_dict.comment_waypoint(wp_id)
                waypoint_dict.pop(wp_id)

        waypoints = [w for w in waypoint_dict.values() if not w.raw_csv_path.exists() and (w.type == 'H' or w.type == 'S')]

    yn = input(f'Did everything download successfully? (y/n): ').lower()
    if yn == 'n' or yn == 'no':
        exit(0)

    print(f'\nSpline fitting subordinate waypoints')
    subordinate_waypoints = [wp for wp in waypoint_dict.values() if wp.type == 'S' and not wp.velocity_csv_path.exists()]
    keys = [job_manager.submit_job(SplineJob(args['year'], wp)) for wp in subordinate_waypoints]
    # for wp in subordinate_waypoints:
    #     job = SplineJob(args['year'], wp)
    #     result = job.execute()
    job_manager.wait()

    print(f'\nProcessing spline fitting results')
    results_dict = {wp_id: job_manager.get_result(wp_id) for wp_id in keys}
    success_dict = {k: v for k, v in results_dict.items() if isinstance(v, SplineFrame)}
    error_dict = {k: v for k, v in results_dict.items() if isinstance(v, Exception)}
    for wp_id, result in error_dict.items():
        yn = input(f'Exclude {result.__class__.__name__} {wp_id} from StationDict processing? (y/n): ').lower()
        if yn == 'y' or yn == 'yes':
            station_dict.comment_waypoint(wp_id)
            waypoint_dict.pop(wp_id)

    print(f'\nCopying harmonic results that do not need fitting')
    harmonic_waypoints = [wp for wp in waypoint_dict.values() if wp.type == 'H' and not wp.velocity_csv_path.exists()]
    for wp in harmonic_waypoints:
        shutil.copy(wp.raw_csv_path, wp.velocity_csv_path)

    job_manager.stop_queue()

