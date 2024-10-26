import time
from datetime import datetime as dt
from scipy.interpolate import CubicSpline
from pathlib import Path
import pandas as pd
import requests
from io import StringIO
from os import mkdir
from shutil import copyfile

from tt_file_tools.file_tools import SoupFromXMLResponse, SoupFromXMLFile, write_df, read_df, print_file_exists

from dateutil.relativedelta import relativedelta


class CurrentWaypoint:

    template_path = Path('/users/jason/Documents/OpenCPN/mooring.gpx')

    def __init__(self, wp_row: pd.Series):

        self.soup = None
        tree = SoupFromXMLFile(CurrentWaypoint.template_path).tree
        tree.find('name').string = wp_row['name'].strip()
        tree.find('wpt')['lat'] = wp_row['lat']
        tree.find('wpt')['lon'] = wp_row['lng']

        if wp_row['type'] == 'S':  # subordinate
            tree.find('sym').string = 'Symbol-Pin-Green'
        elif wp_row['type'] == 'H':  # harmonic
            tree.find('sym').string = 'Symbol-Spot-Green'
        elif wp_row['type'] == 'W':  # weak & variable
            tree.find('sym').string = 'Symbol-Pin-Yellow'
        else:
            print(wp_row['id'], wp_row['type'])

        id_tag = tree.new_tag('id')
        id_tag.string = wp_row['id']
        tree.find('name').insert_before(id_tag)

        desc_tag = tree.new_tag('desc')
        desc_tag.string = wp_row['id'] + ' - ' + str(int(wp_row['min_bin'])) + ' - ' + wp_row['type'] if bool(pd.isna(wp_row)['min_bin']) is False else wp_row['id'] + ' - ' + wp_row['type']
        tree.find('name').insert_after(desc_tag)

        self.soup = tree


class CubicSplineVelocityFrame:

    def __init__(self, frame: pd.DataFrame):

        self.frame = None

        if not downloaded_velocity['timestamp'].is_monotonic_increasing:
            raise SystemExit(f'frame is not monotonically increasing')
        elif 'datetime' in frame.columns.tolist() and 'timestamp' in frame.columns.tolist() and 'velocity' in frame.columns.tolist():
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
            raise SystemExit(f'frame does not contain datetime, timestamp or velocity')


class DataNotAvailable(Exception):
    def __init__(self, station_id: str):
        self.message = 'Currents predictions are not available from ' + station_id
        super().__init__(self.message)


class EmptyDataframe(Exception):
    def __init__(self, station_id: str):
        self.message = station_id + " dataframe is empty or all NaN"
        super().__init__(self.message)


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
                self.frame['datetime'] = pd.to_datetime(frame['Time'])
                self.frame['timestamp'] = self.frame['datetime'].apply(dt.timestamp).astype('int')
                break
            except requests.exceptions.RequestException as e:
                print(str(e))
                time.sleep(1)
            except DataNotAvailable as e:
                print(str(e))
                time.sleep(1)
            except EmptyDataframe as e:
                print(str(e))
                time.sleep(1)


class SixteenMonths:
    def __init__(self, year: int, station_code: str, station_bin: int = None):
        self.frame = None
        months = ([OneMonth(month, year - 1, station_code, station_bin) for month in range(11, 13)] +
                  [OneMonth(month, year, station_code, station_bin) for month in range(1, 13)] +
                  [OneMonth(month, year + 1, station_code, station_bin) for month in range(1, 3)])
        frames = [m.frame for m in months]
        self.frame = pd.concat(frames, axis=0, ignore_index=True)

        for f in frames:
            del f
        for m in months:
            del m


def currents_fetch_stations():

    rows = None
    my_request = "https://api.tidesandcurrents.noaa.gov/mdapi/prod/webapi/stations.xml?type=currentpredictions&units=english"
    for _ in range(3):
        try:
            my_response = requests.get(my_request)
            my_response.raise_for_status()
            print(f'Creating rows')
            stations_tree = SoupFromXMLResponse(StringIO(my_response.content.decode())).tree
            rows = [{'id': station.find_next('id').text, 'name': station.find_next('name').text, 'lat': float(station.find_next('lat').text),
                     'lng': float(station.find_next('lng').text), 'type': station.find_next('type').text} for station in stations_tree.find_all('Station')]

            for wp_row in rows:
                my_request = "https://api.tidesandcurrents.noaa.gov/mdapi/prod/webapi/stations/" + wp_row['id'] + "/bins.xml?units=english"
                for _ in range(3):
                    try:
                        my_response = requests.get(my_request)
                        my_response.raise_for_status()
                        print(f'Creating bins {wp_row['id']}')
                        bins_tree = SoupFromXMLResponse(StringIO(my_response.content.decode())).tree
                        bin_count = int(bins_tree.find("nbr_of_bins").text)
                        if bin_count and bins_tree.find('Bin').find('depth') is not None:
                            bin_dict = {int(tag.num.text): float(tag.depth.text) for tag in bins_tree.find_all('Bin')}
                            wp_row['min_bin'] = min(bin_dict, key=bin_dict.get)
                        break
                    except requests.exceptions.RequestException as e:
                        print(str(e))
                        time.sleep(1)
            break
        except requests.exceptions.RequestException as e:
            print(str(e))
            time.sleep(1)

    return pd.DataFrame(rows)


if __name__ == '__main__':

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

    print(f'Fetching all NOAA current stations')
    stations_file = stations_folder.joinpath('stations.csv')
    if stations_file.exists():
        station_frame = read_df(stations_file)
    else:
        station_frame = currents_fetch_stations()
        write_df(station_frame, stations_file)

    print(f'Creating all the NOAA waypoint folders and files')
    for index, row in station_frame.iterrows():
        if '#' not in row['id']:
            wp_folder = stations_folder.joinpath(row['id'])
            if not wp_folder.exists():
                mkdir(wp_folder)
            wp = CurrentWaypoint(row)
            with open(wp_folder.joinpath(row['id'] + '.gpx'), "w") as file:
                file.write(str(wp.soup))
            with open(OpenCPN_folder.joinpath(row['id'] + '.gpx'), "w") as file:
                file.write(str(wp.soup))

    for index, row in station_frame.iterrows():
        if '#' not in row['id'] and row['type'] != 'W':
            print(f'Downloading current data for {row['id']}')
            wp_folder = stations_folder.joinpath(row['id'])
            downloaded_file = wp_folder.joinpath('downloaded_frame.csv')
            wp_file = wp_folder.joinpath('waypoint_velocity_frame.csv')
            cubic_file = wp_folder.joinpath('cubic_spline_frame.csv')

            if not print_file_exists(downloaded_file):
                sixteen_months = SixteenMonths(2024, row['id'])
                downloaded_velocity = sixteen_months.frame
                print_file_exists(write_df(downloaded_velocity, wp_folder.joinpath('downloaded_frame.csv')))
                del downloaded_velocity, sixteen_months

    for index, row in station_frame.iterrows():
        if '#' not in row['id']:
            print(f'Checking {row['id']} station type (H/S)')
            wp_folder = stations_folder.joinpath(row['id'])
            downloaded_file = wp_folder.joinpath('downloaded_frame.csv')
            wp_file = wp_folder.joinpath('waypoint_velocity_frame.csv')
            cubic_file = wp_folder.joinpath('cubic_spline_frame.csv')

            downloaded_velocity = read_df(downloaded_file)
            if not downloaded_velocity['timestamp'].is_monotonic_increasing:
                print(f'{row['id']} timestamps are not monotonically increasing')
                # for r in range(0, len(downloaded_velocity) - 1):
                #     if downloaded_velocity.loc[r]['timestamp'] >= downloaded_velocity.loc[r + 1]['timestamp']:
                #         print(f'Check value in row {r}')
            elif row['type'] == 'H':
                copyfile(wp_folder.joinpath('downloaded_frame.csv'), wp_folder.joinpath('waypoint_velocity_frame.csv'))
            elif row['type'] == 'S':
                if not print_file_exists(cubic_file):
                    print_file_exists(write_df(CubicSplineVelocityFrame(read_df(wp_folder.joinpath('downloaded_frame.csv'))).frame, wp_folder.joinpath('cubic_spline_frame.csv')))
                    copyfile(wp_folder.joinpath('cubic_spline_frame.csv'), wp_folder.joinpath('waypoint_velocity_frame.csv'))
