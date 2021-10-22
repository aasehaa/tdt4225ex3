from pprint import pprint
from DbConnector import DbConnector
import os
# import pandas as pd
from datetime import datetime
from tabulate import tabulate
from typing import Tuple

try:
    from tqdm import tqdm
except:
    # TQDM is a progress bar that has to be pip-intalled.
    # It's used outside the entire os.walk-function
    # If user gets module error, we redefine the function to be indentity so tqm(os.walk(...)) can still run
    def tqdm(*args):
        return args


class InsertDataProgram:

    potential_matches = {}
    transp_mode = ""

    def __init__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db

    def add_all_data(self):

        dataset_path = os.path.dirname(__file__) + "/../dataset"
        test_dataset_path = os.path.dirname(__file__) + "/../testDataset"

        activity_primary_id = 0
        track_point_id = 0

        with open(dataset_path + '/labeled_ids.txt', 'r') as fs:
            labeled_IDs = fs.read().splitlines()

        for level, (root, dirs, files) in enumerate(os.walk(dataset_path + '/Data')):

            if level == 0:
                for id in dirs:
                    user_has_labels = id in labeled_IDs

                    # add user collection to db
                    user_collection = self.db["User"]
                    user_collection.insert_one({
                        "_id": id,
                        "has_labels": user_has_labels,
                        "activities": []
                    })

                    # Create a 2D list for each ID in a dictionary made for matching activities with their labels
                    self.potential_matches[id] = [[], [], []]

            activity_docs = []
            activity_ids_for_user = []

            if files != []:
                for fn in files:

                    file_type = fn[-3:]

                    if file_type == 'txt':
                        self.add_labeled_files_to_dict(root, fn)

                    if file_type == "plt":
                        activity = self.get_activity(root, fn)
                        valid_activity = self.check_valid_activity(activity)

                        if valid_activity:

                            track_points = []

                            self.transp_mode = "NULL"
                            activity_start, activity_end = self.format_start_and_end_time(
                                activity)
                            user = root[-14:-11]

                            self.correct_start_and_end_time(
                                activity_start, activity_end, user)

                            track_point_docs = []

                            for point in activity:

                                lat, long, _, altitude, timestamp, date, time = point.split(
                                    ',')
                                time_datetime = date + " " + time

                                track_point_docs.append({
                                    "_id": track_point_id,
                                    "lat": float(lat),
                                    "lon": float(long),
                                    "altitude": float(altitude),
                                    "date_days": float(timestamp),
                                    "date_time": time_datetime,
                                    "activity_id": activity_primary_id
                                })

                                track_points.append(track_point_id)
                                track_point_id += 1

                            # add all track point docs tocollection in db
                            track_point_collection = self.db["TrackPoint"]
                            if track_point_docs != []:
                                track_point_collection.insert_many(
                                    track_point_docs)

                            activity_docs.append({
                                "_id": activity_primary_id,
                                "transportation_mode": self.transp_mode,
                                "start_date_time": activity_start,
                                "end_date_time": activity_end,
                            })

                            # user_doc["activities"].append(activity_primary_id)
                            activity_ids_for_user.append(activity_primary_id)
                            activity_primary_id += 1

            self.add_trackpoints_to_db(activity_docs)
            self.update_activity_list(activity_ids_for_user)

            # next user should have [] from start
            activity_ids_for_user = []

    def add_trackpoints_to_db(self, activity_docs):
        # add all track point docs to collection in db
        activity_collection = self.db["Activity"]
        if activity_docs != []:
            activity_collection.insert_many(activity_docs)

    def update_activity_list(self, activity_ids_for_user):
        # update acitvity list in user_doc
        user_query = {"activities": []}
        new_activities_for_user = {
            "$set": {'activities': activity_ids_for_user}}
        self.db['User'].update_one(user_query, new_activities_for_user)

    def add_labeled_files_to_dict(self, root, fn):
        with open(root + '/' + fn, 'r') as labels_file:
            all_rows = labels_file.read().splitlines()
            for row in all_rows[1:]:
                start_time, end_time, mode = row.split('\t')

                # Convert to DateTime:
                start_time = start_time.replace("/", "-")
                end_time = end_time.replace("/", "-")

                # Add to dictionary
                self.potential_matches[root[-3:]][0].append(start_time)
                self.potential_matches[root[-3:]][1].append(end_time)
                self.potential_matches[root[-3:]][2].append(mode)

    def get_activity(self, root, fn):
        with open(root + '/' + fn, 'r') as f:
            return f.read().splitlines()[6:]

    def check_valid_activity(self, activity):
        if len(activity) <= 2500:
            return True

        return False

    def format_start_and_end_time(self, activity):
        # Save time as YYYY-MM-DD HH:MM:SS strings
        activity_start = activity[0].split(
            ',')[5] + ' ' + activity[0].split(',')[6]
        activity_end = activity[-1].split(
            ',')[5] + ' ' + activity[-1].split(',')[6]
        return activity_start, activity_end

    def correct_start_and_end_time(self, activity_start, activity_end, user):
        if activity_start in self.potential_matches[user][0]:
            # This triggers when we find a match for the start time.
            # We also have to ensure that the corresponding end time also matches.
            ind = self.potential_matches[user][0].index(
                activity_start)
            if activity_end == self.potential_matches[user][1][ind]:
                self.transp_mode = "'" + \
                    self.potential_matches[user][2][ind] + "'"

    def create_coll(self, collection_name):
        collection = self.db.create_collection(collection_name)
        print('Created collection: ', collection)

    def drop_coll(self, collection_name):
        collection = self.db[collection_name]
        collection.drop()
        print('Collection ' + collection_name + ' dropped. ')

    def drop_all_colls(self, program):
        program.drop_coll(collection_name="User")
        program.drop_coll(collection_name='Activity')
        program.drop_coll(collection_name='TrackPoint')

    def create_all_colls(self, program):
        program.create_coll(collection_name="User")
        program.create_coll(collection_name="Activity")
        program.create_coll(collection_name="TrackPoint")


def main():
    program = None
    try:
        print('start program')
        program = InsertDataProgram()

        # Uncomment to create all collections
        # program.create_all_colls(program)

        # Uncomment to add all data - remember to check dataset path
        # program.add_all_data()

        # Uncomment for dropping collections
        # program.drop_all_colls(program)

    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()
