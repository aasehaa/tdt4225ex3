from pprint import pprint
from DbConnector import DbConnector
import os
# import pandas as pd
from datetime import datetime
from tabulate import tabulate
from typing import Tuple
from models import TrackPointObj

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

    def create_coll(self, collection_name):
        collection = self.db.create_collection(collection_name)
        print('Created collection: ', collection)

    def insert_documents(self, collection_name):
        docs = []  # here comes code that shows the collection somehow
        collection = self.db[collection_name]
        collection.insert_many(docs)

    def fetch_documents(self, collection_name):
        collection = self.db[collection_name]
        documents = collection.find({})
        for doc in documents:
            pprint(doc)

    def add_all_data(self):

        dataset_path = os.path.dirname(__file__) + "/../dataset"
        test_dataset_path = os.path.dirname(__file__) + "/../testDataset"

        activity_primary_id = 0
        track_point_id = 0

        user_doc = {
            "_id": "",
            "has_labels": False,
            "activities": []
        }

        with open(test_dataset_path + '/labeled_ids.txt', 'r') as fs:
            labeled_IDs = fs.read().splitlines()

        for level, (root, dirs, files) in enumerate(os.walk(test_dataset_path + '/Data')):
            print('hurra')
            if level == 0:
                for id in dirs:
                    user_has_labels = id in labeled_IDs
                    user_doc["_id"] = id
                    user_doc["has_labels"] = user_has_labels
                    # Create a 2D list for each ID in a dictionary made for matching activities with their labels
                    self.potential_matches[id] = [[], [], []]

            activity_docs = []

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

                                track_point = self.create_track_point(
                                    point, activity_primary_id)

                                track_point_docs.append({
                                    "_id": track_point_id,
                                    "lat": track_point.lat,
                                    "lon": track_point.long,
                                    "altitude": track_point.altitude,
                                    "date_days": track_point.date_days,
                                    "date_time": track_point.date_time
                                })

                                track_points.append(track_point_id)
                                track_point_id += 1

                            # add all track point docs tocollection in db
                            track_point_collection = self.db["TrackPoint"]
                            track_point_collection.insert_many(
                                track_point_docs)

                            activity_docs.append({
                                "_id": activity_primary_id,
                                "transportation_mode": self.transp_mode,
                                "start_date_time": activity_start,
                                "end_date_time": activity_end,
                            })

                            user_doc["activities"].append(activity_primary_id)
                            activity_primary_id += 1

            if(activity_docs != []):

                # add all track point docs to collection in db
                activity_collection = self.db["Activity"]
                activity_collection.insert_many(activity_docs)

        if(user_doc != []):

            # add user to collection
            user_collection = self.db["User"]
            user_collection.insert_one(user_doc)

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

    def create_track_point(self, point, activity_ID):
        lat, long, _, alt, timestamp, date, time = point.split(
            ',')
        time_datetime = date + " " + time

        track_point = TrackPointObj.TrackPoint(
            activity_ID, lat, long, alt, timestamp, time_datetime)

        return track_point

    def correct_start_and_end_time(self, activity_start, activity_end, user):
        if activity_start in self.potential_matches[user][0]:
            # This triggers when we find a match for the start time.
            # We also have to ensure that the corresponding end time also matches.
            ind = self.potential_matches[user][0].index(
                activity_start)
            if activity_end == self.potential_matches[user][1][ind]:
                self.transp_mode = "'" + \
                    self.potential_matches[user][2][ind] + "'"

    def drop_coll(self, collection_name):
        collection = self.db[collection_name]
        collection.drop()
        print('Collection ' + collection_name + ' dropped. ')

    def fetch_documents(self, collection_name):
        collection = self.db[collection_name]
        documents = collection.find({})
        for doc in documents:
            pprint(doc)

    def drop_all_colls(self):
        self.db.dropDatabase()


def main():
    program = None
    try:
        print('start program')
        program = InsertDataProgram()

        # Uncomment to create all collections
        # program.create_coll(collection_name="User")
        # program.create_coll(collection_name="Activity")
        # program.create_coll(collection_name="TrackPoint")

        # Uncomment to add all data - remember to check dataset path
        # program.add_all_data()

        # Uncomment for dropping collections
        # program.drop_coll(collection_name="User")
        # program.drop_coll(collection_name='Activity')
        # program.drop_coll(collection_name='TrackPoint')

        # Test code to check trackpoints or activities (does not work for user)
        #collection = program.db['TrackPoint']
        #documents = collection.find({}).limit(5)
        # for doc in documents:
        # pprint(doc)
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()
