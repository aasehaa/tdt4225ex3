from functools import partial
# from typing_extensions import final  # For a simple selection menu
from DbConnector import DbConnector
from pprint import pprint
from datetime import datetime
import math


def one(db):
    """Find number of entries in each collection"""
    print("User count", "Activity count", "TrackPoint count", sep="\t")
    print(db['User'].count(), db['Activity'].count(),
          db['TrackPoint'].count(), sep="\t\t")
    return


def two(db):
    """Find average, min and max number of activities per user"""
    avg_pipeline = [
        {"$project": {
            "activity_size": {"$size": "$activities"}
        }},
        {"$group": {
            "_id": "null",
            "count": {"$avg": "$activity_size"}
        }}
    ]
    avg_res = db['User'].aggregate(avg_pipeline)

    max_pipeline = [
        {"$project": {
            "activity_size": {"$size": "$activities"}
        }},
        {"$group": {
            "_id": "null",
            "count": {"$max": "$activity_size"}
        }}
    ]
    max_res = db['User'].aggregate(max_pipeline)

    min_pipeline = [
        {"$project": {
            "activity_size": {"$size": "$activities"}
        }},
        {"$group": {
            "_id": "null",
            "count": {"$min": "$activity_size"}
        }}
    ]
    min_res = db['User'].aggregate(min_pipeline)

    print("Average", "Max", "Min", sep="\t")
    print(
        round(dict(list(avg_res)[-1])['count'], 2),
        dict(list(max_res)[-1])['count'],
        dict(list(min_res)[-1])['count'], sep='\t')
    return avg_res, max_res, min_res


def three(db):
    """10 users with the most number of activities"""
    pipeline = [
        {"$project": {
            "activity_size": {"$size": "$activities"}
        }},
        {"$sort": {
            "activity_size": -1
        }},
        {"$limit": 10}
    ]
    docs = db['User'].aggregate(pipeline)
    for doc in docs:
        pprint(doc)
    return


def four(db):
    activity_list = []
    users = set()
    for doc in db['Activity'].find({}):
        start_day = doc['start_date_time'][8:10]
        end_day = doc['end_date_time'][8:10]
        if start_day != end_day:
            activity_list.append(doc["_id"])
    for act in activity_list:
        users.add(db['User'].find({'activities': act}, {'_id': 1}))
    return len(users), users


def seven(db):
    """Find all users that have never taken a taxi."""

    # loop though activities and filter by not taxi
    taxi_pipeline = [
        {
            "$match": {"transportation_mode": {"$nin": ["taxi"]}}
        },
    ]

    not_taxi_activity_docs = db['Activity'].aggregate(taxi_pipeline)

    not_taxi_users = []
    for user_doc in db['User'].find({}):
        user_count_taxi = 0
        for no_taxi_activity in not_taxi_activity_docs:
            if no_taxi_activity in user_doc['activities']:
                continue
            else:
                user_count_taxi += 1

        if user_count_taxi == 0:
            not_taxi_users.append(user_doc['_id'])

    print('Users with no taxi activities: ')
    print(not_taxi_users)


def nine(db):
    """ a) Find the year and month with the most activities.
        b) Which user had the most activities this year and month, and how many
        recorded hours do they have? Do they have more hours recorded than the user
        with the second most activities?"""

    year_and_month = {}
    for doc in db['Activity'].find({}):
        end_year_and_month = doc['end_date_time'][0:7]
        if end_year_and_month in year_and_month:
            year_and_month[end_year_and_month] += 1
        else:
            year_and_month[end_year_and_month] = 1
    max_year_and_month = max(year_and_month, key=year_and_month.get)

    print("Year and month with most activities: ", max_year_and_month)

    activities_in_year_and_month = {}
    for activity_doc in db['Activity'].find({}):
        end_date = datetime.strptime(
            activity_doc['end_date_time'], "%Y-%m-%d %H:%M:%S")
        start_date = datetime.strptime(
            activity_doc['start_date_time'], "%Y-%m-%d %H:%M:%S")

        if (start_date.year == 2008) & (start_date.month == 11):
            difference = end_date - start_date
            difference_hours = math.floor(difference.days * 24 +
                                          difference.seconds/3600)
            activities_in_year_and_month[activity_doc['_id']
                                         ] = difference_hours

    # find users with those activities
    users_with_relevant_activities = {}
    users_with_relevant_hours = {}

    for user_doc in db['User'].find({}):
        for activity in activities_in_year_and_month:
            activity_id = activity
            user_id = user_doc['_id']
            user_activities = user_doc['activities']
            if activity_id in user_activities:
                hours = activities_in_year_and_month[activity]
                if user_id in users_with_relevant_activities:
                    users_with_relevant_activities[user_id
                                                   ] += 1
                    users_with_relevant_hours[user_id] += hours
                else:
                    users_with_relevant_activities[user_id
                                                   ] = 1
                    users_with_relevant_hours[user_id] = hours

    sorted_by_hours = sorted(
        users_with_relevant_activities, key=users_with_relevant_activities.get, reverse=True)
    print('user with most activities and their hours: ')
    print('id: ', sorted_by_hours[0], '\n', 'hours: ',
          users_with_relevant_hours[sorted_by_hours[0]])
    print('user with next most activities and their hours: ')
    print('id: ', sorted_by_hours[1], '\n', 'hours: ',
          users_with_relevant_hours[sorted_by_hours[1]])


def select_menu(*args):
    """Selection menu so user may choose tasks easily"""
    menu_selection = ''
    menu = {
        "1": partial(one, *args),
        "2": partial(two, *args),
        "3": partial(three, *args),
        "4": partial(four, *args),
        "5": partial(print, ""),
        "6": partial(print, ""),
        "7": partial(seven, *args),
        "8": partial(print, ""),
        "9": partial(nine, *args),
        "10": partial(print, ""),
        "11": partial(print, ""),
        "12": partial(print, ""),
        "q": partial(print, "")
    }
    while menu_selection != 'q':
        print("Selection menu", "Select query number from 1 to 12",
              "Enter 'q' to exit", sep='\n')
        menu_selection = input("Choose task: ").lower()
        try:
            menu[menu_selection]()
            print(25*"-")
        except KeyError:
            print("Invalid selection, try again.")


def main():
    connection = DbConnector()
    db = connection.db
    try:
        select_menu(db)
    except Exception as e:
        print("Unexpected error while querying database", e, sep="\n")
    finally:
        connection.close_connection()
    pass


if __name__ == '__main__':
    main()
