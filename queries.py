
from functools import partial  # For a simple selection menu
from datetime import datetime
from DbConnector import DbConnector
from haversine import haversine, Unit
from pprint import pprint
import math
from bson.json_util import dumps, loads
import utils
try:
    from tqdm import tqdm
except:
    def tqdm(*args):
        return args



def one(db):
    """Find number of entries in each collection"""
    print("User count", "Activity count", "TrackPoint count", sep="\t")
    print(db['User'].count(), db['Activity'].count(),
          db['TrackPoint'].count(), sep="\t\t")
    return

def two(db):
    """Find average, min and max number of activities per user"""
    avg_pipeline = [ # $size gets length of user['activities'] list
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
    # Prints results
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
    return docs

def four(db):
    """Number of users with activity ending other day than it started"""
    activity_list = []
    users = set()
    for doc in db['Activity'].find({}):
        start_day = doc['start_date_time'][8:10] # Since dates are stored as strings, we need to slice it
        end_day = doc['end_date_time'][8:10]     # to get the days. We also assume no activities end
        if start_day != end_day:                 # 1 month after start (but same day of month)
            activity_list.append(doc["_id"])
    for act in activity_list:

        # TODO this seems slow, can probably be optimized
        user_ID = utils.single_val(cursor=db['User'].find( { 'activities': act }, {'_id': 1} ), key='_id')
        users.add(user_ID) # Sets skip adding duplicates so we only get unique UserIDs
    print("Number of users that have started the activity in one day,\nand ended the activity the next day:")
    print(len(users))

def five(db):
    """Find activities that are registered multiple times. You should find the query
    even if you get zero results.
    """

    activity_hash = {}
    activities = db['Activity'].find({})

    for act in activities:
        hash_value = act['start_date_time'] + \
            act['end_date_time'] + act['transportation_mode']

        if hash_value in activity_hash:
            activity_hash[hash_value].append(act['_id'])

        else:
            activity_hash[hash_value] = [act['_id']]

    only_multiple_hashes = {}
    for act in activity_hash:
        if len(activity_hash[act]) > 1:
            only_multiple_hashes[act] = activity_hash[act]

    users = db['User'].find({})
    num_of_duplicates = 0
    for user in users:
        for hash_value in only_multiple_hashes:
            count = 0
            for id in only_multiple_hashes[hash_value]:
                if id in user['activities']:
                    count += 1
            if count > 1:
                num_of_duplicates += 1

    print('Number of duplicated activities:', num_of_duplicates)

def six(db):
    """Find user_ids 'close' to given infected person"""

    SIXTY_SECONDS_DAYS = 60/86_400 # 86,400 seconds = 1 day
    infected_position =  (39.97548, 116.33031)

    # Get infected time and convert to same format as in database
    infected_time = datetime.strptime(
        '2008-08-24 15:38:00', '%Y-%m-%d %H:%M:%S')
    infected_time = utils.posix_to_excel(datetime.timestamp(infected_time))

    close_activities = set() # For storing activity IDs
    
    TP_given_time = db['TrackPoint'].find({ # First limit to trackpoints from the same time frame
        "date_days": {
            "$gt": infected_time - SIXTY_SECONDS_DAYS,
            "$lt":  infected_time + SIXTY_SECONDS_DAYS
        }
    })
    for TP in TP_given_time:
        if TP['activity_id'] not in close_activities: # Skip if activity is already added
            coords = (TP['lat'], TP['lon'])
            distance = haversine(infected_position, coords, unit=Unit.METERS)
            if distance <= 100:
                # Here we would've added an altitude check, but we don't know what altitude the infected is at
                # Instead, just add the activity
                close_activities.add(TP['activity_id'])

    # Now we need to get unique user IDs from the collection of activities that match
    close_list = list(close_activities)
    close_contacts = db['User'].distinct(
        "_id",
        {"activities": {"$in": close_list}
         })
    print("Close contacts with infected:")
    for person in close_contacts:


        print(person)
    
    return close_contacts

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

def eight(db):
    """Find all types of transportation modes and count how many distinct users that
    have used the different transportation modes. Do not count the rows where the
    transportation mode is null"""

    # find the activities of the users with labels 
    has_labels = db['User'].find({"has_labels": True }, {"_id":0,"activities":1})

    activities = []
    for doc in has_labels:
        activities.append(doc['activities'])
        
    j = dumps(activities)
    act = loads(j)
   
    # find the transportation mode for each activity for each user
    transportation_modes = []
    for user in act:
        res = []
        for activity in user:
            mode = db['Activity'].find({"_id": activity, "transportation_mode": {"$ne": "NULL"}}, {"_id":0, "transportation_mode":1})
            res.append(mode)
        transportation_modes.append(res)

    j = dumps(transportation_modes)
    t_mode = loads(j)

    # remove dictionary-format, only list actual transportation-modes
    mode = []
    for transportation_mode in t_mode:
        only_trans_mode = []
        for el in transportation_mode:
            if el == []:
                continue
            for dict in el:
                only_trans_mode.append(dict["transportation_mode"])
        mode.append(only_trans_mode)

    # make each users list of transportation modes into a set to find distinct values
    distinct = {}
    for el in mode:
        distinct_trans_modes_per_user = set(el)
        for x in distinct_trans_modes_per_user:
            if x not in distinct:
                distinct[x] = 1
            else:
                distinct[x] += 1

    print(distinct)
    return distinct

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

def ten(db):
    """Find the total distance (in km) walked in 2008, by user with id=112."""

    activities = None
    for a in db['User'].find({"_id":"112"}, {"_id":0, "activities":1}):
        activities = a['activities']

    activity_with_information = []
    for activity in activities:
        activity_with_information.append(db['Activity'].find({"_id": activity}))

    act = dumps(activity_with_information)
    activities_json = loads(act)
    
    walk = []
    for doc in activities_json:
        for activity in doc:
            if activity['transportation_mode'] == "'walk'" and (activity['start_date_time'][0:3] == 2008 or activity['end_date_time'][0:3] == 2008):
                walk.append(activity)
        
    tp = []
    for el in walk:
        tp.append(db['TrackPoint'].find({"activity_id" : el["_id"]}, {"_id":0,"lat":1, "lon":1}))
    
    a = dumps(tp)
    points = loads(a)

    total_distance = 0
    for s in range(0, len(points)-1):
        if len(points) <= 1:
            break
        for g in range(0, len(points[s])-1):
            total_distance += haversine((float(points[s][g]["lat"]), float(points[s][g]["lon"])), (float(points[s][g+1]["lat"]), float(points[s][g+1]["lon"])))

    print('The total distance walked by user with id=112 in 2008 is:')
    print(total_distance)
    return total_distance

def eleven(db):
    """Get top 20 users with most altitude gained over all activities"""
    ONE_METER_FEET = 0.3048 # 1 foot ~0.3 feet
    alt_gained = dict()
    for i in range(1, 182):
        alt_gained[str(i).zfill(3)] = 0

    for user in tqdm(alt_gained.keys()):
        # For each user, get the list of activities:
        activities_to_user = utils.single_val(db.User.find({"_id": user}), 'activities')
        # TODO 'single_val` could be optimized better as it re-casts query result several times
        for act in tqdm(activities_to_user): # act_list:
            TP_list = list(db.TrackPoint.find({"activity_id": act}))
            for i in range(1, len(TP_list)):
                if TP_list[i-1] == -777 or TP_list[i] == -777: # Skip invalid altitude all-together
                    continue
                altitude_difference = TP_list[i] - TP_list[i-1]
                if altitude_difference > 0:
                    # Add difference between last and current altitude to dictionary
                    alt_gained[user] += altitude_difference
    
    # Get the top 20 highest total altitude
    top_users = sorted(alt_gained, key=alt_gained.get, reverse=True)[:20]
    print("Query 11\nPlace\tUserID\tAltitude gained")
    for num, usr in enumerate(top_users):
        print(num+1, usr, alt_gained[usr]*ONE_METER_FEET, sep='\t') # Translating feet to meter here

    return alt_gained

def twelve(db):
    """Find number of invalid activities for each user"""
    TIMEOUT_CRITERIA_FROM_TIMESTAMP = 5 * 60/86_400 # 5 minutes in "day unit"
    invalid_dict = dict()
    for i in range(1,182):
        invalid_dict[str(i).zfill(3)] = 0

    for user in tqdm(invalid_dict.keys()):
        act_list = utils.single_val(db.User.find({"_id": user}), "activities")
        for act in tqdm(act_list):
            TP_list = list(db.TrackPoint.find({"activity_id": act}))
            for i in range(1, len(TP_list)):
                time_difference = abs(TP_list[i]['date_days'] - TP_list[i-1]['date_days'])
                if time_difference >= TIMEOUT_CRITERIA_FROM_TIMESTAMP:
                    # Triggers if activity is invalid
                    invalid_dict[user] += 1
                    break # Breaks out of TP loop so we jump to the next activity
    
    # Print results 
    print("UserID", "# Invalid activities", sep="\t")
    for user_ID, num_invalid in invalid_dict.items():
        if num_invalid != 0:
            print(user_ID, num_invalid, sep="\t\t")
    return invalid_dict



def select_menu(*args):
    """Selection menu so user may choose tasks easily"""
    menu_selection = ''
    menu = {
        "1": partial(one, *args),
        "2": partial(two, *args),
        "3": partial(three, *args),
        "4": partial(four, *args),
        "5": partial(five, *args),
        "6": partial(six, *args),
        "7": partial(seven, *args),
        "8": partial(eight, *args),
        "9": partial(nine, *args),
        "10": partial(ten, *args),
        "11": partial(eleven, *args),
        "12": partial(twelve, *args),
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

