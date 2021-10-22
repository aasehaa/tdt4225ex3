from functools import partial  # For a simple selection menu
from datetime import datetime
from DbConnector import DbConnector
from haversine import haversine, Unit
from pprint import pprint
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
        # TODO this seems slow, can probably be optimized
        user_ID = utils.single_val(cursor=db['User'].find(
            {'activities': act}, {'_id': 1}), key='_id')
        users.add(user_ID)
    print("Number of users that have started the activity in one day,\nand ended the activity the next day:")
    print(len(users))


def five(db):
    """Find activities that are registered multiple times. You should find the query
    even if you get zero results."""

    # activities that are registerd multiple times - not same id
    activity_hash = []
    multiple_activities_registered = []
    activities = db['Activity'].find({})
    count = 0

    for act in activities:
        if count < 3:
            hash_value = act['start_date_time'] + \
                act['end_date_time'] + act['transportation_mode']
            # print(hash_value)

            if hash_value in activity_hash:
                print(hash_value)
                print(activity_hash)
                count += 1
                multiple_activities_registered.append(act['_id'])
            else:
                activity_hash.append(hash_value)
    """
    activities = db['Activity'].aggregate([
        {"$group": {"_id": "start_date_time", "count": {"$sum": 1}}},
        {"$match": {"_id": {"$ne": "null"}, "count": {"$gt": 1}}},
        {"$project": {"name": "$_id", "_id": 0}}
    ])

    for doc in activities:
        pprint(doc)
    """

    # print(count)
    # print(multiple_activities_registered)


def six(db):
    """Find user_ids 'close' to given infected person"""
    HOUNDED_METER_FEET = 328.08399  # 100m ~ 328 feet'
    SIXTY_SECONDS_DAYS = 60/86_400  # 86,400 seconds = 1 day
    infected_position = (39.97548, 116.33031)
    # Get infected time and convert to same format as in database
    infected_time = datetime.strptime(
        '2008-08-24 15:38:00', '%Y-%m-%d %H:%M:%S')
    infected_time = utils.posix_to_excel(datetime.timestamp(infected_time))

    close_activities = set()

    TP_given_time = db.TrackPoint.find({
        "date_days": {
            "$gt$": infected_time - SIXTY_SECONDS_DAYS,
            "$lt":  infected_time + SIXTY_SECONDS_DAYS
        }
    })
    for TP in TP_given_time:
        if TP['activity_id'] not in close_activities:
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
        print(person['_id'])

    return close_contacts


def eleven(db):
    ONE_METER_FEET = 0.3048  # 1 foot ~0.3 feet
    alt_gained = dict()
    for i in range(1, 182):
        alt_gained[str(i).zfill(3)] = 0

    for user in tqdm(alt_gained.keys()):
        act_list = db.User.find({"_id": user})
        for act in act_list:
            TP_list = db.TrackPoint.find({"activity_id": act['_id']})
            prev_TP_alt = 0
            for TP in TP_list:
                if TP['altitude'] == -777:  # Skip invalid altitude all-together
                    prev_TP_alt = 0
                    continue
                if TP['altitude'] > prev_TP_alt:
                    alt_gained[user] += TP['altitude'] - prev_TP_alt
                prev_TP_alt = TP['altitude']

    # Get the top 20 highest total altitude
    top_users = sorted(alt_gained, key=alt_gained.get, reverse=True)[:20]
    print("Query 11\nPlace\tUserID\tAltitude gained")
    for num, usr in enumerate(top_users):
        print(num+1, usr, alt_gained[usr]*ONE_METER_FEET, sep='\t')

    return alt_gained


def twelve(db):
    pass
    # "Psudeocode" (actually pretty much ready but not tested):
    """ 
    TIMEOUT_CRITERIA_FROM_TIMESTAMP = 5 * 60/86_400
    invalid_dict = dict()
    for i in range(1,182):
        invalid_dict[str(i).zfill(3)] = 0

    for user in tqdm(invalid_dict.keys()):
        act_list = db.User.find({"_id": user})
        for act in act_list:
            TP_list = db.TrackPoint.find({"activity_id": act['_id']})
            prev_TP_time = 0
            for TP in TP_list:
                time_difference = abs(TP['date_days'] - prev_TP_time)
                if time_difference >= TIMEOUT_CRITERIA_FROM_TIMESTAMP:
                    # Triggers if activity is invalid
                    invalid_dict[user] += 1
                    break # Breaks out of TP loop so we jump to the next activity
    pprint(invalid_dict)
    return invalid_dict
    """


def select_menu(*args):
    """Selection menu so user may choose tasks easily"""
    menu_selection = ''
    menu = {
        "1": partial(one, *args),
        "2": partial(two, *args),
        "3": partial(three, *args),
        "4": partial(four, *args),
        "5": partial(five, *args),
        "6": partial(print, ""),
        "7": partial(print, ""),
        "8": partial(print, ""),
        "9": partial(print, ""),
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
