from functools import partial # For a simple selection menu
from datetime import datetime
from DbConnector import DbConnector
from haversine import haversine, Unit
from pprint import pprint
import utils

def one(db):
    """Find number of entries in each collection"""
    print("User count", "Activity count", "TrackPoint count", sep="\t")
    print(db['User'].count(), db['Activity'].count(), db['TrackPoint'].count(), sep="\t\t")
    return 

def two(db):
    """Find average, min and max number of activities per user"""
    avg_pipeline = [
        {"$project": {
            "activity_size": { "$size": "$activities" }
        } },
        { "$group": {
            "_id" : "null",
            "count": { "$avg": "$activity_size"}
        } }
    ]
    avg_res = db['User'].aggregate(avg_pipeline)

    max_pipeline = [
        {"$project": {
            "activity_size": { "$size": "$activities" }
        } },
        { "$group": {
            "_id" : "null",
            "count": { "$max": "$activity_size"}
        } }
    ]
    max_res  = db['User'].aggregate(max_pipeline)

    min_pipeline = [
        {"$project": {
            "activity_size": { "$size": "$activities" }
        } },
        { "$group": {
            "_id" : "null",
            "count": { "$min": "$activity_size"}
        } }
    ]
    min_res = db['User'].aggregate(min_pipeline)
    
    print("Average", "Max", "Min", sep="\t")
    print(
        round(dict(list(avg_res)[-1])['count'], 2),
        dict(list(max_res)[-1])['count'],
        dict(list(min_res)[-1])['count']
    , sep='\t')
    return avg_res, max_res, min_res

def three(db):
    """10 users with the most number of activities"""
    pipeline = [
        {"$project": {
            "activity_size": { "$size": "$activities" }
        } },
        { "$sort": {
            "activity_size": -1
        } },
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
        user_ID = utils.single_val(cursor=db['User'].find( { 'activities': act }, {'_id': 1} ), key='_id')
        users.add(user_ID)
    print("Number of users that have started the activity in one day,\nand ended the activity the next day:")
    print(len(users))

def six(db):
    """Find user_ids 'close' to given infected person"""
    HOUNDED_METER_FEET = 328.08399 # 100m ~ 328 feet'
    SIXTY_SECONDS_DAYS = 60/86_400 # 86,400 seconds = 1 day
    infected_position =  (39.97548, 116.33031)
    # Get infected time and convert to same format as in database
    infected_time = datetime.strptime('2008-08-24 15:38:00', '%Y-%m-%d %H:%M:%S')
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
        "7": partial(print, ""),
        "8": partial(print, ""),
        "9": partial(print, ""),
        "10": partial(print, ""),
        "11": partial(print, ""),
        "12": partial(print, ""),
        "q": partial(print, "")
    }
    while menu_selection != 'q':
        print("Selection menu", "Select query number from 1 to 12", "Enter 'q' to exit", sep='\n')
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