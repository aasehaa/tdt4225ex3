from functools import partial # For a simple selection menu
from datetime import datetime
from DbConnector import DbConnector
from haversine import haversine, Unit
from pprint import pprint
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
    SIXTY_SECONDS_DAYS = 60/86_400 # 86,400 seconds = 1 day
    infected_position =  (39.97548, 116.33031)
    # Get infected time and convert to same format as in database
    infected_time = datetime.strptime('2008-08-24 15:38:00', '%Y-%m-%d %H:%M:%S')
    infected_time = utils.posix_to_excel(datetime.timestamp(infected_time))

    close_activities = set()
    
    TP_given_time = db['TrackPoint'].find({
        "date_days": {
            "$gt": infected_time - SIXTY_SECONDS_DAYS,
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
        print(person)
    
    return close_contacts

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
    tm = []
    for user in act:
        res = []
        for activity in user:
            mode = db['Activity'].find({"_id": activity, "transportation_mode": {"$ne": "NULL"}}, {"_id":0, "transportation_mode":1})
            res.append(mode)
        tm.append(res)

    j = dumps(tm)
    t_mode = loads(j)

    # remove dictionary-format, only list actual transportation-modes
    mode = []
    for t in t_mode:
        f = []
        for g in t:
            if g == []:
                continue
            for s in g:
                f.append(s["transportation_mode"])
        mode.append(f)

    # make each users list of transportation modes into a set to find distinct values
    d = {}
    for el in mode:
        s = set(el)
        for x in s:
            if x not in d:
                d[x] = 1
            else:
                d[x] += 1

    print(d)
    return d

def ten(db):
    """Find the total distance (in km) walked in 2008, by user with id=112."""

    activities = None
    for a in db['User'].find({"_id":"112"}, {"_id":0, "activities":1}):
        activities = a['activities']

    cursor = []
    for activity in activities:
        cursor.append(db['Activity'].find({"_id": activity}))

    act = dumps(cursor)
    liste = loads(act)
    
    walk = []
    for doc in liste:
        for c in doc:
            if c['transportation_mode'] == "'walk'" and (c['start_date_time'][0:3] == 2008 or c['end_date_time'][0:3] == 2008):
                walk.append(c)
        
    x = []
    for el in walk:
        x.append(db['TrackPoint'].find({"activity_id" : el["_id"]}, {"_id":0,"lat":1, "lon":1}))
    
    a = dumps(x)
    li = loads(a)

    total_distance = 0
    for s in range(0, len(li)-1):
        if len(li) <= 1:
            break
        for g in range(0, len(li[s])-1):
            total_distance += haversine((float(li[s][g]["lat"]), float(li[s][g]["lon"])), (float(li[s][g+1]["lat"]), float(li[s][g+1]["lon"])))

    print('The total distance walked by user with id=112 in 2008 is:')
    print(total_distance)
    return total_distance


def eleven(db):
    ONE_METER_FEET = 0.3048 # 1 foot ~0.3 feet
    alt_gained = dict()
    for i in range(1,182):
        alt_gained[str(i).zfill(3)] = 0
    
    for user in tqdm(alt_gained.keys()):
        activities_to_user = utils.single_val(db.User.find({"_id": user}), 'activities')
        # act_list = db.User.find({"_id": user}) # user dictionary result
        for act in tqdm(activities_to_user): # act_list:
            TP_list = db.TrackPoint.find({"activity_id": act})
            prev_TP_alt = 0
            for TP in TP_list:
                if TP['altitude'] == -777: # Skip invalid altitude all-together
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

def select_menu(*args):
    """Selection menu so user may choose tasks easily"""
    menu_selection = ''
    menu = {
        "1": partial(one, *args),
        "2": partial(two, *args),
        "3": partial(three, *args),
        "4": partial(four, *args),
        "5": partial(print, ""),
        "6": partial(six, *args),
        "7": partial(print, ""),
        "8": partial(eight, *args),
        "9": partial(print, ""),
        "10": partial(ten, *args),
        "11": partial(eleven, *args),
        "12": partial(twelve, *args),
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

