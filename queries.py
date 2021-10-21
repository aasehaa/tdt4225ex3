from functools import partial
import pprint
from DbConnector import DbConnector
from bson.json_util import dumps, loads
from haversine import haversine

def one(db):
    """Find number of entries in each collection"""
    return db['User'].count(), db['Activity'].count(), db['TrackPoint'].count()

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
    
    return avg_res, max_res, min_res

    
def eight(db):
    """Find all types of transportation modes and count how many distinct users that
    have used the different transportation modes. Do not count the rows where the
    transportation mode is null"""

    res = db['Activity'].aggregate([
        {"$group": {
            "_id": "$transportation_mode", 
            "count": {"$sum":1}
            }
        }
    ])

    for doc in res:
        if doc['_id'] == 'NULL':
            continue
        print(doc)

    return res

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



def select_menu(*args):
    """Selection menu so user may choose tasks easily"""
    menu_selection = ''
    menu = {
        "1": partial(one, *args),
        "2": partial(two, *args),
        # "3": partial(three, *args),
        # "4": partial(four, *args),
        "5": partial(print, ""),
        "6": partial(print, ""),
        "7": partial(print, ""),
        "8": partial(eight, *args),
        "9": partial(print, ""),
        "10": partial(ten, *args),
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

