from functools import partial # For a simple selection menu

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
    return db['User'].aggregate(pipeline)

def four(db):
    activity_list = []
    users = set()
    for doc in db['Activity'].find({}):
        start_day = doc['start_date_time'][8:10]
        end_day = doc['end_date_time'][8:10]
        if start_day != end_day:
            activity_list.append(doc["_id"])
    for act in activity_list:
        users.add(db['User'].find( { 'activities': act }, {'_id': 1}))
    return len(users), users



def select_menu():
    """Selection menu so user may choose tasks easily"""
    menu_selection = ''
    menu = {
        "1": partial(one),
        "2": partial(two),
        "3": partial(three),
        "4": partial(four),
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
        except KeyError:
            print("Invalid selection, try again.")


def main():
    select_menu()
    pass

if __name__ == '__main__':
    main()