from datetime import datetime

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
    pipeline = [
    {"$project": {
        "activity_size": { "$size": "$activities" }
    } },
    { "$sort": {
        "activity_size": -1
    } },
    {"$limit": 10}
    ]
    activity_list = []
    for doc in db['Activity'].find({}):
        start_day = doc['start_date_time'][8:10]
        end_day = doc['end_date_time'][8:10]
        if start_day != end_day:
            activity_list.append(doc["_id"])
    # db['User'].find({'_id': act})
    # Psudeocode
    # QUERY acitivities where start_day != end_day
    # for r in res:
    #   find user_id where r._id is in user_id['activities']
    #   place in set
    #   return len(set)
    pass

def main():
    pass

if __name__ == '__main__':
    main()