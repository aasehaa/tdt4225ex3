import pymongo

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
    



def main():
    pass

if __name__ == '__main__':
    main()