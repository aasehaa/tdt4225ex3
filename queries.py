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
            "count": { "$avg": "$activity_size"}
        } }
    ] # Not yet tested...
    avg_res = db['User'].aggregate(avg_pipeline)

    max_pipeline = 0
    max_res  = 0

    min_pipline = 0
    min_res = 0
    
    return avg_res, max_res, min_res
    



def main():
    pass

if __name__ == '__main__':
    main()