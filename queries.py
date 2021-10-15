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



def main():
    pass

if __name__ == '__main__':
    main()