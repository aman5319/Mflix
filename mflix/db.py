from pymongo import MongoClient, ASCENDING, DESCENDING
from pymongo.errors import DuplicateKeyError
from bson.objectid import ObjectId
from bson.errors import InvalidId
from flask import g
from mflix import app
from bson.json_util import dumps
import datetime


def get_db():
    """Opens a new database connection if there is none yet for the
    current application context.
    """
    if not hasattr(g, 'db'):
        MFLIX_DB_URI = "mongodb+srv://amidezcod:coder6120164@cluster0-ksslj.mongodb.net/"
        try:
            conn = MongoClient(MFLIX_DB_URI)
            g.db = conn['mflix']
        except KeyError:
            raise Exception("You haven't configured your MFLIX_DB_URI!")
    return g.db


@app.teardown_appcontext
def close_db(error):
    """Closes the database again at the end of the request."""
    pass


def timeneeded(func):
    def func1(*args, **kwargs):
        tiem = datetime.datetime.now().microsecond
        func(*args, **kwargs)
        print(datetime.datetime.now().microsecond - tiem)

    return func1

@app.route("/<int:page>")
@app.route("/<int:page>/<string:filters>")
def get_movies(page, filters={}, movies_per_page=20):
    sort_key = "tomatoes.viewer.numReviews"
    if "$text" in filters:
        score_meta_doc = {"$meta": "textScore"}
        movies = get_db().movies.find(filters, {"score": score_meta_doc}).sort([("score", score_meta_doc)]).skip(
            movies_per_page * page).limit(movies_per_page)
    else:
        movies = get_db().movies.find({}).sort(sort_key, DESCENDING).skip(movies_per_page * page).limit(movies_per_page)
    return dumps(movies)


'''
Returns a MongoDB movie given an ID.
'''


def get_movie(id):
    try:
        return get_db().movies.find_one({"_id": ObjectId(id)})
    except InvalidId:
        return None


'''
Returns list of all genres in the database.
'''


def get_all_genres():
    return list(get_db().movies.aggregate([
        {"$unwind": "$genres"},
        {"$project": {"_id": 0, "genres": 1}},
        {"$group": {"_id": None, "genres": {"$addToSet": "$genres"}}}
    ]))[0]["genres"]


'''
Returns a MongoDB user given an email.
'''


def get_user(email):
    return get_db().users.find_one({"email": email})


'''
Takes in the three required fields needed to add a user,
    and adds one to MongoDB.
'''


def add_user(name, email, hashedpw):
    try:
        get_db().users.insert_one({"name": name, "email": email, "pw": hashedpw})
        return {"success": True}
    except DuplicateKeyError:
        return {"error": "A user with the given email already exists."}


'''
Takes in the three required fields needed to add a user,
    and adds one to MongoDB.
'''


def add_comment_to_movie(movieid, user, comment, date):
    MOVIE_COMMENT_CACHE_LIMIT = 10

    comment_doc = {
        "name": user.name,
        "email": user.email,
        "movie_id": movieid,
        "text": comment,
        "date": date
    }

    movie = get_movie(movieid)
    if movie:
        update_doc = {
            "$inc": {
                "num_mflix_comments": 1
            },
            "$push": {
                "comments": {
                    "$each": [comment_doc],
                    "$sort": {"date": -1},
                    "$slice": MOVIE_COMMENT_CACHE_LIMIT
                }
            }
        }

        # let's set an `_id` for the comments collection document
        comment_doc["_id"] = "{0}-{1}-{2}".format(movieid, user.name, \
                                                  date.timestamp())

        get_db().comments.insert_one(comment_doc)

        get_db().movies.update_one({"_id": ObjectId(movieid)}, update_doc)


'''
Takes in the two required fields needed to remove a comment,
    and removes it from the appropriate places
'''


def delete_comment_from_movie(movieid, commentid):
    get_db().comments.delete_one({"_id": commentid})

    movie = get_db().movies.find_one({"_id": ObjectId(movieid)})

    # check to see if the comment is on the movie doc too
    movie = get_db().movies.find_one({"comments._id": commentid})

    # regardless, decrement the count
    update_doc = {
        "$inc": {
            "num_mflix_comments": -1
        }
    }

    # if so, query to find new list of comments, update the movie doc with
    # them, and decrement the count
    if movie:
        embedded_comments = get_db().comments.find({"movie_id": ObjectId(movieid)}) \
            .sort("date", DESCENDING) \
            .limit(10)
        update_doc["$set"] = {"comments": list(embedded_comments)}

    get_db().movies.update_one({"_id": ObjectId(movieid)}, update_doc)


'''
Returns all comments from just the comments collection given a movie ID.
'''


def get_movie_comments(id):
    try:
        return get_db().comments.find({"movie_id": ObjectId(id)}) \
            .sort("date", DESCENDING)
    except InvalidId:
        return None
