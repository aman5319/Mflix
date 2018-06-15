from mflix.db import get_movie, get_movies, get_all_genres, \
    add_comment_to_movie, get_movie_comments, add_user, \
    delete_comment_from_movie
from flask import  request, redirect, url_for, render_template
import flask_login
from bson.objectid import ObjectId
from urllib.parse import urlencode
from datetime import datetime
from mflix import app


@app.route('/')
def show_movies():
    MOVIES_PER_PAGE = 20

    # first determine the page of the movies to collect
    try:
        page = int(request.args.get('page'))
    except (TypeError, ValueError) as e:
        page = 0

    # format query string correctly for pagination
    args_copy = dict(request.args)

    args_copy["page"] = page - 1
    previous_page = urlencode(args_copy)

    args_copy["page"] = page + 1
    next_page = urlencode(args_copy)

    # determine any filters
    filters = {}
    genre = request.args.get('genre')
    if genre:
        filters["genres"] = genre
    search = request.args.get('search')
    if search:
        filters["$text"] = {"$search": search}

    # finally use the database and get what is necessary
    (movies, total_num_entries) = get_movies(filters, page, MOVIES_PER_PAGE)
    all_genres = get_all_genres()
    print(filters)

    # return the list of movies that works
    return render_template('movies.html',
                           movies=movies, page=page, filters=filters,
                           entries_per_page=MOVIES_PER_PAGE, total_num_entries=total_num_entries,
                           previous_page=previous_page, next_page=next_page,
                           all_genres=all_genres)


@app.route('/movies/<id>', methods=['GET', 'POST'])
@flask_login.login_required
def show_movie(id):
    return render_template('movie.html', movie=get_movie(id),
                           new_comment=request.form.get("comment"))


@app.route('/movies/<id>/comments', methods=['GET', 'POST'])
@flask_login.login_required
def show_movie_comments(id):
    if request.method == 'POST':
        comment = request.form["comment"]

        add_comment_to_movie(ObjectId(id), flask_login.current_user, comment, datetime.now())

        return redirect(url_for('show_movie', id=id))

    comments = get_movie_comments(id)
    num_comments = comments.count()
    movie = get_movie(id)
    if "comments" in movie:
        num_comments += len(movie["comments"])

    return render_template('moviecomments.html', movie=get_movie(id),
                           older_comments=comments, num_comments=comments.count())


@app.route('/movies/<id>/comments/<comment_id>/delete', methods=['POST'])
@flask_login.login_required
def delete_movie_comment(id, comment_id):
    delete_comment_from_movie(id, comment_id)

    return redirect(url_for('show_movie', id=id))


@app.route('/movies/watch/<id>')
@flask_login.login_required
def watch_movie(id):
    return render_template('moviewatch.html', movie=get_movie(id))



