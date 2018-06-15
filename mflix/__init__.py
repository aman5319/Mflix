from flask import Flask
from flask_bcrypt import Bcrypt
import flask_login


app = Flask(__name__)  # create the application instance
app.config.from_object(__name__)  # load config from this file, mflix.py

# Load default config and override config from an environment variable
app.config.update(dict(SECRET_KEY="mflix-app-mongodb"))
app.config.from_envvar('MFLIX_SETTINGS', silent=True)
bcrypt = Bcrypt(app)
login_manager = flask_login.LoginManager()
login_manager.init_app(app)

from mflix import db