import os
import uuid
from functools import wraps
import json

from flask import Flask
from flask import redirect
from flask import render_template
from flask import session
from authlib.integrations.flask_client import OAuth
from six.moves.urllib.parse import urlencode

from linkinpark.lib.common.flask_monitor import FlaskMonitorServing

ROOT_PATH = os.path.join(os.path.dirname(
    os.path.realpath(__file__)), './')

BASE_URL = os.getenv("REDIRECT_URL")
CLIENT_IDENTITY = os.getenv("CLIENT_IDENTITY")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
AUTH0_BASE_URL = os.getenv("AUTH0_BASE_URL")

app = Flask(__name__)
app.secret_key = uuid.uuid4().hex

oauth = OAuth(app)

auth0 = oauth.register(
    "auth0",
    client_id=CLIENT_IDENTITY,
    client_secret=CLIENT_SECRET,
    api_base_url=AUTH0_BASE_URL,
    access_token_url=AUTH0_BASE_URL + "/oauth/token",
    authorize_url=AUTH0_BASE_URL + "/authorize",
    client_kwargs={
        "scope": "openid profile email",
    },
    server_metadata_url=AUTH0_BASE_URL + "/.well-known/openid-configuration", )
# Root endpoint returns a simple message to verify the app's started working.
app_with_monitor = FlaskMonitorServing(app)


@app.route('/')
def entry_point():
    if 'authinfo' not in session:
        return 'Home page without login<br>'\
               f'<a href={BASE_URL}/login >Login</a>'
    else:
        return f'Home page login with {session["authinfo"]["userinfo"]["email"]}'


@app.route("/callback", methods=["GET", "POST"])
# callback after successful login.
def callback():
    token = oauth.auth0.authorize_access_token()
    session["authinfo"] = token
    return redirect(BASE_URL + "/dashboard")


@app.route('/login')
def login():
    # redirect to this address after successful login.
    return auth0.authorize_redirect(redirect_uri=BASE_URL + "/callback")


def requires_auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if 'authinfo' not in session:
            # redirect to Login page here
            return redirect(BASE_URL + "/")
        return f(*args, **kwargs)

    return decorated


@app.route('/dashboard')
@requires_auth
def dashboard():
    # generate a userinfo dashboard.
    return render_template('dashboard.html',
                           authinfo=session['authinfo'],
                           authinfo_pretty=json.dumps(
                               session['authinfo'], indent=4),
                           base_url=BASE_URL,
                           template_folder=os.path.join(ROOT_PATH, 'templates'))


# log out will clear session data and redirect to the address specified on the Auth0 application dashboard.
@app.route('/logout')
def logout():
    # clear session information
    session.clear()
    # redirect user to logout endpoint.
    params = {'returnTo': BASE_URL, 'client_id': CLIENT_IDENTITY}
    return redirect(auth0.api_base_url + '/v2/logout?' + urlencode(params))


def main():
    app_with_monitor.run()


# Make this different
if __name__ == "__main__":
    main()
