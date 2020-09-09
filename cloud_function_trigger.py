from google.auth.transport.requests import Request
from google.oauth2 import id_token

import requests

IAM_SCOPE = "https://www.googleapis.com/auth/iam"
OAUTH_TOKEN_URI = "https://www.googleapis.com/oauth2/v4/token"


def trigger_dag(data, context=None):
    """Makes a POST request to the Composer DAG Trigger API"""
    # check this https://cloud.google.com/composer/docs/how-to/using/triggering-with-gcf#getting_the_client_id
    # on how to get client id
    client_id = (
        "client id goes here"
    )
    # check in airflow web ui
    webserver_id = "webserver id goes here"
    dag_name = "dataflow-pipeline"
    webserver_url = (
        "https://"
        + webserver_id
        + ".appspot.com/api/experimental/dags/"
        + dag_name
        + "/dag_runs"
    )
    make_iap_request(webserver_url, client_id, method="POST", json={"conf": data})


# taken from
# https://cloud.google.com/composer/docs/how-to/using/triggering-with-gcf#triggering-with-gcf-creating-a-function-python
def make_iap_request(url, client_id, method="GET", **kwargs):
    """Makes a request to an application protected by Identity-Aware Proxy.
    Args:
      url: The Identity-Aware Proxy-protected URL to fetch.
      client_id: The client ID used by Identity-Aware Proxy.
      method: The request method to use
              ('GET', 'OPTIONS', 'HEAD', 'POST', 'PUT', 'PATCH', 'DELETE')
      **kwargs: Any of the parameters defined for the request function:
                https://github.com/requests/requests/blob/master/requests/api.py
                If no timeout is provided, it is set to 90 by default.
    Returns:
      The page body, or raises an exception if the page couldn't be retrieved.
    """
    # Set the default timeout, if missing
    if "timeout" not in kwargs:
        kwargs["timeout"] = 90

    # Obtain an OpenID Connect (OIDC) token from metadata server or using service
    # account.
    google_open_id_connect_token = id_token.fetch_id_token(Request(), client_id)

    # Fetch the Identity-Aware Proxy-protected URL, including an
    # Authorization header containing "Bearer " followed by a
    # Google-issued OpenID Connect token for the service account.
    resp = requests.request(
        method,
        url,
        headers={"Authorization": "Bearer {}".format(google_open_id_connect_token)},
        **kwargs
    )
    if resp.status_code == 403:
        raise Exception(
            "Service account does not have permission to "
            "access the IAP-protected application."
        )
    elif resp.status_code != 200:
        raise Exception(
            "Bad response from application: {!r} / {!r} / {!r}".format(
                resp.status_code, resp.headers, resp.text
            )
        )
    else:
        return resp.text
