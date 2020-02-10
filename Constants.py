import dotenv
import os
dotenv.load_dotenv()

API_METABASE_AUTHENTICATION_ENDPOINT = "https://dataverse-dev.ada.edu.au/metabase/api/session"

API_WP_AUTHENTICATION_ENDPOINT = "http://sw-dev.ada.edu.au:8085/wp-json/aam/v2/authenticate"

API_WP_GETPOSTS_PUBLISH = "http://sw-dev.ada.edu.au:8085/wp-json/wp/v2/posts?categories=26"

API_WP_GETPOSTS_UPDATE = "http://sw-dev.ada.edu.au:8085/wp-json/wp/v2/posts?categories=27"

API_WP_UPDATEPOSTS = "http://sw-dev.ada.edu.au:8085/wp-json/wp/v2/posts/"

API_WP_VALIDATE = "http://sw-dev.ada.edu.au:8085/wp-json/aam/v1/validate-jwt"

API_DATASETS_QUERY_NEWPUBLICATION = "https://dataverse-dev.ada.edu.au/metabase/api/card/295/query/json"

API_DATASETS_QUERY_NEWPUPDATE = "https://dataverse-dev.ada.edu.au/metabase/api/card/296/query/json"

API_WP_CREATEPOSTS = "http://sw-dev.ada.edu.au:8085/wp-json/wp/v2/posts"

API_DATAVERSES_PUBLISHDATASET = "https://dataverse-dev.ada.edu.au/api/datasets/"

API_DATAVERSES_CREATEDATASET = "https://dataverse-dev.ada.edu.au/api/dataverses/DEV-ADA/datasets/"


API_METABASE_AUTHENTICATION_HEADER = {
    'Content-Type': 'application/json'
}

API_METABASE_AUTHENTICATION_BODY = {
    "username": os.getenv("METABASE_USERNAME"),
    "password": os.getenv("METABASE_PASSWORD")
}

API_WP_AUTHENTICATION_HEADER = {
    'Content-Type': 'application/json',
    'Accept': 'application/json'
}

API_WP_POSTS_HEADER = {
    'Content-Type': 'application/json',
    'Accept': 'application/json'
}

API_WP_AUTHENTICATION_BODY = {
    "username": os.getenv("WP_USERNAME"),
    "password": os.getenv("WP_PASSWORD"),
    "issueJWT": "true"
}

API_WP_VALIDATE_HEADER = {
    'Content-Type': 'application/json'
}

API_WP_CREATEPOTS_HEADER = {
    'Content-Type': 'application/x-www-form-urlencoded'
}

API_DATAVERSES_PUBLISHDATASET_HEADER = {
    "X-Dataverse-key": os.getenv("DATAVERSE_TOKEN")
}

dateDiff = 14

consumer_key = os.getenv("CONSUMER_KEY")
consumer_secret = os.getenv("CONSUMER_SECRET")
access_token = os.getenv("ACCESS_TOKEN")
access_token_secret = os.getenv("ACCESS_TOKEN_SECRET")

