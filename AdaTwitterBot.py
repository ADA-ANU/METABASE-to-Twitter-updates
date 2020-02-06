#!/usr/bin/env python
import requests
import json
import Constants
import tweepy

newlyPublished = []
newlyUpdated = []
wpToken = ""
waitingToTweet = []
tweetCount = 0


def datasetHeader(session_token):

    header = {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'X-Metabase-Session': session_token
    }
    return header


def fetchMetabaseSessionToken():
    try:
        r = requests.post(Constants.API_METABASE_AUTHENTICATION_ENDPOINT, data=json.dumps(Constants.API_METABASE_AUTHENTICATION_BODY), headers=Constants.API_METABASE_AUTHENTICATION_HEADER)
        if r.status_code == 200:
            token = (json.loads(r.text)["id"])
            return token
    except Exception as error:
        print('ERROR', error)


def fetchDatasets():
    print("Ada Twitter Bot is fetching data from Metabase")
    sessionToken = fetchMetabaseSessionToken()
    try:
        r = requests.post(Constants.API_DATASETS_QUERY_NEWPUBLICATION, headers=datasetHeader(sessionToken))
        if r.status_code == 200:
            res = json.loads(r.text)

            if len(res) > 0:
                for i in res:
                    newlyPublished.append(i)
    except Exception as error:
        print('ERROR', error)

    try:
        r = requests.post(Constants.API_DATASETS_QUERY_NEWPUPDATE, headers=datasetHeader(sessionToken))
        if r.status_code == 200:
            res = json.loads(r.text)

            if len(res) > 0:
                for i in res:
                    newlyUpdated.append(i)
    except Exception as error:
        print('ERROR', error)

    print("Fetch done.")

def createTwitterAPI():
    # authentication of consumer key and secret

    auth = tweepy.OAuthHandler(Constants.consumer_key, Constants.consumer_secret)

    # authentication of access token and secret
    auth.set_access_token(Constants.access_token, Constants.access_token_secret)
    api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)
    try:
        api.verify_credentials()
    except:
        print("Error during authentication")

    return api


def updateTwitter(content, category):
    global tweetCount
    api = createTwitterAPI()
    tweet = ""
    if len(content) > 0:
        if category == "26":
            print("Ada Twitter Bot is updating the status with Newly Published Dataset.")
        elif category == "27":
            print("Ada Twitter Bot is updating the status with Recently Updated Dataset.")

        for i in range(len(content)):

            temp = tweetCompositionSimple(content[i], i, category)

            if len(temp) > 479:
                tempT = temp[0:479] + "..."
                waitingToTweet.append(tempT)
            elif len(tweet) > 0 and len(tweet + tweetCompositionSimple(content[i], i, category)) > 479:
                waitingToTweet.append(tweet)
                tweet = tweetCompositionSimple(content[i], i, category)
                if i == len(content) - 1:
                    waitingToTweet.append(tweet)
            else:
                tweet += tweetCompositionSimple(content[i], i, category)
                if i == len(content) - 1:
                    waitingToTweet.append(tweet)

    # update the status

    if len(waitingToTweet) > 0:
        for i in waitingToTweet:

            try:
                api.update_status(status=i)
                tweetCount += 1
            except Exception as error:
                print(error)

        print(str(tweetCount) + " tweets have been updated.")

    waitingToTweet.clear()



def tweetCompositionSimple(content, num, category):
    title = content['dataset_title']
    description = content['dataset_description']
    url = content['URL']
    doi = content['DOI']

    if category == "27":
        publicationDate = content['publication date']
        version = str(content['versionnumber']) + "." + str(content['minorversionnumber'])
        if num == 0:
            tweet = "Recently updated datasets on our Dataverse: " + "\r\n" \
                + "\r\n" \
                + str(num+1) + ". " + title + " (" + doi + ") " + "V" + version + "\r\n" \
                + url + "\r\n"
        else:
            tweet = str(num + 1) + ". " + title + " (" + doi + ") " + "V" + version + "\r\n" \
                + url + "\r\n"


    elif category == "26":
        publicationDate = content['publish date']
        if num == 0:
            tweet = "Newly published datasets on our Dataverse: " + "\r\n" \
                + "\r\n" \
                + str(num + 1) + ". " + title + " (" + doi + ")" + "\r\n" \
                + url + "\r\n"

        else:
            tweet = str(num + 1) + ". " + title + " (" + doi + ")" + "\r\n" \
                + url + "\r\n" \

    return tweet


def main():
    fetchDatasets()
    if len(newlyPublished) > 0:
        updateTwitter(newlyPublished, "26")
    else:
        print("There is no Newly Published Dataset.")
    if len(newlyUpdated) > 0:
        updateTwitter(newlyUpdated, "27")
    else:
        print("There is no Recently Updated Dataset")


if __name__ == "__main__":
    main()

