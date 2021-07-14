#!/usr/bin/env python
import requests
import json
import Constants
import tweepy
from datetime import datetime
import pytz


newlyPublished = []
newlyUpdated = []
wpToken = ""
waitingToTweet = []
tweetCount = 0
newDS_Sorted = {}
updatedDS_Sorted = {}


def currentDateTime():
    d_naive = datetime.now()
    timezone = pytz.timezone("Australia/ACT")
    d_aware = timezone.localize(d_naive).strftime('%Y-%m-%d %H:%M:%S')
    return d_aware

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
    print(currentDateTime() + " Ada Twitter Bot is fetching data from Metabase")
    sessionToken = fetchMetabaseSessionToken()
    # print(sessionToken, datasetHeader(sessionToken))
    try:
        r = requests.post(Constants.API_DATASETS_QUERY_NEWPUBLICATION, headers=datasetHeader(sessionToken))
        if 200 <= r.status_code <= 299:
            res = json.loads(r.text)

            if len(res) > 0:
                for i in res:
                    if i["owner_id"] in newDS_Sorted:
                        newDS_Sorted[i["owner_id"]].append(i)
                    else:
                        newDS_Sorted[i["owner_id"]] = [i]

            print("keys: ", newDS_Sorted.keys(), len(newDS_Sorted.keys()))
            # print("15490: ", len(newDS_Sorted[15409]))
            # newlyPublished.append(i)
    except Exception as error:
        print('ERROR 56', error)

    try:
        r = requests.post(Constants.API_DATASETS_QUERY_NEWPUPDATE, headers=datasetHeader(sessionToken))
        if 200 <= r.status_code <= 299:
            res = json.loads(r.text)

            if len(res) > 0:
                for i in res:
                    if i["owner_id"] in updatedDS_Sorted:
                        updatedDS_Sorted[i["owner_id"]].append(i)
                    else:
                        updatedDS_Sorted[i["owner_id"]] = [i]

                    # newlyUpdated.append(i)
            print("keys: ", updatedDS_Sorted.keys(), len(updatedDS_Sorted.keys()))
    except Exception as error:
        print('ERROR 67', error)

    print(currentDateTime() + " Fetch done.")


def createTwitterAPI():
    # authentication of consumer key and secret

    auth = tweepy.OAuthHandler(Constants.consumer_key, Constants.consumer_secret)

    # authentication of access token and secret
    auth.set_access_token(Constants.access_token, Constants.access_token_secret)
    api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)
    try:
        api.verify_credentials()
    except:
        print("Error found in authentication")

    return api


def updateTwitter(content, category):
    global tweetCount, waitingToTweet
    api = createTwitterAPI()
    tweet = ""
    if len(content) > 0:
        if category == "26":
            print(currentDateTime() + " Ada Twitter Bot is updating the status with Newly Published Dataset.")
        elif category == "27":
            print(currentDateTime() + " Ada Twitter Bot is updating the status with Recently Updated Dataset.")

        for i in content.keys():
            if len(content[i]) >= 5:
                print("more than 5 datasets: ", i)
                temp = tweetCompositionBulk(content[i], category)
                waitingToTweet.append(temp)

            else:
                print("less than 5 datasets: ", i)
                for ele in range(len(content[i])):
                    # print(content[i])
                    temp = tweetCompositionSimple(content[i][ele], ele, category)
                    waitingToTweet.append(temp)

        # print(waitingToTweet)

        # for i in range(len(content)):
        #     print(content[i])
            # temp = tweetCompositionSimple(content[i], i, category)
            # waitingToTweet.append(temp)

            # if len(temp) > 400:
            #     tempT = temp[0:400] + "..."
            #     waitingToTweet.append(tempT)
            # elif len(tweet) > 0 and len(tweet + tweetCompositionSimple(content[i], i, category)) > 400:
            #     waitingToTweet.append(tweet)
            #     tweet = tweetCompositionSimple(content[i], i, category)
            #     if i == len(content) - 1:
            #         waitingToTweet.append(tweet)
            # else:
            #     tweet += tweetCompositionSimple(content[i], i, category)
            #     if i == len(content) - 1:
            #         waitingToTweet.append(tweet)

    # update the status

    if len(waitingToTweet) > 0:
        waitingToTweet = waitingToTweet[0:10]
        for i in waitingToTweet:

            try:
                api.update_status(status=i)
                tweetCount += 1
            except Exception as error:
                print(error)

        print(currentDateTime() + " " + str(tweetCount) + " tweets have been updated.")
    tweetCount = 0
    waitingToTweet = []


def tweetCompositionSimple(content, num, category):
    # print(content)
    title = content['dataset_title']
    if len(title) > 280:
        title = title[0:280] + " ..."
    description = content['dataset_description']
    url = content['URL']
    doi = content['DOI']

    if category == "27":
        publicationDate = content['publication date']
        version = str(content['versionnumber']) + "." + str(content['minorversionnumber'])

        tweet = "Updated dataset on our Dataverse: " + "\r\n" \
            + "\r\n" \
            + title + " V" + version + " " +"\r\n" \
            + url + "\r\n"



    elif category == "26":
        publicationDate = content['publish date']

        tweet = "New dataset on our Dataverse: " + "\r\n" \
            + "\r\n" \
            + title + "\r\n" \
            + url + "\r\n"

    return tweet


def tweetCompositionBulk(content, category):
    dvTitle = content[0]['owner_name']
    if len(dvTitle) > 280:
        dvTitle = dvTitle[0:280] + " ..."
    # description = content['dataset_description']
    owner_url = content[0]['owner_URL']
    # doi = content['DOI']

    if category == "27":
        # publicationDate = content['publication date']
        # version = str(content['versionnumber']) + "." + str(content['minorversionnumber'])

        tweet = str(len(content)) + " datasets updated on our Dataverse: " + "\r\n" \
            + "\r\n" \
            + dvTitle + "\r\n" \
            + owner_url + "\r\n"



    elif category == "26":
        # publicationDate = content['publish date']

        tweet = str(len(content)) + " new datasets published on our Dataverse: " + "\r\n" \
            + "\r\n" \
            + dvTitle + "\r\n" \
            + owner_url + "\r\n"

    return tweet


def main():
    fetchDatasets()
    if len(newDS_Sorted.values()) > 0:
        # print('skipped')
        updateTwitter(newDS_Sorted, "26")
    else:
        print(currentDateTime() + " There is no Newly Published Dataset.")
    if len(updatedDS_Sorted.values()) > 0:
        updateTwitter(updatedDS_Sorted, "27")
    else:
        print(currentDateTime() + " There is no Recently Updated Dataset")


if __name__ == "__main__":
    main()

