import requests
import json
import pandas as pd
import sys
import time
from multiprocessing import Pool


df = pd.DataFrame()
artistName = []
collectionName = []
genres = []

def RateLimited(maxPerSecond):
    minInterval = 1.0 / float(maxPerSecond)
    def decorate(func):
        lastTimeCalled = [0.0]
        def rateLimitedFunction(*args,**kargs):
            elapsed = time.clock() - lastTimeCalled[0]
            leftToWait = minInterval - elapsed
            if leftToWait>0:
                time.sleep(leftToWait)
            ret = func(*args,**kargs)
            lastTimeCalled[0] = time.clock()
            return ret
        return rateLimitedFunction
    return decorate
    
def podcastCall(page):
    baseUrl = "https://itunes.apple.com/search?term=podcast&country=us&entity=podcast&offset=200&limit=200&page="
    url = baseUrl + str(page)
    response = requests.get(url)
    print(response)
    blob = response.content   
    data = json.loads(blob)   
    results = data["results"]
    
    for i in range(0,len(results)):
        artist_name = DictQuery(results[i]).get("artistName")
        collection_name = DictQuery(results[i]).get("collectionName")
        genreList = DictQuery(results[i]).get("genres")
        genre = genreList[0]
        artistName.append(artist_name)
        collectionName.append(collection_name)
        genres.append(genre)

#helper function to retrive data within nested JSON
class DictQuery(dict):
    def get(self, path, default = None):
        keys = path.split("/")
        val = None
        for key in keys:
            if val:
                if isinstance(val, list):
                    val = [ v.get(key, default) if v else None for v in val]
                else:
                    val = val.get(key, default)
            else:
                val = dict.get(self, key, default)
            if not val:
                break
        return val

def writeToCSV():
    df["Artist Name"] = artistName
    df["Podcast Title"] = collectionName
    df["Genres"] = genres
    df.to_csv("podcast.csv")

# if  __name__ == '__main__':
#     p = Pool(5)

@RateLimited(0.1) #0.1 requests per second
def getPodcast():
    for i in range(1,51):
        podcastCall(i)
    writeToCSV()

getPodcast()