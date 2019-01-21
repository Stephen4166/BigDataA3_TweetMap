import json
import tweepy
import socket
import re
from geopy.geocoders import Nominatim


ACCESS_TOKEN = '718663823-kc1J0I3vVVKkfflyZtZRz4TzfNxKgFtdiEBwcsWS'
ACCESS_SECRET = 'yUgqRcItMIcsDQQcNRWepupXiIsAXkQdzrjM8PDThSf28'
CONSUMER_KEY = '83fZeRNW7BVpyiSqLDoQoNAo3'
CONSUMER_SECRET = 'fpGp6rf4IcaxX72FZvwXJixwrOjKchiGj1J4Hg1fqDc39GGflQ'

auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_SECRET)


hashtag = '#Trump'

TCP_IP = 'localhost'
TCP_PORT = 3000


# create sockets
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
# s.connect((TCP_IP, TCP_PORT))
s.bind((TCP_IP, TCP_PORT))
s.listen(1)
conn, addr = s.accept()
geolocator = Nominatim()


class MyStreamListener(tweepy.StreamListener):
   def on_status(self, status):
      print("------------------------------------------------------------")
      print('Original Tweet: ')
      print(status.text)
      print("**************************************************************")
      UID = self.getUID()
      tweet = self.formatTweet(status.text)
      if (not status.user.location):
         print("the place is: null")
      else:
         # remove the emojis in status.user.location
         uLocation = self.formatString(status.user.location)
         location = geolocator.geocode(uLocation, timeout = 5)
         print("the place is:",status.user.location)
         if(location and (int(location.latitude) != 39 and int(location.longitude) != -100)):
            tweet = tweet + "\t" + str(location.latitude) + "," + str(location.longitude) +"\t"+ str(UID) +"\n"
            print("The coordinates: ", (location.latitude, location.longitude))
         else:
            print("The place has no corresponding coordinates")
         print("filtered Tweet: ")
         print(tweet)
         conn.send(tweet.encode('utf-8'))
         globals()['UID'] = UID + 1

   def getUID(self):
      if('UID' not in globals()):
         globals()['UID'] = 0
      return globals()['UID']

   def on_error(self, status_code):
      if status_code == 420:
         return False
      else:
         print(status_code)

   def formatTweet(self, tweet):
      # remove the emojis and special characters and hashtags and urls
      newTweet = ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)|RT"," ",tweet).split())
      return newTweet

   # filter out emojis and special characters
   def formatString(self, line):
      newLine = re.sub('[^\w\s#@/:?.,_-]','',line)
      return newLine
myStream = tweepy.Stream(auth=auth, listener=MyStreamListener())
# if(input()):
#    hashtag = input()
#    print("the input is",hashtag)
myStream.filter(track=[hashtag])
