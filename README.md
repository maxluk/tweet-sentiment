Realtime social sentiment analysis app
======
This app connects to the real-time stream of geo tagged tweets, performs sentiment analysis on them, stores tweets in HBase and visualizes positive/negative tweets that match particular keyword as a heatmap. See the running app here: http://tweetsentiment.azurewebsites.net/ (works in IE11-only at the moment).

Building
======
* The app needs twitter api keys to be put in https://github.com/maxluk/tweet-sentiment/blob/master/tweet-sentiment/SimpleStreamingService/App.config. 
You can get api keys from twitter here: https://apps.twitter.com/

* Another piece of credentials is for hbase cluster. Once you created your HBase cluster create credentials file in this folder: https://github.com/maxluk/tweet-sentiment/tree/master/tweet-sentiment/SimpleStreamingService, the file format has three lines: uri of the cluster, user name, password.

* You will also need to put hbase credentials in this file for web application: https://github.com/maxluk/tweet-sentiment/blob/master/tweet-sentiment/WebApp/Web.config.

* Then build solution in VS2013.
