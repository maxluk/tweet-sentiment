using System;
using System.Collections.Generic;
using System.Configuration;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Tweetinvi;
using Tweetinvi.Core.Enum;
using Tweetinvi.Core.Interfaces.Models;
using Tweetinvi.Core.Interfaces.Models.Parameters;

namespace SimpleStreamingService
{
    class Program
    {
        static void Main(string[] args)
        {
            TwitterCredentials.SetCredentials(
                ConfigurationManager.AppSettings["token_AccessToken"],
                ConfigurationManager.AppSettings["token_AccessTokenSecret"],
                ConfigurationManager.AppSettings["token_ConsumerKey"],
                ConfigurationManager.AppSettings["token_ConsumerSecret"]);

            //Search_FilteredSearch();
            Stream_FilteredStreamExample();
            //Stream_SampleStreamExample();
        }

        private static void Stream_SampleStreamExample()
        {
            var hbase = new HBaseWriter();

            for(;;)
            {
                try
                {
                    var stream = Stream.CreateSampleStream();
                    stream.FilterTweetsToBeIn(Language.English);

                    stream.TweetReceived += (sender, args) =>
                    {
                        var tweet = args.Tweet;
                        hbase.WriteTweet(tweet);
                        if (tweet.Coordinates != null)
                        {
                            Console.WriteLine("{0}: {1}", tweet.Id, tweet.Text);
                            Console.WriteLine("\tLocation: {0}, {1}", tweet.Coordinates.Longitude, tweet.Coordinates.Latitude);
                        }
                        else
                        {
                            //Console.WriteLine("\tLocation: null");
                        }
                        if (tweet.Place != null)
                        {
                            Console.WriteLine("\tPlace: {0}", tweet.Place.FullName);
                        }

                        /*IEnumerable<ILocation> matchingLocations = args.Tweet.;
                        foreach (var matchingLocation in matchingLocations)
                        {
                            Console.Write("({0}, {1}) ;", matchingLocation.Coordinate1.Latitude, matchingLocation.Coordinate1.Longitude);
                            Console.WriteLine("({0}, {1})", matchingLocation.Coordinate2.Latitude, matchingLocation.Coordinate2.Longitude);
                        }*/
                    };

                    //stream.StartStreamMatchingAllConditions();
                    stream.StartStream();
                }
                catch(Exception ex)
                {
                }
            }
        }

        private static void Stream_FilteredStreamExample()
        {
            for (; ; )
            {
                try
                {
                    var hbase = new HBaseWriter(); 
                    var stream = Stream.CreateFilteredStream();
                    var location = Geo.GenerateLocation(-180, -90, 180, 90);
                    stream.AddLocation(location);

                    var tweetCount = 0;
                    var timer = Stopwatch.StartNew();

                    stream.MatchingTweetReceived += (sender, args) =>
                    {
                        tweetCount++;
                        var tweet = args.Tweet;
                        hbase.WriteTweet(tweet);

                        if (timer.ElapsedMilliseconds > 1000)
                        {
                            if (tweet.Coordinates != null)
                            {
                                Console.WriteLine("{0}: {1} {2}", tweet.Id, tweet.Language.ToString(), tweet.Text);
                                Console.WriteLine("\tLocation: {0}, {1}", tweet.Coordinates.Longitude, tweet.Coordinates.Latitude);
                            }

                            timer.Restart();
                            Console.WriteLine("===== Tweets/sec: {0} =====", tweetCount);
                            tweetCount = 0;
                        }

                        /*IEnumerable<ILocation> matchingLocations = args.Tweet.;
                        foreach (var matchingLocation in matchingLocations)
                        {
                            Console.Write("({0}, {1}) ;", matchingLocation.Coordinate1.Latitude, matchingLocation.Coordinate1.Longitude);
                            Console.WriteLine("({0}, {1})", matchingLocation.Coordinate2.Latitude, matchingLocation.Coordinate2.Longitude);
                        }*/
                    };

                    stream.StartStreamMatchingAllConditions();
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Exception: {0}", ex.Message);
                }
            }
        }

        private static void Search_FilteredSearch()
        {
            var searchParameter = Search.GenerateSearchTweetParameter("airlines");
            searchParameter.TweetSearchFilter = TweetSearchFilter.All;
            searchParameter.Since = DateTime.Now.Subtract(new TimeSpan(1, 0, 0));

            var tweets = Search.SearchTweets(searchParameter);
            tweets.ForEach(t => Console.WriteLine(t.Text));
        }
    }
}
