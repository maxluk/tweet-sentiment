using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SCP;
using System.Threading;
using Tweetinvi;
using System.Diagnostics;
using Tweetinvi.Core.Interfaces;
using System.Configuration;
using Tweetinvi.Core.Interfaces.Streaminvi;

namespace TweetSentimentTopology
{
    class TwitterSpout : ISCPSpout
    {
        //Context
        private Context ctx;

        // Twitter stream listener
        Thread listenerThread;
        IFilteredStream stream;
        Queue<ITweet> queue = new Queue<ITweet>();
        bool threadRunning = true;

        public TwitterSpout(Context ctx)
        {
            //Log that we are starting
            Context.Logger.Info("TwitterSpout constructor called");
            //Store the context that was passed
            this.ctx = ctx;

            //Define the schema for the emitted tuples
            Dictionary<string, List<Type>> outputSchema = new Dictionary<string, List<Type>>();
            //In this case, just a string tuple
            outputSchema.Add("default", new List<Type>() { typeof(string), typeof(long), typeof(string), typeof(string), typeof(string) });
            //Declare the schema for the stream
            this.ctx.DeclareComponentSchema(new ComponentStreamSchema(null, outputSchema));

            StartTwitterListenerThread();
        }

        ~TwitterSpout()
        {
            threadRunning = false;
        }

        //Return a new instance of the spout
        public static TwitterSpout Get(Context ctx, Dictionary<string, Object> parms)
        {
            return new TwitterSpout(ctx);
        }

        //Emit the next tuple
        //NOTE: When using data from an external data source
        //such as Service Bus, Event Hub, Twitter, etc.,
        //you would read and emit it in NextTuple
        public void NextTuple(Dictionary<string, object> parms)
        {
            Context.Logger.Info("NextTuple enter");

            try
            {
                if (queue.Count > 0)
                {
                    lock (queue)
                    {
                        do
                        {
                            var tweet = queue.Dequeue();

                            //Context.Logger.Info("Emit: {0}", indexItem.Text);

                            //Emit the indexItem
                            this.ctx.Emit(new Values(tweet.Text,
                                                     tweet.CreatedAt.ToBinary(),
                                                     tweet.IdStr, 
                                                     tweet.Language.ToString(),
                                                     tweet.Coordinates != null ? tweet.Coordinates.Longitude.ToString() + "," +
                                                                                 tweet.Coordinates.Latitude.ToString()
                                                                               : ""));

                        } while (queue.Count > 0);
                    }
                }
            }
            catch (Exception ex)
            {
                Context.Logger.Error("Exception: " + ex.Message + "\nStackTrace: \n" + ex.StackTrace);
            }

            Context.Logger.Info("NextTuple exit");
        }

        //Ack's are not implemented
        public void Ack(long seqId, Dictionary<string, object> parms)
        {
            throw new NotImplementedException();
        }

        //Ack's are not implemented, so
        //fail should never be called
        public void Fail(long seqId, Dictionary<string, object> parms)
        {
            throw new NotImplementedException();
        }

        private void StartTwitterListenerThread()
        {
            listenerThread = new Thread(new ThreadStart(ListenerThreadFunction));
            listenerThread.Start();
        }

        private void ListenerThreadFunction()
        {
            TwitterCredentials.SetCredentials(
                ConfigurationManager.AppSettings["token_AccessToken"],
                ConfigurationManager.AppSettings["token_AccessTokenSecret"],
                ConfigurationManager.AppSettings["token_ConsumerKey"],
                ConfigurationManager.AppSettings["token_ConsumerSecret"]);

            while (threadRunning)
            {
                try
                {
                    //var hbase = new HBaseWriter();
                    stream = Stream.CreateFilteredStream();
                    var location = Geo.GenerateLocation(-180, -90, 180, 90);
                    stream.AddLocation(location);

                    var tweetCount = 0;
                    var timer = Stopwatch.StartNew();

                    stream.MatchingTweetReceived += (sender, args) =>
                    {
                        tweetCount++;
                        var tweet = args.Tweet;

                        // Store indexItem in the buffer
                        lock (queue)
                        {
                            queue.Enqueue(tweet);
                        }

                        if (timer.ElapsedMilliseconds > 1000)
                        {
                            if (tweet.Coordinates != null)
                            {
                                Context.Logger.Info("{0}: {1} {2}", tweet.Id, tweet.Language.ToString(), tweet.Text);
                                Context.Logger.Info("\tLocation: {0}, {1}", tweet.Coordinates.Longitude, tweet.Coordinates.Latitude);
                            }

                            timer.Restart();
                            Context.Logger.Info("===== Tweets/sec: {0} =====", tweetCount);
                            tweetCount = 0;
                        }
                    };

                    stream.StartStreamMatchingAllConditions();
                }
                catch (Exception ex)
                {
                    Context.Logger.Fatal("Exception: {0}", ex.Message);
                }
            }
        }

        internal void StopListenerThread()
        {
            threadRunning = false;
            stream.StopStream();
        }
    }
}