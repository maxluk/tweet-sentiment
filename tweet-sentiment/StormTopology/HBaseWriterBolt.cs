using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SCP;
using System.Threading;
using System.Diagnostics;

namespace TweetSentimentTopology
{
    class HBaseWriterBolt : ISCPBolt
    {
        //Context
        private Context ctx;

        // Buffer
        Queue<SCPTuple> buffer = new Queue<SCPTuple>();
        Stopwatch timer = Stopwatch.StartNew();

        // HBase context
        HBaseStore client;

        //Constructor
        public HBaseWriterBolt(Context ctx)
        {
            Context.Logger.Info("HBaseWriterBolt constructor called");
            //Set context
            this.ctx = ctx;
            //Define the schema for the incoming tuples from spout
            Dictionary<string, List<Type>> inputSchema = new Dictionary<string, List<Type>>();
            //In this case, just a string tuple
            inputSchema.Add("default", new List<Type>() { typeof(string), typeof(long), typeof(string), typeof(string), typeof(string), typeof(int) });
            //Declare both incoming and outbound schemas
            this.ctx.DeclareComponentSchema(new ComponentStreamSchema(inputSchema, null));

            // Initialize HBase connection
            client = new HBaseStore();
        }

        //Get a new instance
        public static HBaseWriterBolt Get(Context ctx, Dictionary<string, Object> parms)
        {
            return new HBaseWriterBolt(ctx);
        }

        //Process a tuple from the stream
        public void Execute(SCPTuple tuple)
        {
            Context.Logger.Info("Execute enter");

            try
            {
                // Buffer batch of tuples
                buffer.Enqueue(tuple);

                if (buffer.Count >= 1000 || timer.ElapsedMilliseconds >= 50)
                {
                    // Write buffer to HBase
                    var list = new List<TweetIndexItem>();

                    do
                    {
                        var tweetWord = buffer.Dequeue();

                        list.Add(new TweetIndexItem
                        {
                            Word = tweetWord.GetString(0),
                            CreatedAt = tweetWord.GetLong(1),
                            IdStr = tweetWord.GetString(2),
                            Language = tweetWord.GetString(3),
                            Coordinates = tweetWord.GetString(4),
                            SentimentScore = tweetWord.GetInteger(5)
                        });

                    } while (buffer.Count > 0);

                    client.WriteIndexItems(list);
                    Context.Logger.Info("===== {0} rows written =====", list.Count);

                    timer.Restart();
                }
            }
            catch (Exception ex)
            {
                Context.Logger.Error("HBaseWriterBolt Exception: " + ex.Message + "\nStackTrace: \n" + ex.StackTrace);
            }

            Context.Logger.Info("Execute exit");
        }
    }
}