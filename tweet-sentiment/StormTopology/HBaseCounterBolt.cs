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
    class HBaseCounterBolt : ISCPBolt
    {
        //Context
        private Context ctx;

        // Buffer
        long RowCount;
        Stopwatch timer = Stopwatch.StartNew();

        // HBase context
        HBaseStore client;

        //Constructor
        public HBaseCounterBolt(Context ctx)
        {
            Context.Logger.Info("HBaseCounterBolt constructor called");
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

            // Read current row count cell
            RowCount = client.GetRowCount();
        }

        //Get a new instance
        public static HBaseCounterBolt Get(Context ctx, Dictionary<string, Object> parms)
        {
            return new HBaseCounterBolt(ctx);
        }

        //Process a tuple from the stream
        public void Execute(SCPTuple tuple)
        {
            Context.Logger.Info("Execute enter");

            try
            {
                // Count indexItem
                RowCount++;

                if (timer.ElapsedMilliseconds >= 20)
                {
                    // Update count in HBase
                    client.UpdateRowCount(RowCount);
                    Context.Logger.Info("===== Total Row Count: {0} =====", RowCount);

                    timer.Restart();
                }
            }
            catch (Exception ex)
            {
                Context.Logger.Error("HBaseCounterBolt Exception: " + ex.Message + "\nStackTrace: \n" + ex.StackTrace);
            }

            Context.Logger.Info("Execute exit");
        }
    }
}