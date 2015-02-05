using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SCP;
using Microsoft.AspNet.SignalR.Client;
using System.Diagnostics;
using Microsoft.HBase.Client;

namespace TweetSentimentTopology
{
    public class SignalRBroadcastBolt : ISCPBolt
    {
        private Context ctx;

        long RowCount;

        //SingnalR Connection
        HubConnection hubConnection;
        IHubProxy twitterHubProxy;
        Stopwatch timer1 = Stopwatch.StartNew();
        Stopwatch timer2 = Stopwatch.StartNew();

        // HBase connection
        HBaseStore client;

        //Constructor
        public SignalRBroadcastBolt(Context ctx)
        {
            Context.Logger.Info("SignalRBroadcastBolt constructor called");
            //Set context
            this.ctx = ctx;
            //Define the schema for the incoming tuples from spout
            Dictionary<string, List<Type>> inputSchema = new Dictionary<string, List<Type>>();
            //Input schema counter updates
            inputSchema.Add("default", new List<Type>() { typeof(string), typeof(long), typeof(string), typeof(string), typeof(string), typeof(int) });

            //Declare both incoming and outbound schemas
            this.ctx.DeclareComponentSchema(new ComponentStreamSchema(inputSchema, null));

            // Initialize HBase connection
            client = new HBaseStore();

            // Read current row count cell
            RowCount = client.GetRowCount();

            // Initialize SignalR connection
            StartSignalRHubConnection();
        }

        //Get a new instance
        public static SignalRBroadcastBolt Get(Context ctx, Dictionary<string, Object> parms)
        {
            return new SignalRBroadcastBolt(ctx);
        }

        //Process a tuple from the stream
        public void Execute(SCPTuple tuple)
        {
            Context.Logger.Info("Execute enter");

            try
            {
                // Count indexItem
                RowCount++;

                // Skip updates for 10 milliseconds and send regularly around every 100 milliseconds
                if (timer1.ElapsedMilliseconds >= 10 &&
                    timer2.ElapsedMilliseconds >= 100)
                {
                    timer2.Restart();

                    SendSingnalRUpdate(RowCount);

                    timer1.Restart();
                }
            }
            catch (Exception ex)
            {
                Context.Logger.Error("SignalRBroadcastBolt Exception: " + ex.Message + "\nStackTrace: \n" + ex.StackTrace);
            }

            Context.Logger.Info("Execute exit");
        }

        private void StartSignalRHubConnection()
        {
            this.hubConnection = new HubConnection("http://tweetsentiment.azurewebsites.net/");
            this.twitterHubProxy = hubConnection.CreateHubProxy("TwitterHub");
            hubConnection.Start().Wait();
        }

        private void SendSingnalRUpdate(long rowCount)
        {
            if (hubConnection.State != ConnectionState.Connected)
            {
                hubConnection.Stop();
                StartSignalRHubConnection();
            }
            twitterHubProxy.Invoke("UpdateCounter", rowCount);
        }
    }
}
