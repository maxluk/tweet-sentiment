using Microsoft.SCP;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace TweetSentimentTopology
{
    class LocalTest
    {
        //Run tests
        public void RunTestCase()
        {
            var hubConnection = new Microsoft.AspNet.SignalR.Client.HubConnection("http://tweetsentiment.azurewebsites.net/");
            var twitterHubProxy = hubConnection.CreateHubProxy("TwitterHub");
            hubConnection.Start().Wait();

            for (int i = 0; i < 100000; i++)
            {
                twitterHubProxy.Invoke("UpdateCounter", i);
                Thread.Sleep(50);
            }


            Dictionary<string, Object> emptyDictionary = new Dictionary<string, object>();
            //Spout tests
            {
                //Get local context
                LocalContext spoutCtx = LocalContext.Get();
                //Get an instance of the spout
                TwitterSpout spout = TwitterSpout.Get(spoutCtx, emptyDictionary);

                // Collect some tweets
                Thread.Sleep(5000);

                //Call NextTuple to emit data
                for (int i = 0; i < 10; i++)
                {
                    spout.NextTuple(emptyDictionary);
                }
                //Store the stream for the next component
                spoutCtx.WriteMsgQueueToFile("spout.txt");
                spout.StopListenerThread();
            }

            //HBaseWriterBolt tests
            //{
            //    LocalContext splitterCtx = LocalContext.Get();
            //    HBaseWriterBolt splitter = HBaseWriterBolt.Get(splitterCtx, emptyDictionary);
            //    //Read from the 'stream' emitted by the spout
            //    splitterCtx.ReadFromFileToMsgQueue("spout.txt");
            //    List<SCPTuple> batch = splitterCtx.RecvFromMsgQueue();
            //    foreach (SCPTuple tuple in batch.Take(batch.Count - 1))
            //    {
            //        splitter.Execute(tuple);
            //    }
            //    Thread.Sleep(100);
            //    splitter.Execute(batch.Last());
            //}

            //HBaseCounterBolt tests
            {
                LocalContext counterCtx = LocalContext.Get();
                HBaseCounterBolt counter = HBaseCounterBolt.Get(counterCtx, emptyDictionary);
                //Read from the 'stream' emitted by the spout
                counterCtx.ReadFromFileToMsgQueue("spout.txt");
                List<SCPTuple> batch = counterCtx.RecvFromMsgQueue();
                foreach (SCPTuple tuple in batch.Take(batch.Count - 1))
                {
                    counter.Execute(tuple);
                }
                Thread.Sleep(100);
                counter.Execute(batch.Last());
                counterCtx.WriteMsgQueueToFile("counter.txt");
            }

            //SignalRBroadcastBolt tests
            {
                LocalContext broadcastCtx = LocalContext.Get();
                SignalRBroadcastBolt broadcast = SignalRBroadcastBolt.Get(broadcastCtx, emptyDictionary);
                //Read from the 'stream' emitted by the spout
                broadcastCtx.ReadFromFileToMsgQueue("counter.txt");
                List<SCPTuple> batch = broadcastCtx.RecvFromMsgQueue();
                foreach (SCPTuple tuple in batch)
                {
                    broadcast.Execute(tuple);
                }
            }
        }
    }
}
