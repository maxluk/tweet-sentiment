using Microsoft.SCP;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TweetSentimentTopology
{
    class Program
    {
        static void Main(string[] args)
        {
            if (args.Count() > 0)
            {
                //The component to run
                string compName = args[0];
                //Run the component
                if ("twitterspout".Equals(compName))
                {
                    //Set the prefix for logging
                    System.Environment.SetEnvironmentVariable("microsoft.scp.logPrefix", "TweetSentiment-Spout");
                    //Initialize the runtime
                    SCPRuntime.Initialize();
                    //Run the plugin (WordSpout)
                    SCPRuntime.LaunchPlugin(new newSCPPlugin(TwitterSpout.Get));
                }
                else if ("sentimentindexerbolt".Equals(compName))
                {
                    System.Environment.SetEnvironmentVariable("microsoft.scp.logPrefix", "TweetSentiment-Indexer");
                    SCPRuntime.Initialize();
                    SCPRuntime.LaunchPlugin(new newSCPPlugin(SentimentIndexerBolt.Get));
                }
                else if ("hbasewriterbolt".Equals(compName))
                {
                    System.Environment.SetEnvironmentVariable("microsoft.scp.logPrefix", "TweetSentiment-Writer");
                    SCPRuntime.Initialize();
                    SCPRuntime.LaunchPlugin(new newSCPPlugin(HBaseWriterBolt.Get));
                }
                else if ("hbasecounterbolt".Equals(compName))
                {
                    System.Environment.SetEnvironmentVariable("microsoft.scp.logPrefix", "TweetSentiment-Counter");
                    SCPRuntime.Initialize();
                    SCPRuntime.LaunchPlugin(new newSCPPlugin(HBaseCounterBolt.Get));
                }
                else if ("signalrbroadcastbolt".Equals(compName))
                {
                    System.Environment.SetEnvironmentVariable("microsoft.scp.logPrefix", "TweetSentiment-Broadcast");
                    SCPRuntime.Initialize();
                    SCPRuntime.LaunchPlugin(new newSCPPlugin(SignalRBroadcastBolt.Get));
                }
                else
                {
                    throw new Exception(string.Format("unexpected compName: {0}", compName));
                }
            }
            else
            {
                //Set log prefix information for the component being tested
                System.Environment.SetEnvironmentVariable("microsoft.scp.logPrefix", "TweetSentiment-LocalTest");
                //Initialize the runtime
                SCPRuntime.Initialize();

                //If we are not running under the local context, throw an error
                if (Context.pluginType != SCPPluginType.SCP_NET_LOCAL)
                {
                    throw new Exception(string.Format("unexpected pluginType: {0}", Context.pluginType));
                }
                //Create an instance of LocalTest
                LocalTest localTest = new LocalTest();
                //Run the tests
                localTest.RunTestCase();
            }
        }
    }
}