using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.HBase.Client;
using org.apache.hadoop.hbase.rest.protobuf.generated;
using System.IO;
using System.Globalization;
using System.Threading;
using Tweetinvi.Core.Interfaces;

namespace SimpleStreamingService
{
    public class HBaseWriter
    {
        HBaseClient client;
        //string tableByIdName = "tweets_by_id";
        string tableByWordsName = "tweets_by_words";

        Dictionary<string, DictionaryItem> dictionary;

        Thread writerThread;
        Queue<ITweet> queue = new Queue<ITweet>();
        bool threadRunning = true;

        public HBaseWriter()
        {
            var credentials = CreateFromFile(@"..\..\credentials.txt");
            client = new HBaseClient(credentials);

            if (!client.ListTables().name.Contains(tableByWordsName))
            {
                // Create the table
                var tableSchema = new TableSchema();
                tableSchema.name = tableByWordsName;
                tableSchema.columns.Add(new ColumnSchema { name = "d" });
                client.CreateTable(tableSchema);
                Console.WriteLine("Table \"{0}\" created.", tableByWordsName);
            }

            // Load sentiment dictionary file
            LoadDictionary();

            writerThread = new Thread(new ThreadStart(WriterThreadFunction));
            writerThread.Start();
        }

        ~HBaseWriter ()
        {
            threadRunning = false;
        }

        public void WriteTweet(Tweetinvi.Core.Interfaces.ITweet tweet)
        {
            lock(queue)
            {
                queue.Enqueue(tweet);
            }
        }

        public void WriterThreadFunction()
        {
            try
            {
                while(threadRunning)
                {
                    if (queue.Count > 0)
                    {
                        var set = new CellSet();
                        lock (queue)
                        {
                            do
                            {
                                var tweet = queue.Dequeue();

                                CreateTweetByWordsCells(set, tweet);
                            } while (queue.Count > 0);
                        }
                        client.StoreCells(tableByWordsName, set);
                        Console.WriteLine("===== {0} rows written =====", set.rows.Count);
                    }
                    Thread.Sleep(100);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Exception: " + ex.Message);
            }
        }

        private static char[] _punctuationChars = new[] { 
            ' ', '!', '\"', '#', '$', '%', '&', '\'', '(', ')', '*', '+', ',', '-', '.', '/',   //ascii 23--47
            ':', ';', '<', '=', '>', '?', '@', '[', ']', '^', '_', '`', '{', '|', '}', '~' };   //ascii 58--64 + misc.

        private void CreateTweetByWordsCells(CellSet set, ITweet tweet)
        {
            var words = tweet.Text.ToLower().Split(_punctuationChars);
            int sentimentScore = CalcSentimentScore(words);
            var word_pairs = words.Take(words.Length - 1)
                                  .Select((word, idx) => string.Format("{0} {1}", word, words[idx + 1]));
            var all_words = words.Concat(word_pairs).ToList();

            foreach (var word in all_words)
            {
                var time_index = (ulong.MaxValue - 
                    (ulong)tweet.CreatedAt.ToBinary()).ToString().PadLeft(20) + tweet.IdStr;
                var key = word + "_" + time_index;
                var row = new CellSet.Row { key = Encoding.UTF8.GetBytes(key) };

                var value = new Cell { 
                    column = Encoding.UTF8.GetBytes("d:id_str"),
                    data = Encoding.UTF8.GetBytes(tweet.IdStr) };
                row.values.Add(value);
                value = new Cell { 
                    column = Encoding.UTF8.GetBytes("d:lang"),
                    data = Encoding.UTF8.GetBytes(tweet.Language.ToString()) };
                row.values.Add(value);
                if (tweet.Coordinates != null)
                {
                    var str = tweet.Coordinates.Longitude.ToString() + "," + 
                              tweet.Coordinates.Latitude.ToString();
                    value = new Cell { 
                        column = Encoding.UTF8.GetBytes("d:coor"),
                        data = Encoding.UTF8.GetBytes(str) };
                    row.values.Add(value);
                }

                value = new Cell { 
                    column = Encoding.UTF8.GetBytes("d:sentiment"),
                    data = Encoding.UTF8.GetBytes(sentimentScore.ToString()) };
                row.values.Add(value);

                set.rows.Add(row);
            }
        }

        private int CalcSentimentScore(string[] words)
        {
            var total = 0;
            foreach (var word in words)
            {
                if (dictionary.Keys.Contains(word))
                {
                    switch(dictionary[word].Polarity)
                    {
                        case "negative": total -= 1; break;
                        case "positive": total += 1; break;
                    }
                }
            }
            if (total > 0)
            {
                return 1;
            }
            else if (total < 0)
            {
                return -1;
            }
            else
            {
                return 0;
            }
        }
        
        private static void CreateTweetCells(CellSet set, ITweet tweet)
        {
            var key = tweet.IdStr;
            var row = new CellSet.Row { key = Encoding.UTF8.GetBytes(key) };
            var value = new Cell { column = Encoding.UTF8.GetBytes("d:created_at"), data = Encoding.UTF8.GetBytes(tweet.CreatedAt.ToLongTimeString()) };
            row.values.Add(value);
            value = new Cell { column = Encoding.UTF8.GetBytes("d:text"), data = Encoding.UTF8.GetBytes(tweet.Text) };
            row.values.Add(value);
            value = new Cell { column = Encoding.UTF8.GetBytes("d:lang"), data = Encoding.UTF8.GetBytes(tweet.Language.ToString()) };
            row.values.Add(value);
            if (tweet.Coordinates != null)
            {
                var str = tweet.Coordinates.Longitude.ToString() + "," + tweet.Coordinates.Latitude.ToString();
                value = new Cell { column = Encoding.UTF8.GetBytes("d:coor"), data = Encoding.UTF8.GetBytes(str) };
                row.values.Add(value);
            }
            if (tweet.Place != null)
            {
                value = new Cell { column = Encoding.UTF8.GetBytes("d:place_fullname"), data = Encoding.UTF8.GetBytes(tweet.Place.FullName) };
                row.values.Add(value);
            }
            set.rows.Add(row);
        }

        private void LoadDictionary()
        {
            List<string> lines = File.ReadAllLines(@"..\..\data\dictionary\dictionary.tsv").ToList();
            var items = lines.Select(line =>
            {
                var fields = line.Split('\t');
                var pos = 0;
                return new DictionaryItem
                {
                    Type = fields[pos++],
                    Length = Convert.ToInt32(fields[pos++]),
                    Word = fields[pos++],
                    Pos = fields[pos++],
                    Stemmed = fields[pos++],
                    Polarity = fields[pos++]
                };
            });

            dictionary = new Dictionary<string, DictionaryItem>();
            foreach(var item in items)
            {
                if (!dictionary.Keys.Contains(item.Word))
                {
                    dictionary.Add(item.Word, item);
                }
            }
        }

        internal static ClusterCredentials CreateFromFile(string path)
        {
            List<string> lines = File.ReadAllLines(path).ToList();
            return CreateFromList(lines);
        }

        internal static ClusterCredentials CreateFromList(List<string> lines)
        {
            if (lines.Count() != 3)
            {
                throw new ArgumentException(
                    string.Format(
                        CultureInfo.InvariantCulture,
                        "Expected the credentials file to have exactly three lines, " +
                        "first containing the cluster URL, second the username, third the password. " + "Given {0} lines!",
                        lines.Count()),
                    "lines");
            }

            var rv = new ClusterCredentials(new Uri(lines[0]), lines[1], lines[2]);
            return rv;
        }
    }

    public class DictionaryItem
    {
        public string Type { get; set; }
        public int Length { get; set; }
        public string Word { get; set; }
        public string Pos { get; set; }
        public string Stemmed { get; set; }
        public string Polarity { get; set; }
    }
}
