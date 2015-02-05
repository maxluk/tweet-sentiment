using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SCP;
using System.Threading;
using System.Globalization;
using System.IO;
using System.Diagnostics;

namespace TweetSentimentTopology
{
    class SentimentIndexerBolt : ISCPBolt
    {
        //Context
        private Context ctx;

        // Sentiment dictionary
        Dictionary<string, DictionaryItem> dictionary;

        //Constructor
        public SentimentIndexerBolt(Context ctx)
        {
            Context.Logger.Info("SentimentIndexerBolt constructor called");
            //Set context
            this.ctx = ctx;
            //Define the schema for the incoming tuples from spout
            Dictionary<string, List<Type>> inputSchema = new Dictionary<string, List<Type>>();
            inputSchema.Add("default", new List<Type>() { typeof(string), typeof(long), typeof(string), typeof(string), typeof(string) });

            //Define the output schema
            Dictionary<string, List<Type>> outputSchema = new Dictionary<string, List<Type>>();
            outputSchema.Add("default", new List<Type>() { typeof(string), typeof(long), typeof(string), typeof(string), typeof(string), typeof(int) });

            //Declare both incoming and outbound schemas
            this.ctx.DeclareComponentSchema(new ComponentStreamSchema(inputSchema, outputSchema));

            // Load sentiment dictionary file
            LoadDictionary();
        }

        //Get a new instance
        public static SentimentIndexerBolt Get(Context ctx, Dictionary<string, Object> parms)
        {
            return new SentimentIndexerBolt(ctx);
        }

        private static char[] _punctuationChars = new[] { 
            ' ', '!', '\"', '#', '$', '%', '&', '\'', '(', ')', '*', '+', ',', '-', '.', '/',   //ascii 23--47
            ':', ';', '<', '=', '>', '?', '@', '[', ']', '^', '_', '`', '{', '|', '}', '~' };   //ascii 58--64 + misc.

        //Process a tuple from the stream
        public void Execute(SCPTuple tuple)
        {
            Context.Logger.Info("Execute enter");

            try
            {
                var words = tuple.GetString(0).ToLower().Split(_punctuationChars);
                int sentimentScore = CalcSentimentScore(words);
                var word_pairs = words.Take(words.Length - 1)
                                      .Select((word, idx) => string.Format("{0} {1}", word, words[idx + 1]));
                var all_words = words.Concat(word_pairs).ToList();

                // Emit all index entries for counting and writing downstream
                foreach (var word in all_words)
                {
                    this.ctx.Emit(new Values(word,
                                             tuple.GetLong(1),
                                             tuple.GetString(2),
                                             tuple.GetString(3),
                                             tuple.GetString(4),
                                             sentimentScore));
                }
            }
            catch (Exception ex)
            {
                Context.Logger.Error("SentimentIndexerBolt Exception: " + ex.Message + "\nStackTrace: \n" + ex.StackTrace);
            }

            Context.Logger.Info("Execute exit");
        }

        private int CalcSentimentScore(string[] words)
        {
            var total = 0;
            foreach (var word in words)
            {
                if (dictionary.Keys.Contains(word))
                {
                    switch (dictionary[word].Polarity)
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

        private void LoadDictionary()
        {
            List<string> lines = File.ReadAllLines(@"data\dictionary.tsv").ToList();
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
            foreach (var item in items)
            {
                if (!dictionary.Keys.Contains(item.Word))
                {
                    dictionary.Add(item.Word, item);
                }
            }
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