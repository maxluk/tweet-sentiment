using Microsoft.HBase.Client;
using org.apache.hadoop.hbase.rest.protobuf.generated;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TweetSentimentTopology
{
    // Class that encapsulates read/write operations with HBase twitter index table
    public class HBaseStore
    {
        // HBase context
        HBaseClient client;
        const string TABLE_BY_WORDS_NAME = "tweets_by_words";
        const string COUNT_ROW_KEY = "~ROWCOUNT";
        const string COUNT_COLUMN_NAME = "d:COUNT";

        public HBaseStore()
        {
            // Initialize HBase connection
            var credentials = CreateFromFile(@"credentials.txt");
            client = new HBaseClient(credentials);

            if (!client.ListTablesAsync().Result.name.Contains(TABLE_BY_WORDS_NAME))
            {
                // Create the table
                var tableSchema = new TableSchema();
                tableSchema.name = TABLE_BY_WORDS_NAME;
                tableSchema.columns.Add(new ColumnSchema { name = "d" });
                client.CreateTableAsync(tableSchema).Wait();
            }
        }

        public void WriteIndexItems(List<TweetIndexItem> items)
        {
            var set = new CellSet();

            foreach(var item in items)
            {
                CreateTweetByWordsCells(set, item);
            }

            client.StoreCellsAsync(TABLE_BY_WORDS_NAME, set).Wait();
        }

        public long GetRowCount()
        {
            try
            {
                var cellSet = client.GetCellsAsync(TABLE_BY_WORDS_NAME, COUNT_ROW_KEY).Result;
                if (cellSet.rows.Count != 0)
                {
                    var countCol = cellSet.rows[0].values.Find(cell => Encoding.UTF8.GetString(cell.column) == COUNT_COLUMN_NAME);
                    if (countCol != null)
                    {
                        return Convert.ToInt64(Encoding.UTF8.GetString(countCol.data));
                    }
                }
            }
            catch (Exception ex)
            {
                return 0;
            }

            return 0;
        }

        public void UpdateRowCount(long rowCount)
        {
            var set = new CellSet();
            CreateRowCountCell(set, rowCount);

            client.StoreCellsAsync(TABLE_BY_WORDS_NAME, set).Wait();
        }

        private void CreateRowCountCell(CellSet set, long count)
        {
            var row = new CellSet.Row { key = Encoding.UTF8.GetBytes(COUNT_ROW_KEY) };

            var value = new Cell
            {
                column = Encoding.UTF8.GetBytes(COUNT_COLUMN_NAME),
                data = Encoding.UTF8.GetBytes(count.ToString())
            };
            row.values.Add(value);
            set.rows.Add(row);
        }

        private void CreateTweetByWordsCells(CellSet set, TweetIndexItem indexItem)
        {
            var word = indexItem.Word;
            var time_index = (ulong.MaxValue -
                              (ulong)indexItem.CreatedAt).ToString().PadLeft(20) + indexItem.IdStr;
            var key = word + "_" + time_index;
            var row = new CellSet.Row { key = Encoding.UTF8.GetBytes(key) };

            var value = new Cell
            {
                column = Encoding.UTF8.GetBytes("d:id_str"),
                data = Encoding.UTF8.GetBytes(indexItem.IdStr)
            };
            row.values.Add(value);
            value = new Cell
            {
                column = Encoding.UTF8.GetBytes("d:lang"),
                data = Encoding.UTF8.GetBytes(indexItem.Language)
            };
            row.values.Add(value);

            value = new Cell
            {
                column = Encoding.UTF8.GetBytes("d:coor"),
                data = Encoding.UTF8.GetBytes(indexItem.Coordinates)
            };
            row.values.Add(value);

            value = new Cell
            {
                column = Encoding.UTF8.GetBytes("d:sentiment"),
                data = Encoding.UTF8.GetBytes(indexItem.SentimentScore.ToString())
            };
            row.values.Add(value);

            set.rows.Add(row);
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

    public class TweetIndexItem 
    {
        public string Word {get;set;}
        public long CreatedAt {get;set;}
        public string IdStr {get;set;}
        public string Language {get;set;}
        public string Coordinates {get;set;}
        public int SentimentScore {get;set;}
    }
}
