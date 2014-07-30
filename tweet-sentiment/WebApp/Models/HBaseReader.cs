using Microsoft.HBase.Client;
using org.apache.hadoop.hbase.rest.protobuf.generated;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Web;

namespace WebApp.Models
{
    public class HBaseReader
    {
        HBaseClient client;
        //string tableByIdName = "tweets_by_id";
        string tableByWordsName = "tweets_by_words";

        public HBaseReader()
        {
            var creds = new ClusterCredentials(
                            new Uri(ConfigurationManager.AppSettings["Cluster"]),
                            ConfigurationManager.AppSettings["User"],
                            ConfigurationManager.AppSettings["Pwd"]);
            client = new HBaseClient(creds);
        }

        public async Task<IEnumerable<Tweet>> QueryTweetsByKeywordAsync(string keyword)
        {
            var list = new List<Tweet>();
            var exists = await CheckTable();
            if (!exists)
            {
                return list;
            }

            var startRow = keyword + "_";
            var endRow = keyword + "|";
            var scanSettings = new Scanner { batch = 100000, startRow = Encoding.UTF8.GetBytes(startRow), endRow = Encoding.UTF8.GetBytes(endRow) };
            ScannerInformation scannerInfo = await client.CreateScannerAsync(tableByWordsName, scanSettings);

            CellSet next;
            while ((next = await client.ScannerGetNextAsync(scannerInfo)) != null)
            {
                foreach (CellSet.Row row in next.rows)
                {
                    var coordinates = row.values.Find(c => Encoding.UTF8.GetString(c.column) == "d:coor");
                    if (coordinates != null)
                    {
                        var lonlat = Encoding.UTF8.GetString(coordinates.data).Split(',');

                        var sentimentField = row.values.Find(c => Encoding.UTF8.GetString(c.column) == "d:sentiment");
                        var sentiment = 0;
                        if (sentimentField != null)
                        {
                            sentiment = Convert.ToInt32(Encoding.UTF8.GetString(sentimentField.data));
                        }

                        list.Add(new Tweet {
                            Longtitude = Convert.ToDouble(lonlat[0]),
                            Latitude = Convert.ToDouble(lonlat[1]),
                            Sentiment = sentiment
                        });
                    }

                    if (coordinates != null)
                    {
                        var lonlat = Encoding.UTF8.GetString(coordinates.data).Split(',');
                    }
                }
            }

            return list;
        }

        internal async Task<bool> CheckTable()
        {
            return (await client.ListTablesAsync()).name.Contains(tableByWordsName);
        }
    }
}