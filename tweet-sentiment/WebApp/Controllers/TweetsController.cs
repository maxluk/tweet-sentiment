using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using System.Web.Http;
using WebApp.Models;

namespace WebApp.Controllers
{
    public class TweetsController : ApiController
    {
        HBaseReader hbase = new HBaseReader();

        List<Tweet> list = new List<Tweet> {
                new Tweet { IdStr = "1", Longtitude = -120, Latitude = 30 },
                new Tweet { IdStr = "2", Longtitude = -110, Latitude = 40 }
            };

        public IEnumerable<Tweet> GetAllTweets()
        {
            return list;
        }

        public IHttpActionResult GetTweet(int id)
        {
            return Ok(list[0]);
        }

        public async Task<IEnumerable<Tweet>> GetTweetsByQuery(string query)
        {
            return await hbase.QueryTweetsByKeywordAsync(query);
        }
    }
}
