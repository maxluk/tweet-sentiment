using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using Microsoft.AspNet.SignalR;

namespace WebApp
{
    public class TwitterHub : Hub
    {
        long latestRowCount = 0;

        public void UpdateCounter(long rowCount)
        {
            if (rowCount > latestRowCount)
            {
                latestRowCount = rowCount;
                Clients.All.updateCounter(latestRowCount);
            }
        }
    }
}