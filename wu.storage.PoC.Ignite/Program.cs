using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Apache.Ignite.Core;
using Apache.Ignite.Linq;
using Apache.Ignite.Core.Cache.Configuration;
using Apache.Ignite.Core.Configuration;
using Humanizer;
using Tynamix.ObjectFiller;

namespace wu.storage.PoC.Ignite
{
    class Program
    {
        static void Main(string[] args)
        {
            var cfg = new IgniteConfiguration();
            cfg.DataStorageConfiguration = new DataStorageConfiguration()
            {
                DefaultDataRegionConfiguration = new DataRegionConfiguration()
                {
                    Name = "default_data",
                    MaxSize = 3 * (long)0x40000000
                } // 4 Gigs
            };
            using (var ignite = Ignition.Start(cfg))
            {
                var cache = ignite.GetOrCreateCache<string, Order>(new CacheConfiguration("orders", 
                    new QueryEntity(typeof(string), typeof(Order)) { TableName = "OrderObject"})
                    );

                var count = 1000000;
                var random = new Random();
                var filler = new Filler<Order>();
                filler.Setup().OnProperty(x => x.ClientId).Use(() => $"C{(random.Next() % 30):D5}");

                var orders = Enumerable.Repeat(0, count).Select(_ => new Order()).Select(o => filler.Fill(o));
                Console.WriteLine("Putting {0} objects...", count);
                var putSw = Stopwatch.StartNew();
                string clientId = null;
                int eaten = 0;
                try
                {
                    foreach( var x in orders)
                        //.Select(x => new KeyValuePair<string, Order>(x.Id1, x))
                    {
                        eaten++;
                        clientId = clientId ?? x.ClientId;
                        cache.Put(x.Id1, x);
                    }
                    putSw.Stop();

                }
                catch (Exception e)
                {
                    Console.WriteLine("Eaten: {0}", eaten);
                    throw;
                }
                Console.WriteLine("Putting {0} objects took {1} ({2}/s)", count, putSw.Elapsed, count * 1000 / (double)putSw.ElapsedMilliseconds);

                var qcache = cache.AsCacheQueryable();

                var whereSw = Stopwatch.StartNew();
                var result = qcache.Where(x => x.Value.ClientId == clientId);
                var whereRawLinqTime = whereSw.Elapsed;

                var resultList = result.ToList();
                Console.WriteLine("Fetched {0} items by clientId={1}, took {2} (where~{3})", resultList.Count, clientId, whereSw.Elapsed, whereRawLinqTime);

            }
        }
    }
}
