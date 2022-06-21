using StackExchange.Redis;
using System;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace RedisDemo
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.Title = "redis Demo by 蓝创精英团队";
            var connMultiplexer = ConnectionMultiplexer.Connect("127.0.0.1");
            Console.WriteLine($"redis连接状态:{connMultiplexer.IsConnected}");
            var db = connMultiplexer.GetDatabase(1);//可以选择指定的db，0-15
            //String
            db.StringSet("str1", "123");
            var stringValue = db.StringGet("str1");
            Console.WriteLine($"获取到string 类型的值:{stringValue}");

            //hash 就是一个key 下面  有一个集合的hash  key,value
            db.HashSet("hash", 123, 123);
            db.HashSet("hash", 123, 456);

            var HashValue = db.HashGet("hash", 123);
            Console.WriteLine($"获取到hash 类型的值:{HashValue}");

            // List ,就是一个有序的集合,可以选择push和pop，左右都可以
            db.KeyDelete("list");
            db.ListLeftPush("list", "123");
            db.ListLeftPush("list", "456");

            var listValue = db.ListLeftPop("list");
            Console.WriteLine($"获取到list 类型的值:{listValue}");
            listValue = db.ListLeftPop("list");
            Console.WriteLine($"获取到list 类型的值:{listValue}");

            // Set 一个无序不重复集合
            db.SetAdd("set", 123);
            db.SetAdd("set", 456);
            db.SetAdd("set", 789);
            db.SetAdd("set", 789);
            var setValue = db.SetScan("set");
            Console.WriteLine($"获取到set 类型的值:{string.Join(",", setValue)}");

            // Zset(SortedSet)
            db.SortedSetAdd("SortedSet", 123, 1);
            db.SortedSetAdd("SortedSet", 123, 2);
            db.SortedSetAdd("SortedSet", 456, 2);
            var sortedSetValue = db.SortedSetScan("SortedSet");
            Console.WriteLine($"获取到sortedSetValue 类型的值:{string.Join(",", sortedSetValue)}");

            // geo
            db.GeoAdd("geo", 104.056257, 30.651897, "成都锦里");
            db.GeoAdd("geo", 104.063684, 30.663607, "成都人民公园");
            var Distance = db.GeoDistance("geo", "成都锦里", "成都人民公园");
            Console.WriteLine($"获取到 geo 地理类型 距离{Distance}");

            // HyperLogLog 它是一种算法
            db.HyperLogLogAdd("HyperLogLog1", 123);
            db.HyperLogLogAdd("HyperLogLog1", 123);
            db.HyperLogLogAdd("HyperLogLog1", 456);
            var HyperLogLogLength = db.HyperLogLogLength("HyperLogLog1");
            Console.WriteLine($"HyperLogLogLength 长度:{HyperLogLogLength}");

            db.HyperLogLogAdd("HyperLogLog2", 789);
            db.HyperLogLogAdd("HyperLogLog2", 789);
            db.HyperLogLogAdd("HyperLogLog2", 123);

            //可以合并集合统计
            db.HyperLogLogMerge("HyperLogLog3", "HyperLogLog1", "HyperLogLog2");
            HyperLogLogLength = db.HyperLogLogLength("HyperLogLog3");
            Console.WriteLine($"HyperLogLogLength 长度:{HyperLogLogLength}");

            // Stream 新版消息队列
            db.StreamDeleteConsumerGroup("stream", "消费者组");
            //消费者组创建
            var result = db.StreamCreateConsumerGroup("stream", "消费者组");
            if (result)
            {
                Console.WriteLine("创建消费者组成功");
            }
            else
            {
                Console.WriteLine("创建消费者1失败");
            }
            //生产者
            Task.Run(() =>
            {
                int i = 0;
                while (true)
                {
                    var result = db.StreamAdd("stream", "123", i.ToString());
                    Thread.Sleep(1000);
                    i++;
                }
            });
            //消费者1
            Task.Run(() =>
            {
                while (true)
                {
                    var result = db.StreamReadGroup("stream", "消费者组", "消费者1");
                    if (result?.Any() == true)
                    {
                        foreach (var item in result)
                        {
                            Console.WriteLine($"消费者1：{item.Id} {item["123"]}");
                        }
                    }
                }
            });
            //消费者2
            Task.Run(() =>
            {
                while (true)
                {
                    var result = db.StreamReadGroup("stream", "消费者组", "消费者2");
                    if (result?.Any() == true)
                    {
                        foreach (var item in result)
                        {
                            Console.WriteLine($"消费者2：{item.Id} {item["123"]}");
                        }
                    }
                }
            });


            Console.WriteLine("连接redis");
            Console.ReadLine();
        }
    }
}
