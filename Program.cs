using System;
using System.Collections.Generic;
using System.Threading;
using StackExchange.Redis;

namespace Redis_QueuewithThreads
{
    public static class RedisQueue
    {
        private static ConnectionMultiplexer _connectionMultiplexer;
        private static string QueueKey = "Key";


        public static void CreateConnection()
        {
            _connectionMultiplexer = ConnectionMultiplexer.Connect("127.0.0.1:6379");
        }

        public static void Push(RedisKey queueName, RedisValue value)
        {
            _connectionMultiplexer.GetDatabase().ListRightPush(queueName, value);
        }

        public static RedisValue Pop(RedisKey queueName)
        {
            return _connectionMultiplexer.GetDatabase().ListLeftPop(queueName);
        }

        public static void Enqueue(string value)
        {
            var key = new RedisKey(QueueKey);
            var redisValue = new RedisValue(value);
            Push(key, redisValue);
        }

        public static string Dequeue()
        {
            var key = new RedisKey(QueueKey);
            var redisValue = Pop(key);
            return redisValue;
        }
    }

    class QueuewithThreads
    {
        private static object _lockObject = new object();

        public static event Action<bool> ThreadPause;

        static List<Thread> _threads = new List<Thread>();

        static Thread timerThread = null;


        public static void Main(string[] args)
        {
            RedisQueue.CreateConnection();
            ThreadPause += StopDequeueThread;

            Console.WriteLine("What is the thread number ?");
            Console.WriteLine();
            int tAmount;
            Int32.TryParse(Console.ReadLine(), out tAmount);

            tAmount = tAmount == 0 ? 1 : tAmount;

            for (int i = 0; i < tAmount; i++)
            {
                var thread = new Thread(DequeueThread);
                thread.Start();
                _threads.Add(thread);
            }

            var tEnqueue = new Thread(EnqueueThread);
            tEnqueue.Start();
        }

        private static bool _canDequeue = false;

        private static void StopDequeueThread(bool isPause)
        {
            //Console.WriteLine(isPause);
            if (isPause)
            {
                _canDequeue = false;
                if (timerThread != null)
                {
                    timerThread.Abort();
                }

                timerThread = new Thread(TimerThread);
                timerThread.Start();
            }
            else
            {
                _canDequeue = true;
                Console.WriteLine("\nDequeuing now...");
            }
        }

        public static void DequeueThread()
        {
            while (true)
            {
                if (!_canDequeue)
                {
                    continue;
                }

                lock (_lockObject)
                {
                    var data = RedisQueue.Dequeue();
                    if (data != null)
                        Console.WriteLine("Dequeue: " + data + " : " + Thread.CurrentThread.ManagedThreadId);
                }

                Thread.Sleep(3000);
            }
        }

        public static void EnqueueThread()
        {
            while (true)
            {
                ThreadPause?.Invoke(true);
                Console.Write("Enqueue: ");
                var line = Console.ReadLine();

                if (line == "/")
                {
                    WriteQueue();
                    continue;
                }

                RedisQueue.Enqueue(line);
                WriteQueue();
            }
        }

        private static void TimerThread()
        {
            Thread.Sleep(3000);
            ThreadPause?.Invoke(false);
        }

        public static void WriteQueue()
        {
            // string word = "Queue: ";
            // // foreach (var w in _dataQueue)
            // // {
            // //     word += w + ", ";
            // // }
            //
            // Console.WriteLine(word);
        }
    }
}