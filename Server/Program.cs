using Grpc.Core;
using Grpc.Core.Interceptors;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;

namespace Server
{

    public class ObjectKey
    {
        public string Partition_id { get; private set; }

        public string Object_id { get; private set; }

        public ObjectKey(string partition_id, string object_id)
        {
            Partition_id = partition_id;
            Object_id = object_id;
        }

        public ObjectKey(Key key) : this (key.PartitionId, key.ObjectId) { }

        public class ObjectKeyComparer : IEqualityComparer<ObjectKey>
        {
            public bool Equals(ObjectKey objectKey1, ObjectKey objectKey2)
            {
                return objectKey1.Partition_id == objectKey2.Partition_id && objectKey1.Object_id == objectKey2.Object_id;
            }

            public int GetHashCode(ObjectKey objectKey)
            {
                return objectKey.Object_id.GetHashCode() ^ objectKey.Partition_id.GetHashCode();
            }
        }
    }

    public class ObjectValueManager
    {
        public string Value;
        private object WriteLock = new object();
        private bool Writing = false;
        private object NumReadersLock = new object();
        private int NumReaders = 0;

        public ObjectValueManager(string value)
        {
            Value = value;
        }

        public ObjectValueManager() { }

        public void LockRead()
        {
            lock (WriteLock)
            {
                while (Writing) Monitor.Wait(WriteLock);

                lock(NumReadersLock)
                {
                    NumReaders++;
                }
                
            }
        }

        public void UnlockRead()
        {
            lock(NumReadersLock)
            {
                NumReaders--;
                if (NumReaders == 0) Monitor.PulseAll(NumReadersLock);
            }
        }

        public void LockWrite()
        {
            lock(WriteLock)
            {
                while (Writing) Monitor.Wait(WriteLock);
                
                lock(NumReadersLock)
                {
                    while (NumReaders != 0) Monitor.Wait(NumReadersLock);
                    Writing = true;
                }
                
            }
        }

        public void UnlockWrite(string value)
        {
            lock(WriteLock)
            {
                Value = value;
                Writing = false;
                Monitor.PulseAll(WriteLock);
            }
        }

        
    }

    public class Program
    {

        static void Main(string[] args)
        {
            if (args.Length != 5)
            {
                Console.WriteLine("Usage: Server.exe server_id host port min_delay max_delay");
                return;
            }


            if (!int.TryParse(args[2], out int Port))
            {
                Console.WriteLine("Invalid port value");
                return;
            }

            if (!int.TryParse(args[3], out int minDelay)) {
                Console.WriteLine("Invalid min delay");
                return;
            }

            if (!int.TryParse(args[4], out int maxDelay)) {
                Console.WriteLine("Invalid max delay");
                return;
            }

            if (minDelay > maxDelay)
            {
                Console.WriteLine("Max delay must be greater of equal than min delay");
                return;
            }

            AppContext.SetSwitch(
   "System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);

            string id = args[0];

            string host = args[1];

            // Dictionary with values
            ConcurrentDictionary<ObjectKey, ObjectValueManager> keyValuePairs = new ConcurrentDictionary<ObjectKey, ObjectValueManager>(new ObjectKey.ObjectKeyComparer());


            // Dictionary <partition_id, List<URLs>> all servers by partition
            ConcurrentDictionary<string, List<string>> ServersByPartition = new ConcurrentDictionary<string, List<string>>();
            //ServersByPartition.TryAdd("part-1", new List<string> { "s1", "s2" });
            //ServersByPartition.TryAdd("part-2", new List<string> { "s2" });

            ConcurrentDictionary<string, string> serverUrls = new ConcurrentDictionary<string, string>();
            //serverUrls.TryAdd("s1", "http://localhost:10010");
            //serverUrls.TryAdd("s2", "http://localhost:10011");

            // List of crashed servers
            ConcurrentBag<string> CrashedServers = new ConcurrentBag<string>();

            // List partition which im master of
            List<string> MasteredPartitions = new List<string>(); // { Port == 10010 ? "part-1" : "part-2" };

            var interceptor = new DelayMessagesInterceptor(minDelay, maxDelay);

            // ReadWriteLock for listMe functions
            var localReadWriteLock = new ReaderWriterLock();

            var clientServerService = new ClientServerService(keyValuePairs, ServersByPartition, serverUrls, MasteredPartitions, localReadWriteLock, CrashedServers)
            {
                MyId = id
            };

            var serverSyncService = new ServerSyncService(keyValuePairs, ServersByPartition, localReadWriteLock, CrashedServers);

            var puppetMasterService = new PuppetMasterServerService(ServersByPartition, serverUrls, MasteredPartitions, CrashedServers, interceptor);

            Grpc.Core.Server server = new Grpc.Core.Server
            {
                Services = { 
                    ClientServerGrpcService.BindService(clientServerService).Intercept(interceptor), 
                    ServerSyncGrpcService.BindService(serverSyncService).Intercept(interceptor),
                    PuppetMasterServerGrpcService.BindService(puppetMasterService)
                },
                Ports = { new ServerPort(host, Port, ServerCredentials.Insecure)}
            };

            server.Start();

            Console.WriteLine("Press any key to stop the server...");
            Console.ReadKey();

            server.ShutdownAsync().Wait();
        }
    }
}
