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
        public long Partition_id { get; private set; }

        public long Object_id { get; private set; }

        public ObjectKey(long partition_id, long object_id)
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
            if (args.Length != 2)
            {
                Console.WriteLine("Usage: Server.exe host port");
                return;
            }


            if(!int.TryParse(args[1], out int Port))
            {
                Console.WriteLine("Invalid port value");
                return;
            }

            AppContext.SetSwitch(
   "System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);

            string host = args[0]; // Maybe pass as parameter when instanciating server

            // Dictionary with values
            ConcurrentDictionary<ObjectKey, ObjectValueManager> keyValuePairs = new ConcurrentDictionary<ObjectKey, ObjectValueManager>(new ObjectKey.ObjectKeyComparer());


            // Dictionary <partition_id, List<URLs>> all servers by partition
            ConcurrentDictionary<long, List<string>> ServersByPartition = new ConcurrentDictionary<long, List<string>>();
            ServersByPartition.TryAdd(1, new List<string> { "http://localhost:10001", "http://localhost:10002" });
            ServersByPartition.TryAdd(2, new List<string> { "http://localhost:10002" });

            // List of crashed servers
            ConcurrentBag<string> CrashedServers = new ConcurrentBag<string>();

            // List partition which im master of
            List<long> MasteredPartitions = new List<long> { Port == 10001 ? 1 : 2 };

            // Min and max delays for communication
            int minDelay = 0;
            int maxDelay = 100;

            var interceptor = new DelayMessagesInterceptor(minDelay, maxDelay);

            // ReadWriteLock for listMe functions
            var localReadWriteLock = new ReaderWriterLock();

            var clientServerService = new ClientServerService(keyValuePairs, ServersByPartition, MasteredPartitions, localReadWriteLock, CrashedServers)
            {
                MyHost = host,
                MyPort = Port
            };

            var serverSyncService = new ServerSyncService(keyValuePairs, ServersByPartition, localReadWriteLock, CrashedServers);

            var puppetMasterService = new PuppetMasterServerService(ServersByPartition, MasteredPartitions, CrashedServers, interceptor);

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
