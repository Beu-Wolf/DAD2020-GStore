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

        public ObjectKey(ObjectId key) : this (key.PartitionId, key.ObjectKey) { }

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


            var interceptor = new DelayMessagesInterceptor(minDelay, maxDelay);
            GStoreServer gStoreServer = new GStoreServer(id, interceptor);

            var gStoreService = new GStoreService(gStoreServer);
            var serverSyncService = new ServerSyncService(gStoreServer);
            var pmCommunicationService = new PMCommunicationService(gStoreServer);

            Grpc.Core.Server server = new Grpc.Core.Server
            {
                Services = { 
                    ClientServerGrpcService.BindService(gStoreService).Intercept(interceptor), 
                    ServerSyncGrpcService.BindService(serverSyncService).Intercept(interceptor),
                    PuppetMasterServerGrpcService.BindService(pmCommunicationService)
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
