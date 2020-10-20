using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Grpc.Core;
using Grpc.Core.Utils;

namespace Server
{
    public class ClientServerService : ClientServerGrpcService.ClientServerGrpcServiceBase
    {
        private int delay = 0;
        public ClientServerService() {}

        public ClientServerService(int delay)
        {
            this.delay = delay;
        }

        // Read Object
        public override Task<ReadObjectReply> ReadObject(ReadObjectRequest request, ServerCallContext context )
        {
            return Task.FromResult(Read(request));
        }

        public ReadObjectReply Read(ReadObjectRequest request)
        {
            Console.WriteLine("Received Read with params:");
            Console.WriteLine($"Partition_id: {request.Key.PartitionId}");
            Console.WriteLine($"Object_id: {request.Key.ObjectId}");

            return new ReadObjectReply
            {
                Value = "test"
            };
        }

        // Write Object
        public override Task<WriteObjectReply> WriteObject(WriteObjectRequest request, ServerCallContext context)
        {
            return Task.FromResult(Write(request));
        }

        public WriteObjectReply Write(WriteObjectRequest request)
        {
            Console.WriteLine("Received write with params:");
            Console.WriteLine($"Partition_id: {request.Key.PartitionId}");
            Console.WriteLine($"Object_id: {request.Key.ObjectId}");
            Console.WriteLine($"Value: {request.Value}");

            return new WriteObjectReply
            {
                Ok = false
            };
        }

        // List Server
        public override Task<ListServerReply> ListServer(ListServerRequest request, ServerCallContext context)
        {
            return Task.FromResult(ListMe(request));
        }

        public ListServerReply ListMe(ListServerRequest request)
        {
            Console.WriteLine("Received ListServer");

            List<ObjectInfo> lst = new List<ObjectInfo>();

            for(int i = 0; i < 3; i++)
            {
                lst.Add(new ObjectInfo
                {
                    IsPartitionMaster = false,
                    Value = i.ToString(),
                    Key = new Key
                    {
                        PartitionId = i,
                        ObjectId = i*10
                    }
                });
            }

            return new ListServerReply
            {
                Objects = { lst }
            };
        }

        // Call to List Global
        public override Task<ListGlobalReply> ListGlobal(ListGlobalRequest request, ServerCallContext context)
        {
            return Task.FromResult(ListMeGlobal(request));
        }

        public ListGlobalReply ListMeGlobal(ListGlobalRequest request)
        {
            Console.WriteLine("Received ListGlobal");
            List<Key> lst = new List<Key>
            {
                new Key
                {
                    ObjectId = 1,
                    PartitionId = 2
                },
                new Key
                {
                    ObjectId = 2,
                    PartitionId = 2
                }
            };

            return new ListGlobalReply
            {
                Keys = { lst }
            };
        }

    }
}
