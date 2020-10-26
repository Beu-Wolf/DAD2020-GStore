﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Grpc.Core.Utils;
using Grpc.Net.Client;

namespace Server
{
    public class ClientServerService : ClientServerGrpcService.ClientServerGrpcServiceBase
    {
        public int MyPort { get; set; }
        public string MyHost { get; set; }

        private object WriteGlobalLock = new object();

        private readonly Dictionary<ObjectKey, ObjectValueManager> KeyValuePairs;
        private readonly Dictionary<long, List<string>> ServersByPartition;
        private readonly List<long> MasteredPartitions;

        private object myLock = new object();

        public ClientServerService() {}

        public ClientServerService(Dictionary<ObjectKey, ObjectValueManager> keyValuePairs, Dictionary<long, List<string>> serversByPartitions, List<long> masteredPartitions)
        {
            KeyValuePairs = keyValuePairs;
            ServersByPartition = serversByPartitions;
            MasteredPartitions = masteredPartitions;
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

            var requestedObject = new ObjectKey(request.Key);


            if (KeyValuePairs.TryGetValue(requestedObject, out ObjectValueManager objectValueManager)) {

                objectValueManager.LockRead();
                ReadObjectReply reply = new ReadObjectReply
                {
                    Value = objectValueManager.Value
                };
                objectValueManager.UnlockRead();
                return reply;
                
            } else
            {
                throw new RpcException(new Status(StatusCode.NotFound, $"Object <{request.Key.PartitionId}, {request.Key.ObjectId}> not found here"));
            }         
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

            if (MasteredPartitions.Contains(request.Key.PartitionId))
            {
                lock(WriteGlobalLock)
                {
                    // I'm master of this object's partition
                    // Send request to all other servers of partition
                    ServersByPartition.TryGetValue(request.Key.PartitionId, out List<string> serverUrls);

                    if (!KeyValuePairs.TryGetValue(new ObjectKey(request.Key), out ObjectValueManager objectValueManager))
                    {
                        objectValueManager = new ObjectValueManager();
                        KeyValuePairs[new ObjectKey(request.Key)] = objectValueManager;
                    }

                    objectValueManager.LockWrite();

                    foreach (var serverUrl in serverUrls.Where(x => !x.Contains($"http://{MyHost}:{MyPort}")))
                    {
                        var channel = GrpcChannel.ForAddress(serverUrl);
                        var client = new ServerSyncGrpcService.ServerSyncGrpcServiceClient(channel);
                        // What to do if success returns false ?
                        client.LockObject(new LockObjectRequest
                        {
                            Key = request.Key
                        });
                    }


                    foreach (var serverUrl in serverUrls.Where(x => !x.Contains($"http://{MyHost}:{MyPort}")))
                    {
                        var channel = GrpcChannel.ForAddress(serverUrl);
                        var client = new ServerSyncGrpcService.ServerSyncGrpcServiceClient(channel);
                        // What to do if success returns false ?
                        client.ReleaseObjectLock(new ReleaseObjectLockRequest
                        {
                            Key = request.Key,
                            Value = request.Value

                        });
                    }

                    objectValueManager.UnlockWrite(request.Value);


                    return new WriteObjectReply
                    {
                        Ok = true
                    };
                }
            }
            else
            {
                // Tell him I'm not the master
                throw new RpcException(new Status(StatusCode.PermissionDenied, $"Server {MyHost}:{MyPort} is not the master of partition {request.Key.PartitionId}"));
            }
                      
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

            foreach (ObjectKey obj in KeyValuePairs.Keys)
            {
                lst.Add(new ObjectInfo
                {
                    IsPartitionMaster = MasteredPartitions.Contains(obj.GetPartitionId()),
                    Key = new Key
                    {
                        PartitionId = obj.GetPartitionId(),
                        ObjectId = obj.GetObjectId()
                    },
                    Value = KeyValuePairs[obj].Value

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
            List<Key> lst = new List<Key>();

            foreach (var key in KeyValuePairs.Keys)
            {
                lst.Add(new Key
                {
                    PartitionId = key.GetPartitionId(),
                    ObjectId = key.GetObjectId()
                });
            }

            return new ListGlobalReply
            {
                Keys = { lst }
            };
        }

    }
}
