using System;
using System.Collections.Concurrent;
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
        public string MyId { get; set; }

        private readonly object WriteGlobalLock = new object();

        private readonly ReaderWriterLock LocalReadWriteLock;

        private readonly ConcurrentDictionary<ObjectKey, ObjectValueManager> KeyValuePairs;
        private readonly ConcurrentDictionary<string, List<string>> ServersByPartition;
        private readonly ConcurrentDictionary<string, string> ServerUrls;
        private readonly List<string> MasteredPartitions;
        private readonly ConcurrentBag<string> CrashedServers;


        public ClientServerService() {}

        public ClientServerService(ConcurrentDictionary<ObjectKey, ObjectValueManager> keyValuePairs, ConcurrentDictionary<string, List<string>> serversByPartitions, ConcurrentDictionary<string, string> serverUrls,
            List<string> masteredPartitions, ReaderWriterLock readerWriterLock, ConcurrentBag<string> crashedServers)
        {
            KeyValuePairs = keyValuePairs;
            ServersByPartition = serversByPartitions;
            ServerUrls = serverUrls;
            MasteredPartitions = masteredPartitions;
            LocalReadWriteLock = readerWriterLock;
            CrashedServers = crashedServers;
        }

        private void UpdateCrashedServers(string partition, HashSet<string> crashedServers)
        {
            CrashedServers.Union(crashedServers);

            ServersByPartition[partition].RemoveAll(x => crashedServers.Contains(x));
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

                LocalReadWriteLock.AcquireReaderLock(-1);
                objectValueManager.LockRead();
                ReadObjectReply reply = new ReadObjectReply
                {
                    Value = objectValueManager.Value
                };
                objectValueManager.UnlockRead();

                LocalReadWriteLock.ReleaseReaderLock();
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
                    ServersByPartition.TryGetValue(request.Key.PartitionId, out List<string> serverIds);

                    if (!KeyValuePairs.TryGetValue(new ObjectKey(request.Key), out ObjectValueManager objectValueManager))
                    {
                        LocalReadWriteLock.AcquireWriterLock(-1);
                        objectValueManager = new ObjectValueManager();
                        KeyValuePairs[new ObjectKey(request.Key)] = objectValueManager;
                        objectValueManager.LockWrite();
                        LocalReadWriteLock.ReleaseWriterLock();
                    }
                    else
                    {
                        objectValueManager.LockWrite();
                    }

                    var connectionCrashedServers = new HashSet<string>();

                    foreach (var server in ServerUrls.Where(x => serverIds.Contains(x.Key) && x.Key != MyId))
                    {

                        var channel = GrpcChannel.ForAddress(server.Value);
                        var client = new ServerSyncGrpcService.ServerSyncGrpcServiceClient(channel);
                        // What to do if success returns false ?
                        try
                        {
                            client.LockObject(new LockObjectRequest
                            {
                                Key = request.Key
                            });
                        } catch (RpcException e)
                        {
                            // If grpc does no respond, we can assume it has crashed
                            if (e.Status.StatusCode == StatusCode.DeadlineExceeded || e.Status.StatusCode == StatusCode.Unavailable || e.Status.StatusCode == StatusCode.Internal)
                            {
                                // Add to hash Set
                                Console.WriteLine($"Server {server.Key} has crashed");
                                connectionCrashedServers.Add(server.Key);
                            } 
                            else
                            {
                                throw e;
                            }
                        }
                    }

                    foreach (var server in ServerUrls.Where(x => serverIds.Contains(x.Key) && x.Key != MyId))
                    {
                        try
                        {
                            var channel = GrpcChannel.ForAddress(server.Value);
                            var client = new ServerSyncGrpcService.ServerSyncGrpcServiceClient(channel);
                            // What to do if success returns false ?
                            client.ReleaseObjectLock(new ReleaseObjectLockRequest
                            {
                                Key = request.Key,
                                Value = request.Value

                            });
                        }
                        catch (RpcException e)
                        {
                            if (e.Status.StatusCode == StatusCode.DeadlineExceeded || e.Status.StatusCode == StatusCode.Unavailable || e.Status.StatusCode == StatusCode.Internal)
                            {
                                // Add to hash Set
                                Console.WriteLine($"Server {server.Key} has crashed");
                                connectionCrashedServers.Add(server.Key);
                            }
                            else
                            {
                                throw e;
                            }
                        }
                    }


                    if (connectionCrashedServers.Any())
                    {
                        // Update the crashed servers
                        UpdateCrashedServers(request.Key.PartitionId, connectionCrashedServers);
                        
                        // Contact Partition slaves an update their view of the partition
                        foreach (var server in ServerUrls.Where(x => serverIds.Contains(x.Key) && x.Key != MyId))
                        {
                            var channel = GrpcChannel.ForAddress(server.Value);
                            var client = new ServerSyncGrpcService.ServerSyncGrpcServiceClient(channel);

                            client.RemoveCrashedServers(new RemoveCrashedServersRequest
                            {
                                PartitionId = request.Key.PartitionId,
                                ServerIds = {connectionCrashedServers}
                                
                            });
                        }
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
                throw new RpcException(new Status(StatusCode.PermissionDenied, $"Server {MyId} is not the master of partition {request.Key.PartitionId}"));
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

            LocalReadWriteLock.AcquireReaderLock(-1);
            foreach (ObjectKey obj in KeyValuePairs.Keys)
            {
                KeyValuePairs[obj].LockRead();
                lst.Add(new ObjectInfo
                {
                    IsPartitionMaster = MasteredPartitions.Contains(obj.Partition_id),
                    Key = new Key
                    {
                        PartitionId = obj.Partition_id,
                        ObjectId = obj.Object_id
                    },
                    Value = KeyValuePairs[obj].Value

                });
                KeyValuePairs[obj].UnlockRead();
            }
            LocalReadWriteLock.ReleaseReaderLock();

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

            LocalReadWriteLock.AcquireReaderLock(-1);
            foreach (var key in KeyValuePairs.Keys)
            {
                KeyValuePairs[key].LockRead();
                lst.Add(new Key
                {
                    PartitionId = key.Partition_id,
                    ObjectId = key.Object_id
                });
                KeyValuePairs[key].UnlockRead();
            }
            LocalReadWriteLock.ReleaseReaderLock();

            return new ListGlobalReply
            {
                Keys = { lst }
            };
        }
    }
}
