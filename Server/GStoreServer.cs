using Grpc.Core;
using Grpc.Core.Interceptors;
using Grpc.Core;
using Grpc.Core.Utils;
using Grpc.Net.Client;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace Server
{
    public class GStoreServer
    {
        private string MyId { get; }

        // Dictionary with values
        private ConcurrentDictionary<ObjectKey, ObjectValueManager> KeyValuePairs;


        // Dictionary <partition_id, List<URLs>> all servers by partition
        private ConcurrentDictionary<string, List<string>> ServersByPartition;


        private ConcurrentDictionary<string, string> ServerUrls;

        // List of crashed servers
        private ConcurrentBag<string> CrashedServers;

        // List partition which im master of
        private List<string> MasteredPartitions;

        // ReadWriteLock for listMe functions
        private ReaderWriterLock LocalReadWriteLock;


        private readonly DelayMessagesInterceptor Interceptor;

        // TODO: REMOVE LATER: USED BY OLD WRITE
        private readonly object WriteGlobalLock = new object();

        public GStoreServer(string myId, DelayMessagesInterceptor interceptor)
        {
            MyId = myId;
            Interceptor = interceptor;
            KeyValuePairs = new ConcurrentDictionary<ObjectKey, ObjectValueManager>(new ObjectKey.ObjectKeyComparer());
            ServersByPartition = new ConcurrentDictionary<string, List<string>>();
            ServerUrls = new ConcurrentDictionary<string, string>();
            CrashedServers = new ConcurrentBag<string>();
            MasteredPartitions = new List<string>();
            LocalReadWriteLock = new ReaderWriterLock();
        }

        /*
         *  GStoreService Implementation
         */
        public ReadObjectReply Read(ReadObjectRequest request)
        {
            Console.WriteLine("Received Read with params:");
            Console.WriteLine($"Partition_id: {request.Key.PartitionId}");
            Console.WriteLine($"Object_id: {request.Key.ObjectKey}");

            var requestedObject = new ObjectKey(request.Key);


            if (KeyValuePairs.TryGetValue(requestedObject, out ObjectValueManager objectValueManager))
            {

                LocalReadWriteLock.AcquireReaderLock(-1);
                objectValueManager.LockRead();
                ReadObjectReply reply = new ReadObjectReply
                {
                    Value = objectValueManager.Value
                };
                objectValueManager.UnlockRead();

                LocalReadWriteLock.ReleaseReaderLock();
                return reply;

            }
            else
            {
                throw new RpcException(new Status(StatusCode.NotFound, $"Object <{request.Key.PartitionId}, {request.Key.ObjectKey}> not found here"));
            }
        }

        public WriteObjectReply Write(WriteObjectRequest request)
        {
            Console.WriteLine("Received write with params:");
            Console.WriteLine($"Partition_id: {request.Key.PartitionId}");
            Console.WriteLine($"Object_id: {request.Key.ObjectKey}");
            Console.WriteLine($"Value: {request.Value}");

            if (MasteredPartitions.Contains(request.Key.PartitionId))
            {
                lock (WriteGlobalLock)
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
                        }
                        catch (RpcException e)
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
                                ServerIds = { connectionCrashedServers }

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

        public ListServerReply ListServer(ListServerRequest request)
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
                    Key = new ObjectId
                    {
                        PartitionId = obj.Partition_id,
                        ObjectKey = obj.Object_id
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

        public ListGlobalReply ListGlobal(ListGlobalRequest request)
        {
            Console.WriteLine("Received ListGlobal");
            List<ObjectId> lst = new List<ObjectId>();

            LocalReadWriteLock.AcquireReaderLock(-1);
            foreach (var key in KeyValuePairs.Keys)
            {
                KeyValuePairs[key].LockRead();
                lst.Add(new ObjectId
                {
                    PartitionId = key.Partition_id,
                    ObjectKey = key.Object_id
                });
                KeyValuePairs[key].UnlockRead();
            }
            LocalReadWriteLock.ReleaseReaderLock();

            return new ListGlobalReply
            {
                Keys = { lst }
            };
        }

        private void UpdateCrashedServers(string partition, HashSet<string> crashedServers)
        {
            CrashedServers.Union(crashedServers);

            ServersByPartition[partition].RemoveAll(x => crashedServers.Contains(x));
        }


        /*
         *  ServerSyncService Implementation
         */
        public PropagateWriteResponse PropagateWrite(PropagateWriteRequest request)
        {
            return new PropagateWriteResponse { };
        }

        public HeartbeatResponse Heartbeat()
        {
            return new HeartbeatResponse { };
        }

        public ReportCrashResponse ReportCrash(ReportCrashRequest request)
        {
            return new ReportCrashResponse { };
        }

        // OLD VERSION
        public LockObjectReply LockObject(LockObjectRequest request)
        {
            Console.WriteLine("Received LockObjectRequest with params:");
            Console.Write($"Key: \r\n PartitionId: {request.Key.PartitionId} \r\n ObjectId: {request.Key.ObjectKey}\r\n");

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


            return new LockObjectReply
            {
                Success = true
            };
        }

        public ReleaseObjectLockReply ReleaseObjectLock(ReleaseObjectLockRequest request)
        {
            Console.WriteLine("Received ReleaseObjectLockRequest with params:");
            Console.Write($"Key: \r\n PartitionId: {request.Key.PartitionId} \r\n ObjectId: {request.Key.ObjectKey}\r\n");
            Console.WriteLine("Value: " + request.Value);

            var objectValueManager = KeyValuePairs[new ObjectKey(request.Key)];

            objectValueManager.UnlockWrite(request.Value);

            return new ReleaseObjectLockReply
            {
                Success = true
            };
        }

        public RemoveCrashedServersReply RemoveCrashedServers(RemoveCrashedServersRequest request)
        {
            CrashedServers.Union(request.ServerIds);
            ServersByPartition[request.PartitionId].RemoveAll(x => request.ServerIds.Contains(x));
            return new RemoveCrashedServersReply
            {
                Success = true
            };
        }



        /*
         *  PMCommunicationService Implementation
         */
        public ServerStatusReply Status()
        {
            Console.WriteLine("Online Servers");
            foreach (var server in ServersByPartition)
            {
                Console.Write("Servers ");
                server.Value.ForEach(x => Console.Write(x + " "));
                Console.Write($"from partition {server.Key}\r\n");
            }
            Console.WriteLine("Crashed Servers");
            foreach (var server in CrashedServers)
            {
                Console.WriteLine($"Server {server}");
            }
            return new ServerStatusReply
            {
                Success = true
            };
        }

        public CrashReply Crash()
        {
            Environment.Exit(1);

            return new CrashReply
            {
                Success = false
            };
        }

        public FreezeReply Freeze()
        {
            Console.WriteLine("Received freeze");
            Interceptor.FreezeCommands = true;
            return new FreezeReply
            {
                Success = true
            };
        }

        public UnfreezeReply Unfreeze()
        {
            Console.WriteLine("Received unfreeze");
            Interceptor.FreezeCommands = false;
            return new UnfreezeReply
            {
                Success = true
            };
        }

        public ServerPingReply Ping()
        {
            return new ServerPingReply();
        }

        public PartitionSchemaReply PartitionSchema(PartitionSchemaRequest request)
        {
            Console.WriteLine("Received Partition Schema from pm");
            foreach (var partitionDetails in request.PartitionServers)
            {
                if (!ServersByPartition.ContainsKey(partitionDetails.Key))
                {
                    if (!ServersByPartition.TryAdd(partitionDetails.Key, partitionDetails.Value.ServerIds.ToList()))
                    {
                        throw new RpcException(new Status(StatusCode.Unknown, "Could not add element"));
                    }
                }
            }

            foreach (var serverUrl in request.ServerUrls)
            {
                if (!ServerUrls.ContainsKey(serverUrl.Key))
                {
                    ServerUrls[serverUrl.Key] = serverUrl.Value;
                }
            }

            foreach (var masteredPartition in request.MasteredPartitions.PartitionIds)
            {
                MasteredPartitions.Add(masteredPartition);
            }

            return new PartitionSchemaReply();
        }
    }
}
