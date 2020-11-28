using Grpc.Core;
using Grpc.Core.Interceptors;
using Grpc.Core.Utils;
using Grpc.Net.Client;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Server
{

    public class Database
    {
        // Dictionary with values
        private ConcurrentDictionary<ObjectKey, ObjectValueManager> KeyValuePairs;

        // ReadWriteLock for listMe functions
        private ReaderWriterLock LocalReadWriteLock;

        public Database()
        {
            KeyValuePairs = new ConcurrentDictionary<ObjectKey, ObjectValueManager>();
            LocalReadWriteLock = new ReaderWriterLock();
        }

        public bool TryReadFromKVPair(ObjectKey objectKey, out ObjectValueManager objectValue)
        {
            LocalReadWriteLock.AcquireReaderLock(-1);
            if (!KeyValuePairs.TryGetValue(objectKey, out ObjectValueManager currentObjectValue))
            {
                objectValue = null;
                LocalReadWriteLock.ReleaseReaderLock();
                return false;
            } else
            {
                LocalReadWriteLock.ReleaseReaderLock();
                currentObjectValue.LockRead();
                objectValue = currentObjectValue;
                currentObjectValue.UnlockRead();
                return true;
            }
        }


        public bool TryWriteToKVPair(ObjectKey objectKey, ObjectValueManager objectValue)
        {
            LocalReadWriteLock.AcquireWriterLock(-1);
            if (KeyValuePairs.TryGetValue(objectKey, out var currentObjectValue))
            {
                LocalReadWriteLock.ReleaseWriterLock();
                if (CompareVersion(currentObjectValue.ObjectVersion, objectValue.ObjectVersion) > 0)
                {
                    // Our version is bigger than received one, we don't care
                    return false;
                }
                currentObjectValue.LockWrite();
                currentObjectValue.UnlockWrite(objectValue.Value, objectValue.ObjectVersion);
            }
            else
            {

                KeyValuePairs[objectKey] = objectValue;
                LocalReadWriteLock.ReleaseWriterLock();
            }
            return true;
        }

        public List<KeyValuePair<ObjectKey, ObjectValueManager>> ReadAllDatabase() {
            List<KeyValuePair<ObjectKey, ObjectValueManager>> lst = new List<KeyValuePair<ObjectKey, ObjectValueManager>>();
            LocalReadWriteLock.AcquireReaderLock(-1);

            foreach (var obj in KeyValuePairs)
            {
                obj.Value.LockRead();
                lst.Add(obj);
                obj.Value.UnlockRead();
            }
                
            LocalReadWriteLock.ReleaseReaderLock();
            return lst;

        }

        private int CompareVersion(ObjectVersion ov1, ObjectVersion ov2)
        {
            if (ov1.Counter > ov2.Counter || (ov1.Counter == ov2.Counter && ov1.ClientId > ov2.ClientId))
            {
                return 1;
            }
            else
            {
                return 0;
            }
        }
    }


    public class BoolWrapper
    {
        public object WaitForInformationLock { get; }
        public bool Value { get; set; }
        public BoolWrapper(bool value)
        {
            Value = value;
            WaitForInformationLock = new object();
        }
    }


    public class ObjectMessages
    {
        public ConcurrentDictionary<ObjectKey, PropagationMessage> ObjectMessage = new ConcurrentDictionary<ObjectKey, PropagationMessage>();
    }

    public class ReceivedMessages
    {
        // <partition_id, ObjectMessages>
        public ConcurrentDictionary<string, ObjectMessages> PartitionMessages = new ConcurrentDictionary<string, ObjectMessages>();
    
        public void RemoveObject(ObjectKey objectKey)
        {
            foreach (var partition in PartitionMessages.Keys)
            {
                if (PartitionMessages[partition].ObjectMessage.ContainsKey(objectKey))  
                {
                    PartitionMessages[partition].ObjectMessage.TryRemove(objectKey, out PropagationMessage propagationMessage);
                }
            }
        }
    }

    public class RetransmissionBuffer
    {
        // <sender_replica_id, ReceivedMessages>
        private ConcurrentDictionary<string, ReceivedMessages> ReplicaReceivedMessages = new ConcurrentDictionary<string, ReceivedMessages>();
        
        public void RemoveObjectFromBuffer(ObjectKey objectKey)
        {
            foreach (var replicaId in ReplicaReceivedMessages.Keys)
            {
                ReplicaReceivedMessages[replicaId].RemoveObject(objectKey);
            }
        }

        public void AddNewMessage(string senderReplicaId, PropagationMessage propagationMessage)
        {
            ReplicaReceivedMessages[senderReplicaId].PartitionMessages[propagationMessage.PartitionId].ObjectMessage[new ObjectKey(propagationMessage.ObjectId)] = propagationMessage;
        }
    }


    public class GStoreServer
    {
        private string MyId { get; }

        private Database Database;

        // Dictionary <partition_id, List<URLs>> all servers by partition
        private ConcurrentDictionary<string, List<string>> ServersByPartition;


        private ConcurrentDictionary<string, string> ServerUrls;

        // List of crashed servers
        private ConcurrentBag<string> CrashedServers;

        // ReplicaId that I must watch (by partition)
        private ConcurrentDictionary<string, string> WatchedReplicas;

        private RetransmissionBuffer RetransmissionBuffer;

        // List partition which im master of
        // private List<string> MasteredPartitions;

        private readonly DelayMessagesInterceptor Interceptor;

        private int MessageCounter = 0;

        private readonly object IncrementCounterLock = new object();

        // TODO: REMOVE LATER: USED BY OLD WRITE
        private readonly object WriteGlobalLock = new object();

        public GStoreServer(string myId, DelayMessagesInterceptor interceptor)
        {
            MyId = myId;
            Interceptor = interceptor;
            Database = new Database();
            ServersByPartition = new ConcurrentDictionary<string, List<string>>();
            ServerUrls = new ConcurrentDictionary<string, string>();
            CrashedServers = new ConcurrentBag<string>();
            WatchedReplicas = new ConcurrentDictionary<string, string>();
            RetransmissionBuffer = new RetransmissionBuffer();
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

            if (Database.TryReadFromKVPair(requestedObject, out ObjectValueManager objectValueManager))
            {


                ReadObjectReply reply = new ReadObjectReply
                {
                    Value = objectValueManager.Value,
                    ObjectVersion = objectValueManager.ObjectVersion
                };
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
            Console.WriteLine($"ObjectVersion: <{request.ObjectVersion.Counter}, {request.ObjectVersion.ClientId}>");

            // Compute new Object Version
            ObjectVersion newObjectVersion = GetNewVersion(request.ObjectVersion, new ObjectKey(request.Key));

            ObjectKey receivedObjectKey = new ObjectKey(request.Key);
            if(Database.TryWriteToKVPair(receivedObjectKey, new ObjectValueManager(request.Value, newObjectVersion)))
            {
                // Remove messages refering to same object but older
                RetransmissionBuffer.RemoveObjectFromBuffer(receivedObjectKey);
            }
            
            PropagationMessage propagationMessage = new PropagationMessage
            {
                Id = new PropagationMessageId
                {
                    AuthorReplicaId = MyId,
                    Counter = IncrementMessageCounter()
                },
                PartitionId = receivedObjectKey.Partition_id,
                ObjectId = request.Key,
                ObjectVersion = request.ObjectVersion,
                Value = request.Value
            };

            // Broadcast this message
            ServersByPartition.TryGetValue(receivedObjectKey.Partition_id, out List<string> serverIds);

            BoolWrapper waitForFirstAck = new BoolWrapper(false);

            foreach (var server in ServerUrls.Where(x => serverIds.Contains(x.Key) && x.Key != MyId))
            {
                // Paralelyze
                Task.Run(() =>
                {
                    BroadcastMessage(server.Key, server.Value, waitForFirstAck, propagationMessage);
                });
            }

            // Lock this thread
            lock(waitForFirstAck.WaitForInformationLock)
            {
                while (!waitForFirstAck.Value) Monitor.Wait(waitForFirstAck.WaitForInformationLock);
            }
            return new WriteObjectReply
            {
                NewVersion = newObjectVersion,
                Ok = true
            };

        }

        public ListServerReply ListServer(ListServerRequest request)
        {
            Console.WriteLine("Received ListServer");

            List<ObjectInfo> lst = new List<ObjectInfo>();

            List<KeyValuePair<ObjectKey, ObjectValueManager>> databaseObjects = Database.ReadAllDatabase();

            databaseObjects.ForEach(x => lst.Add(new ObjectInfo
            {
                Key = new ObjectId
                {
                    ObjectKey = x.Key.Object_id,
                    PartitionId = x.Key.Partition_id
                },
                ObjectVersion = x.Value.ObjectVersion,
                Value = x.Value.Value
            }));

            return new ListServerReply
            {
                Objects = { lst }
            };

        }

        public ListGlobalReply ListGlobal(ListGlobalRequest request)
        {
            Console.WriteLine("Received ListGlobal");
            List<ObjectId> lst = new List<ObjectId>();

            List<KeyValuePair<ObjectKey, ObjectValueManager>> databaseObjects = Database.ReadAllDatabase();

            databaseObjects.ForEach(x => lst.Add( new ObjectId
            {
                PartitionId = x.Key.Partition_id,
                ObjectKey = x.Key.Object_id
            }));

            return new ListGlobalReply
            {
                Keys = { lst }
            };
        }


        /*
         *  ServerSyncService Implementation
         */
        public PropagateWriteResponse PropagateWrite(PropagateWriteRequest request)
        {
            Console.WriteLine("Received Propagate Write");
            
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
        //public LockObjectReply LockObject(LockObjectRequest request)
        //{
        //    Console.WriteLine("Received LockObjectRequest with params:");
        //    Console.Write($"Key: \r\n PartitionId: {request.Key.PartitionId} \r\n ObjectId: {request.Key.ObjectKey}\r\n");

        //    if (!KeyValuePairs.TryGetValue(new ObjectKey(request.Key), out ObjectValueManager objectValueManager))
        //    {
        //        LocalReadWriteLock.AcquireWriterLock(-1);
        //        objectValueManager = new ObjectValueManager();
        //        KeyValuePairs[new ObjectKey(request.Key)] = objectValueManager;
        //        objectValueManager.LockWrite();
        //        LocalReadWriteLock.ReleaseWriterLock();
        //    }
        //    else
        //    {
        //        objectValueManager.LockWrite();
        //    }


        //    return new LockObjectReply
        //    {
        //        Success = true
        //    };
        //}

        //public ReleaseObjectLockReply ReleaseObjectLock(ReleaseObjectLockRequest request)
        //{
        //    Console.WriteLine("Received ReleaseObjectLockRequest with params:");
        //    Console.Write($"Key: \r\n PartitionId: {request.Key.PartitionId} \r\n ObjectId: {request.Key.ObjectKey}\r\n");
        //    Console.WriteLine("Value: " + request.Value);

        //    var objectValueManager = KeyValuePairs[new ObjectKey(request.Key)];

        //    objectValueManager.UnlockWrite(request.Value);

        //    return new ReleaseObjectLockReply
        //    {
        //        Success = true
        //    };
        //}

        //public RemoveCrashedServersReply RemoveCrashedServers(RemoveCrashedServersRequest request)
        //{
        //    CrashedServers.Union(request.ServerIds);
        //    ServersByPartition[request.PartitionId].RemoveAll(x => request.ServerIds.Contains(x));
        //    return new RemoveCrashedServersReply
        //    {
        //        Success = true
        //    };
        //}



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
                // if already existed, do nothing
                ServersByPartition.TryAdd(partitionDetails.Key, partitionDetails.Value.ServerIds.ToList());
            }

            foreach (var serverUrl in request.ServerUrls)
            {
                if (!ServerUrls.ContainsKey(serverUrl.Key))
                {
                    ServerUrls[serverUrl.Key] = serverUrl.Value;
                }
            }

            // WE WILL NO LONGER USE PARTITION MASTERS
            //foreach (var masteredPartition in request.MasteredPartitions.PartitionIds)
            //{
            //    MasteredPartitions.Add(masteredPartition);
            //}

            return new PartitionSchemaReply();
        }

        

        private ObjectVersion GetNewVersion(ObjectVersion clientObjectVersion, ObjectKey objectKey) 
        {
            return new ObjectVersion
            {
                Counter = Math.Max(clientObjectVersion.Counter, Database.TryReadFromKVPair(objectKey, out var objectValue) ? objectValue.ObjectVersion.Counter : 0) + 1,
                ClientId = clientObjectVersion.ClientId
            };
        }

        private int IncrementMessageCounter()
        {
            lock(IncrementCounterLock)
            {
                MessageCounter++;
                return MessageCounter;
            }
        }

        private void BroadcastMessage(string serverId, string serverUrl, BoolWrapper waitForFirstAck, PropagationMessage propagationMessage)
        {
            try
            {
                var channel = GrpcChannel.ForAddress(serverUrl);
                var client = new ServerSyncGrpcService.ServerSyncGrpcServiceClient(channel);
                PropagateWriteResponse response = client.PropagateWrite(new PropagateWriteRequest
                {
                    SenderReplicaId = serverId,
                    PropMsg = propagationMessage
                });
                // successful
                lock(waitForFirstAck.WaitForInformationLock)
                {
                    waitForFirstAck.Value = true;
                    Monitor.PulseAll(waitForFirstAck.WaitForInformationLock);
                }
            }
            catch (RpcException e)
            {
                if (e.Status.StatusCode == StatusCode.DeadlineExceeded || e.Status.StatusCode == StatusCode.Unavailable || e.Status.StatusCode == StatusCode.Internal)
                {
                    // Trigger crash event for the server that crashed
                    
                }
                else
                {
                    throw e;
                }
            }
        }

    }
}
