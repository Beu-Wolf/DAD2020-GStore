using Grpc.Core;
using Grpc.Core.Interceptors;
using Grpc.Core.Utils;
using Grpc.Net.Client;
using System;
using System.Timers;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Collections;

namespace Server
{

    public class Database
    {
        // Dictionary with values
        private ConcurrentDictionary<ObjectKey, ObjectInfo> KeyValuePairs;

        // Dictionary with lock for each object
        private ConcurrentDictionary<ObjectKey, object> ObjectLocks;

        public Database()
        {
            KeyValuePairs = new ConcurrentDictionary<ObjectKey, ObjectInfo>(new ObjectKey.ObjectKeyComparer());
            ObjectLocks = new ConcurrentDictionary<ObjectKey, object>(new ObjectKey.ObjectKeyComparer());
        }

        public bool TryGetValue(ObjectKey objectKey, out ObjectInfo objectValue)
        {
            return KeyValuePairs.TryGetValue(objectKey, out objectValue);
        }


        public bool TryReplicaWrite(ObjectKey objectKey, ObjectInfo objectValue)
        {
            
            object objectLock = ObjectLocks.GetOrAdd(objectKey, new object());
            lock(objectLock)
            {
                if (KeyValuePairs.TryGetValue(objectKey, out var currentObjectValue))
                {
                    if (CompareVersion(currentObjectValue.Version, objectValue.Version) > 0)
                    {
                        // Our version is bigger than received one, we don't care
                        return false;
                    }
                }
                KeyValuePairs[objectKey] = objectValue;

                return true;
            }
        }

        public ObjectVersion TryClientWrite(ObjectKey objectKey, ObjectInfo objectValue)
        {
            object objectLock = ObjectLocks.GetOrAdd(objectKey, new object());
            lock (objectLock)
            {
                ObjectVersion newObjectVersion = GetNewVersion(objectValue.Version, KeyValuePairs.TryGetValue(objectKey, out var objectInfo) ? objectInfo.Version : null);
                objectValue.Version = newObjectVersion;
                KeyValuePairs[objectKey] = objectValue;
                return newObjectVersion;
            }
        }

        public List<KeyValuePair<ObjectKey, ObjectInfo>> ReadAllDatabase() {
            List<KeyValuePair<ObjectKey, ObjectInfo>> lst = new List<KeyValuePair<ObjectKey, ObjectInfo>>();

            foreach (var obj in KeyValuePairs)
            {
                lst.Add(obj);
            }
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

        private ObjectVersion GetNewVersion(ObjectVersion clientObjectVersion, ObjectVersion storedVersion)
        {
            Console.WriteLine("Stored version: " + storedVersion);
            return new ObjectVersion
            {
                Counter = Math.Max(clientObjectVersion.Counter, storedVersion != null ? storedVersion.Counter : 0) + 1,
                ClientId = clientObjectVersion.ClientId
            };
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


    public class RetransmissionBuffer
    {
        // <sender_replica_id, ReceivedMessages>
        private ConcurrentDictionary<string, ReceivedMessages> ReplicaReceivedMessages = new ConcurrentDictionary<string, ReceivedMessages>();

        internal object OperationLock = new object();

        internal class ReceivedMessages
        {
            // <partition_id, ObjectMessages>
            internal ConcurrentDictionary<string, ObjectMessages> PartitionMessages = new ConcurrentDictionary<string, ObjectMessages>();

            internal ReceivedMessages(string partitionId, PropagationMessage propagationMessage)
            {
                PartitionMessages[partitionId] = new ObjectMessages(propagationMessage);
            }

            internal void RemoveObject(ObjectKey objectKey)
            {
                
                foreach (var partition in PartitionMessages.Keys)
                {
                    PartitionMessages[partition].MessagesByPartition.Remove(objectKey, out PropagationMessage propagationMessage);     
                }
            }
        }

        internal class ObjectMessages
        {
            internal ConcurrentDictionary<ObjectKey, PropagationMessage> MessagesByPartition = new ConcurrentDictionary<ObjectKey, PropagationMessage>(new ObjectKey.ObjectKeyComparer());
            
            internal ObjectMessages(PropagationMessage propagationMessage)
            {
                MessagesByPartition[new ObjectKey(propagationMessage.ObjectId)] = propagationMessage;
            }
        
        }

        public List<PropagationMessage> GetReplicaPropMessages(string replicaId)
        {
            lock(OperationLock)
            {
                List<PropagationMessage> lst = new List<PropagationMessage>();
                if (ReplicaReceivedMessages.ContainsKey(replicaId))
                    ReplicaReceivedMessages[replicaId].PartitionMessages.Keys.ToList().ForEach(x => lst.AddRange(ReplicaReceivedMessages[replicaId].PartitionMessages[x].MessagesByPartition.Values));
                return lst;
            }
        }

        public void RemoveObjectFromBuffer(ObjectKey objectKey)
        {
            lock(OperationLock)
            {
                foreach (var replicaId in ReplicaReceivedMessages.Keys)
                {
                    ReplicaReceivedMessages[replicaId].RemoveObject(objectKey);
                }
            }
           
        }

        public void AddNewMessage(string senderReplicaId, PropagationMessage propagationMessage)
        {
            lock(OperationLock)
            {
                if(!ReplicaReceivedMessages.ContainsKey(senderReplicaId))
                {
                    ReplicaReceivedMessages[senderReplicaId] = new ReceivedMessages(propagationMessage.PartitionId, propagationMessage);
                } else if (!ReplicaReceivedMessages[senderReplicaId].PartitionMessages.ContainsKey(propagationMessage.PartitionId))
                {
                    ReplicaReceivedMessages[senderReplicaId].PartitionMessages[propagationMessage.PartitionId] = new ObjectMessages(propagationMessage);
                } else
                {
                    ReplicaReceivedMessages[senderReplicaId].PartitionMessages[propagationMessage.PartitionId].MessagesByPartition[new ObjectKey(propagationMessage.ObjectId)] = propagationMessage;
                }
            }

        }

        public void RemoveReplica(string deadReplicaId)
        {
            ReplicaReceivedMessages.Remove(deadReplicaId, out ReceivedMessages receivedMessages);
        }
    }


    public class GStoreServer
    {
        private string MyId { get; }

        private Database Database;

        // Dictionary <partition_id, List<URLs>> all servers by partition
        private ConcurrentDictionary<string, SynchronizedCollection<string>> ServersByPartition;

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

        private static System.Timers.Timer HeartbeatTimer;


        public GStoreServer(string myId, DelayMessagesInterceptor interceptor)
        {
            MyId = myId;
            Interceptor = interceptor;
            Database = new Database();
            ServersByPartition = new ConcurrentDictionary<string, SynchronizedCollection<string>>();
            ServerUrls = new ConcurrentDictionary<string, string>();
            CrashedServers = new ConcurrentBag<string>();
            WatchedReplicas = new ConcurrentDictionary<string, string>();
            RetransmissionBuffer = new RetransmissionBuffer();

            SetHeartbeatTimer();
        }

        private void SetHeartbeatTimer()
        {
            HeartbeatTimer = new System.Timers.Timer(5000);
            HeartbeatTimer.Elapsed += HeartbeatEvent;
            HeartbeatTimer.AutoReset = true;
            HeartbeatTimer.Enabled = true;
        }



        /*
         *  GStoreService Implementation
         */
        public ReadObjectReply Read(ReadObjectRequest request)
        {
            Console.WriteLine($"[READ] Received Read with Key <{request.Key.PartitionId},{request.Key.ObjectKey}>");

            var requestedObject = new ObjectKey(request.Key);

            if (Database.TryGetValue(requestedObject, out ObjectInfo objectInfo))
            {
                ReadObjectReply reply = new ReadObjectReply
                {
                    Object = objectInfo
                };
                Console.WriteLine($"[READ] Success!");

                return reply;
            }
            else
            {
                throw new RpcException(new Status(StatusCode.NotFound, $"Object <{request.Key.PartitionId}, {request.Key.ObjectKey}> not found here"));
            }
        }

        public WriteObjectReply Write(WriteObjectRequest request)
        {
            Console.WriteLine("[WRITE] Received write with params:");
            Console.WriteLine($"[WRITE] Partition_id: {request.Object.Key.PartitionId}");
            Console.WriteLine($"[WRITE] Object_id: {request.Object.Key.ObjectKey}");
            Console.WriteLine($"[WRITE] Value: {request.Object.Value}");
            Console.WriteLine($"[WRITE] ObjectVersion: <{request.Object.Version.Counter}, {request.Object.Version.ClientId}>");

            

            ObjectKey receivedObjectKey = new ObjectKey(request.Object.Key);
            ObjectVersion newObjectVersion = Database.TryClientWrite(receivedObjectKey, request.Object);
            Console.WriteLine($"[WRITE] Wrote to database");
            
            // Remove messages refering to same object but older
            RetransmissionBuffer.RemoveObjectFromBuffer(receivedObjectKey);
            
            PropagationMessage propagationMessage = new PropagationMessage
            {
                Id = new PropagationMessageId
                {
                    AuthorReplicaId = MyId,
                    Counter = IncrementMessageCounter()
                },
                PartitionId = receivedObjectKey.Partition_id,
                ObjectId = request.Object.Key,
                ObjectVersion = request.Object.Version,
                Value = request.Object.Value
            };
            
            BoolWrapper waitForFirstAck = new BoolWrapper(false);

            
            // Paralelyze
            Task.Run(() =>
            {
                BroadcastMessage(waitForFirstAck, propagationMessage.PartitionId, propagationMessage);
            });

            Console.WriteLine($"[WRITE] Waiting for first ack");
            // Lock this thread
            lock (waitForFirstAck.WaitForInformationLock)
            {
                while (!waitForFirstAck.Value) Monitor.Wait(waitForFirstAck.WaitForInformationLock);
            }
            
            
            return new WriteObjectReply
            {
                NewVersion = newObjectVersion
            };

        }

        public ListServerReply ListServer(ListServerRequest request)
        {
            Console.WriteLine("Received ListServer");

            List<ObjectInfo> lst = new List<ObjectInfo>();

            List<KeyValuePair<ObjectKey, ObjectInfo>> databaseObjects = Database.ReadAllDatabase();

            databaseObjects.ForEach(x => lst.Add(x.Value));

            return new ListServerReply
            {
                Objects = { lst }
            };

        }

        public ListGlobalReply ListGlobal(ListGlobalRequest request)
        {
            Console.WriteLine("Received ListGlobal");
            List<ObjectInfo> lst = new List<ObjectInfo>();

            List<KeyValuePair<ObjectKey, ObjectInfo>> databaseObjects = Database.ReadAllDatabase();

            databaseObjects.ForEach(x => lst.Add(x.Value));

            return new ListGlobalReply
            {
                Objects = { lst }
            };
        }


        /*
         *  ServerSyncService Implementation
         */

        public void HeartbeatEvent(Object source, ElapsedEventArgs eventArgs)
        {
            foreach (var watchedReplica in WatchedReplicas)
            {
                Task.Run(() =>
                {
                    Console.WriteLine("Sending heartbeat to " + watchedReplica.Value);
                    try
                    {
                        var channel = GrpcChannel.ForAddress(ServerUrls[watchedReplica.Value]);
                        var client = new ServerSyncGrpcService.ServerSyncGrpcServiceClient(channel);

                        client.Heartbeat(new HeartbeatRequest());
                        Console.WriteLine("Received Hearbeat of " + watchedReplica.Value);
                    }
                    catch (RpcException exception)
                    {
                        if (exception.Status.StatusCode == StatusCode.Unavailable || exception.Status.StatusCode == StatusCode.Internal)
                        {
                            PropagateCrash(watchedReplica.Key, watchedReplica.Value);
                        }
                        else if(exception.Status.StatusCode == StatusCode.DeadlineExceeded)
                        {
                            Console.WriteLine($"Server {watchedReplica.Value} timed out...");
                        }
                        else
                        {
                            throw exception;
                        }
                    }
                });
            }
        }

        public PropagateWriteResponse PropagateWrite(PropagateWriteRequest request)
        {
            Console.WriteLine($"Received Propagate Write message <{request.PropMsg.Id.AuthorReplicaId}, {request.PropMsg.Id.AuthorReplicaId}> from {request.SenderReplicaId}");
            ObjectKey receivedObjectKey = new ObjectKey(request.PropMsg.ObjectId);
            ObjectInfo receivedObjectInfo = new ObjectInfo
            {
                Key = request.PropMsg.ObjectId,
                Version = request.PropMsg.ObjectVersion,
                Value = request.PropMsg.Value
            };
            if (Database.TryReplicaWrite(receivedObjectKey, receivedObjectInfo))
            {
                // Remove messages refering to same object but older
                RetransmissionBuffer.RemoveObjectFromBuffer(receivedObjectKey);
                // add to retransmission buffers
                RetransmissionBuffer.AddNewMessage(request.SenderReplicaId, request.PropMsg);
            }

           
            return new PropagateWriteResponse { };
        }

        public HeartbeatResponse Heartbeat()
        {
            Console.WriteLine("Heartbeat Received");
            return new HeartbeatResponse { };
        }

        public void PropagateCrash(string partitionId, string deadReplicaId)
        {
            Console.WriteLine("Propagating crash of " + deadReplicaId);
            RemoveDeadReplica(deadReplicaId);

            // Tell other replicas of partition some replica died
            foreach (var serverToSend in ServersByPartition[partitionId].Where(x => !x.Equals(MyId)))
            {
                var channel = GrpcChannel.ForAddress(ServerUrls[serverToSend]);
                var server = new ServerSyncGrpcService.ServerSyncGrpcServiceClient(channel);
                server.ReportCrash(new ReportCrashRequest
                {
                    DeadReplicaId = deadReplicaId
                });
            }

            // broadcast buffered replica messages
            // Do we need it to be parallel?
            var propMessages = RetransmissionBuffer.GetReplicaPropMessages(deadReplicaId);
            propMessages.ForEach(x =>
            {
                BroadcastMessage(new BoolWrapper(false), x.PartitionId, x);
            });

            // Remove buffered replica messages
            RetransmissionBuffer.RemoveReplica(deadReplicaId);

            UpdateWatchedReplicas(deadReplicaId);
        }

        public ReportCrashResponse ReportCrash(ReportCrashRequest request)
        {
            Console.WriteLine("Received crash report of replica " + request.DeadReplicaId);
            RemoveDeadReplica(request.DeadReplicaId);

            // Remove buffered replica messages
            RetransmissionBuffer.RemoveReplica(request.DeadReplicaId);
            UpdateWatchedReplicas(request.DeadReplicaId);

            return new ReportCrashResponse { };
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
                server.Value.ToList().ForEach(x => Console.Write(x + " "));
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
                var list = new SynchronizedCollection<string>();
                partitionDetails.Value.ServerIds.ToList().ForEach(x => list.Add(x));
                if (ServersByPartition.TryAdd(partitionDetails.Key, list))
                {
                    // If we added a new partition, compute which server to check in heartbeat messages only if I belong to that partition
                    if (partitionDetails.Value.ServerIds.ToList().Contains(MyId))
                        SetWatchedReplica(partitionDetails.Key, list);
                }
            }

            foreach (var serverUrl in request.ServerUrls)
            {
                if (!ServerUrls.ContainsKey(serverUrl.Key))
                {
                    ServerUrls[serverUrl.Key] = serverUrl.Value;
                }
            }
            

            return new PartitionSchemaReply();
        }

        
        private int IncrementMessageCounter()
        {
            lock(IncrementCounterLock)
            {
                MessageCounter++;
                return MessageCounter;
            }
        }

        private void BroadcastMessage(BoolWrapper waitForFirstAck, string partitionId, PropagationMessage propagationMessage)
        {
            List<string> sentServerIds = new List<string>();

            int count = 0;
            do
            {
                foreach (var server in ServerUrls.Where(x => ServersByPartition[partitionId].Contains(x.Key) && x.Key != MyId && !sentServerIds.Contains(x.Key)))
                {
                    Console.WriteLine($"[BROADCAST] Trying to send to server {server.Key}");
                    if(SendWrite(server.Key, server.Value, propagationMessage))
                    {
                        sentServerIds.Add(server.Key);
                        if (count == 0)
                        {
                            lock (waitForFirstAck.WaitForInformationLock)
                            {
                                waitForFirstAck.Value = true;
                                Monitor.PulseAll(waitForFirstAck.WaitForInformationLock);
                            }
                        }

                        count++;
                    }
                }
            } while (!ServersByPartition[partitionId].Where(x => x!= MyId).All(x=>sentServerIds.Contains(x)));
            // if count == 0 here, it means the foreach did not execute because we are the only server up in this partition
            if (count == 0)
            {
                lock (waitForFirstAck.WaitForInformationLock)
                {
                    waitForFirstAck.Value = true;
                    Monitor.PulseAll(waitForFirstAck.WaitForInformationLock);
                }
            }
            
        }

        private bool SendWrite(string serverId, string serverUrl, PropagationMessage propagationMessage)
        {
            try
            {
                var channel = GrpcChannel.ForAddress(serverUrl);
                var client = new ServerSyncGrpcService.ServerSyncGrpcServiceClient(channel);
                PropagateWriteResponse response = client.PropagateWrite(new PropagateWriteRequest
                {
                    SenderReplicaId = serverId,
                    PropMsg = propagationMessage
                }, new CallOptions(deadline:DateTime.UtcNow.AddSeconds(2)));
                // successful
                return true;
            }
            catch (RpcException e)
            {
                if (e.Status.StatusCode == StatusCode.DeadlineExceeded) {
                    Console.WriteLine("[PROPAGATE] Server timed out");
                }
                else if (e.Status.StatusCode == StatusCode.Unavailable || e.Status.StatusCode == StatusCode.Internal)
                {
                    Task.Run(() =>
                    {
                       PropagateCrash(propagationMessage.PartitionId, serverId); // TODO: Discuss we maybe don't want to propagate here           
                    });
                }
                else
                {
                    throw e;
                }
            }
            return false;
        }

        private void UpdateWatchedReplicas(string deadReplicaId)
        {
            // Update watched replicas
            foreach (var watchedPartition in WatchedReplicas.Keys)
            {
                if (WatchedReplicas[watchedPartition].Equals(deadReplicaId))
                {
                    SetWatchedReplica(watchedPartition, ServersByPartition[watchedPartition]);
                }
            }
        }

        private void SetWatchedReplica(string partitionId, SynchronizedCollection<string> serverIds)
        {
            if(serverIds.Count > 1)
            {
                // Only makes sense to watch for replicas in partitions with more than 1 server
                serverIds.OrderBy(x => x);
                WatchedReplicas[partitionId] = serverIds[(serverIds.IndexOf(MyId) + 1) % serverIds.Count];
            } else
            {
                WatchedReplicas.TryRemove(partitionId, out string _); // we dont need to watch for ourselves
            }
        }

        private void RemoveDeadReplica(string deadReplicaId)
        {
            // Remove replica from correct replicas
            CrashedServers.Add(deadReplicaId);
            foreach (var serversByPartition in ServersByPartition)
            {
                serversByPartition.Value.Remove(deadReplicaId);
            }         
        }
    }
}
