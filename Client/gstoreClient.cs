using System;
using System.Threading;
using System.Collections.Generic;
using System.Collections.Concurrent;
using Grpc.Net.Client;
using Grpc.Core;
using System.Linq;
using System.Diagnostics;

namespace Client
{
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


    public class GStoreClient
    {

        private GrpcChannel Channel { get; set; }
        private ClientServerGrpcService.ClientServerGrpcServiceClient ConnectedServer;
        BoolWrapper ContinueExecution;

        private readonly ConcurrentDictionary<string, List<string>> ServersIdByPartition;
        private readonly ConcurrentDictionary<string, string> ServerUrls;
        private readonly ConcurrentBag<string> CrashedServers;

        private readonly Cache ObjectCache;
        private string currentServerId = "-1";
        private int Id;


        private object readCounterLock = new object();
        private long ReadTotalTime = 0;
        private long NumReads = 0;
        private long NumReadFails = 0;

        private object writeCounterLock = new object();
        private long WriteTotalTime = 0;
        private long NumWrites = 0;
        private long NumWriteFails = 0;

        public GStoreClient(int id)
        {
            Id = id;
            ContinueExecution = new BoolWrapper(false);
            ServersIdByPartition = new ConcurrentDictionary<string, List<string>>();
            ServerUrls = new ConcurrentDictionary<string, string>();
            CrashedServers = new ConcurrentBag<string>();
            ObjectCache = new Cache();
        }

        public void PrintTimes()
        {
            Console.WriteLine("Finished executing.");
            float avgRead = 0;
            float avgWrite = 0;

            if(NumReads > 0)
            {
                avgRead = ReadTotalTime / NumReads;
            }

            if(NumWrites > 0)
            {
                avgWrite = WriteTotalTime / NumWrites;
            }

            Console.WriteLine($"READ: {ReadTotalTime} / {NumReads} => {avgRead} ({NumReadFails} failed)");
            Console.WriteLine($"WRIT: {WriteTotalTime} / {NumWrites} => {avgWrite} ({NumWriteFails} failed)");
        }

        private bool TryConnectToServer(string server_id)
        {
            if(server_id == currentServerId)
            { // already connected to this server
                return true;
            }
            
            try
            {
                currentServerId = server_id;
                Channel = GrpcChannel.ForAddress(ServerUrls[server_id]);
                ConnectedServer = new ClientServerGrpcService.ClientServerGrpcServiceClient(Channel);
                Console.WriteLine($"[+] Connected to server {server_id}");
                return true;
            }
            catch (Exception)
            {
                Console.WriteLine($"[-] Failed to connect to server {server_id}");
                HandleCrashedServer(server_id);
                return false;
            }
        }

        private bool TryConnectToPartition(string partition_id)
        {
            Console.WriteLine($"[*] Trying to connect to partition {partition_id}");
            
            if(!ServersIdByPartition.ContainsKey(partition_id))
            {
                Console.WriteLine($"[-] Partition {partition_id} does not exist");
                return false;
            }

            // connect to a random partition server
            Random rnd = new Random();
            foreach (var serverId in ServersIdByPartition[partition_id].OrderBy(x => rnd.Next()))
            {
                if(TryConnectToServer(serverId))
                {
                    return true;
                }
            }

            Console.WriteLine($"[-] Could not connect to partition {partition_id}");
            return false;
        }

        public void ReadObject(string partition_id, string object_id, string server_id)
        {
            Console.WriteLine($"[READ] Requesting read: <{partition_id},{object_id}>");

            if (!ServersIdByPartition[partition_id].Contains(currentServerId))
            { // current server does not belong to the asked partition!
                Console.WriteLine($"[READ] Current server does not belong to partition {partition_id}");
                if (server_id == string.Empty || !TryConnectToServer(server_id))
                {
                    if (!TryConnectToPartition(partition_id))
                    {
                        return;
                    }
                }
            }

            ReadObjectRequest request = new ReadObjectRequest
            {
                Key = new ObjectId
                {
                    PartitionId = partition_id,
                    ObjectKey = object_id
                }
            };

            Stopwatch sw = new Stopwatch();
            sw.Start();
            ReadObjectReply reply;
            try
            {
                reply = ConnectedServer.ReadObject(request);
                sw.Stop();

                lock (readCounterLock)
                {
                    ReadTotalTime += sw.ElapsedMilliseconds;
                    NumReads += 1;
                }
            }
            catch (RpcException e)
            {
                // If error is because Server failed, update list of crashed Servers
                if (e.Status.StatusCode == StatusCode.Unavailable || e.Status.StatusCode == StatusCode.Internal)
                {
                    HandleCrashedServer(currentServerId);
                }

                // TODO: non-existing objects will generate an exception
                Console.WriteLine($"[READ] Error: {e.Status.StatusCode}");
                Console.WriteLine($"[READ] Error message: {e.Status.Detail}");
                Console.WriteLine("[READ] N/A");
                sw.Stop();

                lock(readCounterLock)
                {
                    ReadTotalTime += sw.ElapsedMilliseconds;
                    NumReadFails += 1;
                    NumReads += 1;
                }

                return;
            }

            Console.WriteLine("Received: " + reply.Object);
            if(!ObjectCache.RegisterObject(reply.Object))
            {
                // Got older read. Do something if you want a newer one
                Console.WriteLine("Object is outdated");
            }
        }

        public void WriteObject(string partition_id, string object_id, string value)
        {
            Console.WriteLine("[WRITE]");
            List<string> ServersOfPartition = ServersIdByPartition[partition_id];
            if (!ServersOfPartition.Contains(currentServerId)) 
            {
                if(!TryConnectToPartition(partition_id))
                {
                    Console.WriteLine($"[WRITE] No available servers for partition {partition_id}");
                    return;
                }
            }

            ObjectId objKey = new ObjectId
            {
                PartitionId = partition_id,
                ObjectKey = object_id
            };

            WriteObjectRequest request = new WriteObjectRequest
            {
                Object = new ObjectInfo
                {
                    Key = objKey,
                    Value = value,
                    Version = new ObjectVersion
                    {
                        ClientId = Id,
                        Counter = ObjectCache.GetObjectCounter(objKey),
                    },
                }
            };
            bool success = false;
            Stopwatch sw = new Stopwatch();
            sw.Start();
            do
            {
                try
                {
                    var reply = ConnectedServer.WriteObject(request);
                    var newVersion = reply.NewVersion;
                    
                    // update cached object
                    ObjectCache.RegisterObject(new ObjectInfo
                    {
                        Key = objKey,
                        Version = newVersion,
                        Value = value
                    });

                    Console.WriteLine($"[WRITE] New version: <{newVersion.Counter},{newVersion.ClientId}>");
                    success = true;
                } catch (RpcException e)
                {
                    if (e.Status.StatusCode == StatusCode.Unavailable || e.Status.StatusCode == StatusCode.Internal)
                    {
                        // remove crashed server so we don't pick it again
                        HandleCrashedServer(currentServerId);
                    }
                    else
                    {
                        sw.Stop();
                        lock (writeCounterLock)
                        {
                            WriteTotalTime += sw.ElapsedMilliseconds;
                            NumWrites += 1;
                        }
                        throw e;
                    }
                }
            } while (!success && TryConnectToPartition(partition_id));
            sw.Stop();
            lock(writeCounterLock)
            {
                WriteTotalTime += sw.ElapsedMilliseconds;
                NumWrites += 1;
            }

            if(!success)
            {
                lock(writeCounterLock)
                {
                    NumWriteFails += 1;
                }
                Console.WriteLine("[WRITE] Failed to write value. Every server was down.");
            }
        }

        public void ListServer(string server_id)
        {
            if (!TryConnectToServer(server_id)) {
                return;
            }

            try
            {
                ListServerRequest request = new ListServerRequest();
                var reply = ConnectedServer.ListServer(request);

                Console.WriteLine("[LIST SERVER] Received from server: " + server_id);
                foreach (var obj in reply.Objects)
                {
                    Console.WriteLine($"[LIST SERVER]  -> {{ Key: <{obj.Key.PartitionId}, {obj.Key.ObjectKey}>, Value: {obj.Value}, Version: <{obj.Version.Counter},{obj.Version.ClientId}> }}");
                }
            }
            catch (RpcException e)
            {
                if (e.Status.StatusCode == StatusCode.Unavailable || e.Status.StatusCode == StatusCode.Internal)
                {
                    Console.WriteLine(e.Status);
                    HandleCrashedServer(server_id);
                }
                else
                {
                    throw e;
                }
            }


        }

        public void ListGlobal()
        {
            Console.WriteLine("[LIST GLOBAL]");
            foreach (var serverId in ServerUrls.Keys)
            {
                TryConnectToServer(serverId);

                try
                {
                    ListGlobalRequest request = new ListGlobalRequest();
                    var reply = ConnectedServer.ListGlobal(request);
                    Console.WriteLine($"[LIST GLOBAL] Received from {serverId}:");
                    foreach (var objectInfo in reply.Objects)
                    {
                        Console.WriteLine($"[LIST GLOBAL]  -> object <{objectInfo.Key.PartitionId}, {objectInfo.Key.ObjectKey}> with version <{objectInfo.Version.ClientId}, {objectInfo.Version.Counter}> and value {objectInfo.Value}");
                    }
                }
                catch (RpcException e)
                {
                    if (e.Status.StatusCode == StatusCode.Unavailable || e.Status.StatusCode == StatusCode.DeadlineExceeded || e.Status.StatusCode == StatusCode.Internal)
                    {
                        HandleCrashedServer(serverId);
                    }
                    else
                    {
                        throw e;
                    }
                }
            }
        }


        private void HandleCrashedServer(string server_id)
        {
            Console.WriteLine($"[-] Server {server_id} is down. Removing from local network");
            CrashedServers.Add(server_id);
            ServerUrls.Remove(server_id, out var _);
            foreach (var partition in ServersIdByPartition.Values)
            {
                // if item doesn't exist, Remove returns false
                partition.Remove(server_id);
            }
        }

        public void WaitForNetworkInformation()
        {
            Console.WriteLine("[*] Waiting for network information...");
            lock (ContinueExecution.WaitForInformationLock)
            {
                while (!ContinueExecution.Value) Monitor.Wait(ContinueExecution.WaitForInformationLock);
            }
        }


        /*
         * PM Communication Service Implementation
         */
        public ClientStatusReply Status()
        {
            Console.WriteLine("[STATUS]");
            Console.WriteLine("[STATUS] Available Servers:");
            foreach (var partition in ServersIdByPartition)
            {
                Console.WriteLine($"[STATUS]  -> Partition ${partition.Key}: [{string.Join(", ", partition.Value)}]");
            }
            Console.WriteLine("[STATUS] Crashed Servers:");
            foreach (var server in CrashedServers)
            {
                Console.WriteLine($"[STATUS]  -> {server}");
            }
            return new ClientStatusReply();
        }

        public NetworkInformationReply NetworkInformation(NetworkInformationRequest request)
        {
            Console.WriteLine("[NETWORK INFO]");
            Console.WriteLine("[NETWORK INFO] Received Servers:");
            foreach (var serverUrl in request.ServerUrls)
            {
                if (!ServerUrls.ContainsKey(serverUrl.Key))
                {
                    Console.WriteLine($"[NETWORK INFO]  -> Server {serverUrl.Key} at {serverUrl.Value}");
                    ServerUrls[serverUrl.Key] = serverUrl.Value;
                }
            }

            Console.WriteLine("[NETWORK INFO] Received Partitions:");
            foreach (var partition in request.ServerIdsByPartition)
            {
                if (ServersIdByPartition.ContainsKey(partition.Key))
                {
                    continue;
                }
                
                if (!ServersIdByPartition.TryAdd(partition.Key, partition.Value.ServerIds.ToList()))
                {
                    throw new RpcException(new Status(StatusCode.Unknown, "Could not add element"));
                }

                Console.WriteLine($"[NETWORK INFO]  -> Partition ${partition.Key}: [{string.Join(", ", partition.Value.ServerIds)}]");
            }
            lock (ContinueExecution.WaitForInformationLock)
            {
                ContinueExecution.Value = true;
                Monitor.PulseAll(ContinueExecution.WaitForInformationLock);
            }
            return new NetworkInformationReply();
        }

        public ClientPingReply Ping()
        {
            Console.WriteLine("[PING]");
            return new ClientPingReply();
        }
    }
}
