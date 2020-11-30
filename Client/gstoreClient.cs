using System;
using System.Threading;
using System.Collections.Generic;
using System.Collections.Concurrent;
using Grpc.Net.Client;
using Grpc.Core;
using System.Linq;

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


        public GStoreClient(int id)
        {
            Id = id;
            ContinueExecution = new BoolWrapper(false);
            ServersIdByPartition = new ConcurrentDictionary<string, List<string>>();
            ServerUrls = new ConcurrentDictionary<string, string>();
            CrashedServers = new ConcurrentBag<string>();
            ObjectCache = new Cache();
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
                Console.WriteLine($"Connected to server {server_id}");
                return true;
            }
            catch (Exception)
            {
                Console.WriteLine($"Failed to connect to server {server_id}");
                HandleCrashedServer(server_id);
                return false;
            }
        }

        private bool TryConnectToPartition(string partition_id)
        {
            if(!ServersIdByPartition.ContainsKey(partition_id))
            {
                Console.WriteLine($"Partition {partition_id} does not exist");
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

            return false;
        }

        public void ReadObject(string partition_id, string object_id, string server_id)
        {
            Console.WriteLine($"[READ] Requesting read: <{partition_id},{object_id}>");
            if (server_id != string.Empty)
            { // we have to connect to a specific server
                
                if (!ServersIdByPartition[partition_id].Contains(currentServerId))
                { // specified server does not belong to the asked partition!
                    Console.WriteLine($"[READ] Specified server does not belong to partition {partition_id}");
                    return;
                }

                if(!TryConnectToServer(server_id))
                {
                    Console.WriteLine($"[READ] Could not connect to server {server_id}");
                    return;
                }
            } else
            {
                if(!TryConnectToPartition(partition_id))
                {
                    Console.WriteLine($"[READ] Could not connect to any server from partition {partition_id}");
                    return;
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

            ReadObjectReply reply;
            try
            {
                reply = ConnectedServer.ReadObject(request);
            }
            catch (RpcException e)
            {
                // If error is because Server failed, update list of crashed Servers
                if (e.Status.StatusCode == StatusCode.Unavailable || e.Status.StatusCode == StatusCode.DeadlineExceeded || e.Status.StatusCode == StatusCode.Internal)
                {
                    HandleCrashedServer(currentServerId);
                }

                // TODO: non-existing objects will generate an exception
                Console.WriteLine($"[READ] Error: {e.Status.StatusCode}");
                Console.WriteLine($"[READ] Error message: {e.Status.Detail}");
                Console.WriteLine("[READ] N/A");
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
            List<string> ServersOfPartition = ServersIdByPartition[partition_id];
            if (!ServersOfPartition.Contains(currentServerId)) 
            {
                if(!TryConnectToPartition(partition_id))
                {
                    Console.WriteLine($"No available servers for partition {partition_id}");
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

                    Console.WriteLine("Received: " + newVersion);
                    success = true;
                } catch (RpcException e)
                {
                    if (e.Status.StatusCode == StatusCode.Unavailable || e.Status.StatusCode == StatusCode.DeadlineExceeded || e.Status.StatusCode == StatusCode.Internal)
                    {
                        Console.WriteLine($"Server {currentServerId} is down");
                        // remove crashed server so we don't pick it again
                        HandleCrashedServer(currentServerId);
                    }
                    else
                    {
                        throw e;
                    }
                }
            } while (!success && TryConnectToPartition(partition_id));

            if(!success)
            {
                Console.WriteLine("Failed to write value. Every server was down.");
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
                Console.WriteLine("Received from server: " + server_id);
                foreach (var obj in reply.Objects)
                {
                    Console.WriteLine($"object <{obj.Key.PartitionId}, {obj.Key.ObjectKey}>");
                }
            }
            catch (RpcException e)
            {
                if (e.Status.StatusCode == StatusCode.Unavailable || e.Status.StatusCode == StatusCode.DeadlineExceeded || e.Status.StatusCode == StatusCode.Internal)
                {
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
            foreach (var serverId in ServerUrls.Keys)
            {
                TryConnectToServer(serverId);

                try
                {
                    ListGlobalRequest request = new ListGlobalRequest();
                    var reply = ConnectedServer.ListGlobal(request);
                    Console.WriteLine("Received from " + serverId);
                    foreach (var key in reply.Keys)
                    {
                        Console.WriteLine($"object <{key.PartitionId}, {key.ObjectKey}>");
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
            Console.WriteLine($"Reporting crashed server: {server_id}");
            CrashedServers.Add(server_id);
            foreach (var partition in ServersIdByPartition.Values)
            {
                // if item doesn't exist, Remove returns false
                partition.Remove(server_id);
            }
        }

        public void WaitForNetworkInformation()
        {
            lock (ContinueExecution.WaitForInformationLock)
            {
                while (!ContinueExecution.Value) Monitor.Wait(ContinueExecution.WaitForInformationLock);
            }
        }


        /*
         * gRPC Services
         */
        public ClientStatusReply Status()
        {
            Console.WriteLine("Online Servers:");
            foreach (var server in ServersIdByPartition)
            {
                Console.Write("Server ");
                server.Value.ForEach(x => Console.Write(x + " "));
                Console.Write("from partition " + server.Key + "\r\n");
            }
            Console.WriteLine("Crashed Servers");
            foreach (var server in CrashedServers)
            {
                Console.WriteLine($"Server {server}");
            }
            return new ClientStatusReply();
        }

        public NetworkInformationReply NetworkInformation(NetworkInformationRequest request)
        {
            Console.WriteLine("Received NetworkInfo");
            foreach (var serverUrl in request.ServerUrls)
            {
                if (!ServerUrls.ContainsKey(serverUrl.Key))
                {
                    ServerUrls[serverUrl.Key] = serverUrl.Value;
                }
            }
            foreach (var partition in request.ServerIdsByPartition)
            {
                if (!ServersIdByPartition.ContainsKey(partition.Key))
                {
                    if (!ServersIdByPartition.TryAdd(partition.Key, partition.Value.ServerIds.ToList()))
                    {
                        throw new RpcException(new Status(StatusCode.Unknown, "Could not add element"));
                    }
                }
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
            return new ClientPingReply();
        }
    }
}
