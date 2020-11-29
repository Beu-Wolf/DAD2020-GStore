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
        }

        private bool TryConnectToServer(string server_id)
        {
            Console.WriteLine("Trying to connect to " + server_id);
            try
            {
                currentServerId = server_id;
                Channel = GrpcChannel.ForAddress(ServerUrls[server_id]);
                ConnectedServer = new ClientServerGrpcService.ClientServerGrpcServiceClient(Channel);
                return true;
            }
            catch (Exception)
            {
                // Print Exception?
                // Add to crashed servers?
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

            foreach (var serverId in ServersIdByPartition[partition_id])
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
            
            if (server_id != string.Empty)
            { // we have to connect to a specific server
                
                if (!ServersIdByPartition[partition_id].Contains(currentServerId))
                { // specified server does not belong to the asked partition!
                    Console.WriteLine($"Specified server does not belong to partition {partition_id}");
                    return;
                }

                if(!TryConnectToServer(server_id))
                {
                    Console.WriteLine($"Could not connect to server {server_id}");
                    return;
                }
            } else
            {
                if(!TryConnectToPartition(partition_id))
                {
                    Console.WriteLine($"Could not connect to any server from partition {partition_id}");
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
                    UpdateCrashedServersList();
                }

                // TODO: non-existing objects will generate an exception
                Console.WriteLine($"Error: {e.Status.StatusCode}");
                Console.WriteLine($"Error message: {e.Status.Detail}");
                Console.WriteLine("N/A");
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

            var success = false;
            int numTries = 0;
            WriteObjectRequest request = new WriteObjectRequest
            {
                Object = new ObjectInfo
                {
                    Key = new ObjectId
                    {
                        PartitionId = partition_id,
                        ObjectKey = object_id
                    },
                    ObjectVersion = new ObjectVersion
                    {
                        ClientId = Id,
                        Counter = 1
                    },
                    Value = value
                }                      
            };
            var crashedServers = new ConcurrentBag<string>();
            while (!success && numTries < ServersOfPartition.Count)
            {
                try
                {
                    var reply = ConnectedServer.WriteObject(request);
                    Console.WriteLine("Received: " + reply.Ok);
                    success = true;
                }
                catch (RpcException e)
                {
                    if (e.Status.StatusCode == StatusCode.PermissionDenied)
                    {
                        Console.WriteLine($"Cannot write in server {currentServerId}");
                    }
                    else
                    {
                        // If error is because Server failed, keep it
                        if (e.Status.StatusCode == StatusCode.Unavailable || e.Status.StatusCode == StatusCode.DeadlineExceeded || e.Status.StatusCode == StatusCode.Internal)
                        {
                            Console.WriteLine($"Server {currentServerId} is down");
                            crashedServers.Add(currentServerId);
                        }
                        else
                        {
                            throw e;
                        }
                    }

                    if (++numTries < ServersOfPartition.Count)
                    {
                        // Connect to next server in list
                        currentServerPartitionIndex = (currentServerPartitionIndex + 1) % ServersOfPartition.Count;
                        TryConnectToServer(ServersOfPartition[currentServerPartitionIndex]);
                    }

                }
            }

            // Remove crashed servers from list and update CrashedServers list
            CrashedServers.Union(crashedServers);
            foreach (var crashedServer in crashedServers)
            {
                foreach (var kvPair in ServersIdByPartition)
                {
                    if (kvPair.Value.Contains(crashedServer))
                    {
                        kvPair.Value.Remove(crashedServer);
                    }
                }
            }
        }

        public void ListServer(string server_id)
        {
            if (currentServerId != server_id)
            {
                TryConnectToServer(server_id);
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
                    UpdateCrashedServersList();
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
                        UpdateCrashedServersList();
                    }
                    else
                    {
                        throw e;
                    }
                }
            }
        }

        private void UpdateCrashedServersList()
        {
            // Update Crashed Server List
            CrashedServers.Add(currentServerId);
            foreach (var kvPair in ServersIdByPartition)
            {
                if (kvPair.Value.Contains(currentServerId))
                {
                    kvPair.Value.Remove(currentServerId);
                }
            }
            Console.WriteLine($"Server {currentServerId} is down");
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
