using System;
using System.Threading;
using System.Collections.Generic;
using System.Collections.Concurrent;
using Grpc.Net.Client;
using Grpc.Core;
using System.Linq;

namespace Client
{
    public class GStoreClient
    {
        // Mapping of partitions and masters
        // URL of all servers

        private GrpcChannel Channel { get; set; }
        private ClientServerGrpcService.ClientServerGrpcServiceClient Client;
        BoolWrapper ContinueExecution;

        private readonly ConcurrentDictionary<string, List<string>> ServersIdByPartition;
        private readonly ConcurrentDictionary<string, string> ServerUrls;
        private readonly ConcurrentBag<string> CrashedServers;
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

        public bool TryChangeCommunicationChannel(string server_id)
        {
            Console.WriteLine("Trying to connect to " + server_id);
            try
            {
                currentServerId = server_id;
                Channel = GrpcChannel.ForAddress(ServerUrls[server_id]);
                Client = new ClientServerGrpcService.ClientServerGrpcServiceClient(Channel);
                return true;
            }
            catch (Exception)
            {
                // Print Exception?
                return false;
            }
        }

        public void ReadObject(string partition_id, string object_id, string server_id)
        {
            // Check if connected Server has requested partition

            if (ServersIdByPartition[partition_id].Count == 0)
            {
                Console.WriteLine($"No available server for partition {partition_id}");
                return;
            }

            if (!ServersIdByPartition[partition_id].Contains(currentServerId))
            {
                if (server_id == "-1")
                {
                    // Not connected to correct partition, and no optional server stated, connect to random server from partition
                    Random rnd = new Random();
                    var randomServerFromPartition = ServersIdByPartition[partition_id][rnd.Next(ServersIdByPartition[partition_id].Count)];
                    TryChangeCommunicationChannel(randomServerFromPartition);
                }
                else
                {
                    TryChangeCommunicationChannel(server_id);
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
            try
            {
                var reply = Client.ReadObject(request);
                Console.WriteLine("Received: " + reply.Value);
            }
            catch (RpcException e)
            {
                // If error is because Server failed, update list of crashed Servers
                if (e.Status.StatusCode == StatusCode.Unavailable || e.Status.StatusCode == StatusCode.DeadlineExceeded || e.Status.StatusCode == StatusCode.Internal)
                {
                    UpdateCrashedServersList();
                }

                Console.WriteLine($"Error: {e.Status.StatusCode}");
                Console.WriteLine($"Error message: {e.Status.Detail}");
                Console.WriteLine("N/A");
            }
        }

        public void WriteObject(string partition_id, string object_id, string value)
        {

            int currentServerPartitionIndex;
            List<string> ServersOfPartition = ServersIdByPartition[partition_id];

            if (ServersOfPartition.Count == 0)
            {
                Console.WriteLine($"No available servers for partition {partition_id}");
                return;
            }

            // Check if connected to server with desired partition
            if (!ServersOfPartition.Contains(currentServerId))
            {
                // If not connect to first server of partition
                TryChangeCommunicationChannel(ServersOfPartition[0]);
                currentServerPartitionIndex = 0;
            }
            else
            {
                currentServerPartitionIndex = ServersOfPartition.IndexOf(currentServerId);
            }

            var success = false;
            int numTries = 0;
            WriteObjectRequest request = new WriteObjectRequest
            {
                Key = new ObjectId
                {
                    PartitionId = partition_id,
                    ObjectKey = object_id
                },
                Value = value
            };
            var crashedServers = new ConcurrentBag<string>();
            while (!success && numTries < ServersOfPartition.Count)
            {
                try
                {
                    var reply = Client.WriteObject(request);
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
                        TryChangeCommunicationChannel(ServersOfPartition[currentServerPartitionIndex]);
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
                TryChangeCommunicationChannel(server_id);
            }

            try
            {
                ListServerRequest request = new ListServerRequest();
                var reply = Client.ListServer(request);
                Console.WriteLine("Received from server: " + server_id);
                foreach (var obj in reply.Objects)
                {
                    Console.WriteLine($"object <{obj.Key.PartitionId}, {obj.Key.ObjectKey}>, is {server_id} partition master? {obj.IsPartitionMaster}");
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
                TryChangeCommunicationChannel(serverId);

                try
                {

                    ListGlobalRequest request = new ListGlobalRequest();
                    var reply = Client.ListGlobal(request);
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
