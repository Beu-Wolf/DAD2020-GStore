using System;
using System.Threading;
using System.Collections.Concurrent;
using System.Threading.Tasks;
using Grpc.Net.Client;
using System.Collections.Generic;
using System.Collections;
using System.Security.Policy;
using System.Linq;
using System.Windows.Forms;
using System.Threading.Channels;
using Grpc.Core;

namespace PuppetMaster
{
    public class PuppetMaster
    {

        static private ConcurrentDictionary<string, ServerInfo> Servers = new ConcurrentDictionary<string, ServerInfo>();
        static private ConcurrentDictionary<string, ClientInfo> Clients = new ConcurrentDictionary<string, ClientInfo>();
        static private ConcurrentDictionary<string, Partition> Partitions = new ConcurrentDictionary<string, Partition>();

        private class ServerInfo
        {
            private bool Ready = false;
            public string Id { get; }
            public string Url { get; }
            public PuppetMasterServerGrpcService.PuppetMasterServerGrpcServiceClient Grpc { get; }

            private object FreezeLock = new object();
            private bool IsFrozen = false;

            public ServerInfo(string id, string url)
            {
                Id = id;
                Url = url;
                GrpcChannel serverChannel = GrpcChannel.ForAddress(url);
                Grpc = new PuppetMasterServerGrpcService.PuppetMasterServerGrpcServiceClient(serverChannel);
            }

            public void Init()
            {
                bool alive = false;
                ServerPingRequest request = new ServerPingRequest();
                while (!alive)
                {
                    try
                    {
                        Grpc.Ping(request);
                        alive = true;
                    }
                    catch (Exception)
                    {
                        Thread.Sleep(500);
                    }
                }

                lock (this)
                {
                    Ready = true;
                    Monitor.PulseAll(this);
                }
            }

            public void WaitForReady()
            {
                lock (this)
                {
                    while (!Ready) Monitor.Wait(this);
                }
            }

            public void SendInformationToServer()
            {
                var request = new PartitionSchemaRequest();
                List<string> mastered = new List<string>();
                foreach (var partition in Partitions)
                {
                    request.PartitionServers[partition.Key] = new PartitionInfo
                    {
                        ServerIds = { partition.Value.ServerIds }
                    };
                    if (partition.Value.ServerIds[0] == Id)
                    { // server master of this partition
                        mastered.Add(partition.Key);
                    }
                }

                foreach (var server in Servers.Values)
                {
                    request.ServerUrls[server.Id] = server.Url;
                }

                request.MasteredPartitions = new MasteredInfo
                {
                    PartitionIds = { mastered }
                };

                Grpc.PartitionSchema(request);
            }

            public void FreezeServer()
            {
                lock(FreezeLock)
                {
                    this.Grpc.Freeze(new FreezeRequest());
                    IsFrozen = true;
                    Monitor.PulseAll(FreezeLock);
                }
            }

            public void WaitForFreeze()
            {
                lock(FreezeLock)
                {
                    while (!IsFrozen) Monitor.Wait(FreezeLock);
                    this.Grpc.Unfreeze(new UnfreezeRequest());
                    IsFrozen = false;
                }
                
            }
        }

        private class ClientInfo
        {
            public string Username { get; }
            public string Url { get; }
            public PuppetMasterClientGrpcService.PuppetMasterClientGrpcServiceClient Grpc { get; }


            public ClientInfo(string username, string url, PuppetMasterClientGrpcService.PuppetMasterClientGrpcServiceClient grpc)
            {
                Username = username;
                Url = url;
                Grpc = grpc;
            }

            public void Init()
            {
                List<Partition> partitions = new List<Partition>(Partitions.Values);

                bool alive = false;
                ClientPingRequest request = new ClientPingRequest();
                while (!alive)
                {
                    try
                    {
                        Grpc.Ping(request);
                        alive = true;
                    }
                    catch
                    {
                        Thread.Sleep(500);
                    }
                }
                SendInformationToClient(partitions);
            }
            private void SendInformationToClient(List<Partition> partitions)
            {
                Dictionary<string, string> serverUrls = new Dictionary<string, string>();
                var request = new NetworkInformationRequest();

                foreach (var partition in partitions)
                {
                    partition.WaitForReady();
                    request.ServerIdsByPartition.Add(partition.Id, new PartitionServers
                    {
                        ServerIds = { partition.ServerIds }
                    });

                    foreach (var serverId in partition.ServerIds)
                    {
                        if (!serverUrls.ContainsKey(serverId))
                        {
                            serverUrls.Add(serverId, Servers[serverId].Url);
                        }
                    }
                }

                foreach (var serverUrl in serverUrls)
                {
                    request.ServerUrls.Add(serverUrl.Key, serverUrl.Value);
                }

                Grpc.NetworkInformation(request);
            }
        }

        private class Partition
        {
            private bool Ready = false;

            public string Id { get; }
            public List<string> ServerIds;


            public Partition(string id, List<string> serverIds) {
                Id = id;
                ServerIds = serverIds;
            }

            public void Init() {
                foreach (var server in ServerIds)
                {
                    while (!Servers.ContainsKey(server)) Thread.Sleep(100);
                    Servers[server].WaitForReady();
                }
                foreach (var serverId in ServerIds)
                {
                    Servers[serverId].SendInformationToServer();
                }
                lock (this)
                {
                    Ready = true;
                    Monitor.PulseAll(this);
                }
            }

            public void WaitForReady()
            {
                lock (this)
                {
                    while (!Ready) Monitor.Wait(this);
                }
            }
        }

        // We need to detect if this value was already assigned
        // Cannot use readonly since will be initialized after the constructor
        private int ReplicationFactor = -1;

        private int ClientCount = 0;
        private object ClientCountLock = new object();

        private PuppetMasterForm Form;
        private ConcurrentDictionary<string, PCSGrpcService.PCSGrpcServiceClient> PCSClients
            = new ConcurrentDictionary<string, PCSGrpcService.PCSGrpcServiceClient>();
        private const int PCS_PORT = 10000;



        public PuppetMaster()
        {
            AppContext.SetSwitch(
    "System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);
        }

        public void LinkForm(PuppetMasterForm form)
        {
            this.Form = form;
            this.Form.LinkPuppetMaster(this);
        }


        public void ParseCommand(string command)
        {
            string[] args = command.Split((char[])null, StringSplitOptions.RemoveEmptyEntries);

            if (args.Length == 0) return;
            switch (args[0])
            {
                case "ReplicationFactor":
                    Task.Run(() => HandleReplicationFactorCommand(args));
                    break;
                case "Server":
                    Task.Run(() => HandleServerCommand(args));
                    break;
                case "Partition":
                    HandlePartitionCommand(args);
                    break;
                case "Client":
                    Task.Run(() => HandleClientCommand(args));
                    break;
                case "Status":
                    Task.Run(() => HandleStatusCommand(args));
                    break;
                case "Crash":
                    Task.Run(() => HandleCrashCommand(args));
                    break;
                case "Freeze":
                    Task.Run(() => HandleFreezeCommand(args));
                    break;
                case "Unfreeze":
                    Task.Run(() => HandleUnfreezeCommand(args));
                    break;
                case "Wait":
                    HandleWaitCommand(args);
                    break;
                default:
                    this.Form.Error($"Unknown command: {args[0]}");
                    break;
            }
        }
        
        private void HandleReplicationFactorCommand(string[] args)
        {
            if (args.Length != 1+1)
            {
                this.Form.Error("Replication: wrong number of arguments");
                goto ReplicationUsage;
            }

            if (!int.TryParse(args[1], out int replicationFactor) || replicationFactor <= 0)
            {
                this.Form.Error("Replication: r must be a positive number");
                return;
            }

            if (this.ReplicationFactor != -1 && replicationFactor != this.ReplicationFactor)
            {
                this.Form.Error($"Replication: replication factor already assigned to {this.ReplicationFactor}");
                return;
            }

            this.ReplicationFactor = replicationFactor;

            return;
        ReplicationUsage:
            this.Form.Error("ReplicationFactor usage: ReplicationFactor r");
        }
        
        private void HandleServerCommand(string[] args)
        {
            if (args.Length != 1+4)
            {
                this.Form.Error("Server: wrong number of arguments");
                goto ServerUsage;
            }
            string id = args[1];
            string url = args[2];

            if (!url.StartsWith("http://"))
            {
                goto InvalidURL;
            }

            string[] urlElements = url.Replace("http://", "").Split(":", StringSplitOptions.RemoveEmptyEntries);
            if (urlElements.Length != 2)
            {
                this.Form.Error(urlElements.ToString());
                goto InvalidURL;
            }
            string host = urlElements[0];
            if (!int.TryParse(urlElements[1], out int port))
            {
                goto InvalidPort;
            }
            if (port < 1024 || 65535 < port)
            {
                goto InvalidPort;
            }

            if(!int.TryParse(args[3], out int min_delay)
                || !int.TryParse(args[4], out int max_delay)
                || min_delay < 0
                || max_delay < 0)
            {
                this.Form.Error("Server: delay arguments must be non negative numbers");
                return;
            }

            if (min_delay > max_delay)
            {
                this.Form.Error("Server: max_delay must be greater or equal than min_delay");
                return;
            }

            if (Servers.ContainsKey(id))
            {
                this.Form.Error($"Server: server {id} already exists");
                return;
            }

            PCSGrpcService.PCSGrpcServiceClient grpcClient;
            if (PCSClients.ContainsKey(host))
            {
                grpcClient = PCSClients[host];
            }
            else
            {
                string address = "http://" + host + ":" + PCS_PORT;
                GrpcChannel channel = GrpcChannel.ForAddress(address);

                try
                {
                    grpcClient = new PCSGrpcService.PCSGrpcServiceClient(channel);
                    PCSClients[host] = grpcClient;
                }
                catch (Exception)
                {
                    this.Form.Error("Server: unable to connect to PCS");
                    return;
                }
            }


            if (grpcClient.LaunchServer(new LaunchServerRequest { Id = id, Port = port, MinDelay = min_delay, MaxDelay = max_delay }).Ok)
            {
                this.Form.Log("Server: successfully launched server at " + host + ":" + port);
            }
            else
            {
                this.Form.Error("Server: failed launching server");
            }

            // register server
            ServerInfo server = new ServerInfo(id, url);
            Servers[id] = server;
            server.Init();

            return;

        InvalidPort:
            this.Form.Error("Server: Invalid port number");
            goto ServerUsage;
        InvalidURL:
            this.Form.Error("Server: Invalid URL");
            goto ServerUsage;
        ServerUsage:
            this.Form.Error("Server usage: Server server_id URL min_delay max_delay");
        }

        private void HandlePartitionCommand(string[] args)
        {
            if (args.Length < 1+3)
            {
                this.Form.Error("Partition: wrong number of arguments");
                goto PartitionUsage;
            }

            if(!int.TryParse(args[1], out int replicationFactor) || replicationFactor <= 0)
            {
                this.Form.Error("Partition: r must be a positive number");
                return;
            }

            // check if replication factor
            if (this.ReplicationFactor != -1 && replicationFactor != this.ReplicationFactor)
            {
                this.Form.Error($"Partition: replication factor already assigned to {this.ReplicationFactor}");
                return;
            }

            // even if command fails, set replication factor
            this.ReplicationFactor = replicationFactor;

            // check number of given servers
            if (this.ReplicationFactor != args.Length - 3)
            {
                this.Form.Error($"Partition: you must supply {this.ReplicationFactor} servers to create this partition");
                return;
            }

            // check if unique partition name
            string partitionName = args[2];
            if(Partitions.ContainsKey(partitionName))
            {
                this.Form.Error($"Partition: partition {partitionName} already exists");
                return;
            }

            // check if all partition servers exist
            List<string> servers = new List<string>();
            for(int i = 3; i < args.Length; i++)
            {
                servers.Add(args[i]);
            }

            // create partition
            Partition partition = new Partition(partitionName, servers);
            Partitions[partitionName] = partition;


            Task.Run(() => partition.Init());

            return;
        PartitionUsage:
            this.Form.Error("Partition usage: Partition r partition_name server_id_1 ... server_id_r");
        }

        private void HandleClientCommand(string[] args)
        {
            if (args.Length != 1+3)
            {
                this.Form.Error("Client: wrong number of arguments");
                goto ClientUsage;
            }

            string username = args[1];
            string url = args[2];
            string scriptFile = args[3];

            if (Clients.ContainsKey(username))
            {
                this.Form.Error($"Client: client {username} already exists");
                return;
            }

            if (!url.StartsWith("http://"))
            {
                goto InvalidURL;
            }
            string[] urlElements = url.Replace("http://", "").Split(":", StringSplitOptions.RemoveEmptyEntries);
            if (urlElements.Length != 2)
            {
                this.Form.Error(urlElements.ToString());
                goto InvalidURL;
            }
            string host = urlElements[0];
            if (!int.TryParse(urlElements[1], out int port))
            {
                goto InvalidPort;
            }
            if (port < 1024 || 65535 < port)
            {
                goto InvalidPort;
            }

            PCSGrpcService.PCSGrpcServiceClient grpcClient;
            if (PCSClients.ContainsKey(host))
            {
                grpcClient = PCSClients[host];
            }
            else
            {
                try
                {
                    string address = "http://" + host + ":" + PCS_PORT;
                    GrpcChannel channel = GrpcChannel.ForAddress(address);
                    grpcClient = new PCSGrpcService.PCSGrpcServiceClient(channel);
                }
                catch (Exception)
                {
                    this.Form.Error("Client: unable to connect to PCS");
                    return;
                }
            }

            grpcClient.Ping(new PCSPingRequest());
            int clientId;
            lock(ClientCountLock)
            {
                clientId = ++ClientCount;
            }

            try {
                if (grpcClient.LaunchClient(new LaunchClientRequest { ScriptFile = scriptFile , Port = port, Id = clientId }).Ok)
                {
                    this.Form.Log("Client: successfully launched client at " + host);
                }
                else
                {
                    this.Form.Error("Client: failed launching client");
                }
            }
            catch (Exception)
            {
                this.Form.Error("Client: failed sending request to PCS");
            }

            // register client
            GrpcChannel clientChannel = GrpcChannel.ForAddress(url);
            var clientGrpc = new PuppetMasterClientGrpcService.PuppetMasterClientGrpcServiceClient(clientChannel);
            ClientInfo client = new ClientInfo(username, url, clientGrpc);
            Clients[username] = client;

            client.Init();

            return;

        InvalidPort:
            this.Form.Error("Client: Invalid port number");
            goto ClientUsage;
        InvalidURL:
            this.Form.Error("Client: Invalid URL");
        ClientUsage:
            this.Form.Error("Client usage: Client username client_URL script_file");
        }

        private void HandleStatusCommand(string[] args)
        {
            if(args.Length != 1)
            {
                this.Form.Error("Status: wrong number of arguments");
                goto StatusUsage;
            }

            foreach (var server in Servers.Values)
            {
                server.Grpc.Status(new ServerStatusRequest());
            }

            foreach (var client in Clients.Values)
            {
                client.Grpc.Status(new ClientStatusRequest());
            }


            return;
        StatusUsage:
            this.Form.Error("Status usage: Status");
        }

        private void HandleCrashCommand(string[] args)
        {
            if (args.Length != 1+1)
            {
                this.Form.Error("Crash: wrong number of arguments");
                goto CrashUsage;
            }

            foreach (var partition in Partitions.Values)
            {
                partition.WaitForReady();
            }

            string server_id = args[1];
            if (!Servers.ContainsKey(server_id))
            {
                this.Form.Error($"Crash: server {server_id} does not exist");
                return;
            }

            ServerInfo server = Servers[server_id];
            try
            {
                server.Grpc.Crash(new CrashRequest());
            }
            catch(RpcException)
            {
                // Success!
            }
            this.Form.Log("Crashing server " + server_id);

            return;
        CrashUsage:
            this.Form.Error("Crash usage: Crash server_id");
        }

        private void HandleFreezeCommand(string[] args)
        {
            if (args.Length != 1+1)
            {
                this.Form.Error("Freeze: wrong number of arguments");
                goto FreezeUsage;
            }

            foreach (var partition in Partitions.Values)
            {
                partition.WaitForReady();
            }

            string server_id = args[1];
            if (!Servers.ContainsKey(server_id))
            {
                this.Form.Error($"Freeze: server {server_id} does not exist");
                return;
            }

            ServerInfo server = Servers[server_id];
            server.FreezeServer();
            this.Form.Log("Freezing server " + server_id);


            return;
        FreezeUsage:
            this.Form.Error("Freeze usage: Freeze server_id");
        }

        private void HandleUnfreezeCommand(string[] args)
        {
            if (args.Length != 1+1)
            {
                this.Form.Error("Unfreeze: wrong number of arguments");
                goto UnfreezeUsage;
            }

            foreach (var partition in Partitions.Values)
            {
                partition.WaitForReady();
            }

            string server_id = args[1];
            if(!Servers.ContainsKey(server_id))
            {
                this.Form.Error($"Unfreeze: server {server_id} does not exist");
                return;
            }

            ServerInfo server = Servers[server_id];
            server.WaitForFreeze();
            this.Form.Log("Unfreezing server " + server_id);

            return;
        UnfreezeUsage:
            this.Form.Error("Unfreeze usage: Unreeze server_id");
        }
            
        private void HandleWaitCommand(string[] args)
        {
            if (args.Length != 1+1)
            {
                this.Form.Error("Wait: wrong number of arguments");
                goto WaitUsage;
            }

            if(!int.TryParse(args[1], out int x_ms) || x_ms <= 0)
            {
                this.Form.Error("Wait: x_mx must be a positive number");
                return;
            }

            // maybe disable form input for x_ms ms
            this.Form.DisableInput();
            this.Form.Log($"Waiting for {x_ms} ms");
            Thread.Sleep(x_ms);
            this.Form.EnableInput();

            return;
        WaitUsage:
            this.Form.Error("Wait usage: Wait x_ms");
        }
    }
}
