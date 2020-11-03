using System;
using System.Collections.Concurrent;
using System.Threading.Tasks;
using Grpc.Net.Client;
using System.Collections.Generic;

namespace PuppetMaster
{
    public class PuppetMaster
    {

        private readonly struct ServerInfo
        {
            internal readonly string Id { get; }
            internal readonly string Url { get; }
            internal readonly PuppetMasterServerGrpcService.PuppetMasterServerGrpcServiceClient Grpc { get; }

            internal ServerInfo(string id, string url, PuppetMasterServerGrpcService.PuppetMasterServerGrpcServiceClient grpc)
            {
                Id = id;
                Url = url;
                Grpc = grpc;
            }
        }

        private readonly struct ClientInfo
        {
            internal string Username { get; }
            internal string Url { get; }
            // will have gRPC handles in the future

            internal ClientInfo(string username, string url)
            {
                Username = username;
                Url = url;
            }
        }

        // We need to detect if this value was already assigned
        // Cannot use readonly since will be initialized after the constructor
        private int ReplicationFactor = -1;
        private ConcurrentDictionary<string, ServerInfo> Servers = new ConcurrentDictionary<string, ServerInfo>();
        private ConcurrentDictionary<string, ClientInfo> Clients = new ConcurrentDictionary<string, ClientInfo>();
        private ConcurrentDictionary<string, List<string>> Partitions = new ConcurrentDictionary<string, List<string>>();

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
                    Task.Run(() => HandlePartitionCommand(args));
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
                || min_delay <= 0
                || max_delay <= 0)
            {
                this.Form.Error("Server: delay arguments must be positive numbers");
                return;
            }

            if (min_delay > max_delay)
            {
                this.Form.Error("Server: max_delay must be greater or equal than min_delay");
                return;
            }

            if (this.Servers.ContainsKey(id))
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
                }
                catch (Exception)
                {
                    this.Form.Error("Server: unable to connect to PCS");
                    return;
                }
            }

            if (grpcClient.LaunchServer(new LaunchServerRequest { Port = port, MinDelay = min_delay, MaxDelay = max_delay }).Ok)
            {
                this.Form.Log("Server: successfully launched server at " + host + ":" + port);
            }
            else
            {
                this.Form.Error("Server: failed launching server");
            }

            // register server
            GrpcChannel serverChannel = GrpcChannel.ForAddress(url);
            var serverGrpc = new PuppetMasterServerGrpcService.PuppetMasterServerGrpcServiceClient(serverChannel);
            ServerInfo server = new ServerInfo(id, url, serverGrpc);
            this.Servers[id] = server;

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
            if(this.Partitions.ContainsKey(partitionName))
            {
                this.Form.Error($"Partition: partition {partitionName} already exists");
                return;
            }

            // check if all partition servers exist
            List<string> servers = new List<string>();
            bool failed = false;
            for(int i = 3; i < args.Length; i++)
            {
                if(!this.Servers.ContainsKey(args[i]))
                {
                    this.Form.Error($"Partition: server {args[i]} does not exist");
                    failed = true;
                    continue;
                }
                servers.Add(args[i]);
            }
            if (failed) return;

            // create partition

            this.Partitions[partitionName] = servers;

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

            if (this.Clients.ContainsKey(username))
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

            try {
                if (grpcClient.LaunchClient(new LaunchClientRequest { ScriptFile = scriptFile , Port = port }).Ok)
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
            ClientInfo client = new ClientInfo(username, url);
            this.Clients[username] = client;

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

            string server_id = args[1];
            if (!this.Servers.ContainsKey(server_id))
            {
                this.Form.Error($"Crash: server {server_id} does not exist");
                return;
            }

            ServerInfo server = this.Servers[server_id];
            server.Grpc.Crash(new CrashRequest());

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

            string server_id = args[1];
            if (!this.Servers.ContainsKey(server_id))
            {
                this.Form.Error($"Freeze: server {server_id} does not exist");
                return;
            }

            ServerInfo server = this.Servers[server_id];
            server.Grpc.Freeze(new FreezeRequest());

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

            string server_id = args[1];
            if(!this.Servers.ContainsKey(server_id))
            {
                this.Form.Error($"Unfreeze: server {server_id} does not exist");
                return;
            }

            ServerInfo server = this.Servers[server_id];
            server.Grpc.Unfreeze(new UnfreezeRequest());

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

            return;
        WaitUsage:
            this.Form.Error("Wait usage: Wait x_ms");
        }

        private void SendInformationToClient(ConcurrentDictionary<string, List<string>> serverIdsByPartitionCopy, ConcurrentDictionary<string, ServerInfo> serverUrlsCopy, ClientInfo client)
        {
            GrpcChannel channel = GrpcChannel.ForAddress(client.Url);
            var grpcClient = new PuppetMasterClientGrpcService.PuppetMasterClientGrpcServiceClient(channel);

            var request = new NetworkInformationRequest();
            foreach (var serverIds in serverIdsByPartitionCopy)
            {
                request.ServerIdsByPartition.Add(serverIds.Key, new PartitionServers
                {
                    ServerIds = { serverIds.Value }
                });
            }
            foreach (var serverUrl in serverUrlsCopy)
            {
                request.ServerUrls.Add(serverUrl.Key, serverUrl.Value.Url);
            }

            grpcClient.NetworkInformation(request);
        }
    }
}
