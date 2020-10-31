using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading;

namespace PuppetMaster
{
    public class PuppetMaster
    {
        private PuppetMasterForm Form;

        private readonly struct ServerInfo
        {
            private readonly string Id { get; }
            private readonly string Url { get; }
            // will have gRPC handles in the future

            internal ServerInfo(string id, string url)
            {
                Id = id;
                Url = url;
            }
        }

        private readonly struct ClientInfo
        {
            private readonly string Username { get; }
            private readonly string Url { get; }
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
        private Dictionary<string, ServerInfo> Servers = new Dictionary<string, ServerInfo>();
        private Dictionary<string, ClientInfo> Clients = new Dictionary<string, ClientInfo>();
        private Dictionary<string, List<string>> Partitions = new Dictionary<string, List<string>>();


        public PuppetMaster()
        {
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

            string server_id = args[1];
            string server_url = args[2];

            if(!int.TryParse(args[3], out int min_delay)
                || !int.TryParse(args[4], out int max_delay)
                || min_delay < 0
                || max_delay < 0)
            {
                this.Form.Error("Server: delay arguments must be positive numbers");
                return;
            }

            if (min_delay > max_delay)
            {
                this.Form.Error("Server: max_delay must be greater or equal than min_delay");
                return;
            }


            if (this.Servers.ContainsKey(server_id))
            {
                this.Form.Error($"Server: server {server_id} already exists");
                return;
            }

            // instantiate server

            // register server
            ServerInfo server = new ServerInfo(server_id, server_url);
            this.Servers[server_id] = server;

            return;
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
            string client_url = args[2];


            if (this.Clients.ContainsKey(username))
            {
                this.Form.Error($"Client: client {username} already exists");
                return;
            }

            // instantiate client

            // register client
            ClientInfo client = new ClientInfo(username, client_url);
            this.Clients[username] = client;


            return;
        ClientUsage:
            this.Form.Error("Client usage: Client username client_URL script_file");
        }

        private void HandleStatusCommand(string[] args)
        {

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

            // send crash command

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

            // send freeze command

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

            // send unfreeze command

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

            if(!int.TryParse(args[1], out int x_ms) || x_ms < 0)
            {
                this.Form.Error("Wait: x_mx must be a positive number");
                return;
            }

            // maybe disable form input for x_ms ms

            return;
        WaitUsage:
            this.Form.Error("Wait usage: Wait x_ms");
        }
    }
}
