using System;
using System.Collections.Concurrent;
using System.Threading.Tasks;
using Grpc.Net.Client;

namespace PuppetMaster
{
    public class PuppetMaster
    {
        private PuppetMasterForm Form;
        private ConcurrentDictionary<string, PCSGrpcService.PCSGrpcServiceClient> PCSClients
            = new ConcurrentDictionary<string, PCSGrpcService.PCSGrpcServiceClient>();
        private ConcurrentDictionary<int, string> ServerURLs
            = new ConcurrentDictionary<int, string>();
        private const int PCS_PORT = 10000;

        public PuppetMaster()
        {
            AppContext.SetSwitch(
    "System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);
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

        private void HandleWaitCommand(string[] args)
        {
            if (args.Length != 1+1)
            {
                this.Form.Error("Wait: wrong number of arguments");
                goto WaitUsage;
            }

            return;
        WaitUsage:
            this.Form.Error("Wait usage: Wait x_ms");
        }

        private void HandleUnfreezeCommand(string[] args)
        {
            if (args.Length != 1+1)
            {
                this.Form.Error("Unfreeze: wrong number of arguments");
                goto UnfreezeUsage;
            }

            return;
        UnfreezeUsage:
            this.Form.Error("Unfreeze usage: Unreeze server_id");
        }

        private void HandleFreezeCommand(string[] args)
        {
            if (args.Length != 1+1)
            {
                this.Form.Error("Freeze: wrong number of arguments");
                goto FreezeUsage;
            }

            return;
        FreezeUsage:
            this.Form.Error("Freeze usage: Freeze server_id");
        }

        private void HandleCrashCommand(string[] args)
        {
            if (args.Length != 1+1)
            {
                this.Form.Error("Crash: wrong number of arguments");
                goto CrashUsage;
            }

            return;
        CrashUsage:
            this.Form.Error("Crash usage: Crash server_id");
        }

        private void HandleStatusCommand(string[] args)
        {

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

            if (!url.StartsWith("http://"))
            {
                goto InvalidURL;
            }
            string host = url.Replace("http://", "");

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
                if (grpcClient.LaunchClient(new LaunchClientRequest { ScriptFile = scriptFile }).Ok)
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

            return;

        InvalidURL:
            this.Form.Error("Client: Invalid URL");
        ClientUsage:
            this.Form.Error("Client usage: Client username client_URL script_file");
        }

        private void HandlePartitionCommand(string[] args)
        {
            if (args.Length < 1+3)
            {
                this.Form.Error("Partition: wrong number of arguments");
                goto PartitionUsage;
            }
            return;
        PartitionUsage:
            this.Form.Error("Partition usage: Partition r partition_name server_id_1 ... server_id_r");
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
            string min = args[3];
            string max = args[4];

            if (!int.TryParse(id, out int id_int))
            {
                goto InvalidId;
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
            if (port < 1 || 65535 < port)
            {
                goto InvalidPort;
            }

            ServerURLs[id_int] = url;

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

            if (grpcClient.LaunchServer(new LaunchServerRequest { Port = port }).Ok)
            {
                this.Form.Log("Server: successfully launched server at " + host + ":" + port);
            }
            else
            {
                this.Form.Error("Server: failed launching server");
            }

            return;

        InvalidPort:
            this.Form.Error("Server: Invalid port number");
            goto ServerUsage;
        InvalidURL:
            this.Form.Error("Server: Invalid URL");
            goto ServerUsage;
        InvalidId:
            this.Form.Error("Server: Invalid id");
        ServerUsage:
            this.Form.Error("Server usage: Server server_id URL min_delay max_delay");
        }

        private void HandleReplicationFactorCommand(string[] args)
        {
            if (args.Length != 1+1)
            {
                this.Form.Error("Replication: wrong number of arguments");
                goto ReplicationUsage;
            }

            return;
        ReplicationUsage:
            this.Form.Error("ReplicationFactor usage: ReplicationFactor r");
        }

        public void LinkForm(PuppetMasterForm form)
        { 
            this.Form = form;
            this.Form.LinkPuppetMaster(this);
        }
    }
}
