using System;
using System.Threading;
using System.Collections.Generic;
using Grpc.Net.Client;
using Grpc.Core;
using System.Linq;
using System.Collections.Concurrent;

namespace Client
{

    public class GSTOREClient
    {
        // Mapping of partitions and masters
        // URL of all servers

        private GrpcChannel Channel { get; set; }
        private ClientServerGrpcService.ClientServerGrpcServiceClient Client;

        private readonly ConcurrentDictionary<long, List<int>> ServersIdByPartition;
        private readonly ConcurrentDictionary<int, string> ServerUrls;
        private readonly ConcurrentBag<int> CrashedServers;
        private int currentServerId;



        public GSTOREClient(ConcurrentDictionary<long, List<int>> serversIdByPartition, ConcurrentDictionary<int, string> serverUrls, ConcurrentBag<int> crashedServers)
        {
            ServersIdByPartition = serversIdByPartition;
            ServerUrls = serverUrls;
            CrashedServers = crashedServers;

            // Connect to random server
            Random rnd = new Random();
            var keyValPair = serverUrls.ElementAt(rnd.Next(serverUrls.Count));
            Console.WriteLine($"Connecting to id {keyValPair.Key} at {keyValPair.Value}");
            currentServerId = keyValPair.Key;
            Channel = GrpcChannel.ForAddress(keyValPair.Value);
            Client = new ClientServerGrpcService.ClientServerGrpcServiceClient(Channel);
        }

        public bool TryChangeCommunicationChannel(int server_id)
        {
            Console.WriteLine("Trying to connect to " + server_id);
            try
            {
                currentServerId = server_id;
                Channel = GrpcChannel.ForAddress(ServerUrls[server_id]);
                Client = new ClientServerGrpcService.ClientServerGrpcServiceClient(Channel);
                return true;
            } catch(Exception)
            {
                // Print Exception?
                return false;
            }
        }

        public void ReadObject(int partition_id, int object_id, int server_id)
        {
            // Check if connected Server has requested partition

            if(ServersIdByPartition[partition_id].Count == 0)
            {
                Console.WriteLine($"No available server for partition {partition_id}");
                return;
            }

            if (!ServersIdByPartition[partition_id].Contains(currentServerId))
            {
                if (server_id == -1)
                {
                    // Not connected to correct partition, and no optional server stated, connect to random server from partition
                    Random rnd = new Random();
                    var randomServerFromPartition = ServersIdByPartition[partition_id][rnd.Next(ServersIdByPartition[partition_id].Count)];
                    TryChangeCommunicationChannel(randomServerFromPartition);
                } else
                {
                    TryChangeCommunicationChannel(server_id);
                }
            }

            ReadObjectRequest request = new ReadObjectRequest
            {
                Key = new Key
                {
                    PartitionId = partition_id,
                    ObjectId = object_id
                }
            };
            try
            {
                var reply = Client.ReadObject(request);
                Console.WriteLine("Received: " + reply.Value);
            } catch (RpcException e)
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

        public void WriteObject(int partition_id, int object_id, string value)
        {

            int currentServerPartitionIndex;
            List<int> ServersOfPartition = ServersIdByPartition[partition_id];

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
            } else
            {
                currentServerPartitionIndex = ServersOfPartition.IndexOf(currentServerId); 
            }

            var success = false;
            int numTries = 0;
            WriteObjectRequest request = new WriteObjectRequest
            {
                Key = new Key
                {
                    PartitionId = partition_id,
                    ObjectId = object_id
                },
                Value = value
            };
            var crashedServers = new ConcurrentBag<int>();
            while (!success && numTries < ServersOfPartition.Count)
            {
                try
                {
                    var reply = Client.WriteObject(request);
                    Console.WriteLine("Received: " + reply.Ok);
                    success = true;
                } catch (RpcException e)
                {
                    if (e.Status.StatusCode != StatusCode.PermissionDenied)
                    {
                        // If error is because Server failed, keep it
                        if (e.Status.StatusCode == StatusCode.Unavailable || e.Status.StatusCode == StatusCode.DeadlineExceeded || e.Status.StatusCode == StatusCode.Internal)
                        {
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
                        currentServerPartitionIndex = (currentServerPartitionIndex+1) % ServersOfPartition.Count;
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

        public void ListServer(int server_id)
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
                    Console.WriteLine($"object <{obj.Key.PartitionId}, {obj.Key.ObjectId}>, is {server_id} partition master? {obj.IsPartitionMaster}");
                }
            } 
            catch (RpcException e)
            {
                if (e.Status.StatusCode == StatusCode.Unavailable || e.Status.StatusCode == StatusCode.DeadlineExceeded || e.Status.StatusCode == StatusCode.Internal)
                {
                    UpdateCrashedServersList();
                } else
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
                        Console.WriteLine($"object <{key.PartitionId}, {key.ObjectId}>");
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
    }


    class Program { 
    
        static void Main(string[] args) {

            if (args.Length != 3)
            {
                Console.WriteLine("Usage: Client.exe host port <script_file>");
                return;
            }


            if (!int.TryParse(args[1], out int Port))
            {
                Console.WriteLine("Invalid port value");
                return;
            }

            AppContext.SetSwitch(
    "System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);

            string host = args[0];

            var serverIdsByPartition = new ConcurrentDictionary<long, List<int>>();
            serverIdsByPartition.TryAdd(1, new List<int> { 1, 2 });
            serverIdsByPartition.TryAdd(2, new List<int> { 2 });

            var serverUrls = new ConcurrentDictionary<int, string>();
            serverUrls.TryAdd(1, "http://localhost:10001");
            serverUrls.TryAdd(2, "http://localhost:10002");

            var crashedServers = new ConcurrentBag<int>();

            var client = new GSTOREClient(serverIdsByPartition, serverUrls, crashedServers);

            var server = new Grpc.Core.Server
            {
                Services =
                {
                    PuppetMasterClientGrpcService.BindService(new PuppetMasterCommunicationService(serverIdsByPartition, crashedServers))
                },
                Ports = { new ServerPort(host, Port, ServerCredentials.Insecure) }

            };

            server.Start();

            try {
                    string line;

                    System.IO.StreamReader file = new System.IO.StreamReader(args[2]);
                    while ((line = file.ReadLine()) != null) {
                        string[] cmd = line.Split((char[])null, StringSplitOptions.RemoveEmptyEntries);
                        CommandDispatcher(cmd, file, client);
                    }

                    file.Close();

                    // We need to stay up, in order to respond to status commands by the Puppet Master
                    // Start gRPC server of connection with PM
                    // For now, just wait for user input
                    Console.ReadKey();



            } catch (System.IO.FileNotFoundException) {
                Console.WriteLine("File not found. Exiting...");
            } finally
            {
                server.ShutdownAsync().Wait();
            }
        }

        static void CommandDispatcher(string[] cmd, System.IO.StreamReader file, GSTOREClient client) {
            switch (cmd[0]) {
                case "read":
                    Handle_read(cmd, client);
                    break;
                case "write":
                    Handle_write(cmd, client);
                    break;
                case "listServer":
                    Handle_listServer(cmd, client);
                    break;
                case "listGlobal":
                    Handle_listGlobal(cmd, client);
                    break;
                case "wait":
                    Handle_wait(cmd, client);
                    break;
                case "begin-repeat":
                    List<string[]> commands = new List<string[]>();
                    string line;
                    while ((line = file.ReadLine()) != null && !line.Equals("end-repeat")) {
                        commands.Add(line.Split());
                    }
                    if (line == null) {
                        Console.WriteLine("Repeat command does not end. Exiting...");
                    }
                    Handle_repeat(cmd, commands, file, client);
                    break;
                case "end-repeat":
                    Console.WriteLine("Invalid end-repeat: Not inside repeat statement!");
                    break;
                default:
                    Console.WriteLine("Command not recognized! >:(");
                    break;
            }
        }

        static void Handle_read(string[] cmd, GSTOREClient client) {
            if (cmd.Length < 3) {
                Console.WriteLine("Invalid command format!");
                Console.WriteLine("Use: `read <partition_id> <object_id> [<server_id>]`");
                return;
            }

            string partitionId = cmd[1];
            string objectId = cmd[2];
            string serverId = string.Empty;
            if (cmd.Length == 4)
                serverId = cmd[3];

            // Console.WriteLine($"read {partitionId} {objectId} {serverId}");
            Console.WriteLine("read " + partitionId + " " + objectId + " " + serverId);


            if (int.TryParse(partitionId, out int partitionIdInt) && int.TryParse(objectId, out int objectIdInt))
            {
                if (serverId != string.Empty)
                {
                    if (!int.TryParse(serverId, out int serverIdInt))
                    {
                        Console.WriteLine("Unable to parse arguments");
                        Environment.Exit(-1);
                    }
                    client.ReadObject(partitionIdInt, objectIdInt, serverIdInt);
                }
                else
                {
                    client.ReadObject(partitionIdInt, objectIdInt, -1);
                }
            } else
            {
                Console.WriteLine("Unable to parse arguments");
                Environment.Exit(-1);
            }
           
        }
        static void Handle_write(string[] cmd, GSTOREClient client) {
            if (cmd.Length != 4) {
                Console.WriteLine("Invalid command format!");
                Console.WriteLine("Use: `write <partition_id> <object_id> <value>`");
                return;
            }

            string partitionId = cmd[1];
            string objectId = cmd[2];
            string value = cmd[3];

            // Console.WriteLine($"write {partitionId} {objectId} {value}");
            Console.WriteLine("write " + partitionId + " " + objectId + " " + value);

            if (int.TryParse(partitionId, out int partitionIdInt) && int.TryParse(objectId, out int objectIdInt))
            {
                client.WriteObject(partitionIdInt, objectIdInt, value);
            } else
            {
                Console.WriteLine("Unable to parse arguments");
                Environment.Exit(-1);
            }      
        }
        static void Handle_listServer(string[] cmd, GSTOREClient client) {
            if (cmd.Length != 2) {
                Console.WriteLine("Invalid command format!");
                Console.WriteLine("Use: `listServer <server_id>`");
                return;
            }

            string serverId = cmd[1];

            // Console.WriteLine($"listServer {serverId}");
            Console.WriteLine("listServer " + serverId);

            if(int.TryParse(serverId, out int serverIdInt))
            {
                client.ListServer(serverIdInt);
            } else
            {
                Console.WriteLine("Unable to parse server id");
                Environment.Exit(-1);
            }
        }
        static void Handle_listGlobal(string[] cmd, GSTOREClient client) {
            if (cmd.Length != 1) {
                Console.WriteLine("Invalid command format!");
                Console.WriteLine("Use: `listGlobal`");
                return;
            }

            Console.WriteLine("listGlobal");
            client.ListGlobal();
        }
        static void Handle_wait(string[] cmd, GSTOREClient client) {
            if (cmd.Length != 2) {
                Console.WriteLine("Invalid command format!");
                Console.WriteLine("Use: `wait <miliseconds>`");
                return;
            }

            string miliseconds = cmd[1];

            // Console.WriteLine($"listServer {miliseconds}");
            Console.WriteLine("wait " + miliseconds);
            if (!int.TryParse(miliseconds, out int n))
            {
                Console.WriteLine("Unable to parse miliseconds");
                Environment.Exit(-1);
            }
            Thread.Sleep(n);
        }

        static void Handle_repeat(string[] command, List<string[]> commands, System.IO.StreamReader file, GSTOREClient client) {
            if (!int.TryParse(command[1], out int n))
            {
                Console.WriteLine("Unable to parse repeat");
                Environment.Exit(-1);
            }

            Console.WriteLine("Iterating " + n + " times");

            for (var i = 1; i <= n; i++) {
                foreach (string[] cmd in commands) {
                    string[] tmp_command = new string[cmd.Length];
                    for (var arg_ix = 0; arg_ix < cmd.Length; arg_ix++) {
                        tmp_command[arg_ix] = cmd[arg_ix].Replace("$i", i.ToString());
                    }
                    CommandDispatcher(tmp_command, file, client);
                }
            }
        }

    }
}
