using System;
using System.Threading;
using System.Collections.Generic;
using Grpc.Net.Client;
using Grpc.Core;
using System.Linq;
using System.Collections.Concurrent;
using System.IO;
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

    public class GSTOREClient
    {
        // Mapping of partitions and masters
        // URL of all servers

        private GrpcChannel Channel { get; set; }
        private ClientServerGrpcService.ClientServerGrpcServiceClient Client;

        private readonly ConcurrentDictionary<string, List<string>> ServersIdByPartition;
        private readonly ConcurrentDictionary<string, string> ServerUrls;
        private readonly ConcurrentBag<string> CrashedServers;
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


        public GSTOREClient(int id, ConcurrentDictionary<string, List<string>> serversIdByPartition, ConcurrentDictionary<string, string> serverUrls, ConcurrentBag<string> crashedServers)
        {
            Id = id;
            ServersIdByPartition = serversIdByPartition;
            ServerUrls = serverUrls;
            CrashedServers = crashedServers;
        }

        public void PrintTimes()
        {
            Console.WriteLine("Finished executing.");
            float avgRead = 0;
            float avgWrite = 0;

            if (NumReads > 0)
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

        public bool TryChangeCommunicationChannel(string server_id)
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

        public void ReadObject(string partition_id, string object_id, string server_id)
        {
            // Check if connected Server has requested partition

            if(ServersIdByPartition[partition_id].Count == 0)
            {
                Console.WriteLine($"No available server for partition {partition_id}");
                lock (writeCounterLock)
                {
                    NumReadFails++;
                }
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
            Stopwatch sw = new Stopwatch();
            sw.Start();
            try
            {
                var reply = Client.ReadObject(request);
                sw.Stop();
                lock (readCounterLock)
                {
                    NumReads += 1;
                }
                Console.WriteLine("Received: " + reply.Value);
            } catch (RpcException e)
            {
                sw.Stop();
                lock(readCounterLock)
                {
                    NumReadFails += 1;
                }
                // If error is because Server failed, update list of crashed Servers
                if (e.Status.StatusCode == StatusCode.Unavailable ||  e.Status.StatusCode == StatusCode.Internal)
                {
                    UpdateCrashedServersList();
                }

                Console.WriteLine($"Error: {e.Status.StatusCode}");
                Console.WriteLine($"Error message: {e.Status.Detail}");
                Console.WriteLine("N/A");
            }

            lock (readCounterLock)
            {
                ReadTotalTime += sw.ElapsedMilliseconds;
            }
        }

        public void WriteObject(string partition_id, string object_id, string value)
        {

            int currentServerPartitionIndex;
            List<string> ServersOfPartition = ServersIdByPartition[partition_id];

            if (ServersOfPartition.Count == 0)
            {
                Console.WriteLine($"No available servers for partition {partition_id}");
                lock (writeCounterLock)
                {
                    NumWriteFails++;
                }
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
            var crashedServers = new ConcurrentBag<string>();
            Stopwatch sw = new Stopwatch();
            sw.Start();
            while (!success && numTries < ServersOfPartition.Count)
            {
                try
                {
                    var reply = Client.WriteObject(request);
                    Console.WriteLine("Received: " + reply.Ok);
                    success = true;
                } catch (RpcException e)
                {
                    if (e.Status.StatusCode == StatusCode.PermissionDenied)
                    {
                        Console.WriteLine($"Cannot write in server {currentServerId}");
                    } else {
                        // If error is because Server failed, keep it
                        if (e.Status.StatusCode == StatusCode.Unavailable || e.Status.StatusCode == StatusCode.Internal)
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
                        currentServerPartitionIndex = (currentServerPartitionIndex+1) % ServersOfPartition.Count;
                        TryChangeCommunicationChannel(ServersOfPartition[currentServerPartitionIndex]);
                    }

                }
            }

            sw.Stop();
            lock (writeCounterLock)
            {
                if(!success)
                {
                    NumWriteFails += 1;
                } else
                {
                    WriteTotalTime += sw.ElapsedMilliseconds;
                }
                NumWrites += 1;
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

            if (args.Length != 4)
            {
                Console.WriteLine("Usage: Client.exe <host> <port> <script_file> <id>");
                return;
            }


            if (!int.TryParse(args[1], out int Port))
            {
                Console.WriteLine("Invalid port value");
                return;
            }

            if (!int.TryParse(args[3], out int id))
            {
                Console.WriteLine("Invalid id");
                return;
            }

            AppContext.SetSwitch(
    "System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);

            string host = args[0];

            var serverIdsByPartition = new ConcurrentDictionary<string, List<string>>();

            var serverUrls = new ConcurrentDictionary<string, string>();

            var crashedServers = new ConcurrentBag<string>();

            var client = new GSTOREClient(id, serverIdsByPartition, serverUrls, crashedServers);

            BoolWrapper continueExecution = new BoolWrapper(false);

            var server = new Grpc.Core.Server
            {
                Services =
                {
                    PuppetMasterClientGrpcService.BindService(new PuppetMasterCommunicationService(serverIdsByPartition, serverUrls, crashedServers, continueExecution))
                },
                Ports = { new ServerPort(host, Port, ServerCredentials.Insecure) }

            };

            server.Start();

            // Lock until information is received
            lock (continueExecution.WaitForInformationLock)
            {
                while (!continueExecution.Value) Monitor.Wait(continueExecution.WaitForInformationLock);
            }

            try {
                string line;

                string filePath = Directory.GetCurrentDirectory() + "\\" + args[2] + ".txt";
                Console.WriteLine("File Path: " + filePath); 

                System.IO.StreamReader file = new System.IO.StreamReader(filePath);
                while ((line = file.ReadLine()) != null) 
                {
                    string[] cmd = line.Split((char[])null, StringSplitOptions.RemoveEmptyEntries);
                    CommandDispatcher(cmd, file, client);
                }

                file.Close();

                client.PrintTimes();

                // We need to stay up, in order to respond to status commands by the Puppet Master
                // Start gRPC server of connection with PM
                // For now, just wait for user input
                Console.ReadKey();



            } catch (System.IO.FileNotFoundException) {
                Console.WriteLine("File not found. Exiting...");
            } finally
            {
                Console.ReadKey();
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


            
            if (serverId != string.Empty)
            {
                client.ReadObject(partitionId, objectId, serverId);
            }
            else
            {
                client.ReadObject(partitionId, objectId, "-1");
            }
            
           
        }
        static void Handle_write(string[] cmd, GSTOREClient client) {
            if (cmd.Length < 4) {
                Console.WriteLine("Invalid command format!");
                Console.WriteLine("Use: `write <partition_id> <object_id> <value>`");
                return;
            }

            // Join value
            string value = String.Join(' ', cmd.Skip(3));

            // Verify if bigger then 4

            string partitionId = cmd[1];
            string objectId = cmd[2];

            // Console.WriteLine($"write {partitionId} {objectId} {value}");
            Console.WriteLine("write " + partitionId + " " + objectId + " " + value);

            
            client.WriteObject(partitionId, objectId, value);
                 
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

            client.ListServer(serverId);
            
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
