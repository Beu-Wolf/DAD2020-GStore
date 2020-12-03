using System;
using System.Threading;
using System.Collections.Generic;
using Grpc.Core;
using System.Linq;
using System.IO;

namespace Client
{

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

            var client = new GStoreClient(id);

            var server = new Grpc.Core.Server
            {
                Services =
                {
                    PuppetMasterClientGrpcService.BindService(new PMCommunicationService(client))
                },
                Ports = { new ServerPort(host, Port, ServerCredentials.Insecure) }

            };

            server.Start();

            // blocks until client receives network information
            client.WaitForNetworkInformation();


            try {
                string filePath = Directory.GetCurrentDirectory() + "\\" + args[2] + ".txt";
                Console.WriteLine("File Path: " + filePath);
                StreamReader file = new StreamReader(filePath);
                
                string line;
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

        static void CommandDispatcher(string[] cmd, System.IO.StreamReader file, GStoreClient client) {
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

        static void Handle_read(string[] cmd, GStoreClient client) {
            if (cmd.Length < 3) {
                Console.WriteLine("Invalid command format!");
                Console.WriteLine("Use: `read <partition_id> <object_id> [<server_id>]`");
                return;
            }

            string partitionId = cmd[1];
            string objectId = cmd[2];
            string serverId = string.Empty;
            if (cmd.Length == 4 && cmd[3] != "-1")
                serverId = cmd[3];

            
            client.ReadObject(partitionId, objectId, serverId);
        }

        static void Handle_write(string[] cmd, GStoreClient client) {
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
        
        static void Handle_listServer(string[] cmd, GStoreClient client) {
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
        
        static void Handle_listGlobal(string[] cmd, GStoreClient client) {
            if (cmd.Length != 1) {
                Console.WriteLine("Invalid command format!");
                Console.WriteLine("Use: `listGlobal`");
                return;
            }

            Console.WriteLine("listGlobal");
            client.ListGlobal();
        }
        
        static void Handle_wait(string[] cmd, GStoreClient client) {
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

        static void Handle_repeat(string[] command, List<string[]> commands, System.IO.StreamReader file, GStoreClient client) {
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
