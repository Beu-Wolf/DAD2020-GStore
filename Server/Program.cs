using System;
using Grpc.Core;
using Grpc.Net.Client;

namespace Server
{
    public class Program
    {

        // My partitions
        // Partitions which I'm master of
        // Select random port to be in, like 10000 + random(partition which I'm master)

        // List of other servers

        static void Main(string[] args)
        {
            if (args.Length != 2)
            {
                Console.WriteLine("Usage: Server.exe host port");
                return;
            }


            if(!int.TryParse(args[1], out int Port))
            {
                Console.WriteLine("Invalid port value");
                return;
            }

            string host = args[0]; // Maybe pass as parameter when instanciating server

            // Dictionary with <server_id, URL>

            Grpc.Core.Server server = new Grpc.Core.Server
            {
                Services = { 
                    ClientServerGrpcService.BindService(new ClientServerService()), 
                    ServerSyncGrpcService.BindService(new ServerSyncService())
                },
                Ports = { new ServerPort(host, Port, ServerCredentials.Insecure)}
            };

            server.Start();

            Console.WriteLine("Press any key to stop the server...");
            Console.ReadKey();

            server.ShutdownAsync().Wait();
        }
    }
}
