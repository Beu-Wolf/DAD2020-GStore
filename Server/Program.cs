using System;
using Grpc.Core;

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

            int Port = 10001;
            string host = "localhost"; // Maybe pass as parameter when instanciating server

            // Dictionary with <server_id, URL>

            Grpc.Core.Server server = new Grpc.Core.Server
            {
                Services = { ClientServerGrpcService.BindService(new ClientServerService())},
                Ports = { new ServerPort(host, Port, ServerCredentials.Insecure)}
            };

            server.Start();

            Console.WriteLine("Press any key to stop the server...");
            Console.ReadKey();

            server.ShutdownAsync().Wait();
        }
    }
}
