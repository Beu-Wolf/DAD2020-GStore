using System;
using System.Diagnostics;
using System.Threading.Tasks;
using Grpc.Core;

namespace PCS
{
    class PCS
    {
        private string ServerPath;
        private string ClientPath;

        public PCS(string serverPath, string clientPath)
        {
            ServerPath = serverPath;
            ClientPath = clientPath;
        }

        public void LaunchServer(string host, int port)
        {
            Process.Start(ServerPath, host + " " + port);
        }

        public void LaunchClient(string scriptPath)
        {
            Process.Start(ClientPath, scriptPath);
        }
    }

    class Program
    {
        static void Main(string[] args)
        {
            if (args.Length != 2)
            {
                Console.WriteLine("Usage: pcs.exe <server path> <client path>");
                return;
            }

            const int PCS_PORT = 10000;

            string serverPath = args[0];
            string clientPath = args[1];

            PCS pcs = new PCS(serverPath, clientPath);

            AppContext.SetSwitch(
   "System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);

            PCSService pcsService = new PCSService(pcs);

            Grpc.Core.Server server = new Grpc.Core.Server
            {
                Services = { 
                    PCSGrpcService.BindService(pcsService) 
                },
                Ports = { new ServerPort("0.0.0.0", PCS_PORT, ServerCredentials.Insecure)}
            };

            server.Start();

            Console.WriteLine("Press any key to stop PCS...");
            Console.ReadKey();

            server.ShutdownAsync().Wait();
        }
    }

    class PCSService : PCSGrpcService.PCSGrpcServiceBase
    {
        private PCS Pcs;
        public PCSService(PCS pcs)
        {
            Pcs = pcs;
        }

        public override Task<LaunchClientReply> LaunchClient(LaunchClientRequest request, ServerCallContext context)
        {
            Pcs.LaunchClient(request.ScriptFile);
            return Task.FromResult(new LaunchClientReply { Ok = true });
        }

        public override Task<LaunchServerReply> LaunchServer(LaunchServerRequest request, ServerCallContext context)
        {
            Pcs.LaunchServer("0.0.0.0", request.Port);
            return Task.FromResult(new LaunchServerReply { Ok = true });
        }
    }
}
