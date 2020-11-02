using Grpc.Core;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Server
{
    public class PuppetMasterServerService : PuppetMasterServerGrpcService.PuppetMasterServerGrpcServiceBase
    {
        public int MyPort { get; set; }
        public string MyHost { get; set; }


        private readonly ConcurrentDictionary<string, List<string>> ServersByPartition;
        private readonly ConcurrentBag<string> CrashedServers;
        private readonly List<string> MasteredPartitions;

        private readonly DelayMessagesInterceptor Interceptor;

        public PuppetMasterServerService(ConcurrentDictionary<string, List<string>> serversByPartitions,
            List<string> masteredPartitions, ConcurrentBag<string> crashedServers, DelayMessagesInterceptor interceptor)
        {
            ServersByPartition = serversByPartitions;
            MasteredPartitions = masteredPartitions;
            CrashedServers = crashedServers;
            Interceptor = interceptor;

        }

        public override Task<CrashReply> Crash(CrashRequest request, ServerCallContext context)
        {
            Environment.Exit(1);
            return Task.FromResult(new CrashReply
            {
                Success = false
            }); 
        }

        public override Task<FreezeReply> Freeze(FreezeRequest request, ServerCallContext context)
        {
            return Task.FromResult(FreezeServer());
        }

        public FreezeReply FreezeServer()
        {
            Interceptor.FreezeCommands = true;
            return new FreezeReply
            {
                Success = true
            };
        }

        public override Task<UnfreezeReply> Unfreeze(UnfreezeRequest request, ServerCallContext context)
        {
            return Task.FromResult(UnfreezeServer());
        }

        public UnfreezeReply UnfreezeServer()
        {
            Interceptor.FreezeCommands = false;
            return new UnfreezeReply
            {
                Success = true
            };
        }


        public override Task<StatusReply> Status(StatusRequest request, ServerCallContext context)
        {
            return Task.FromResult(PrintStatus());
        }

        public StatusReply PrintStatus()
        {
            Console.WriteLine("Online Servers");
            foreach (var server in ServersByPartition)
            {
                Console.WriteLine($"Server {server.Value} from partition {server.Key}");
            }
            Console.WriteLine("Crashed Servers");
            foreach (var server in CrashedServers)
            {
                Console.WriteLine($"Server {server}");
            }
            return new StatusReply
            {
                Success = true
            };
        }

    }
}
