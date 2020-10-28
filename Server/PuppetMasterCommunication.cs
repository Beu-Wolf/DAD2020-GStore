using Grpc.Core;
using System;
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


        private readonly Dictionary<long, List<string>> ServersByPartition;
        private readonly HashSet<string> CrashedServers;
        private readonly List<long> MasteredPartitions;
        private readonly object FreezeLock;

        private readonly DelayMessagesInterceptor Interceptor;

        public PuppetMasterServerService(Dictionary<long, List<string>> serversByPartitions,
            List<long> masteredPartitions, HashSet<string> crashedServers, object freezeLock, DelayMessagesInterceptor interceptor)
        {
            ServersByPartition = serversByPartitions;
            MasteredPartitions = masteredPartitions;
            CrashedServers = crashedServers;
            FreezeLock = freezeLock;
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
            lock(FreezeLock)
            {
                Interceptor.FreezeCommands = true;
            }
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
            lock(FreezeLock)
            {
                Interceptor.FreezeCommands = false;
                Monitor.PulseAll(FreezeLock);
            }

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
