using Grpc.Core;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Client
{
    class PuppetMasterCommunicationService : PuppetMasterClientGrpcService.PuppetMasterClientGrpcServiceBase
    {
        private readonly ConcurrentDictionary<long, List<int>> ServersIdByPartition;
        private readonly ConcurrentBag<int> CrashedServers;

        public PuppetMasterCommunicationService(ConcurrentDictionary<long, List<int>> serversIdByPartition, ConcurrentBag<int> crashedServers)
        {
            ServersIdByPartition = serversIdByPartition;
            CrashedServers = crashedServers;
        }

        public override Task<StatusReply> Status(StatusRequest request, ServerCallContext context)
        {
            return Task.FromResult(ClientStatus());
        }

        public StatusReply ClientStatus()
        {
            Console.WriteLine("Online Servers");
            foreach (var server in ServersIdByPartition)
            {
                Console.WriteLine($"Server {server.Value} from partition {server.Key}");
            }
            Console.WriteLine("Crashed Servers");
            foreach (var server in CrashedServers)
            {
                Console.WriteLine($"Server {server}");
            }
            return new StatusReply();
        }

    }
}
