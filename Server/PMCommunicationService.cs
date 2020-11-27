using Grpc.Core;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Server
{
    public class PMCommunicationService : PuppetMasterServerGrpcService.PuppetMasterServerGrpcServiceBase
    {
        private GStoreServer Server;

        public PMCommunicationService(GStoreServer server)
        {
            Server = server;
        }

        public override Task<CrashReply> Crash(CrashRequest request, ServerCallContext context)
        {
            Environment.Exit(1);

            return Task.FromResult(Server.Crash());
        }

        public override Task<FreezeReply> Freeze(FreezeRequest request, ServerCallContext context)
        {
            return Task.FromResult(Server.Freeze());
        }

        public override Task<UnfreezeReply> Unfreeze(UnfreezeRequest request, ServerCallContext context)
        {
            return Task.FromResult(Server.Unfreeze());
        }


        public override Task<ServerStatusReply> Status(ServerStatusRequest request, ServerCallContext context)
        {
            return Task.FromResult(Server.Status());
        }

        public override Task<ServerPingReply> Ping(ServerPingRequest request, ServerCallContext context)
        {
            return Task.FromResult(Server.Ping());
        }

        public override Task<PartitionSchemaReply> PartitionSchema(PartitionSchemaRequest request, ServerCallContext context)
        {
            return Task.FromResult(Server.PartitionSchema(request));
        }
    }
}
