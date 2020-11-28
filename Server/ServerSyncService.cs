using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Grpc.Core;
using Grpc.Core.Utils;
using System.Linq;
using System.Threading;
using System.Collections.Concurrent;

namespace Server
{

    public class ServerSyncService : ServerSyncGrpcService.ServerSyncGrpcServiceBase
    {

        private GStoreServer Server { get; }

        public ServerSyncService(GStoreServer server)
        {
            Server = server;
        }

        public override Task<PropagateWriteResponse> PropagateWrite(PropagateWriteRequest request, ServerCallContext context)
        {
            return Task.FromResult(Server.PropagateWrite(request));
        }

        public override Task<HeartbeatResponse> Heartbeat(HeartbeatRequest request, ServerCallContext context)
        {
            return Task.FromResult(Server.Heartbeat());
        }

        public override Task<ReportCrashResponse> ReportCrash(ReportCrashRequest request, ServerCallContext context)
        {
            return Task.FromResult(Server.ReportCrash(request));
        }


        //public override Task<LockObjectReply> LockObject(LockObjectRequest request, ServerCallContext context)
        //{
        //    return Task.FromResult(Server.LockObject(request));
        //}

        //public override Task<ReleaseObjectLockReply> ReleaseObjectLock(ReleaseObjectLockRequest request, ServerCallContext context)
        //{
        //    return Task.FromResult(Server.ReleaseObjectLock(request));
        //}

        //public override Task<RemoveCrashedServersReply> RemoveCrashedServers(RemoveCrashedServersRequest request, ServerCallContext context)
        //{
        //    return Task.FromResult(Server.RemoveCrashedServers(request));
        //}

    }
}
