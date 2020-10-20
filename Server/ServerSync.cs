using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Grpc.Core;
using Grpc.Core.Utils;

namespace Server
{

    public class ServerSyncService : ServerSyncGrpcService.ServerSyncGrpcServiceBase
    {
        // Dict with all values

        public override Task<LockObjectReply> LockObject(LockObjectRequest request, ServerCallContext context)
        {
            return Task.FromResult(Lock(request));
        }

        public LockObjectReply Lock(LockObjectRequest request)
        {
            Console.WriteLine("Received LockObjectRequest with params:");
            Console.Write($"Key: \r\n PartitionId: {request.Key.PartitionId} \r\n ObjectId: {request.Key.ObjectId}\r\n");
            return new LockObjectReply
            {
                Ack = false
            };
        }

        public override Task<ReleaseObjectLockReply> ReleaseObjectLock(ReleaseObjectLockRequest request, ServerCallContext context)
        {
            return Task.FromResult(ReleaseObject(request));
        }

        public ReleaseObjectLockReply ReleaseObject(ReleaseObjectLockRequest request)
        {
            Console.WriteLine("Received LockObjectRequest with params:");
            Console.Write($"Key: \r\n PartitionId: {request.Key.PartitionId} \r\n ObjectId: {request.Key.ObjectId}\r\n");
            Console.WriteLine("Value: " + request.Value);

            return new ReleaseObjectLockReply
            {
                Ack = false
            };
        }


    }
}
