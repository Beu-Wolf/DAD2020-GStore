using System.Threading.Tasks;
using Grpc.Core;

namespace Server
{
    public class GStoreService : ClientServerGrpcService.ClientServerGrpcServiceBase
    {
        private GStoreServer Server { get; }

        public GStoreService(GStoreServer server)
        {
            Server = server;
        }

        // Read Object
        public override Task<ReadObjectReply> ReadObject(ReadObjectRequest request, ServerCallContext context )
        {
            return Task.FromResult(Server.Read(request));
        }

        // Write Object
        public override Task<WriteObjectReply> WriteObject(WriteObjectRequest request, ServerCallContext context)
        {
            return Task.FromResult(Server.Write(request));
        }

        // List Server
        public override Task<ListServerReply> ListServer(ListServerRequest request, ServerCallContext context)
        {
            return Task.FromResult(Server.ListServer(request));
        }

        // Call to List Global
        public override Task<ListGlobalReply> ListGlobal(ListGlobalRequest request, ServerCallContext context)
        {
            return Task.FromResult(Server.ListGlobal(request));
        }
    }
}
