using Grpc.Core;
using Grpc.Core.Interceptors;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace Server
{
    public class DelayMessagesInterceptor : Interceptor
    {
        private readonly int MinDelay;
        private readonly int MaxDelay;

        private readonly object FreezeLock = new object();
        private bool freezeCommands;

        public bool FreezeCommands { get => freezeCommands;  
            set {
                lock (FreezeLock)
                {
                    freezeCommands = value;
                    if (!value) Monitor.PulseAll(FreezeLock);
                }
            }  
        }

        private readonly Random Rnd = new Random();

        public DelayMessagesInterceptor(int minDelay, int maxDelay)
        {
            MinDelay = minDelay;
            MaxDelay = maxDelay;
            FreezeCommands = false;
        }

        public override Task<TResponse> UnaryServerHandler<TRequest, TResponse>(TRequest request, ServerCallContext context, UnaryServerMethod<TRequest, TResponse> continuation)
        {
            lock (FreezeLock)
            {
                while (FreezeCommands) Monitor.Wait(FreezeLock);
            }

            Thread.Sleep(Rnd.Next(MinDelay, MaxDelay));
            return continuation(request, context);
        }
    }
}
