using Grpc.Core;
using Grpc.Core.Interceptors;
using System;
using System.Collections.Generic;
using System.Reflection.Metadata.Ecma335;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Server
{
    public class DelayMessagesInterceptor : Interceptor
    {
        private readonly int MinDelay;
        private readonly int MaxDelay;

        private readonly object FreezeLock;
        private Boolean freezeCommands;

        public Boolean FreezeCommands { get => freezeCommands;  
            set {
                lock (FreezeLock)
                {
                    freezeCommands = value;
                    if (!value) Monitor.PulseAll(FreezeLock);
                }
            }  
        }

        private readonly Random Rnd = new Random();

        public DelayMessagesInterceptor(int minDelay, int maxDelay, object freezeLock)
        {
            MinDelay = minDelay;
            MaxDelay = maxDelay;
            FreezeLock = freezeLock;
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
