using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.NetworkInformation;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

using TcpServiceSample.Libs;

namespace TcpServiceSample
{
    public class Worker5 : BackgroundService
    {
        private readonly ILogger<Worker5> _logger;

        const int Port = 8888;
        private Socket listenSocket;

        private int m_maxConnectNum;    //最大连接数  
        private int m_revBufferSize;    //最大接收字节数  
        BufferManager m_bufferManager; //处理信息的工具
        const int opsToAlloc = 2;

        SocketEventPool m_pool;
        volatile int m_clientCount;              //连接的客户端数量  
        Semaphore m_maxNumberAcceptedClients;
        List<AsyncUserToken> m_clients; //客户端列表  

        CancellationToken _stoppingToken;

        public Worker5(ILogger<Worker5> logger)
        {
            _logger = logger;

            m_clientCount = 0;
            m_maxConnectNum = 1000;
            m_revBufferSize = 1024;
            m_bufferManager = new BufferManager(m_revBufferSize * m_maxConnectNum * opsToAlloc, m_revBufferSize);
            m_pool = new SocketEventPool(m_maxConnectNum);
            m_maxNumberAcceptedClients = new Semaphore(m_maxConnectNum, m_maxConnectNum);
        }

        public override Task StartAsync(CancellationToken cancellationToken)
        {
            m_bufferManager.InitBuffer();
            m_clients = new List<AsyncUserToken>();
            SocketAsyncEventArgs readWriteEventArg;
            for (int i = 0; i < m_maxConnectNum; i++)
            {
                readWriteEventArg = new SocketAsyncEventArgs();
                readWriteEventArg.Completed += new EventHandler<SocketAsyncEventArgs>(IO_Completed);
                readWriteEventArg.UserToken = new AsyncUserToken();

                m_bufferManager.SetBuffer(readWriteEventArg);
                m_pool.Push(readWriteEventArg);
            }

            IPEndPoint listenEndpoint = new IPEndPoint(IPAddress.Any, Port);
            listenSocket = new Socket(listenEndpoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
            listenSocket.Bind(listenEndpoint);

            listenSocket.Listen(m_maxConnectNum);

            _logger.LogInformation($"开始监听: 0.0.0.0:{Port}");

            return base.StartAsync(cancellationToken);
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _stoppingToken = stoppingToken;
            try
            {
                StartAccept(null);

                #region
                //await Task.Factory.StartNew(async () =>
                //{
                //    while (!stoppingToken.IsCancellationRequested)
                //    {
                //        var checkItems = m_pool.GetItems();

                //        Dictionary<string, TcpState> tcpConnectionStates = new Dictionary<string, TcpState>(StringComparer.OrdinalIgnoreCase);
                //        foreach (var tcpConnection in IPGlobalProperties.GetIPGlobalProperties().GetActiveTcpConnections())
                //        {
                //            tcpConnectionStates[$"{tcpConnection.RemoteEndPoint}:{tcpConnection.LocalEndPoint}"] = tcpConnection.State;
                //        }

                //        for (int i = 0; i < checkItems.Length; i++)
                //        {
                //            try
                //            {
                //                var checkItem = checkItems[i];
                //                AsyncUserToken token = checkItem.UserToken as AsyncUserToken;
                //                if (token.Socket == null) continue;

                //                IPEndPoint clientAddress = token.Socket.RemoteEndPoint as IPEndPoint;
                //                EndPoint localEndPoint = token.Socket.LocalEndPoint;

                //                if (tcpConnectionStates.TryGetValue($"{clientAddress}:{localEndPoint}", out TcpState tcpState) == false)
                //                {
                //                    tcpState = TcpState.Unknown;
                //                }

                //                if (tcpState == TcpState.Unknown
                //                    || tcpState == TcpState.Closed
                //                    || tcpState == TcpState.CloseWait
                //                    || tcpState == TcpState.Closing
                //                    || tcpState == TcpState.FinWait1
                //                    || tcpState == TcpState.FinWait2
                //                    || tcpState == TcpState.LastAck
                //                    || tcpState == TcpState.TimeWait)
                //                {
                //                    CloseClientSocket(checkItem);
                //                    _logger.LogInformation($"{localEndPoint} <=> {clientAddress} 断开连接");
                //                }
                //            }
                //            catch (Exception ex)
                //            {
                //                _logger.LogError(ex, ex.Message);
                //            }
                //        }

                //        tcpConnectionStates.Clear();

                //        await Task.Delay(TimeSpan.FromSeconds(30), stoppingToken);
                //    }
                //}, stoppingToken, TaskCreationOptions.LongRunning, TaskScheduler.Current).Unwrap(); 
                #endregion

                try
                {
                    await Task.Delay(Timeout.Infinite, stoppingToken);
                }
                catch (TaskCanceledException)
                {
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, ex.Message);
            }
        }

        public void StartAccept(SocketAsyncEventArgs acceptEventArg)
        {
            if (acceptEventArg == null)
            {
                acceptEventArg = new SocketAsyncEventArgs();
                acceptEventArg.Completed += new EventHandler<SocketAsyncEventArgs>(AcceptEventArg_Completed);
            }
            else
            {
                acceptEventArg.AcceptSocket = null; // 重用,必须设为null
            }

            m_maxNumberAcceptedClients.WaitOne();
            if (!listenSocket.AcceptAsync(acceptEventArg))
            {
                ProcessAccept(acceptEventArg);
            }
        }

        private void ProcessAccept(SocketAsyncEventArgs acceptEventArg)
        {
            try
            {
                if (acceptEventArg.AcceptSocket == null) return;

                Interlocked.Increment(ref m_clientCount);

                IPEndPoint clientAddress = acceptEventArg.AcceptSocket.RemoteEndPoint as IPEndPoint;
                EndPoint localEndPoint = acceptEventArg.AcceptSocket.LocalEndPoint;
                _logger.LogInformation($"{localEndPoint} <=> {clientAddress} 连接");

                SocketAsyncEventArgs readEventArgs = m_pool.Pop();
                AsyncUserToken userToken = (AsyncUserToken)readEventArgs.UserToken;
                userToken.Socket = acceptEventArg.AcceptSocket;
                userToken.ConnectTime = DateTime.Now;
                userToken.Remote = acceptEventArg.AcceptSocket.RemoteEndPoint;
                userToken.Local = ((IPEndPoint)(acceptEventArg.AcceptSocket.LocalEndPoint)).Address;

                lock (m_clients) { m_clients.Add(userToken); }

                if (!acceptEventArg.AcceptSocket.ReceiveAsync(readEventArgs))
                {
                    ProcessReceive(readEventArgs);
                }

                //while (_stoppingToken.IsCancellationRequested == false)
                //{
                //    AsyncUserToken token = (AsyncUserToken)readEventArgs.UserToken;
                //    if (token.Stop) break;

                //    if (!token.Socket.ReceiveAsync(readEventArgs))
                //        ProcessReceive(readEventArgs);
                //}
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, ex.Message);
            }

            if (acceptEventArg.SocketError == SocketError.OperationAborted) return;
            StartAccept(acceptEventArg);
        }

        private void AcceptEventArg_Completed(object sender, SocketAsyncEventArgs e)
        {
            //IPEndPoint clientAddress = e.AcceptSocket.RemoteEndPoint as IPEndPoint;
            //EndPoint localEndPoint = e.AcceptSocket.LocalEndPoint;
            //_logger.LogInformation($"{localEndPoint} <=> {clientAddress}");
            ProcessAccept(e);
        }

        void IO_Completed(object sender, SocketAsyncEventArgs args)
        {
            switch (args.LastOperation)
            {
                case SocketAsyncOperation.Receive:
                    ProcessReceive(args);
                    //while (_stoppingToken.IsCancellationRequested == false) 
                    //{
                    //    AsyncUserToken token = (AsyncUserToken)args.UserToken;
                    //    if (token.Stop) break;

                    //    if (!token.Socket.ReceiveAsync(args))
                    //        ProcessReceive(args);
                    //}
                    break;
                case SocketAsyncOperation.Send:
                    ProcessSend(args);
                    break;
                //case SocketAsyncOperation.None:
                //case SocketAsyncOperation.Accept:
                //case SocketAsyncOperation.Connect:
                //case SocketAsyncOperation.Disconnect:
                //case SocketAsyncOperation.ReceiveFrom:
                //case SocketAsyncOperation.ReceiveMessageFrom:
                //case SocketAsyncOperation.SendPackets:
                //case SocketAsyncOperation.SendTo:
                default:
                    throw new ArgumentException("The last operation completed on the socket was not a receive or send");
            }
        }

        private void ProcessSend(SocketAsyncEventArgs args)
        {
            try
            {
                AsyncUserToken token = args.UserToken as AsyncUserToken;
                token.Socket.SendAsync(args);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, ex.Message);
            }
        }

        public void SendMessage(AsyncUserToken token, byte[] message)
        {
            if (token == null || token.Socket == null || !token.Socket.Connected)
                return;

            try
            {
                using SocketAsyncEventArgs sendArg = new SocketAsyncEventArgs();
                sendArg.UserToken = token;
                sendArg.SetBuffer(message, 0, message.Length);
                token.Socket.SendAsync(sendArg);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, ex.Message);
            }
        }

        private void ProcessReceive(SocketAsyncEventArgs args)
        {
            AsyncUserToken token = (AsyncUserToken)args.UserToken;
            try
            {
                if (args.BytesTransferred > 0 && args.SocketError == SocketError.Success)
                {
                    byte[] data = new byte[args.BytesTransferred];
                    Array.Copy(args.Buffer, args.Offset, data, 0, args.BytesTransferred);

                    //lock (token.Buffer)
                    //{
                    //    token.Buffer.AddRange(data);
                    //}

                    ReceiveClientData(token, data);

                    if (!token.Socket.ReceiveAsync(args))
                        ProcessReceive(args);

                    //token.Stop = args.SocketError == SocketError.OperationAborted;
                }
                else
                {
                    //token.Stop = true;
                    CloseClientSocket(args);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, ex.Message);
            }
        }

        private void CloseClientSocket(SocketAsyncEventArgs args)
        {
            AsyncUserToken token = args.UserToken as AsyncUserToken;

            lock (m_clients) { m_clients.Remove(token); }
            try
            {
                token.Socket.Shutdown(SocketShutdown.Send);
            }
            catch (Exception) { }
            token.Socket.Close();
            token.Socket = null;
            Interlocked.Decrement(ref m_clientCount);
            m_maxNumberAcceptedClients.Release();
            args.UserToken = new AsyncUserToken();
            m_pool.Push(args);

            _logger.LogInformation($"{token.Local} <=> {token.Remote} 断开连接");
        }

        private void ReceiveClientData(AsyncUserToken token, byte[] data)
        {
            string reciveMsg = Encoding.UTF8.GetString(data);
            _logger.LogInformation($"接收: {reciveMsg}");

            string sendMsg = $"回复: {reciveMsg}, 发送时间{DateTime.Now}";
            SendMessage(token, Encoding.UTF8.GetBytes(sendMsg));
        }

        public override Task StopAsync(CancellationToken cancellationToken)
        {
            foreach (AsyncUserToken token in m_clients.ToArray())
            {
                try
                {
                    if (token.Socket == null) continue;

                    if (token.Socket.Connected)
                        token.Socket.Shutdown(SocketShutdown.Both);
                    token.Socket.Dispose();
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, ex.Message);
                }
            }

            listenSocket.Close();
            lock (m_clients) { m_clients.Clear(); }

            _logger.LogInformation("停止监听");

            return base.StopAsync(cancellationToken);
        }
    }
}

// C# 高性能 Socket 服务器 SocketAsyncEventArgs 的实现 (IOCP)
// https://blog.csdn.net/zhujunxxxxx/article/details/43573879/

// C# SocketAsyncEventArgs 高性能Socket代码
// https://www.php.cn/csharp-article-345509.html

// https://github.com/search?l=C%23&q=IOCP&type=Repositories
// https://github.com/danielpalme/IocPerformance
// https://github.com/fengma312/socket.core
// https://github.com/yswenli/GFF
// https://github.com/miniboom360/IOCP_UDP-TCP

// 基于SocketAsyncEventArgs(IOCP)的高性能TCP服务器实现（一）——封装SocketAsyncEventArgs
// https://blog.csdn.net/aplsc/article/details/101360660