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

namespace TcpServiceSample
{
    public class Worker4 : BackgroundService
    {
        private readonly ILogger<Worker4> _logger;

        const int Port = 8888;
        private volatile int ClientCounter = default;
        private TcpListener _tcpListener;
        private static byte[] DefaultResult = new byte[0];

        private readonly List<ClientPoolItem> _clientPoolItems;

        public Worker4(ILogger<Worker4> logger)
        {
            _logger = logger;
            _clientPoolItems = new List<ClientPoolItem>(1000);
        }

        public override Task StartAsync(CancellationToken cancellationToken)
        {
            _tcpListener = new TcpListener(IPAddress.Any, Port);
            _tcpListener.Start();

            _logger.LogInformation($"开始监听: 0.0.0.0:{Port}");

            return base.StartAsync(cancellationToken);
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            Task workerTask = Task.Factory.StartNew(async () =>
            {
                while (!stoppingToken.IsCancellationRequested)
                {
                    try
                    {
                        TcpClient tcpClient = await _tcpListener.AcceptTcpClientAsync();
                        Task task = AcceptTcpClientAsync(tcpClient, stoppingToken);

                        lock(this)
                        {
                            _clientPoolItems.Add(new ClientPoolItem
                            {
                                TcpClient = tcpClient,
                                Worker = task
                            });
                        }
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, ex.Message);
                    }
                }
            }, stoppingToken, TaskCreationOptions.LongRunning, TaskScheduler.Current).Unwrap();

            Task checkTask = Task.Factory.StartNew(async () =>
            {
                while (!stoppingToken.IsCancellationRequested)
                {
                    var checkItems = _clientPoolItems.ToArray();

                    Dictionary<string, TcpState> tcpConnectionStates = new Dictionary<string, TcpState>(StringComparer.OrdinalIgnoreCase);
                    foreach (var tcpConnection in IPGlobalProperties.GetIPGlobalProperties().GetActiveTcpConnections())
                    {
                        tcpConnectionStates[$"{tcpConnection.RemoteEndPoint}:{tcpConnection.LocalEndPoint}"] = tcpConnection.State;
                    }

                    for (int i = 0; i < checkItems.Length; i++)
                    {
                        try
                        {
                            var checkItem = checkItems[i];
                            if (checkItem.TcpClient.Connected == false || checkItem.TcpClient.Client == null)
                            {
                                checkItem.TcpClient.Dispose();
                                lock(this)
                                {
                                    _clientPoolItems.Remove(checkItem);
                                }
                                continue;
                            }

                            if(tcpConnectionStates.TryGetValue($"{checkItem.TcpClient.Client.RemoteEndPoint}:{checkItem.TcpClient.Client.LocalEndPoint}", out TcpState tcpState) == false)
                            {
                                tcpState = TcpState.Unknown;
                            }

                            if (tcpState == TcpState.Unknown
                                || tcpState == TcpState.Closed
                                || tcpState == TcpState.CloseWait
                                || tcpState == TcpState.Closing
                                || tcpState == TcpState.FinWait1
                                || tcpState == TcpState.FinWait2
                                || tcpState == TcpState.LastAck
                                || tcpState == TcpState.TimeWait)
                            {
                                checkItem.TcpClient.Close();
                                checkItem.TcpClient.Dispose();
                                lock (this)
                                {
                                    _clientPoolItems.Remove(checkItem);
                                }
                            }
                        }
                        catch (Exception ex)
                        {
                            _logger.LogError(ex, ex.Message);
                        }
                    }

                    tcpConnectionStates.Clear();

                    await Task.Delay(TimeSpan.FromSeconds(30), stoppingToken);
                }
            }, stoppingToken, TaskCreationOptions.LongRunning, TaskScheduler.Current);

            await Task.WhenAll(workerTask, checkTask);
        }

        async Task AcceptTcpClientAsync(TcpClient tcpClient, CancellationToken stoppingToken)
        {
            _logger.LogInformation($"客户端数量: {++ClientCounter}");
            IPEndPoint clientAddress = tcpClient.Client.RemoteEndPoint as IPEndPoint;
            EndPoint localEndPoint = tcpClient.Client.LocalEndPoint;
            _logger.LogInformation($"{clientAddress} 接入");

            tcpClient.ReceiveTimeout = 10;
            tcpClient.SendTimeout = 10;
            tcpClient.LingerState = new LingerOption(true, 1);

            //using CancellationTokenSource cancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);

            Task workerTask = Task.Factory.StartNew(() =>
            {
                Memory<byte> buffer = new byte[1024];
                while (!stoppingToken.IsCancellationRequested)
                {
                    if (tcpClient.Client == null || tcpClient.Connected == false)
                    {
                        //cancellationTokenSource.Cancel();
                        return;
                    }

                    using SocketAsyncEventArgs receiveArgs = new SocketAsyncEventArgs()
                    {
                        RemoteEndPoint = clientAddress,
                        AcceptSocket = tcpClient.Client
                    };
                    receiveArgs.Completed += ReciveCompleted;
                    receiveArgs.SetBuffer(buffer);

                    SendAndReceive(tcpClient, receiveArgs);
                }
            }, stoppingToken);

            //Task checkTask = Task.Factory.StartNew(async () =>
            //{
            //    while (!stoppingToken.IsCancellationRequested)
            //    {
            //        //if (CheckConnection(localEndPoint) == false) // 检测tcp连接
            //        //{
            //        //    tcpClient.Close();
            //        //    return;
            //        //}

            //        var info = IPGlobalProperties.GetIPGlobalProperties().GetActiveTcpConnections().FirstOrDefault(x => x.LocalEndPoint.Equals(localEndPoint));
            //        var tcpState = info != null ? info.State : TcpState.Unknown;
            //        if (tcpState == TcpState.Unknown
            //            || tcpState == TcpState.Closed
            //            || tcpState == TcpState.CloseWait
            //            || tcpState == TcpState.Closing
            //            || tcpState == TcpState.FinWait1
            //            || tcpState == TcpState.FinWait2
            //            || tcpState == TcpState.LastAck
            //            || tcpState == TcpState.TimeWait)
            //        {
            //            tcpClient.Close();
            //            return;
            //        }
            //        await Task.Delay(5000, cancellationTokenSource.Token);
            //    }
            //}, cancellationTokenSource.Token);

            try
            {
                //await Task.WhenAll(workerTask, checkTask);
                await workerTask;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, ex.Message);
            }
            finally
            {
                if (tcpClient.Connected)
                {
                    tcpClient.Close();
                }

                _logger.LogInformation($"客户端数量: {--ClientCounter}");
                _logger.LogInformation($"{clientAddress} 断开");
            }
        }

        void SendAndReceive(TcpClient tcpClient, SocketAsyncEventArgs receiveArgs)
        {
            if (tcpClient.Client == null) return;
            try
            {
                if (!tcpClient.Client.ReceiveAsync(receiveArgs))
                {
                    ReciveCompleted(tcpClient, receiveArgs);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, ex.Message);
            }
        }

        void ReciveCompleted(object sender, SocketAsyncEventArgs args)
        {
            if (args.SocketError == SocketError.Success && args.BytesTransferred > 0)
            {
                Memory<byte> buffer = args.MemoryBuffer.Slice(0, args.BytesTransferred);

                string requestMsg = Encoding.UTF8.GetString(buffer.Span);
                _logger.LogInformation($"接收:\n\t{requestMsg}\n");

                byte[] responseBuffer = Encoding.UTF8.GetBytes($"{requestMsg}; 响应时间:{DateTime.Now}");
                args.AcceptSocket.Send(responseBuffer);
            }
        }

        bool CheckConnection(EndPoint localEndPoint)
        {
            TcpState tcpState = GetState(localEndPoint);
            //string stateMessage = $"Tcp Client({tcpClient.Client.RemoteEndPoint as IPEndPoint}) State: {tcpState}; Client Counter: {ClientCounter}";
            //_logger.LogInformation(stateMessage);

            if (tcpState == TcpState.Unknown
                        || tcpState == TcpState.Closed
                        || tcpState == TcpState.CloseWait
                        || tcpState == TcpState.Closing
                        || tcpState == TcpState.FinWait1
                        || tcpState == TcpState.FinWait2
                        || tcpState == TcpState.LastAck
                        || tcpState == TcpState.TimeWait)
            {
                return false;
            }

            return true;
        }

        static TcpState GetState(EndPoint localEndPoint)
        {
            var foo = IPGlobalProperties.GetIPGlobalProperties()
              .GetActiveTcpConnections()
              .FirstOrDefault(x => x.LocalEndPoint.Equals(localEndPoint));
            return foo != null ? foo.State : TcpState.Unknown;
        }

        public override Task StopAsync(CancellationToken cancellationToken)
        {
            _tcpListener?.Stop();
            var checkItems = _clientPoolItems.ToArray();

            for (int i = 0; i < checkItems.Length; i++)
            {
                try
                {
                    var checkItem = checkItems[i];
                    if (checkItem.TcpClient.Connected && checkItem.TcpClient.Client != null)
                    {
                        checkItem.TcpClient.Close();
                        continue;
                    }
                    checkItem.TcpClient.Dispose();
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, ex.Message);
                }
            }
            _clientPoolItems.Clear();

            _logger.LogInformation("停止监听");

            return base.StopAsync(cancellationToken);
        }

        public class ClientPoolItem
        {
            public TcpClient TcpClient { get; set; }
            public Task Worker { get; set; }
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