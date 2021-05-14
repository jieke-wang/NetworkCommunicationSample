using System;
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
    public class Worker : BackgroundService
    {
        private readonly ILogger<Worker> _logger;

        const int Port = 8888;
        private volatile int ClientCounter = default;
        private TcpListener _tcpListener;

        public Worker(ILogger<Worker> logger)
        {
            _logger = logger;
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
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    await AcceptTcpClientAsync(stoppingToken);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, ex.Message);
                }
            }
        }

        private async Task AcceptTcpClientAsync(CancellationToken stoppingToken)
        {
            using TcpClient tcpClient = await _tcpListener.AcceptTcpClientAsync();
            _logger.LogInformation($"{tcpClient.Client.RemoteEndPoint as IPEndPoint} 接入");

            tcpClient.ReceiveTimeout = 10;
            tcpClient.SendTimeout = 10;
            tcpClient.LingerState = new LingerOption(true, 1);
            await ReciveDataAsync(tcpClient, stoppingToken);
        }

        private async Task ReciveDataAsync(TcpClient tcpClient, CancellationToken stoppingToken)
        {
            _logger.LogInformation($"客户端数量: {++ClientCounter}");

            try
            {
                await Task.Factory.StartNew(async () =>
                {
                    TimeSpan timeout = new TimeSpan(0, 0, 5, 0, 0);
                    DateTime latestCommunicationTime = DateTime.Now;

                    while (!stoppingToken.IsCancellationRequested && tcpClient.Connected)
                    {
                        if (CheckConnection(tcpClient) == false) return; // 检测tcp连接
                        if (CheckTimeout(tcpClient, latestCommunicationTime, timeout) == false) return; // 检测超时

                        var promise = new TaskCompletionSource<byte[]>(TaskCreationOptions.RunContinuationsAsynchronously);
                        using CancellationTokenSource cancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
                        cancellationTokenSource.CancelAfter(timeout);
                        CancellationToken cancellationToken = cancellationTokenSource.Token;
                        byte[] data = null;

                        NetworkStream stream = tcpClient.GetStream();
                        if (stream.CanRead == false)
                        {
                            await Task.Delay(100);
                            continue;
                        }

                        ReadState readState = new ReadState(promise, tcpClient);
                        stream.BeginRead(readState.Buffer, default, readState.BufferSize, AsyncReadCallBack, readState);

                        await using (cancellationToken.Register(() => promise.TrySetCanceled()))
                        {
                            data = await promise.Task.ConfigureAwait(false);
                        }

                        if (data != null && data.Length > 0)
                        {
                            latestCommunicationTime = DateTime.Now;
                            string requestMsg = Encoding.UTF8.GetString(data);
                            _logger.LogInformation($"接收:\n\t{requestMsg}\n");

                            if (stream.CanWrite)
                            {
                                byte[] responseBuffer = Encoding.UTF8.GetBytes($"{requestMsg}; 响应时间:{DateTime.Now}");
                                await stream.WriteAsync(responseBuffer, stoppingToken);
                            }
                        }
                    }
                }).Unwrap();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, ex.Message);
            }
            finally
            {
                _logger.LogInformation($"客户端数量: {--ClientCounter}");
            }
        }

        void AsyncReadCallBack(IAsyncResult ar)
        {
            ReadState state = ar.AsyncState as ReadState;
            if (state.TcpClient == null || state.TcpClient.Connected == false || state.TaskCompletionSource == null) return;

            NetworkStream ns = state.TcpClient.GetStream();
            int numOfBytesRead = ns.EndRead(ar);
            if (numOfBytesRead > 0)
            {
                byte[] buffer = new byte[numOfBytesRead];
                Array.Copy(state.Buffer, 0, buffer, 0, numOfBytesRead);
                state.TaskCompletionSource.TrySetResult(buffer);
            }
            else
            {
                state.TaskCompletionSource.TrySetCanceled();
            }
        }

        bool CheckConnection(TcpClient tcpClient)
        {
            TcpState tcpState = GetState(tcpClient);
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

        bool CheckTimeout(TcpClient tcpClient, DateTime latestCommunicationTime, TimeSpan timeout)
        {
            if (DateTime.Now.Subtract(latestCommunicationTime) > timeout)
            {
                _logger.LogInformation($"Tcp Client({tcpClient.Client.RemoteEndPoint as IPEndPoint}); Client Counter: {ClientCounter - 1}; Timeout, close connection, Date: {DateTime.Now}");
                return false;
            }

            return true;
        }

        // https://stackoverflow.com/questions/1387459/how-to-check-if-tcpclient-connection-is-closed
        // https://www.microsofttranslator.com/bv.aspx?from=&to=en&a=https://docs.microsoft.com/zh-cn/dotnet/api/system.net.networkinformation.tcpstate?redirectedfrom=MSDN&view=netframework-4.7.2
        TcpState GetState(TcpClient tcpClient)
        {
            var foo = IPGlobalProperties.GetIPGlobalProperties()
              .GetActiveTcpConnections()
              .FirstOrDefault(x => x.LocalEndPoint.Equals(tcpClient.Client.LocalEndPoint));
            return foo != null ? foo.State : TcpState.Unknown;
        }

        public override Task StopAsync(CancellationToken cancellationToken)
        {
            _tcpListener?.Stop();

            _logger.LogInformation("停止监听");

            return base.StopAsync(cancellationToken);
        }

        public class ReadState
        {
            const int DefaultBufferSize = 1024;

            public ReadState(TaskCompletionSource<byte[]> promise, TcpClient tcpClient)
                : this(promise, tcpClient, DefaultBufferSize)
            { }

            public ReadState(TaskCompletionSource<byte[]> promise, TcpClient tcpClient, int bufferSize)
            {
                Buffer = new byte[bufferSize];
                BufferSize = bufferSize;
                TaskCompletionSource = promise;
                TcpClient = tcpClient;
            }

            public byte[] Buffer { get; }
            public int BufferSize { get; }
            public TaskCompletionSource<byte[]> TaskCompletionSource { get; }
            public TcpClient TcpClient { get; }
        }
    }
}

// TcpClient 类异步接收数据
// https://blog.csdn.net/WuLex/article/details/97248347

// https://www.cnblogs.com/coldairarrow/p/7501645.html
// https://www.nuget.org/packages/Network/
// Server Setup https://www.indie-dev.at/?p=1205
// [Server] Send/Receive packets https://www.indie-dev.at/?p=1230
// https://github.com/Toemsel/Network

// https://stackoverflow.com/questions/1904160/getting-the-ip-address-of-a-remote-socket-endpoint
// https://www.nuget.org/packages/Afx.Tcp.Host/

// Socket监听与TcpListener监听的区别和用法
// https://blog.csdn.net/ID_Dexter/article/details/80679735
// https://stackoverflow.com/questions/2717381/how-do-i-get-client-ip-address-using-tcpclient
// https://learn-powershell.net/2015/03/29/checking-for-disconnected-connections-with-tcplistener-using-powershell/

// https://codereview.stackexchange.com/questions/113108/async-task-with-timeout