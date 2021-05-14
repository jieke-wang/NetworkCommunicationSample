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
            TcpClient tcpClient = await _tcpListener.AcceptTcpClientAsync();
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
                    if (CheckConnection(tcpClient) == false) return; // 检测tcp连接

                    #region 首次异步通信
                    var promise = new TaskCompletionSource<byte[]>(TaskCreationOptions.RunContinuationsAsynchronously);
                    ReadState readState = new ReadState(promise, tcpClient) { LatestCommunicationTime = DateTime.Now, CancellationToken = stoppingToken };
                    try
                    {
                        readState.CancellationTokenSource = new CancellationTokenSource(readState.Timeout);
                        CancellationTokenSource cancellationTokenSource = readState.CancellationTokenSource;
                        CancellationToken cancellationToken = cancellationTokenSource.Token;
                        byte[] data = null;

                        NetworkStream stream = tcpClient.GetStream();
                        if (stream.CanRead == false)
                        {
                            await Task.Delay(100);
                            ShutdownClient(readState.TcpClient);
                            return;
                        }

                        readState.CancellationTokenRegistrations.Add(cancellationToken.Register(() => promise.TrySetCanceled()));
                        readState.CancellationTokenRegistrations.Add(readState.CancellationToken.Register(() => promise.TrySetCanceled()));

                        stream.BeginRead(readState.Buffer, default, readState.BufferSize, AsyncReadCallBackAsync, readState);

                        data = await promise.Task.ConfigureAwait(false);
                        if (data != null && data.Length > 0)
                        {
                            string requestMsg = Encoding.UTF8.GetString(data);
                            _logger.LogInformation($"接收:\n\t{requestMsg}\n");

                            if (stream.CanWrite)
                            {
                                byte[] responseBuffer = Encoding.UTF8.GetBytes($"{requestMsg}; 响应时间:{DateTime.Now}");
                                await stream.WriteAsync(responseBuffer, stoppingToken);
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, ex.Message);
                    }
                    finally
                    {
                        readState.Dispose();
                    }
                    #endregion
                }).Unwrap();

                await Task.Factory.StartNew(async () =>
                {
                    while (stoppingToken.IsCancellationRequested == false)
                    {
                        if (CheckConnection(tcpClient) == false) // 检测tcp连接
                        {
                            ShutdownClient(tcpClient);
                            return;
                        }
                        await Task.Delay(TimeSpan.FromSeconds(30));
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

        async void AsyncReadCallBackAsync(IAsyncResult ar)
        {
            try
            {
                ReadState state = ar.AsyncState as ReadState;
                #region 检查
                if (state.TcpClient == null || state.TcpClient.Connected == false || state.TaskCompletionSource == null || state.CancellationToken.IsCancellationRequested) return;

                if (CheckConnection(state.TcpClient) == false) // 检测tcp连接
                {
                    return;
                }
                if (CheckTimeout(state.TcpClient, state.LatestCommunicationTime, state.Timeout) == false) // 检测超时
                {
                    ShutdownClient(state.TcpClient);
                    return;
                }
                #endregion

                #region 读取数据
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

                state.Dispose();
                #endregion

                #region 开启新的异步通信
                NetworkStream stream = state.TcpClient.GetStream();
                if (stream.CanRead == false)
                {
                    await Task.Delay(100);
                    ShutdownClient(state.TcpClient);
                    return;
                }

                var promise = new TaskCompletionSource<byte[]>(TaskCreationOptions.RunContinuationsAsynchronously);
                ReadState readState = state;
                readState.LatestCommunicationTime = DateTime.Now;
                readState.TaskCompletionSource = promise;
                readState.CancellationTokenSource = new CancellationTokenSource(readState.Timeout);

                CancellationTokenSource cancellationTokenSource = readState.CancellationTokenSource;
                CancellationToken cancellationToken = cancellationTokenSource.Token;
                byte[] data = null;

                readState.CancellationTokenRegistrations.Add(cancellationToken.Register(() => promise.TrySetCanceled()));
                readState.CancellationTokenRegistrations.Add(readState.CancellationToken.Register(() => promise.TrySetCanceled()));

                stream.BeginRead(readState.Buffer, default, readState.BufferSize, AsyncReadCallBackAsync, readState);

                data = await promise.Task.ConfigureAwait(false);

                if (data != null && data.Length > 0)
                {
                    string requestMsg = Encoding.UTF8.GetString(data);
                    _logger.LogInformation($"接收:\n\t{requestMsg}\n");

                    if (stream.CanWrite)
                    {
                        byte[] responseBuffer = Encoding.UTF8.GetBytes($"{requestMsg}; 响应时间:{DateTime.Now}");
                        await stream.WriteAsync(responseBuffer, readState.CancellationToken);
                    }
                    else
                    {
                        await Task.Delay(100);
                        ShutdownClient(readState.TcpClient);
                        return;
                    }
                }
                #endregion
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, ex.Message);
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

        void ShutdownClient(TcpClient tcpClient)
        {
            tcpClient?.Close();
            tcpClient?.Dispose();
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

        public class ReadState : IDisposable
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
            public TaskCompletionSource<byte[]> TaskCompletionSource { get; set; }
            public TcpClient TcpClient { get; }

            public DateTime LatestCommunicationTime { get; set; }

            public TimeSpan Timeout { get; set; } = new TimeSpan(0, 0, 5, 0, 0);

            public CancellationToken CancellationToken { get; set; }

            public CancellationTokenSource CancellationTokenSource { get; set; }

            public List<CancellationTokenRegistration> CancellationTokenRegistrations { get; set; } = new List<CancellationTokenRegistration>(4);

            public void Dispose()
            {
                CancellationTokenSource?.Dispose();
                foreach (var cancellationTokenRegistration in CancellationTokenRegistrations)
                {
                    cancellationTokenRegistration.Dispose();
                }
                CancellationTokenRegistrations.Clear();
            }
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