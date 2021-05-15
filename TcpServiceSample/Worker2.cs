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
    public class Worker2 : BackgroundService
    {
        private readonly ILogger<Worker2> _logger;

        const int Port = 8888;
        private volatile int ClientCounter = default;
        private TcpListener _tcpListener;
        private static byte[] DefaultResult = new byte[0];

        public Worker2(ILogger<Worker2> logger)
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

            ReadState state = new(tcpClient) { LatestCommunicationTime = DateTime.Now, CancellationToken = stoppingToken, ClientAddress = tcpClient.Client.RemoteEndPoint as IPEndPoint };

            try
            {
                await Task.Factory.StartNew(async () =>
                {
                    #region 首次异步通信
                    try
                    {
                        state.CancellationTokenSource = new CancellationTokenSource(state.ReadTimeout);

                        NetworkStream stream = tcpClient.GetStream();
                        if (stream.CanRead == false)
                        {
                            await Task.Delay(100);
                            ShutdownClient(state.TcpClient);
                            return;
                        }

                        state.CancellationTokenRegistrations.Add(state.CancellationTokenSource.Token.Register(() =>
                        {
                            lock (state)
                            {
                                if (state.Disposed == false)
                                    state.ManualResetEvent.Set();
                            }
                        }));
                        state.CancellationTokenRegistrations.Add(state.CancellationToken.Register(() =>
                        {
                            lock (state)
                            {
                                if (state.Disposed == false)
                                    state.ManualResetEvent.Set();
                            }
                        }));

                        if (state.TcpClient?.Client?.Connected == false || state.Disposed) return;
                        _logger.LogInformation("BeginRead 111111111111");
                        stream.BeginRead(state.Buffer, default, state.BufferSize, new AsyncCallback(AsyncReadCallBack), state);

                        {
                            byte[] data = null;
                            try
                            {
                                state.ManualResetEvent.WaitOne(state.WaitTimeout);
                                state.RequestQueue.TryDequeue(out data);
                            }
                            catch (Exception ex)
                            {
                                _logger.LogError(ex, ex.Message);
                                if (CheckConnection(tcpClient) == false) // 检测tcp连接
                                {
                                    ShutdownClient(tcpClient);
                                    return;
                                }
                            }
                            finally
                            {
                            }

                            if (data != null && data.Length > 0)
                            {
                                string requestMsg = Encoding.UTF8.GetString(data);
                                _logger.LogInformation($"接收(111111111111):\n\t{requestMsg}\n");

                                if (stream.CanWrite)
                                {
                                    byte[] responseBuffer = Encoding.UTF8.GetBytes($"{requestMsg}; 响应时间:{DateTime.Now}");
                                    await stream.WriteAsync(responseBuffer, stoppingToken);
                                }
                            }
                            else
                            {
                                _logger.LogInformation("超时,没有收到消息 1111111111111111");
                                await Task.Delay(5000);
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, ex.Message);
                    }
                    finally
                    {
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
                        await Task.Delay(TimeSpan.FromSeconds(10));
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
                _logger.LogInformation($"{state.ClientAddress} 断开");

                try
                {
                    state.Dispose();
                    state.RequestQueue.Clear();

                    tcpClient.Client?.Shutdown(SocketShutdown.Both);
                    tcpClient?.Dispose();
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, ex.Message);
                }

                state.Close();
            }
        }

        void AsyncReadCallBack(IAsyncResult ar)
        {
            ReadState state = ar.AsyncState as ReadState;
            try
            {
                #region 检查
                if (state.TcpClient == null || state.TcpClient.Connected == false /*|| state.TaskCompletionSource == null*/ || state.CancellationToken.IsCancellationRequested) return;

                //if (CheckConnection(state.TcpClient) == false) // 检测tcp连接
                //{
                //    return;
                //}
                if (CheckTimeout(state.TcpClient, state.LatestCommunicationTime, state.ReadTimeout) == false) // 检测超时
                {
                    ShutdownClient(state.TcpClient);
                    return;
                }
                #endregion

                #region 读取数据
                LoadDataAsync(state, ar);
                #endregion

                #region 开启新的异步通信
                if (state.TcpClient.Connected == false) return;

                NetworkStream stream = state.TcpClient.GetStream();
                if (stream.CanRead == false)
                {
                    Thread.Sleep(100);
                    ShutdownClient(state.TcpClient);
                    return;
                }

                state.LatestCommunicationTime = DateTime.Now;
                state.CancellationTokenSource = new CancellationTokenSource(state.ReadTimeout);

                state.CancellationTokenRegistrations.Add(state.CancellationTokenSource.Token.Register(() =>
                {
                    lock (state)
                    {
                        if (state.Disposed == false)
                            state.ManualResetEvent.Set();
                    }
                }));
                state.CancellationTokenRegistrations.Add(state.CancellationToken.Register(() =>
                {
                    lock (state)
                    {
                        if (state.Disposed == false)
                            state.ManualResetEvent.Set();
                    }
                }));

                if (state.TcpClient?.Client?.Connected == false || state.Disposed) return;
                _logger.LogInformation("BeginRead 22222222222222");
                stream.BeginRead(state.Buffer, default, state.BufferSize, new AsyncCallback(AsyncReadCallBack), state);

                {
                    byte[] data = null;
                    try
                    {
                        state.ManualResetEvent.WaitOne(state.WaitTimeout);
                        state.RequestQueue.TryDequeue(out data);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, ex.Message);
                        if (CheckConnection(state.TcpClient) == false) // 检测tcp连接
                        {
                            ShutdownClient(state.TcpClient);
                            return;
                        }
                        return;
                    }
                    finally
                    {
                    }

                    if (data != null && data.Length > 0)
                    {
                        string requestMsg = Encoding.UTF8.GetString(data);
                        _logger.LogInformation($"接收(22222222222222):\n\t{requestMsg}\n");

                        if (stream.CanWrite)
                        {
                            byte[] responseBuffer = Encoding.UTF8.GetBytes($"{requestMsg}; 响应时间:{DateTime.Now}");
                            //await stream.WriteAsync(responseBuffer, state.CancellationToken);
                            stream.Write(responseBuffer);
                        }
                        else
                        {
                            Thread.Sleep(100);
                            ShutdownClient(state.TcpClient);
                            return;
                        }
                    }
                    else
                    {
                        _logger.LogInformation("超时,没有收到消息 22222222222222222");
                        Thread.Sleep(1000);
                    }
                }
                #endregion
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, ex.Message);
            }
            finally
            {
            }
        }

        void LoadDataAsync(ReadState state, IAsyncResult ar)
        {
            try
            {
                //CancellationTokenSource cancellationTokenSource = new CancellationTokenSource(state.ReadTimeout);
                //await Task.Factory.StartNew(() =>
                //{
                //    if (state.TcpClient?.Client?.Connected == false || state.Disposed) return;

                //    NetworkStream ns = state.TcpClient.GetStream();
                //    int numOfBytesRead = ns.EndRead(ar);
                //    if (numOfBytesRead > 0)
                //    {
                //        byte[] buffer = new byte[numOfBytesRead];
                //        Array.Copy(state.Buffer, 0, buffer, 0, numOfBytesRead);
                //        state.RequestQueue.Enqueue(buffer);
                //    }
                //}, cancellationTokenSource.Token);

                //ar.AsyncWaitHandle.Dispose();

                //Task.Factory.StartNew(() =>
                //{
                //    if (state.TcpClient?.Client?.Connected == false || state.Disposed) return;

                //    NetworkStream ns = state.TcpClient.GetStream();
                //    int numOfBytesRead = ns.EndRead(ar);
                //    if (numOfBytesRead > 0)
                //    {
                //        byte[] buffer = new byte[numOfBytesRead];
                //        Array.Copy(state.Buffer, 0, buffer, 0, numOfBytesRead);
                //        state.RequestQueue.Enqueue(buffer);
                //    }
                //}).Wait(state.ReadTimeout);

                if (state.TcpClient?.Client?.Connected == false || state.Disposed) return;
                if(ar.AsyncWaitHandle.WaitOne(state.ReadTimeout))
                {
                    NetworkStream ns = state.TcpClient.GetStream();
                    int numOfBytesRead = ns.EndRead(ar);
                    if (numOfBytesRead > 0)
                    {
                        byte[] buffer = new byte[numOfBytesRead];
                        Array.Copy(state.Buffer, 0, buffer, 0, numOfBytesRead);
                        state.RequestQueue.Enqueue(buffer);
                    }
                }

                _logger.LogInformation("等待超时");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, ex.Message);
            }
            finally
            {
                state.Dispose();
                //await Task.Delay(10);
                // await Task.Delay(1);
                //await Task.Delay(0);
                Thread.Sleep(0);
                lock (this)
                {
                    if (state.Disposed == false)
                        state.ManualResetEvent.Set();
                }
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

        static void ShutdownClient(TcpClient tcpClient)
        {
            tcpClient?.Close();
            tcpClient?.Dispose();
        }

        // https://stackoverflow.com/questions/1387459/how-to-check-if-tcpclient-connection-is-closed
        // https://www.microsofttranslator.com/bv.aspx?from=&to=en&a=https://docs.microsoft.com/zh-cn/dotnet/api/system.net.networkinformation.tcpstate?redirectedfrom=MSDN&view=netframework-4.7.2
        static TcpState GetState(TcpClient tcpClient)
        {
            var foo = IPGlobalProperties.GetIPGlobalProperties()
              .GetActiveTcpConnections()
              .FirstOrDefault(x => x.LocalEndPoint.Equals(tcpClient?.Client?.LocalEndPoint));
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

            public ReadState(TcpClient tcpClient)
                : this(tcpClient, DefaultBufferSize)
            { }

            public ReadState(TcpClient tcpClient, int bufferSize)
            {
                Buffer = new byte[bufferSize];
                BufferSize = bufferSize;
                TcpClient = tcpClient;
            }

            public bool Disposed { get; set; } = false;

            public IPEndPoint ClientAddress { get; set; }

            public ManualResetEvent ManualResetEvent { get; set; } = new ManualResetEvent(false);

            public byte[] Buffer { get; }
            public int BufferSize { get; }
            public TcpClient TcpClient { get; }

            public DateTime LatestCommunicationTime { get; set; }

            //public TimeSpan Timeout { get; set; } = new TimeSpan(0, 0, 5, 0, 0);
            public TimeSpan ReadTimeout { get; set; } = new TimeSpan(0, 0, 0, 10, 0);
            public TimeSpan WaitTimeout { get; set; } = new TimeSpan(0, 0, 0, 15, 0);

            public CancellationToken CancellationToken { get; set; }

            public CancellationTokenSource CancellationTokenSource { get; set; }

            public List<CancellationTokenRegistration> CancellationTokenRegistrations { get; set; } = new List<CancellationTokenRegistration>(4);

            public ConcurrentQueue<byte[]> RequestQueue { get; set; } = new ConcurrentQueue<byte[]>();

            public void Dispose()
            {
                CancellationTokenSource?.Dispose();
                foreach (var cancellationTokenRegistration in CancellationTokenRegistrations)
                {
                    cancellationTokenRegistration.Dispose();
                }
                CancellationTokenRegistrations.Clear();
            }

            public void Close()
            {
                lock (this)
                {
                    ManualResetEvent.Dispose();
                    Disposed = true;
                }
            }
        }
    }
}
