using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace TcpClientSample
{
    public class Worker2 : BackgroundService
    {
        private readonly ILogger<Worker2> _logger;

        const string hostname = "127.0.0.1";
        const int port = 8888;

        private static byte[] DefaultResult = new byte[0];

        public Worker2(ILogger<Worker2> logger)
        {
            _logger = logger;
        }

        public override async Task StartAsync(CancellationToken cancellationToken)
        {
            await base.StartAsync(cancellationToken);
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            //TimeSpan timeout = new TimeSpan(0, 0, 0, 5, 0);
            TimeSpan timeout = TimeSpan.FromMinutes(10);

            while (!stoppingToken.IsCancellationRequested)
            {
                TcpClient _tcpClient = new TcpClient();
                try
                {
                    _logger.LogInformation("连接");
                    await _tcpClient.ConnectAsync(hostname, port, stoppingToken);
                    _logger.LogInformation("已连接");
                    NetworkStream ns = _tcpClient.GetStream();

                    for (int i = 0; i < 1000; i++)
                    {
                        try
                        {
                            DateTime startTime = DateTime.Now;
                            if (_tcpClient.Connected == false) // 断线重连
                            {
                                await _tcpClient.ConnectAsync(hostname, port);
                                ns = _tcpClient.GetStream();
                            }

                            #region write
                            if (ns.CanWrite == false)
                            {
                                await Task.Delay(100, stoppingToken);
                                continue;
                            }

                            string msg = $"{Thread.CurrentThread.ManagedThreadId} >> {Guid.NewGuid():n} >> {DateTime.Now}";
                            _logger.LogInformation($"请求:\n\t{msg}\n");

                            byte[] requestBuffer = Encoding.UTF8.GetBytes(msg);
                            await ns.WriteAsync(requestBuffer, stoppingToken);
                            await ns.FlushAsync(stoppingToken);
                            #endregion

                            #region read
                            var promise = new TaskCompletionSource<byte[]>(TaskCreationOptions.RunContinuationsAsynchronously);
                            using CancellationTokenSource cancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
                            cancellationTokenSource.CancelAfter(timeout);
                            CancellationToken cancellationToken = cancellationTokenSource.Token;
                            byte[] data = null;

                            if (ns.CanRead == false)
                            {
                                await Task.Delay(100, stoppingToken);
                                continue;
                            }

                            ReadState readState = new ReadState(promise, _tcpClient);
                            ns.BeginRead(readState.Buffer, default, readState.BufferSize, new AsyncCallback(AsyncReadCallBack), readState);

                            await using (cancellationToken.Register(() => promise.TrySetResult(default)))
                            {
                                data = await promise.Task.ConfigureAwait(false);
                            }

                            if (data != null && data.Length > 0)
                            {
                                _logger.LogInformation($"响应:\n\t{Encoding.UTF8.GetString(data)}\n");
                            }
                            #endregion

                            _logger.LogInformation($"\n耗时: {DateTime.Now - startTime}\n");
                            // await Task.Delay(1000, stoppingToken);
                            await Task.Delay(0, stoppingToken);
                            //await Task.Delay(TimeSpan.FromMinutes(10));
                        }
                        catch (Exception ex)
                        {
                            _logger.LogError(ex, ex.Message);
                            await Task.Delay(5000, stoppingToken);
                        }
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, ex.Message);
                }
                finally
                {
                    _logger.LogInformation("断开连接");
                    _tcpClient?.Close();
                    _tcpClient?.Dispose();
                    _logger.LogInformation("已断开连接");
                }

                await Task.Delay(2000, stoppingToken);
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
                state.TaskCompletionSource.TrySetResult(DefaultResult);
            }
        }

        public override Task StopAsync(CancellationToken cancellationToken)
        {
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
