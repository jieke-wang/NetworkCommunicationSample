using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace UdpClientSample
{
    public class Worker : BackgroundService
    {
        private readonly ILogger<Worker> _logger;
        const string hostname = "127.0.0.1";
        const int port = 8888;

        public Worker(ILogger<Worker> logger)
        {
            _logger = logger;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            while (stoppingToken.IsCancellationRequested == false)
            {
                try
                {
                    await RunAsync(stoppingToken);
                    await Task.Delay(TimeSpan.FromSeconds(10), stoppingToken);
                }
                catch (TaskCanceledException)
                {
                    return;
                }
                catch (Exception ex)
                {
                    _logger.LogInformation(ex, ex.Message);
                }
            }
        }

        async Task RunAsync(CancellationToken stoppingToken)
        {
            using UdpClient udpClient = new UdpClient();
            _logger.LogInformation($"开始连接: {hostname}:{port}");
            udpClient.Connect(hostname, port);
            _logger.LogInformation($"已连接: {hostname}:{port} <=> {udpClient.Client.LocalEndPoint}");
            //await Task.Delay(5000, stoppingToken);

            CancellationTokenRegistration cancellationTokenRegistration = stoppingToken.Register(() => udpClient.Close());

            Task sendTask = Task.Factory.StartNew(async () =>
            {
                while (!stoppingToken.IsCancellationRequested)
                {
                    try
                    {

                        for (int i = 0; i < 1000; i++)
                        {
                            DateTime startTime = DateTime.Now;
                            string msg = $"{Thread.CurrentThread.ManagedThreadId} >> {Guid.NewGuid():n} >> {DateTime.Now}";
                            _logger.LogInformation($"请求:\n\t{msg}\n");
                            byte[] requestBuffer = Encoding.UTF8.GetBytes(msg);

                            if (udpClient.Client == null || udpClient.Client.Connected == false) return;
                            await udpClient.SendAsync(requestBuffer, requestBuffer.Length);
                            // await udpClient.SendAsync(requestBuffer, requestBuffer.Length, hostname, port);

                            _logger.LogInformation($"\n耗时: {DateTime.Now - startTime}\n");
                            //await Task.Delay(1000, stoppingToken);
                            await Task.Delay(0, stoppingToken);
                        }
                    }
                    catch (TaskCanceledException)
                    {
                        return;
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, ex.Message);
                    }

                    await Task.Delay(2000, stoppingToken);
                }
            }, stoppingToken, TaskCreationOptions.LongRunning, TaskScheduler.Current).Unwrap();

            Task reciveTask = Task.Factory.StartNew(async () =>
            {
                while (!stoppingToken.IsCancellationRequested)
                {
                    try
                    {
                        if (udpClient.Client == null || udpClient.Client.Connected == false) return;
                        UdpReceiveResult udpReceiveResult = await udpClient.ReceiveAsync();
                        string responseMsg = Encoding.UTF8.GetString(udpReceiveResult.Buffer);
                        _logger.LogInformation($"\n{udpReceiveResult.RemoteEndPoint}: {responseMsg}\n");
                    }
                    catch (SocketException)
                    {
                        udpClient.Close();
                        return;
                    }
                    catch (ObjectDisposedException)
                    {
                        return;
                    }
                    catch (TaskCanceledException)
                    {
                        return;
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, ex.Message);
                    }
                }
            }, stoppingToken, TaskCreationOptions.LongRunning, TaskScheduler.Current).Unwrap();

            await Task.WhenAll(sendTask, reciveTask);
            cancellationTokenRegistration.Dispose();
            _logger.LogInformation($"断开连接");
        }
    }
}
