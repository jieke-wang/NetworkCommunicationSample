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

namespace UdpServiceSample
{
    public class Worker : BackgroundService
    {
        private readonly ILogger<Worker> _logger;

        const int Port = 8888;
        private UdpClient listenSocket;

        private int m_maxConnectNum;    //最大连接数  

        Semaphore m_maxNumberAcceptedClients;

        public Worker(ILogger<Worker> logger)
        {
            _logger = logger;

            m_maxConnectNum = 1000;
            m_maxNumberAcceptedClients = new Semaphore(m_maxConnectNum, m_maxConnectNum);
        }

        public override Task StartAsync(CancellationToken cancellationToken)
        {
            IPEndPoint listenEndpoint = new IPEndPoint(IPAddress.Any, Port);
            listenSocket = new UdpClient(listenEndpoint);
            _logger.LogInformation($"开始监听: 0.0.0.0:{Port}");

            return base.StartAsync(cancellationToken);
        }

        public override Task StopAsync(CancellationToken cancellationToken)
        {
            listenSocket.Close();
            listenSocket.Dispose();
            _logger.LogInformation("停止监听");

            return base.StopAsync(cancellationToken);
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            CancellationTokenRegistration cancellationTokenRegistration = stoppingToken.Register(() => listenSocket.Close());

            while (stoppingToken.IsCancellationRequested == false)
            {
                try
                {
                    m_maxNumberAcceptedClients.WaitOne();
                    //if (listenSocket.Client == null || listenSocket.Client.Connected == false) return;

                    UdpReceiveResult udpReceiveResult = await listenSocket.ReceiveAsync();
                    string requestMsg = Encoding.UTF8.GetString(udpReceiveResult.Buffer);
                    _logger.LogInformation($"{udpReceiveResult.RemoteEndPoint}: {requestMsg}");

                    string responseMsg = $"回复: {requestMsg}, 响应时间: {DateTime.Now}";
                    byte[] responseBuffer = Encoding.UTF8.GetBytes(responseMsg);
                    await listenSocket.SendAsync(responseBuffer, responseBuffer.Length, udpReceiveResult.RemoteEndPoint);
                }
                catch(ObjectDisposedException)
                {
                    return;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, ex.Message);
                }
                finally
                {
                    m_maxNumberAcceptedClients.Release();
                }
            }

            cancellationTokenRegistration.Dispose();
        }
    }
}
