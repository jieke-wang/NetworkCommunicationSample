using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace TcpClientSample
{
    public class Worker3 : BackgroundService
    {
        private readonly ILogger<Worker3> _logger;
        private ManualResetEvent _waitLock;

        const string hostname = "127.0.0.1";
        const int port = 8888;

        private static byte[] DefaultResult = new byte[0];

        public Worker3(ILogger<Worker3> logger)
        {
            _logger = logger;
            _waitLock = new ManualResetEvent(false);
        }

        public override async Task StartAsync(CancellationToken cancellationToken)
        {
            await base.StartAsync(cancellationToken);
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            //TimeSpan timeout = new TimeSpan(0, 0, 0, 5, 0);
            TimeSpan timeout = TimeSpan.FromMinutes(10);
            BufferManager bufferManager = new BufferManager(100 * 1024 * 2, 1024);
            bufferManager.InitBuffer();

            while (!stoppingToken.IsCancellationRequested)
            {
                using TcpClient _tcpClient = new TcpClient();

                AsyncUserToken asyncUserToken = new AsyncUserToken();

                using SocketAsyncEventArgs sendArg = new SocketAsyncEventArgs();
                using SocketAsyncEventArgs reciveArg = new SocketAsyncEventArgs();
                reciveArg.Completed += new EventHandler<SocketAsyncEventArgs>(IO_Completed);
                reciveArg.UserToken = asyncUserToken;
                bufferManager.SetBuffer(reciveArg);

                try
                {
                    _logger.LogInformation("连接");
                    await _tcpClient.ConnectAsync(hostname, port, stoppingToken);
                    _logger.LogInformation("已连接");
                    NetworkStream ns = _tcpClient.GetStream();
                    asyncUserToken.Socket = _tcpClient.Client;

                    for (int i = 0; i < 1000; i++)
                    {
                        try
                        {
                            DateTime startTime = DateTime.Now;
                            if (_tcpClient.Connected == false) // 断线重连
                            {
                                await _tcpClient.ConnectAsync(hostname, port);
                                ns = _tcpClient.GetStream();
                                asyncUserToken.Socket = _tcpClient.Client;
                            }

                            #region write
                            
                            string msg = $"{Thread.CurrentThread.ManagedThreadId} >> {Guid.NewGuid():n} >> {DateTime.Now}";
                            _logger.LogInformation($"请求:\n\t{msg}\n");

                            byte[] requestBuffer = Encoding.UTF8.GetBytes(msg);
                            sendArg.SetBuffer(requestBuffer);
                            _tcpClient.Client.SendAsync(sendArg);
                            #endregion

                            #region read
                            CancellationTokenSource cancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
                            cancellationTokenSource.CancelAfter(TimeSpan.FromSeconds(3));

                            using (cancellationTokenSource.Token.Register(() => _waitLock.Set()))
                            {
                                _waitLock.WaitOne();
                            }
                            
                            byte[] data = null;
                            lock(asyncUserToken.Buffer)
                            {
                                data = asyncUserToken.Buffer.ToArray();
                                asyncUserToken.Buffer.Clear();
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
                catch (TaskCanceledException)
                {
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, ex.Message);
                }
                finally
                {
                    bufferManager.FreeBuffer(reciveArg);
                    _logger.LogInformation("断开连接");
                    _tcpClient?.Close();
                    _tcpClient?.Dispose();
                    
                    _logger.LogInformation("已断开连接");
                }

                await Task.Delay(2000, stoppingToken);
            }
        }

        void IO_Completed(object sender, SocketAsyncEventArgs args)
        {
            if(args.LastOperation == SocketAsyncOperation.Receive && args.BytesTransferred > 0 && args.SocketError == SocketError.Success)
            {
                AsyncUserToken token = (AsyncUserToken)args.UserToken;

                byte[] data = new byte[args.BytesTransferred];
                Array.Copy(args.Buffer, args.Offset, data, 0, args.BytesTransferred);

                lock (token.Buffer)
                {
                    token.Buffer.AddRange(data);
                }

                _waitLock.Set();
            }
            else
            {
                CloseClientSocket(args);
            }
        }

        private void CloseClientSocket(SocketAsyncEventArgs args)
        {
            AsyncUserToken token = args.UserToken as AsyncUserToken;

            try
            {
                token.Socket.Shutdown(SocketShutdown.Send);
            }
            catch (Exception) { }
            token.Socket.Close();
            token.Socket = null;
            args.UserToken = new AsyncUserToken();
            _waitLock.Set();

            _logger.LogInformation($"{token.Local} <=> {token.Remote} 断开连接");
        }

        public override Task StopAsync(CancellationToken cancellationToken)
        {
            _waitLock.Dispose();
            return base.StopAsync(cancellationToken);
        }

        public class AsyncUserToken
        {
            /// <summary>  
            /// 客户端服务端地址  
            /// </summary>  
            public IPAddress Local { get; set; }

            /// <summary>  
            /// 客户端远程地址
            /// </summary>  
            public EndPoint Remote { get; set; }

            /// <summary>  
            /// 通信SOKET  
            /// </summary>  
            public Socket Socket { get; set; }

            /// <summary>  
            /// 连接时间  
            /// </summary>  
            public DateTime ConnectTime { get; set; }

            /// <summary>  
            /// 数据缓存区  
            /// </summary>  
            public List<byte> Buffer { get; set; }

            public AsyncUserToken(int bufferSize = 1024)
            {
                this.Buffer = new List<byte>(bufferSize);
            }
        }

        public class BufferManager
        {
            int m_numBytes;
            byte[] m_buffer;
            Stack<int> m_freeIndexPool;
            int m_currentIndex;
            int m_bufferSize;

            public BufferManager(int totalBytes, int bufferSize)
            {
                m_numBytes = totalBytes;
                m_currentIndex = 0;
                m_bufferSize = bufferSize;
                m_freeIndexPool = new Stack<int>();
            }

            public void InitBuffer()
            {
                m_buffer = new byte[m_numBytes];
            }

            public bool SetBuffer(SocketAsyncEventArgs args)
            {
                if (m_freeIndexPool.Count > 0)
                {
                    args.SetBuffer(m_buffer, m_freeIndexPool.Pop(), m_bufferSize);
                }
                else
                {
                    if ((m_numBytes - m_bufferSize) < m_currentIndex)
                    {
                        return false;
                    }
                    args.SetBuffer(m_buffer, m_currentIndex, m_bufferSize);
                    m_currentIndex += m_bufferSize;
                }
                return true;
            }

            public void FreeBuffer(SocketAsyncEventArgs args)
            {
                m_freeIndexPool.Push(args.Offset);
                args.SetBuffer(null, 0, 0);
            }
        }
    }
}
