using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;

namespace TcpServiceSample.Libs
{
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

        //public bool Stop { get; set; }
 
        /// <summary>  
        /// 数据缓存区  
        /// </summary>  
        public List<byte> Buffer { get; set; }

        public AsyncUserToken()
        {
            this.Buffer = new List<byte>();
        }
    }
}
