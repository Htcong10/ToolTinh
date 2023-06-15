using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.ServiceProcess;
using System.Text;
using System.ServiceModel;
using System.Net;
using System.ServiceModel.Description;
using System.Windows.Forms;
using WCFApplicationServer;
using System.IO;
using System.Xml;



namespace WCFWinService
{
    partial class WCFWinService : ServiceBase
    {
        #region Attribute
        private ServiceHost host = null;
        private string urlMeta, urlService = "";
        
        #endregion
        public WCFWinService()
        {
            InitializeComponent();
        }

        protected override void OnStart(string[] args)
        {
            StartService();
        }
        protected void StartService()
        {
            try
            {
                IPHostEntry ips = Dns.GetHostEntry(Dns.GetHostName());
                IPAddress _ipAddress = ips.AddressList[0];
                urlService = "net.tcp://" + _ipAddress.ToString() + ":8000/ServiceTinhHDon";
                host = new ServiceHost(typeof(ServiceTinhHDon));
                NetTcpBinding tcpBinding = new NetTcpBinding();
                tcpBinding.TransactionFlow = false;
                tcpBinding.Security.Transport.ProtectionLevel = System.Net.Security.ProtectionLevel.EncryptAndSign;
                tcpBinding.Security.Transport.ClientCredentialType = TcpClientCredentialType.Windows;
                tcpBinding.Security.Mode = SecurityMode.None; // <- Very crucial

                // Add a endpoint
                host.AddServiceEndpoint(typeof(IServiceTinhHDon), tcpBinding, urlService);
                host.Opening += new EventHandler(host_Opening);
                host.Opened += new EventHandler(host_Opened);
                host.Closing += new EventHandler(host_Closing);
                host.Closed += new EventHandler(host_Closed);
                // A channel to describe the service. Used with the proxy scvutil.exe tool
                ServiceMetadataBehavior metadataBehavior;
                metadataBehavior = host.Description.Behaviors.Find<ServiceMetadataBehavior>();
                if (metadataBehavior == null)
                {
                    // This is how I create the proxy object that is generated via the svcutil.exe tool
                    metadataBehavior = new ServiceMetadataBehavior();
                    metadataBehavior.HttpGetUrl = new Uri("http://" + _ipAddress.ToString() + ":8001/ServiceTinhHDon");
                    metadataBehavior.HttpGetEnabled = true;
                    metadataBehavior.ToString();
                    host.Description.Behaviors.Add(metadataBehavior);
                    urlMeta = metadataBehavior.HttpGetUrl.ToString();
                }              
                host.Open();               
            }
            catch (Exception ex1)
            {
                Console.WriteLine(ex1.StackTrace);
            }
        }


        protected override void OnStop()
        {
            host.Close();
        }


        #region "Event"
        void host_Closed(object sender, EventArgs e)
        {
            //Service closed
        }

        void host_Closing(object sender, EventArgs e)
        {
            //Host are closing
        }

        void host_Opened(object sender, EventArgs e)
        {
            
        }       
        
        private void Append(string str)
        {
            //Add text to message here...
        }
        void host_Opening(object sender, EventArgs e)
        {
            //Message Opening here...
        }
        #endregion
    }
}
