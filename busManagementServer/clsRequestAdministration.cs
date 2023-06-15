using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.ServiceModel;
using System.Windows.Forms;
using System.IO;


namespace busManagementServer
{
    public class clsRequestAdministration
    {
        #region Attributes
        private static IServiceTinhHDon service_TinhHDon;
        private static NetTcpBinding tcpBinding = new NetTcpBinding();
        #endregion

        #region Method DũngNT
        public static string getConnectString()
        {
            try
            {
                tcpBinding.TransactionFlow = false;
                tcpBinding.Security.Transport.ProtectionLevel = System.Net.Security.ProtectionLevel.EncryptAndSign;
                tcpBinding.Security.Transport.ClientCredentialType = TcpClientCredentialType.Windows;
                tcpBinding.Security.Mode = SecurityMode.None;
                DataSet ds = ApplicationServerList.dsApplicationServerList;
                string strPath = Application.StartupPath + @"\ApplicationServerList.xml";
                if (ds == null || ds.Tables["APP_SERVER_LIST"] == null || ds.Tables["APP_SERVER_LIST"].Rows.Count == 0)
                {
                    if (!File.Exists(strPath)) return "";
                    ApplicationServerList.dsApplicationServerList.ReadXml(strPath);
                    ds = ApplicationServerList.dsApplicationServerList;
                    if (ds == null || ds.Tables["APP_SERVER_LIST"] == null || ds.Tables["APP_SERVER_LIST"].Rows.Count == 0) return "";
                }
                long lngMin = Int64.MaxValue;
                string strConnection = "";
                foreach (DataRow row in ds.Tables["APP_SERVER_LIST"].Rows)
                {
                    try
                    {
                        //Khởi tạo kết nối tới ApplicationServer
                        string endPointAddr = "net.tcp://" + row["CONNECTION_ADDRESS"].ToString();
                        EndpointAddress endpointAddress = new EndpointAddress(endPointAddr);
                        ChannelFactory<IServiceTinhHDon> applicationserver = new ChannelFactory<IServiceTinhHDon>(tcpBinding, endPointAddr);
                        service_TinhHDon = applicationserver.CreateChannel();
                        if (applicationserver.State == CommunicationState.Opened)
                        {
                            //service_TinhHDon = ChannelFactory<IServiceTinhHDon>.CreateChannel(tcpBinding, endpointAddress);
                            //Lấy số lượng sổ đang có trong hàng đợi của service
                            long lngCount = service_TinhHDon.CountMaSoGCS();
                            if (lngMin > lngCount)
                            {
                                lngMin = lngCount;
                                strConnection = row["CONNECTION_ADDRESS"].ToString();
                            }
                            //End
                        }
                        applicationserver.Close();
                        
                    }
                    catch
                    {
                        MessageBox.Show("Lỗi tại Application Server: " + row["CONNECTION_ADDRESS"].ToString());
                        //applicationserver.Close();
                    }
                    Application.DoEvents();
                }
                return strConnection;
            }
            catch (Exception ex)
            {
                MessageBox.Show("Lỗi lấy chuỗi kết nối đến Application Server: " + ex.Message, "Thông báo", MessageBoxButtons.OK, MessageBoxIcon.Warning);
                return "";
            }
        }

        
        #endregion

    }
}
