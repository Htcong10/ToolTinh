using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;

using System.Text;
using System.Windows.Forms;
using System.Net;
using System.ServiceModel;
using BillingImplementation;
using System.Threading;
using System.Configuration;

namespace Implementation
{
    public partial class frmMain : Form
    {
        #region Attributes
        /* DũngNT sửa ngày 19-10-2009
         * Thay thế service dạng WCF thành dạng WebService => xuất hiện file App.config
         * Sau này khi thay đổi lại cần xóa file App.config đi.
         */
        //private IServiceTinhHDon service;
        private ApplicationServer.ApplicationServer service;
        //End DũngNT
        private System.Windows.Forms.Timer clock = new System.Windows.Forms.Timer();
        private System.Windows.Forms.Timer clockStaticData = new System.Windows.Forms.Timer();
        private Int16 i16Ky = 0;
        private Int16 i16Thang = 0;
        private Int16 i16Nam = 0;
        private long lngCurrentLibID = 0;
        private long lngWorkflowID = 0;
        private string strMaDViQLy = "";
        private string strTenDNhap = "";
        private string strMaSoGCS = "";
        private string strName = "";
        private string strTimeStamp = "";
        private int maxThread = 10, numberOfThread = 0;
        //private List<Thread> threadList;
        #endregion

        public frmMain()
        {
            InitializeComponent();
            try
            {
                //DungxNT bổ sung thêm phần đọc số luồng tính dồng thời
                maxThread = 10;
                string strMaxThread = ConfigurationSettings.AppSettings["MAXTHREAD"];
                try
                {
                    maxThread = Convert.ToInt32(strMaxThread);
                }
                catch
                {
                    maxThread = 10;
                }
                txtMaxThread.Text = maxThread.ToString();
                txtMaxThread.Enabled = false;
                //End
                clock.Tick += new EventHandler(clock_Tick);
                clockStaticData.Tick += new EventHandler(clockStaticData_Tick);
                IPHostEntry ips = Dns.GetHostEntry(Dns.GetHostName());
                // Select the first entry. I hope it's this maschines IP
                IPAddress _ipAddress = ips.AddressList[0];
                /* DũngNT sửa ngày 19-10-2009
                 * Thay thế khởi tạo WCF bằng lệnh khởi tạo WebService                 
                 **************************************************************************************************************/
                //string endPointAddr = "net.tcp://" + _ipAddress.ToString() + ":8000/ServiceTinhHDon";
                //NetTcpBinding tcpBinding = new NetTcpBinding();
                //tcpBinding.TransactionFlow = false;
                //tcpBinding.Security.Transport.ProtectionLevel = System.Net.Security.ProtectionLevel.EncryptAndSign;
                //tcpBinding.Security.Transport.ClientCredentialType = TcpClientCredentialType.Windows;
                //tcpBinding.Security.Mode = SecurityMode.None;
                //EndpointAddress endpointAddress = new EndpointAddress(endPointAddr);
                //service = ChannelFactory<IServiceTinhHDon>.CreateChannel(tcpBinding, endpointAddress);
                service = new ApplicationServer.ApplicationServer();
                service.Url = @"http://" + ips.HostName.Trim() + @"/ApplicationServer/ApplicationServer.asmx";
                //service.Url = @"http://" + "10.9.0.208" + @"/ApplicationServer/ApplicationServer.asmx";
                //service.Url = @"http://" + _ipAddress.ToString() + @"/ApplicationServer/ApplicationServer.asmx";
                //End DũngNT -**************************************************************************************************/
                string strError = "";
                strError = clsBillingImplementation.GetStaticData();
                //Loi khi khong lay duoc danh muc
                if (strError != "")
                {
                    MessageBox.Show("Lỗi: " + strError);
                    this.Close();
                }
                numberOfThread = 0;
                btnStart.Enabled = false;
                rtbMessage.Text = "Running..." + "\n";
            }
            catch (Exception e)
            {
                MessageBox.Show(e.Message);
            }
        }

        void clockStaticData_Tick(object sender, EventArgs e)
        {
            //Mặc định là 1 giờ sáng thì lấy lại dữ liệu danh mục
            int hour = 1;
            if (ConfigurationSettings.AppSettings["GETDATA"] != null)
            {                
                try
                {
                    hour = Convert.ToInt32(ConfigurationSettings.AppSettings["GETDATA"]);
                    hour = (hour < 1 || hour >= 6) ? 1 : hour;
                }
                catch
                {
                    hour = 1;
                }
            }
            if (DateTime.Now.Hour == hour)
            {
                //Lấy lại StaticData
                clsBillingImplementation.SetStaticData();
                string strError = clsBillingImplementation.GetStaticData();
                lock (rtbMessage)
                {
                    if (rtbMessage.Text.Length >= 100000)
                    {
                        rtbMessage.Text = "";
                    }
                    rtbMessage.Text += strError.Trim().Length > 0 ? "Lấy lại dữ liệu danh mục: " + strError + "\n" : "Lấy lại dữ liệu danh mục thành công \n";
                }

            }
        }

        private void frmMain_Load(object sender, EventArgs e)
        {
            clock.Interval = 1000;
            clock.Start();
            clockStaticData.Interval = 2400000;
            clockStaticData.Start();
        }
        void clock_Tick(object sender, EventArgs e)
        {
            try
            {
                if (numberOfThread < maxThread)
                {
                    DataSet ds = null;
                    IPHostEntry ips = Dns.GetHostEntry(Dns.GetHostName());
                    foreach (IPAddress _ipAddr in ips.AddressList)
                    {
                        try
                        {
                            service.Url = @"http://" + _ipAddr.ToString() + @"/ApplicationServer/ApplicationServer.asmx";
                            ds = service.getMaSoGCS();
                            break;
                        }
                        catch
                        {

                        }
                    }
                    //Neu tong so luong < max thi moi cho phep cap phat them luong moi                    
                    //DataSet ds = service.getMaSoGCS();
                    if (ds != null)
                    {
                        //Biến đầu vào từ DataRow
                        DataRow row = ds.Tables[0].Rows[0];
                        strMaDViQLy = row["MA_DVIQLY"].ToString();
                        strMaSoGCS = row["MA_SOGCS"].ToString();
                        strTenDNhap = row["TEN_DNHAP"].ToString();
                        i16Ky = Convert.ToInt16(row["KY"].ToString());
                        i16Thang = Convert.ToInt16(row["THANG"].ToString());
                        i16Nam = Convert.ToInt16(row["NAM"].ToString());
                        /* DũngNT sửa ngày 19-10-2009: Lấy thêm 2 biến phân luồng từ UI
                         **************************************************************************************************************/
                        lngCurrentLibID = Convert.ToInt64(row["CURRENTLIBID"]);
                        lngWorkflowID = Convert.ToInt64(row["WORKFLOWID"]);
                        /* DũngNT sửa ngày 17-01-2011: Lấy thêm 2 biến LockObject từ UI
                         **************************************************************************************************************/
                        strName = row["S_NAME"].ToString();
                        strTimeStamp = row["TIMESTAMP"].ToString();
                        //End DũngNT -**************************************************************************************************/
                        Thread threadItem = new Thread(new ThreadStart(this.Implementation));
                        threadItem.Start();
                    }
                }
            }
            catch (Exception ex)
            {
                System.Windows.Forms.MessageBox.Show(ex.Message);
                clock.Stop();
            }
        }

        private void frmMain_FormClosed(object sender, FormClosedEventArgs e)
        {
            clock.Stop();
        }

        private void Implementation()
        {
            try
            {
                numberOfThread += 1;

                Int16 i16Ky_1 = i16Ky;
                Int16 i16Thang_1 = i16Thang;
                Int16 i16Nam_1 = i16Nam;
                string strMaDViQLy_1 = strMaDViQLy;
                string strTenDNhap_1 = strTenDNhap;
                string strMaSoGCS_1 = strMaSoGCS;
                /* DũngNT sửa ngày 19-10-2009: Gán thêm 2 biến phân luồng từ UI
                 * Truyền 2 biến vào phương thức BillingImplementation
                 **************************************************************************************************************/
                long lngCurrentLibID_1 = lngCurrentLibID;
                long lngWorkflowID_1 = lngWorkflowID;
                string strName_1 = strName;
                string strTimeStamp_1 = strTimeStamp;
                clsBillingImplementation impl = new clsBillingImplementation();

                lock (rtbMessage)
                {
                    if (rtbMessage.Text.Length >= 100000)
                    {
                        rtbMessage.Text = "";
                    }
                    rtbMessage.Text = "Đang tính hóa đơn cho sổ " + strMaSoGCS_1 + " - Giờ hiện tại: " + DateTime.Now + " Người tính: " + strTenDNhap_1 + "\n" + rtbMessage.Text;
                }

                string strError = impl.BillingImplementation(strMaDViQLy_1, strMaSoGCS_1, i16Ky_1, i16Thang_1, i16Nam_1, strTenDNhap_1, lngCurrentLibID_1, lngWorkflowID_1);

                //End DũngNT **************************************************************************************************/

                IPHostEntry ips = Dns.GetHostEntry(Dns.GetHostName());
                foreach (IPAddress _ipAddr in ips.AddressList)
                {
                    try
                    {
                        service.Url = @"http://" + _ipAddr.ToString() + @"/ApplicationServer/ApplicationServer.asmx";
                        service.DeleteMaSoGCS(strMaDViQLy_1, strMaSoGCS_1, i16Ky_1, i16Thang_1, i16Nam_1, strTenDNhap_1, lngCurrentLibID_1, lngWorkflowID_1);
                        service.KillSelfNew(strMaDViQLy_1, strName_1, strTenDNhap_1, lngCurrentLibID_1.ToString(), ref  strTimeStamp_1, ApplicationServer.LoaiDoiTuong.SO_GCS);
                        break;
                    }
                    catch
                    {

                    }
                }

                lock (rtbMessage)
                {
                    rtbMessage.Text = "Kết thúc tính hóa đơn cho sổ " + strMaSoGCS_1 + " - Giờ hiện tại: " + DateTime.Now + " Người tính: " + strTenDNhap_1 + "\n" + rtbMessage.Text;
                }

                numberOfThread -= 1;
            }
            catch (Exception ex)
            {
                MessageBox.Show("Lỗi trong quá trình thực thi: " + ex.Message, "Thông báo", MessageBoxButtons.OK, MessageBoxIcon.Warning);
            }
            finally
            {

            }
        }

        private void btnStart_Click(object sender, EventArgs e)
        {
            try
            {
                rtbMessage.Text += "Running!" + "\n";
                btnStart.Enabled = false;
                btnStop.Enabled = true;
                txtMaxThread.Enabled = false;
                maxThread = Convert.ToInt32(txtMaxThread.Text.Trim());
                clock.Start();
            }
            catch
            {
                maxThread = 10;
            }
        }

        private void btnStop_Click(object sender, EventArgs e)
        {
            try
            {
                rtbMessage.Text += "Stoped!" + "\n";
                btnStart.Enabled = true;
                btnStop.Enabled = false;
                txtMaxThread.Enabled = true;
                clock.Stop();
            }
            catch
            {
            }
        }

        private void txtMaxThread_KeyPress(object sender, KeyPressEventArgs e)
        {
            try
            {
                short val;
                if (!Int16.TryParse(e.KeyChar.ToString(), out val))
                {
                    if (!(e.KeyChar == '\b'))
                    {
                        e.Handled = true;
                    }
                }
                else
                    e.Handled = false;
            }
            catch
            {
            }

        }

        private void frmMain_FormClosing(object sender, FormClosingEventArgs e)
        {
            try
            {
                if (numberOfThread > 0)
                {
                    MessageBox.Show("Đang tính hóa đơn, không thể đóng chương trình", "Cảnh báo", MessageBoxButtons.OK, MessageBoxIcon.Warning);
                    e.Cancel = true;
                }

            }
            catch
            { 
            }
        }


    }
}
