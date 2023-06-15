using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Windows.Forms;

using System.Net;
using System.IO;
using Newtonsoft.Json;
using System.Reflection;
using BillingImplementation;
using System.Configuration;
using System.Threading;
using Newtonsoft.Json.Linq;
using Quobject.SocketIoClientDotNet.Client;
using BillingLibrary;

namespace ImplementationPlus
{
    public partial class frmMain : Form
    {
        #region Attributes
        /* DũngNT sửa ngày 19-10-2009
         * Thay thế service dạng WCF thành dạng WebService => xuất hiện file App.config
         * Sau này khi thay đổi lại cần xóa file App.config đi.
         */
        //private IServiceTinhHDon service;
        //private ApplicationServer.ApplicationServer service;
        //End DũngNT
        private System.Windows.Forms.Timer clock = new System.Windows.Forms.Timer();
        private System.Windows.Forms.Timer clockStaticData = new System.Windows.Forms.Timer();
        private System.Windows.Forms.Timer clockDC = new System.Windows.Forms.Timer();
        private System.Windows.Forms.Timer clockKH = new System.Windows.Forms.Timer();
        private Int16 i16Ky = 0;
        private Int16 i16Thang = 0;
        private Int16 i16Nam = 0;
        private long lngCurrentLibID = 0;
        private long lngWorkflowID = 0;
        private string strMaDViQLy = "";
        private string strTenDNhap = "";
        private string strNgayGhi = "";
        private string strMaSoGCS = "";
        private string strMaKHang = "";
        private string strName = "";
        private string strTimeStamp = "";
        private int maxThread = 10, numberOfThread = 0;
        string strMaDViQLyDC = "";
        int i16KyDC = 0;
        int i16ThangDC = 0;
        int i16NamDC = 0;
        String strIDHDonSai = "";
        string strTenDNhapDC = "";
        string strMaSoGCSDC = "";
        bool isDangTinhDC = false;
        bool isDangTinhKH = false;
        Socket socket = null;
        //private List<Thread> threadList;
        #endregion

        public frmMain()
        {
            InitializeComponent();

            try
            {
                //
                string strURI = ConfigurationManager.AppSettings["SOCKET_URI"];
                socket = IO.Socket(strURI);



                //DungxNT bổ sung thêm phần đọc số luồng tính dồng thời
                maxThread = 10;
                string strMaxThread = ConfigurationManager.AppSettings["MAXTHREAD"];
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
                clockDC.Tick += new EventHandler(clockDC_Tick);
                clockKH.Tick += new EventHandler(clockKH_Tick);
                string strError = "";
                strError = clsBillingImplementation.GetStaticDataPlus();
                //Loi khi khong lay duoc danh muc
                if (strError != "")
                {
                    MessageBox.Show("Lỗi: " + strError);
                    this.Close();
                }
                numberOfThread = 0;
                btnStart.Enabled = false;
                //cls_Connection cls = new cls_Connection();                
                //bool ok = cls.testConnection(ref strError);
                //MessageBox.Show(ok.ToString() + "\n" + strError);
                //cls.dispose();
                //if (!ok) this.Close();
                rtbMessage.Text = "Running..." + "\n";
            }
            catch (Exception e)
            {
                MessageBox.Show(e.Message);
            }
        }

        void clockKH_Tick(object sender, EventArgs e)
        {
            if (isDangTinhKH) return;
            try
            {
                //lấy dữ liệu từ HDN_DATA_DCHINH
                clsBillingImplementation impl = new clsBillingImplementation();
                List<string> lstHangDoi = impl.getMaSoGCSPlus(5);
                if (lstHangDoi != null)
                {
                    strMaDViQLy = lstHangDoi[0];
                    string[] arrStr = lstHangDoi[1].Split(';');
                    //cấu trúc trường MA_SOGCS bảng HDN_HANG_DOI:  MA_SOGCS;MA_KHANG;NGAY_GHI;MA_CNANG

                    strMaSoGCS = arrStr[0];
                    strMaKHang = arrStr[1];
                    strTenDNhap = lstHangDoi[2];
                    strNgayGhi = arrStr[2];
                    i16Ky = Convert.ToInt16(lstHangDoi[3]);
                    i16Thang = Convert.ToInt16(lstHangDoi[4]);
                    i16Nam = Convert.ToInt16(lstHangDoi[5]);
                    /* DũngNT sửa ngày 19-10-2009: Lấy thêm 2 biến phân luồng từ UI
                     **************************************************************************************************************/
                    lngCurrentLibID = Convert.ToInt64(arrStr[3]);
                    lngWorkflowID = 0;//Convert.ToInt64(row["WORKFLOWID"]);
                    /* DũngNT sửa ngày 17-01-2011: Lấy thêm 2 biến LockObject từ UI
                     **************************************************************************************************************/
                    strName = lstHangDoi[7];
                    strTimeStamp = lstHangDoi[8];
                    //Tạo thread tính toán
                    Thread threadItem = new Thread(new ThreadStart(this.ImplementationKH));
                    threadItem.Start();
                }

                //trong quá trình này ko tạo thread nào nữa
            }
            catch (Exception ex)
            {
                MessageBox.Show(ex.Message);
            }

        }
        private void ImplementationKH()
        {
            try
            {
                isDangTinhKH = true;
                //numberOfThread += 1;

                Int16 i16Ky_1 = i16Ky;
                Int16 i16Thang_1 = i16Thang;
                Int16 i16Nam_1 = i16Nam;
                string strMaDViQLy_1 = strMaDViQLy;
                string strTenDNhap_1 = strTenDNhap;
                string strMaKHang_1 = strMaKHang;
                string strMaSoGCS_1 = strMaSoGCS;
                /* DũngNT sửa ngày 19-10-2009: Gán thêm 2 biến phân luồng từ UI
                 * Truyền 2 biến vào phương thức BillingImplementation
                 **************************************************************************************************************/
                long lngCurrentLibID_1 = lngCurrentLibID;
                long lngWorkflowID_1 = lngWorkflowID;
                string strName_1 = strName;
                string strTimeStamp_1 = strTimeStamp;
                string strNgayGhi_1 = strNgayGhi;
                clsBillingImplementation impl = new clsBillingImplementation();

                lock (rtbMessage)
                {
                    if (rtbMessage.Text.Length >= 100000)
                    {
                        rtbMessage.Text = "";
                    }
                    rtbMessage.Text = "Đang tính hóa đơn cho khách hàng " + strMaKHang_1 + " - Giờ hiện tại: " + DateTime.Now + " Người tính: " + strTenDNhap_1 + "\n" + rtbMessage.Text;
                }

                string strError = impl.BillingImplementationKH(strMaDViQLy_1, strMaSoGCS_1, strMaKHang_1, i16Ky_1, i16Thang_1, i16Nam_1, strTenDNhap_1, lngCurrentLibID_1, lngWorkflowID_1, strNgayGhi_1);
                impl.DeleteMaSoGCSPlus(strMaDViQLy_1, strMaSoGCS_1 + ";" + strMaKHang_1 + ";" + strNgayGhi_1 + ";" + lngCurrentLibID_1.ToString().Trim(), Convert.ToInt64("-1"), i16Ky_1, i16Thang_1, i16Nam_1, "Đang tính", 5);
                //End DũngNT **************************************************************************************************/
                JObject jout = new JObject();
                if (strError.Trim().Length <= 0)
                {
                    impl.PushMessage(strMaDViQLy_1, strMaKHang_1, "Khách hàng " + strMaKHang_1 + " đã được tính xong.", strTenDNhap_1, i16Thang_1, i16Nam_1, 0, ref jout);
                }
                else
                {
                    impl.PushMessage(strMaDViQLy_1, strMaKHang_1, "Khách hàng " + strMaKHang_1 + " lỗi khi tính: " + strError, strTenDNhap_1, i16Thang_1, i16Nam_1, 1, ref jout);
                }

                lock (rtbMessage)
                {
                    rtbMessage.Text = "Kết thúc tính hóa đơn cho khách hàng " + strMaKHang_1 + " - Giờ hiện tại: " + DateTime.Now + " Người tính: " + strTenDNhap_1 + "\n" + rtbMessage.Text;
                }

                //numberOfThread -= 1;
                socket.Emit("send_message", jout);
            }
            catch (Exception ex)
            {
                MessageBox.Show("Lỗi trong quá trình thực thi: " + ex.Message, "Thông báo", MessageBoxButtons.OK, MessageBoxIcon.Warning);
            }
            finally
            {
                isDangTinhKH = false;
            }
        }
        void clockDC_Tick(object sender, EventArgs e)
        {
            //clockDC.Stop();
            if (isDangTinhDC) return;
            try
            {
                //lấy dữ liệu từ HDN_DATA_DCHINH
                clsBillingImplementation impl = new clsBillingImplementation();
                List<string> lstHangDoi = impl.getMaSoGCSPlus(1);
                if (lstHangDoi != null)
                {
                    strMaDViQLyDC = lstHangDoi[0];
                    i16KyDC = Convert.ToInt16(lstHangDoi[3]);
                    i16ThangDC = Convert.ToInt16(lstHangDoi[4]);
                    i16NamDC = Convert.ToInt16(lstHangDoi[5]);
                    strTenDNhapDC = lstHangDoi[2];
                    strIDHDonSai = lstHangDoi[9];
                    strMaSoGCSDC = lstHangDoi[1];
                    //Tạo thread tính toán
                    Thread threadItem = new Thread(new ThreadStart(this.ImplementationDC));
                    threadItem.Start();
                }

                //trong quá trình này ko tạo thread nào nữa
            }
            catch (Exception ex)
            {
                MessageBox.Show(ex.Message);
            }

        }
        private void ImplementationDC()
        {
            try
            {
                isDangTinhDC = true;
                clsBillingImplementation impl = new clsBillingImplementation();

                lock (rtbMessage)
                {
                    if (rtbMessage.Text.Length >= 100000)
                    {
                        rtbMessage.Text = "";
                    }
                    rtbMessage.Text = "Đang tính hóa đơn điều chỉnh cho hóa đơn " + strIDHDonSai + " - Giờ hiện tại: " + DateTime.Now + " Người tính: " + strTenDNhapDC + "\n" + rtbMessage.Text;
                }
                //Thực hiện tính hóa đơn điều chỉnh
                string strMaDViQLyDC_1 = strMaDViQLyDC;
                Int16 i16KyDC_1 = Convert.ToInt16(i16KyDC);
                Int16 i16ThangDC_1 = Convert.ToInt16(i16ThangDC);
                Int16 i16NamDC_1 = Convert.ToInt16(i16NamDC);
                string strTenDNhapDC_1 = strTenDNhapDC;
                string strIDHDonSai_1 = strIDHDonSai;
                string strMaSoGCSDC_1 = strMaSoGCSDC;
                string strError = impl.BillingImplementationDCPlus(strMaDViQLyDC_1, strMaSoGCSDC_1, i16KyDC_1, i16ThangDC_1, i16NamDC_1, strIDHDonSai_1);
                impl.DeleteMaSoGCSPlus(strMaDViQLyDC_1, strMaSoGCSDC_1, Convert.ToInt64(strIDHDonSai_1), i16KyDC_1, i16ThangDC_1, i16NamDC_1, "Đang tính", 1);
                JObject jout = new JObject();
                if (strError.Trim().Length <= 0)
                {
                    impl.PushMessage(strMaDViQLyDC_1, strIDHDonSai_1, "Hóa đơn sai " + strIDHDonSai_1 + " đã lập điều chỉnh thành công.", strTenDNhapDC_1, i16ThangDC_1, i16NamDC_1, 0, ref jout);
                }
                else
                {
                    impl.PushMessage(strMaDViQLyDC_1, strIDHDonSai_1, "Hóa đơn sai " + strIDHDonSai_1 + " lập điều chỉnh bị lỗi: " + strError, strTenDNhapDC_1, i16ThangDC_1, i16NamDC_1, 1, ref jout);
                }

                lock (rtbMessage)
                {
                    rtbMessage.Text = "Kết thúc tính hóa đơn cho sổ " + strMaSoGCSDC_1 + " - Giờ hiện tại: " + DateTime.Now + " Người tính: " + strTenDNhapDC_1 + "\n" + rtbMessage.Text;
                }

                numberOfThread -= 1;
                //impl = null;
                socket.Emit("send_message", jout);
            }
            catch (Exception ex)
            {
                MessageBox.Show("Lỗi trong quá trình thực thi: " + ex.Message, "Thông báo", MessageBoxButtons.OK, MessageBoxIcon.Warning);
            }
            finally
            {
                isDangTinhDC = false;
            }
        }
        void clockStaticData_Tick(object sender, EventArgs e)
        {
            //Mặc định là 1 giờ sáng thì lấy lại dữ liệu danh mục
            int hour = 1;
            if (ConfigurationManager.AppSettings["GETDATA"] != null)
            {
                try
                {
                    hour = Convert.ToInt32(ConfigurationManager.AppSettings["GETDATA"]);
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
                string strError = clsBillingImplementation.GetStaticDataPlus();
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
            clockDC.Interval = 1000;
            clockDC.Start();
            clockKH.Interval = 1000;
            clockKH.Start();

        }
        void clock_Tick(object sender, EventArgs e)
        {
            try
            {
                if (numberOfThread < maxThread)
                {
                    DataSet ds = null;
                    clsBillingImplementation impl = new clsBillingImplementation();
                    List<string> lstHangDoi = impl.getMaSoGCSPlus(0);
                    //IPHostEntry ips = Dns.GetHostEntry(Dns.GetHostName());
                    //foreach (IPAddress _ipAddr in ips.AddressList)
                    //{
                    //    try
                    //    {
                    //        service.Url = @"http://" + _ipAddr.ToString() + @"/ApplicationServer/ApplicationServer.asmx";
                    //        ds = service.getMaSoGCS();
                    //        break;
                    //    }
                    //    catch
                    //    {

                    //    }
                    //}
                    //Neu tong so luong < max thi moi cho phep cap phat them luong moi                    
                    //DataSet ds = service.getMaSoGCS();
                    if (lstHangDoi != null)
                    {
                        //Biến đầu vào từ DataRow
                        //DataRow row = ds.Tables[0].Rows[0];
                        //MA_DVIQLY, MA_SOGCS, TEN_DNHAP, KY, THANG, NAM, p_TTHAI_S as TRANG_THAI, S_NAME, TIMESTAMP
                        strMaDViQLy = lstHangDoi[0];
                        string[] arrStr = lstHangDoi[1].Split(';');
                        strMaSoGCS = arrStr[0];
                        strTenDNhap = lstHangDoi[2];
                        strNgayGhi = arrStr[1];
                        i16Ky = Convert.ToInt16(lstHangDoi[3]);
                        i16Thang = Convert.ToInt16(lstHangDoi[4]);
                        i16Nam = Convert.ToInt16(lstHangDoi[5]);
                        /* DũngNT sửa ngày 19-10-2009: Lấy thêm 2 biến phân luồng từ UI
                         **************************************************************************************************************/
                        lngCurrentLibID = Convert.ToInt64(arrStr[2]);
                        lngWorkflowID = 0;//Convert.ToInt64(row["WORKFLOWID"]);
                        /* DũngNT sửa ngày 17-01-2011: Lấy thêm 2 biến LockObject từ UI
                         **************************************************************************************************************/
                        strName = lstHangDoi[7];
                        strTimeStamp = lstHangDoi[8];
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
                string strNgayGhi_1 = strNgayGhi;
                clsBillingImplementation impl = new clsBillingImplementation();

                lock (rtbMessage)
                {
                    if (rtbMessage.Text.Length >= 100000)
                    {
                        rtbMessage.Text = "";
                    }
                    rtbMessage.Text = "Đang tính hóa đơn cho sổ " + strMaSoGCS_1 + " - Giờ hiện tại: " + DateTime.Now + " Người tính: " + strTenDNhap_1 + "\n" + rtbMessage.Text;
                }

                string strError = impl.BillingImplementationPlusAsync(strMaDViQLy_1, strMaSoGCS_1, i16Ky_1, i16Thang_1, i16Nam_1, strTenDNhap_1, lngCurrentLibID_1, lngWorkflowID_1, strNgayGhi_1);
                impl.DeleteMaSoGCSPlus(strMaDViQLy_1, strMaSoGCS_1 + ";" + strNgayGhi_1 + ";" + lngCurrentLibID_1.ToString().Trim(), Convert.ToInt64("-1"), i16Ky_1, i16Thang_1, i16Nam_1, "Đang tính", 0);
                //End DũngNT **************************************************************************************************/
                JObject jout = new JObject();
                if (strError.Trim().Length <= 0)
                {
                    impl.PushMessage(strMaDViQLy_1, strMaSoGCS_1, "Sổ " + strMaSoGCS_1 + " đã được tính xong.", strTenDNhap_1, i16Thang_1, i16Nam_1, 0, ref jout);
                }
                else
                {
                    impl.PushMessage(strMaDViQLy_1, strMaSoGCS_1, "Sổ " + strMaSoGCS_1 + " lỗi khi tính: " + strError, strTenDNhap_1, i16Thang_1, i16Nam_1, 1, ref jout);
                }

                lock (rtbMessage)
                {
                    rtbMessage.Text = "Kết thúc tính hóa đơn cho sổ " + strMaSoGCS_1 + " - Giờ hiện tại: " + DateTime.Now + " Người tính: " + strTenDNhap_1 + "\n" + rtbMessage.Text;
                }

                numberOfThread -= 1;
                socket.Emit("send_message", jout);
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
                clockDC.Start();
                clockKH.Start();

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
                clockDC.Stop();                
                clockKH.Stop();
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
