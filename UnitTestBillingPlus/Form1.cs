using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Windows.Forms;
using BillingImplementation;
using busOutputDataWriting;
using Quobject.SocketIoClientDotNet.Client;
using Newtonsoft.Json.Linq;
using BillingLibrary;

namespace UnitTest
{
    public partial class Form1 : Form
    {
        private Int16 i16Ky = 0;
        private Int16 i16Thang = 0;
        private Int16 i16Nam = 0;
        private long lngCurrentLibID = 0;
        private long lngWorkflowID = 0;
        private string strMaDViQLy = "";
        private string strTenDNhap = "";
        private string strMaSoGCS = "";
        public Form1()
        {
            DateTime dt = DateTime.Now;
            InitializeComponent();
            //cls_Connection cls_Connection = new cls_Connection();
            //string strError = "";
            // bool ok = cls_Connection.testConnection(ref strError);
            //MessageBox.Show(ok.ToString() + "\n"+ strError);
            DataSet ds = new DataSet();
            //BillingImplementation.clsBillingImplementation cls = new clsBillingImplementation();
            //ds = cls.getMaSoGCSPlusList(2, 9, ref strError);
            
            //DataSet ds = new DataSet();
            //ds.ReadXml(@"D:/PD0131605.XML");
            //clsBillingImplementation impl = new clsBillingImplementation();
            //string strError = impl.FilterHDG_BBAN_APGIA(ref ds);

            string s = "";
            //for (int i = 0; i < 10000; i++)
            //{
            //    s += i.ToString();
            //}
            DateTime dt1 = DateTime.Now;
            double a = (dt1 - dt).TotalMilliseconds;
            //MessageBox.Show(a.ToString());
            var socket = IO.Socket("http://10.5.40.73:9000");

            socket.On(Socket.EVENT_CONNECT, () =>
            {
                JObject jout = JObject.FromObject(new { id = 123456, text = "Hello, I am C#" });
                //socket.Emit("test", jout);
                //DataIO data = new DataIO();
                //data.id = 123454;
                //data.text = "Hello, I am C#";
                socket.Emit("send_message", jout);
            });
        }

        private void btnTinhHDon_Click(object sender, EventArgs e)
        {
            try
            {
                i16Ky = Convert.ToInt16(txtKy.Text);
                i16Thang = Convert.ToInt16(txtThang.Text);
                i16Nam = Convert.ToInt16(txtNam.Text);
                lngCurrentLibID = Convert.ToInt64(txtCurrentLibID.Text);
                lngWorkflowID = Convert.ToInt64(txtWorkflowID.Text);
                strMaDViQLy = txtMaDViQLy.Text;
                strTenDNhap = "CMIS";
                strMaSoGCS = txtMaSoGCS.Text.Trim();
                string[] lstSo = strMaSoGCS.Split(';');
                clsBillingImplementation impl = new clsBillingImplementation();
                
                string strError = impl.BillingImplementationPlusAsync(strMaDViQLy, lstSo[0], i16Ky, i16Thang, i16Nam, strTenDNhap, lngCurrentLibID, lngWorkflowID, lstSo[1]);
                strError = strError.Trim().Length == 0 ? "Tính hóa đơn thành công" : "Lỗi: " + strError;
            }
            catch
            {

            }
        }

        private void Form1_Load(object sender, EventArgs e)
        {
            
            try
            {
                string strError = "";
                strError = clsBillingImplementation.GetStaticDataPlus();
                txtMaSoGCS.SelectAll();
                
            }
            catch
            {
            }
        }

        private void btnHuyTinh_Click(object sender, EventArgs e)
        {
            i16Ky = Convert.ToInt16(txtKy.Text);
            i16Thang = Convert.ToInt16(txtThang.Text);
            i16Nam = Convert.ToInt16(txtNam.Text);
            lngCurrentLibID = Convert.ToInt64(txtCurrentLibID.Text);
            lngWorkflowID = Convert.ToInt64(txtWorkflowID.Text);
            strMaDViQLy = txtMaDViQLy.Text;
            strTenDNhap = "LIENHT";
            strMaSoGCS = txtMaSoGCS.Text.Trim();
            string[] arrMaSo = new string[1];
            arrMaSo[0] = strMaSoGCS;
            clsCancelInvoiceCalculation objCancelCalculation = new clsCancelInvoiceCalculation();
            string strResult = objCancelCalculation.CancelInvoiceCalculation(strMaDViQLy, arrMaSo, strTenDNhap, lngCurrentLibID, lngWorkflowID, i16Ky, i16Thang, i16Nam);
            
        }

        private void txtKy_KeyPress(object sender, KeyPressEventArgs e)
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

        private void btnDChinh_Click(object sender, EventArgs e)
        {
            DataSet ds = new DataSet();
            ds.ReadXml(txtPath.Text,XmlReadMode.ReadSchema);
            i16Thang = Convert.ToInt16(txtThang.Text);
            i16Nam = Convert.ToInt16(txtNam.Text);
            clsBillingImplementation impl = new clsBillingImplementation();
            //string strError = impl.BillingImplementation_1(ds,  i16Thang, i16Nam);
                

        }

        private void btnTinhHDonPlus_Click(object sender, EventArgs e)
        {
            try
            {
                i16Ky = Convert.ToInt16(txtKy.Text);
                i16Thang = Convert.ToInt16(txtThang.Text);
                i16Nam = Convert.ToInt16(txtNam.Text);
                lngCurrentLibID = Convert.ToInt64(txtCurrentLibID.Text);
                lngWorkflowID = Convert.ToInt64(txtWorkflowID.Text);
                strMaDViQLy = txtMaDViQLy.Text;
                strTenDNhap = "CMIS_TINHLAI_2";
                strMaSoGCS = txtMaSoGCS.Text.Trim();
                string[] arrStr = strMaSoGCS.Split(';');
                strMaSoGCS = arrStr[0];
                String strNgayGhi = arrStr[1];
                clsBillingImplementation impl = new clsBillingImplementation();
                string strError = clsBillingImplementation.GetStaticDataPlus();
                 strError = impl.BillingImplementationPlusAsync(strMaDViQLy, strMaSoGCS, i16Ky, i16Thang, i16Nam, strTenDNhap, lngCurrentLibID, lngWorkflowID, strNgayGhi);
                strError = strError.Trim().Length == 0 ? "Tính hóa đơn thành công" : "Lỗi: " + strError;
                MessageBox.Show(strError);
            }
            catch
            {

            }
        }

        private void btnTinhHDonDC_Click(object sender, EventArgs e)
        {
            clsBillingImplementation impl = new clsBillingImplementation();
            string strError = clsBillingImplementation.GetStaticDataPlus();
            strError = impl.BillingImplementationDCPlus("PD0600", "1151419080", 1, 4, 2023, "1151419080");
            //PC03FF  1189794164  1   7   2021
            //strError = impl.BillingImplementationDCPlus("PE0700", "1141888401", 1, 5, 2019, "412746531");
            //impl.DeleteMaSoGCSPlus(strMaDViQLy_1, strMaSoGCS_1 + ";" + strNgayGhi_1 + ";" + lngCurrentLibID_1.ToString().Trim(), i16Ky_1, i16Thang_1, i16Nam_1, "Đang tính", 0);

        }

        private void btn_TinhHDonLe_Click(object sender, EventArgs e)
        {
            try
            {
                i16Ky = Convert.ToInt16(txtKy.Text);
                i16Thang = Convert.ToInt16(txtThang.Text);
                i16Nam = Convert.ToInt16(txtNam.Text);
                lngCurrentLibID = Convert.ToInt64(txtCurrentLibID.Text);
                lngWorkflowID = Convert.ToInt64(txtWorkflowID.Text);
                strMaDViQLy = txtMaDViQLy.Text;
                strTenDNhap = "CMIS";
                strMaSoGCS = txtMaSoGCS.Text.Trim();
                string[] arrStr = strMaSoGCS.Split(';');
                strMaSoGCS = arrStr[0];
                string strMaKHang = arrStr[1];
                String strNgayGhi = arrStr[2];
                clsBillingImplementation impl = new clsBillingImplementation();
                string strError = clsBillingImplementation.GetStaticDataPlus();
                strError = impl.BillingImplementationKH(strMaDViQLy, strMaSoGCS, strMaKHang, i16Ky, i16Thang, i16Nam, strTenDNhap, lngCurrentLibID, lngWorkflowID, strNgayGhi);
                strError = strError.Trim().Length == 0 ? "Tính hóa đơn thành công" : "Lỗi: " + strError;
            }
            catch
            {

            }
        }

        

        private void btnTinhOnline_Click(object sender, EventArgs e)
        {
            String strPathDMuc = "D:/DMucWeb.xml";
            String strPathData = "D:/CustomerDataAPI.xml";
            try
            {

                DataSet dsResult = new DataSet();
                DataSet dsDMuc = new DataSet();
                dsDMuc.ReadXml(strPathDMuc, XmlReadMode.ReadSchema);
                DataSet dsData = new DataSet();
                dsData.ReadXml(strPathData, XmlReadMode.ReadSchema);
                DateTime dt1 = DateTime.Now;
                clsBillingImplementation bus = new clsBillingImplementation();
                string strResult = bus.CommonImplementation_API(dsDMuc, ref dsResult, dsData);
                DateTime dt2 = DateTime.Now;
                //result.Message = "";
                //string strOut = JsonConvert.SerializeObject(dsResult);
                ////result.strError = "";
                //result.Data = JsonConvert.DeserializeObject<object>(strOut);


                //result.Time = dt2.Subtract(dt1).TotalMilliseconds;

            }
            catch (Exception ex)
            {
                
            }
        }

        private void btnGetDataXML_Click(object sender, EventArgs e)
        {
            string strError = "";
            try
                {
                    i16Ky = Convert.ToInt16(txtKy.Text);
                    i16Thang = Convert.ToInt16(txtThang.Text);
                    i16Nam = Convert.ToInt16(txtNam.Text);
                    lngCurrentLibID = Convert.ToInt64(txtCurrentLibID.Text);
                    lngWorkflowID = Convert.ToInt64(txtWorkflowID.Text);
                    strMaDViQLy = txtMaDViQLy.Text;
                    strTenDNhap = "CMIS";
                    strMaSoGCS = txtMaSoGCS.Text.Trim();
                    string[] arrStr = strMaSoGCS.Split(';');
                    strMaSoGCS = arrStr[0];
                    String strNgayGhi = arrStr[1];
                    clsBillingImplementation impl = new clsBillingImplementation();
                //cls_InputDataReading inputDataReading = new cls_InputDataReading();
                
                    strError = impl.getDataXML(strMaDViQLy, strMaSoGCS, i16Ky, i16Thang, i16Nam, strTenDNhap, lngCurrentLibID, lngWorkflowID, strNgayGhi);
                    strError = strError.Trim().Length == 0 ? "Tính hóa đơn thành công" : "Lỗi: " + strError;

                }
                catch (Exception ex)
                {
                strError = "Form: "+ ex.ToString();
                }
            MessageBox.Show(strError);
            
        }


        //impl.BillingImplementation(strMaDViQLy_1, strMaSoGCS_1, i16Ky_1, i16Thang_1, i16Nam_1, strTenDNhap_1, lngCurrentLibID_1, lngWorkflowID_1);
    }
}
