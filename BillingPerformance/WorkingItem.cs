using BillingImplementation;
using busInputDataReading;
using Newtonsoft.Json.Linq;
using Quobject.SocketIoClientDotNet.Client;
using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace BillingPerformance
{
    public class WorkingItem : IDisposable
    {
        private ManualResetEvent _doneEvent;
        private List<object> _drData = null;
        private Label _lbl = null;
        private Socket _socket = null;
        private RichTextBox _rtb = null;
        private bool IsDisposed = false;
        private void LogLabel(int iIncrease)
        {
            this._lbl.Invoke(new EventHandler(delegate
            {
            this._lbl.Text = (Convert.ToInt32(this._lbl.Text) + iIncrease).ToString();
             
            }
            ));
        }
        private void LogRtb(string strMes)
        {
            this._rtb.Invoke(new EventHandler(delegate
            {
                this._rtb.Text = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff") + ":" + strMes + "\n" + this._rtb.Text;
            }
            ));
        }
        public WorkingItem(List<object> drData, ManualResetEvent doneEvent, Label lbl, Socket socket, RichTextBox rtb)
        {
            _drData = drData;
            _doneEvent = doneEvent;
            _lbl = lbl;
            _socket = socket;
            _rtb = rtb;
        }
        ~WorkingItem()
        {
            Dispose(false);
        }
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }
        protected virtual void Dispose(bool disposedStatus)
        {

            if (!IsDisposed)
            {
                IsDisposed = true;
                // Released unmanaged Resources
                if (disposedStatus)
                {
                    // Released managed Resources
                }
            }
        }
        public async Task ThreadPoolCallback(int threadContext)
        {
            this.DoWordAsync(threadContext);
        }
        private async Task DoWordAsync(int threadContext)
        {
            string strMaDViQLy_1 = "";
            string strMaSoGCS_1 = "";
            try
            {            
                string[] arrStr = Convert.ToString(_drData[1]).Split(';');
                Int16 i16Ky_1 = Convert.ToInt16(_drData[3]);
                Int16 i16Thang_1 = Convert.ToInt16(_drData[4]);
                Int16 i16Nam_1 = Convert.ToInt16(_drData[5]);
                strMaDViQLy_1 = Convert.ToString(_drData[0]);
                string strTenDNhap_1 = Convert.ToString(_drData[2]);
                strMaSoGCS_1 = arrStr[0];
                /* DũngNT sửa ngày 19-10-2009: Gán thêm 2 biến phân luồng từ UI
                 * Truyền 2 biến vào phương thức BillingImplementation
                 **************************************************************************************************************/
                long lngCurrentLibID_1 = Convert.ToInt64(arrStr[2]);
                long lngWorkflowID_1 = 0;
                string strName_1 = Convert.ToString(_drData[7]);
                string strTimeStamp_1 = Convert.ToString(_drData[8]);
                string strNgayGhi_1 = arrStr[1];
                clsBillingImplementation impl = new clsBillingImplementation();
                //LogRtb("Tính hóa đơn sổ: "+ strMaSoGCS_1);
                string strError = await impl.BillingImplementationPlus(strMaDViQLy_1, strMaSoGCS_1, i16Ky_1, i16Thang_1, i16Nam_1, strTenDNhap_1, lngCurrentLibID_1, lngWorkflowID_1, strNgayGhi_1);
                await impl.DeleteMaSoGCSPlus(strMaDViQLy_1, strMaSoGCS_1 + ";" + strNgayGhi_1 + ";" + lngCurrentLibID_1.ToString().Trim(), Convert.ToInt64("-1"), i16Ky_1, i16Thang_1, i16Nam_1, "Đang tính", 0);
                //End DũngNT **************************************************************************************************/
                //LogRtb("Kết thúc tính hóa đơn sổ: " + strMaSoGCS_1);
                
                JObject jout = new JObject();
                if (strError.Trim().Length <= 0)
                {
                    //LogRtb("Tính toán thành công luồng :  " + threadContext.ToString() + " " + strMaSoGCS_1);
                    impl.PushMessage(strMaDViQLy_1, strMaSoGCS_1, "Sổ " + strMaSoGCS_1 + " đã được tính xong.", strTenDNhap_1, i16Thang_1, i16Nam_1, 0, ref jout);
                }
                else
                {
                    //LogRtb("Tính toán lỗi luồng :  " + threadContext.ToString() + " " + strMaSoGCS_1 + strError);
                    impl.PushMessage(strMaDViQLy_1, strMaSoGCS_1, "Sổ " + strMaSoGCS_1 + " lỗi khi tính: " + strError, strTenDNhap_1, i16Thang_1, i16Nam_1, 1, ref jout);
                }
                impl.Dispose();
                _socket.Emit("send_message", jout);
                LogLabel(-1);
            }
            catch (Exception ex)
            {
                LogRtb("Lỗi trong quá trình thực thi: " + strMaDViQLy_1 + " - " + strMaSoGCS_1 + " - " + ex.Message);
            }
            finally
            {
                
            }
        }

    }
    public class WorkingItemDS
    {
        private ManualResetEvent _doneEvent;
        private DataRow _drData = null;
        private Label _lbl = null;
        private Socket _socket = null;
        private RichTextBox _rtb = null;
        private void LogLabel(int iIncrease)
        {
            this._lbl.Invoke(new EventHandler(delegate
            {
                this._lbl.Text = (Convert.ToInt32(this._lbl.Text) + iIncrease).ToString();

            }
            ));
        }
        private void LogRtb(string strMes)
        {
            this._rtb.Invoke(new EventHandler(delegate
            {
                this._rtb.Text = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff") + ":" + strMes + "\n" + this._rtb.Text;
            }
            ));
        }
        public WorkingItemDS(DataRow drData, ManualResetEvent doneEvent, Label lbl, Socket socket, RichTextBox rtb)
        {
            _drData = drData;
            _doneEvent = doneEvent;
            _lbl = lbl;
            _socket = socket;
            _rtb = rtb;
        }
        public void ThreadPoolCallback(object threadContext)
        {
            //int threadIndex = (int)threadContext;
            //Console.WriteLine($"Thread {threadIndex} started...");
            //Log(true);
            this.DoWordAsync();
            //Console.WriteLine($"Thread {threadIndex} result calculated...");

            this._doneEvent.Set();
            LogLabel(-1);
        }


        private async Task DoWordAsync()
        {
            string strMaDViQLy_1 = "";
            string strMaSoGCS_1 = "";
            try
            {
                string[] arrStr = Convert.ToString(_drData["MA_SOGCS"]).Split(';');
                Int16 i16Ky_1 = Convert.ToInt16(_drData["KY"]);
                Int16 i16Thang_1 = Convert.ToInt16(_drData["THANG"]);
                Int16 i16Nam_1 = Convert.ToInt16(_drData["NAM"]);
                strMaDViQLy_1 = Convert.ToString(_drData["MA_DVIQLY"]);
                string strTenDNhap_1 = Convert.ToString(_drData["TEN_DNHAP"]);
                strMaSoGCS_1 = arrStr[0];
                /* DũngNT sửa ngày 19-10-2009: Gán thêm 2 biến phân luồng từ UI
                 * Truyền 2 biến vào phương thức BillingImplementation
                 **************************************************************************************************************/
                long lngCurrentLibID_1 = Convert.ToInt64(arrStr[2]);
                long lngWorkflowID_1 = 0;
                string strName_1 = Convert.ToString(_drData["S_NAME"]);
                string strTimeStamp_1 = Convert.ToString(_drData["TIMESTAMP"]);
                string strNgayGhi_1 = arrStr[1];
                clsBillingImplementation impl = new clsBillingImplementation();
                //LogRtb("Tính hóa đơn sổ: "+ strMaSoGCS_1);
               // string strError = await impl.BillingImplementationPlus(strMaDViQLy_1, strMaSoGCS_1, i16Ky_1, i16Thang_1, i16Nam_1, strTenDNhap_1, lngCurrentLibID_1, lngWorkflowID_1, strNgayGhi_1,);
                //impl.DeleteMaSoGCSPlus(strMaDViQLy_1, strMaSoGCS_1 + ";" + strNgayGhi_1 + ";" + lngCurrentLibID_1.ToString().Trim(), Convert.ToInt64("-1"), i16Ky_1, i16Thang_1, i16Nam_1, "Đang tính", 0);
                //End DũngNT **************************************************************************************************/
                //LogRtb("Kết thúc tính hóa đơn sổ: " + strMaSoGCS_1);
                /*JObject jout = new JObject();
                if (strError.Trim().Length <= 0)
                {
                    impl.PushMessage(strMaDViQLy_1, strMaSoGCS_1, "Sổ " + strMaSoGCS_1 + " đã được tính xong.", strTenDNhap_1, i16Thang_1, i16Nam_1, 0, ref jout);
                }
                else
                {
                    impl.PushMessage(strMaDViQLy_1, strMaSoGCS_1, "Sổ " + strMaSoGCS_1 + " lỗi khi tính: " + strError, strTenDNhap_1, i16Thang_1, i16Nam_1, 1, ref jout);
                }
                _socket.Emit("send_message", jout);*/
            }
            catch (Exception ex)
            {
                LogRtb("Lỗi trong quá trình thực thi: " + strMaDViQLy_1 + " - " + strMaSoGCS_1 + " - " + ex.Message);
            }
            finally
            {

            }
        }
    }
}
