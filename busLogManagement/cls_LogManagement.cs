using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using LogManagementObject;
using System.IO;
using DbConnect.DB;
using System.Configuration;
using System.Net;
using Newtonsoft.Json;
using BillingLibrary;
using System.Data.OracleClient;

namespace busLogManagement
{
    public class cls_LogManagement
    {
        #region Attributes
        private List<cls_S_LOG_Info> lstInfo;
        private cls_S_LOG_Controller obj_S_LOG_Controller;
        #endregion

        #region Constructor
        public cls_LogManagement()
        {
            lstInfo = new List<cls_S_LOG_Info>();
            obj_S_LOG_Controller = new cls_S_LOG_Controller();
        }
        public cls_LogManagement(DataSet Entity)
        {
            lstInfo = new List<cls_S_LOG_Info>();
            obj_S_LOG_Controller = new cls_S_LOG_Controller();
            try
            {
                foreach (DataRow dr in Entity.Tables["S_LOG"].Rows)
                {
                    cls_S_LOG_Info info = new cls_S_LOG_Info();
                    info.SUBDIVISIONID = dr["SUBDIVISIONID"].ToString();
                    info.NAMESPACE = dr["NAMESPACE"].ToString();
                    info.METHODNAME = dr["METHODNAME"].ToString();
                    info.DETAIL = dr["DETAIL"].ToString();
                    info.BOOKID = dr["BOOKID"].ToString();
                    info.ASSEMBLYNAME = dr["ASSEMBLYNAME"].ToString();
                    if (dr["LOGID"].ToString().Trim().Length > 0)
                        info.LOGID = Convert.ToInt64(dr["LOGID"]);
                    if (dr["TIME"].ToString().Trim().Length > 0)
                        info.TIME = Convert.ToDateTime(dr["TIME"]);
                    lstInfo.Add(info);
                }
                obj_S_LOG_Controller.lstInfo = lstInfo;                
            }
            catch (Exception ex)
            {
                try
                {                    
                    using (FileStream fileStream = new FileStream(@"C:/Log.txt", FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None))
                    {
                        using (StreamWriter streamWriter = new StreamWriter(fileStream))
                        {
                            streamWriter.WriteLine("Lỗi khi khởi tạo: " + ex.Message);
                        }
                    }
                }
                catch
                {
                }
            }


        }
        #endregion

        #region Method DũngNT
        /// <summary>
        /// Lấy log theo số lượng bản ghi gần nhất
        /// </summary>
        /// <param name="intNumberLog">Số bản ghi cần lấy</param>
        /// <returns></returns>
        public DataSet getLogByParameter(int intNumberLog)
        {
            CMIS2 db = new CMIS2();
            try
            {
                obj_S_LOG_Controller = new cls_S_LOG_Controller();
                obj_S_LOG_Controller.CMIS2 = db;
                DataSet ds = obj_S_LOG_Controller.getLogByParameter(intNumberLog);
                return ds;
            }
            catch
            {
                return null;
            }
            finally
            {
                db.ReleaseConnection();
            }
        }
        /// <summary>
        /// Lấy log theo số lượng bản ghi gần nhất và theo mã đơn vị quản lý
        /// </summary>
        /// <param name="strMaDViQLy">Mã đơn vị quản lý</param>
        /// <param name="intNumberLog">Số bản ghi cần lấy</param>        
        /// <returns></returns>
        public DataSet getLogByMaDViQLy(string strMaDViQLy, int intNumberLog)
        {
            CMIS2 db = new CMIS2();
            try
            {
                obj_S_LOG_Controller = new cls_S_LOG_Controller();
                obj_S_LOG_Controller.CMIS2 = db;
                DataSet ds = obj_S_LOG_Controller.getLogByMaDViQLy(strMaDViQLy, intNumberLog);
                return ds;
            }
            catch
            {
                return null;
            }
            finally
            {
                db.ReleaseConnection();
            }
        }
        /// <summary>
        /// Lấy log theo số lượng bản ghi gần nhất và theo mã đơn vị quản lý + mã sổ ghi chỉ số
        /// </summary>
        /// <param name="strMaDViQLy">Mã đơn vị quản lý</param>
        /// <param name="strMaSoGCS">Mã sổ ghi chỉ số</param>   
        /// <param name="intNumberLog">Số bản ghi cần lấy</param>  
        /// <returns></returns>
        public DataSet getLogByMaSoGCS(string strMaDViQLy, string strMaSoGCS, int intNumberLog)
        {
            CMIS2 db = new CMIS2();
            try
            {
                obj_S_LOG_Controller = new cls_S_LOG_Controller();
                obj_S_LOG_Controller.CMIS2 = db;
                DataSet ds = obj_S_LOG_Controller.getLogByMaSoGCS(strMaDViQLy, strMaSoGCS, intNumberLog);
                return ds;
            }
            catch
            {
                return null;
            }
            finally
            {
                db.ReleaseConnection();
            }
        }
         /// <summary>
        /// Insert một mảng đối tượng, sau khi insert xong submit luôn 
        /// Điều kiện yêu cầu: phải khởi tạo busLogManagement với DataSet đầu vào
        /// </summary>
        /// <returns>True or False</returns>
        public bool InsertList()
        {
            CMIS2 db = new CMIS2();
            try
            {
                obj_S_LOG_Controller.CMIS2 = db;
                bool blnResult = obj_S_LOG_Controller.InsertList();
                if (blnResult == true)
                {
                    db.DB.SubmitChanges();
                }
                return blnResult;
            }
            catch
            {
                return false;
            }
            finally
            {
                db.ReleaseConnection();
            }
        }
         /// <summary>
        /// Insert một mảng đối tượng, sau khi insert xong không submit.
        /// Muốn submit có thể gọi hàm SubmitChange (void)
        /// Điều kiện yêu cầu: phải khởi tạo busLogManagement với DataSet đầu vào
        /// </summary>
        /// <returns></returns>
        public bool InsertListNoSubmit()
        {
            CMIS2 db = new CMIS2();
            try
            {
                obj_S_LOG_Controller.CMIS2 = db;
                bool blnResult = obj_S_LOG_Controller.InsertListNoSubmit();
                if (blnResult == true)
                {
                    db.DB.SubmitChanges();
                }
                return blnResult;
            }
            catch
            {
                return false;
            }
            finally
            {
                db.ReleaseConnection();
            }
        }
         /// <summary>
        /// Xóa log theo LogID và mã đơn vị quản lý
        /// </summary>
        /// <param name="strMaDViQLy">Mã đơn vị quản lý</param>
        /// <param name="lngLogID">Log ID cần xóa</param>
        /// <returns></returns>
        public bool DeleteByLogID(string strMaDViQLy, long lngLogID)
        {
            CMIS2 db = new CMIS2();
            try
            {
                if (obj_S_LOG_Controller == null) obj_S_LOG_Controller = new cls_S_LOG_Controller();
                obj_S_LOG_Controller.CMIS2 = db;
                bool blnResult = obj_S_LOG_Controller.DeleteByLogID(strMaDViQLy, lngLogID);
                if (blnResult == true)
                {
                    db.DB.SubmitChanges();
                }
                return blnResult;
            }
            catch
            {
                return false;
            }
            finally
            {
                db.ReleaseConnection();
            }
        }
         /// <summary>
        /// Xóa log theo mã đơn vị quản lý và khoảng ID
        /// nếu truyền vào lngFromID > lngToID, hàm sẽ tự động đổi vai trò 2 biến nhằm đảm bảo lngToID > lngFromID
        /// </summary>
        /// <param name="strMaDViQLy">Mã đơn vị quản lý</param>
        /// <param name="lngFromID">Log ID nhỏ nhất</param>
        /// <param name="lngToID">Log ID lớn nhất</param>
        /// <returns></returns>
        public bool DeleteFromIDToID(string strMaDViQLy, long lngFromID, long lngToID)
        {
            CMIS2 db = new CMIS2();
            try
            {
                if (obj_S_LOG_Controller == null) obj_S_LOG_Controller = new cls_S_LOG_Controller();
                obj_S_LOG_Controller.CMIS2 = db;
                bool blnResult = obj_S_LOG_Controller.DeleteFromIDToID(strMaDViQLy, lngFromID, lngToID);
                if (blnResult == true)
                {
                    db.DB.SubmitChanges();
                }
                return blnResult;
            }
            catch
            {
                return false;
            }
            finally
            {
                db.ReleaseConnection();
            }
        }
         /// <summary>
        /// Xóa log theo mã đơn vị quản lý và mã sổ ghi chỉ số
        /// </summary>
        /// <param name="strMaDViQLy"></param>
        /// <param name="strMaSoGCS"></param>
        /// <returns></returns>
        public bool DeleteByMaSoGCS(string strMaDViQLy, string strMaSoGCS)
        {
            CMIS2 db = new CMIS2();
            try
            {
                if (obj_S_LOG_Controller == null) obj_S_LOG_Controller = new cls_S_LOG_Controller();
                obj_S_LOG_Controller.CMIS2 = db;
                bool blnResult = obj_S_LOG_Controller.DeleteByMaSoGCS(strMaDViQLy, strMaSoGCS);
                if (blnResult == true)
                {
                    db.DB.SubmitChanges();
                }
                return blnResult;
            }
            catch
            {
                return false;
            }
            finally
            {
                db.ReleaseConnection();
            }
        }
        #endregion
        #region Tinh hoa don CMIS 3
        public bool InsertListPlus(DataSet Entity)
        {
            
            try
            {
                String strInput = JsonConvert.SerializeObject(Entity.Tables[0].Rows[0]);
                string strIP = ConfigurationManager.AppSettings["URI"];
                var baseAddress = "http://" + strIP + "/ServiceHDonPSinh-HDonPSinh-context-root/resources/serviceHDonPSinh/writeLog";
                var http = (HttpWebRequest)WebRequest.Create(new Uri(baseAddress));
                http.Accept = "application/json";
                http.ContentType = "application/json";
                http.Method = "POST";

                //string parsedContent = "{'MA_DVIQLY':'PC06AA','TEN_DANH_MUC':'D_SOGCS'}";
                string parsedContent = strInput;
                //ASCIIEncoding encoding = new ASCIIEncoding();
                Byte[] bytes = System.Text.Encoding.UTF8.GetBytes(parsedContent);
                //Byte[] bytes = encoding.GetBytes(parsedContent);

                Stream newStream = http.GetRequestStream();
                newStream.Write(bytes, 0, bytes.Length);
                newStream.Close();

                var response = http.GetResponse();

                var stream = response.GetResponseStream();
                var sr = new StreamReader(stream);
                var content = sr.ReadToEnd();
                ReturnObject obj = JsonConvert.DeserializeObject<ReturnObject>(content);
                if (obj.TYPE == "ERROR") return false;
                else return true;
            }
            catch
            {
                return false;
            }
            finally
            {
                
            }
        }
        public List<string> getMaSoGCSPlus(string strTThaiT, string strTThaiS, int i32LoaiTT, ref string strError)
        {

            try
            {
                String strInput = "{'TTHAI_T':'" + strTThaiT + "','TTHAI_S':'" + strTThaiS + "', 'LOAI_TTAC':" + i32LoaiTT.ToString().Trim() + "}";
                string strIP = ConfigurationManager.AppSettings["URI"];
                var baseAddress = "http://" + strIP + "/ServiceHDonPSinh-HDonPSinh-context-root/resources/serviceHDonPSinh/getMaSoGCSTHD";
                var http = (HttpWebRequest)WebRequest.Create(new Uri(baseAddress));
                http.Accept = "application/json";
                http.ContentType = "application/json";
                http.Method = "POST";

                //string parsedContent = "{'MA_DVIQLY':'PC06AA','TEN_DANH_MUC':'D_SOGCS'}";
                string parsedContent = strInput;
                //ASCIIEncoding encoding = new ASCIIEncoding();
                Byte[] bytes = System.Text.Encoding.UTF8.GetBytes(parsedContent);
                //Byte[] bytes = encoding.GetBytes(parsedContent);

                Stream newStream = http.GetRequestStream();
                newStream.Write(bytes, 0, bytes.Length);
                newStream.Close();

                var response = http.GetResponse();

                var stream = response.GetResponseStream();
                var sr = new StreamReader(stream);
                var content = sr.ReadToEnd();
                ReturnObject obj = JsonConvert.DeserializeObject<ReturnObject>(content);
                if (obj.TYPE == "NODATA") return null;
                else if (obj.TYPE == "ERROR")
                    strError = obj.MESSAGE;
                else
                {
                    //Có dữ liệu
                    ReturnHDoi objHangDoi = JsonConvert.DeserializeObject<ReturnHDoi>(obj.MESSAGE);
                    return objHangDoi.HDN_HANG_DOI[0];
                }
                return null;
            }
            catch(Exception ex)
            {
                strError = ex.Message;
                return null;
            }
            finally
            {

            }
        }

        public bool DeleteMaSoGCSPlus(string strMaDViQLy, string strMaSoGCS, Int64 i64ID_HDON, int ky, int thang, int nam, string strTrangThai, int i32LoaiTT, ref string strError)
        {
            try
            {
                String strInput = "{'MA_DVIQLY':'" + strMaDViQLy + "','MA_SOGCS':'" + strMaSoGCS + "','TRANG_THAI':'" + strTrangThai + "','KY':" + ky + ",'THANG':" + thang + ",'NAM':" + nam + ",'LOAI_TTAC':" + i32LoaiTT.ToString().Trim() + ",'ID_HDON':" + i64ID_HDON.ToString().Trim() + "}";
                string strIP = ConfigurationManager.AppSettings["URI"];
                var baseAddress = "http://" + strIP + "/ServiceHDonPSinh-HDonPSinh-context-root/resources/serviceHDonPSinh/DeleteMaSoGCSTHD";
                string parsedContent = strInput;
                var http = (HttpWebRequest)WebRequest.Create(new Uri(baseAddress));
                http.Accept = "application/json";
                http.ContentType = "application/json";
                http.Method = "POST";

                //string parsedContent = "{'MA_DVIQLY':'PC06AA','TEN_DANH_MUC':'D_SOGCS'}";
                
                //ASCIIEncoding encoding = new ASCIIEncoding();
                Byte[] bytes = System.Text.Encoding.UTF8.GetBytes(parsedContent);
                //Byte[] bytes = encoding.GetBytes(parsedContent);

                Stream newStream = http.GetRequestStream();
                newStream.Write(bytes, 0, bytes.Length);
                newStream.Close();

                var response = http.GetResponse();

                var stream = response.GetResponseStream();
                var sr = new StreamReader(stream);
                var content = sr.ReadToEnd();
                ReturnObject obj = JsonConvert.DeserializeObject<ReturnObject>(content);
                if (obj.TYPE == "ERROR")
                {
                    strError = obj.MESSAGE;
                    return false;
                }
                else return true;
            }
            catch(Exception ex)
            {
                strError = ex.Message;
                return false;
            }
            finally
            {

            }
        }
        public class ReturnObject
        {
            public string TYPE { get; set; }
            public string MESSAGE { get; set; }
            
        }
        public class ReturnHDoi
        {
            public List<List<string>> HDN_HANG_DOI { get; set; }
        }
        public class ReturnHDoiAPI
        {
            public List<List<object>> HDN_HANG_DOI { get; set; }
        }
        #endregion
        #region CMIS 2022
        public DataSet getMaSoGCSPlusList(string strTThaiT, string strTThaiS, int i32LoaiTT, int i32SLuong, ref string strError)
        {
            cls_Connection getConn = new cls_Connection(cls_Connection.Schema.HDONPSINH);
            

            try
            {
                OracleConnection conn = getConn.OraConn;
                DataSet ds = new DataSet();
                OracleCommand cmd = new OracleCommand();
                cmd.Parameters.Clear();
                cmd.Connection = conn;
                cmd.CommandType = CommandType.StoredProcedure;
                cmd.CommandText = "PKG_TINHHOADON.SP_GET_SO_QUEUE_PERFORMANCE";
                cmd.Parameters.AddWithValue("p_TTHAI_T", strTThaiT).Direction = ParameterDirection.Input;
                cmd.Parameters.AddWithValue("p_TTHAI_S", strTThaiS).Direction = ParameterDirection.Input;
                cmd.Parameters.AddWithValue("p_LOAI_TTAC", i32LoaiTT).Direction = ParameterDirection.Input;
                cmd.Parameters.AddWithValue("p_SLUONG", i32SLuong).Direction = ParameterDirection.Input;
                cmd.Parameters.Add(new OracleParameter("p_ERROR", System.Data.OracleClient.OracleType.VarChar, 500)).Direction = ParameterDirection.Output;
                cmd.Parameters.Add(new OracleParameter("p_RESULT", System.Data.OracleClient.OracleType.Cursor)).Direction = ParameterDirection.Output;

                OracleDataAdapter da = new OracleDataAdapter(cmd);
                da.Fill(ds, "HDN_HANG_DOI");
                return ds;
            }
            catch (Exception ex)
            {
                strError = ex.Message;
                return null;
            }
            finally
            {
                getConn.dispose();
            }
        }
        public List<List<object>> getMaSoGCSPlusListAPI(string strTThaiT, string strTThaiS, string maDviQly, int i32LoaiTT, int i32SLuong, ref string strError)
        {

            try
            {
                String strInput = "{\"TTHAI_T\":\"" + strTThaiT + "\",\"MA_DVIQLY\":\"" + maDviQly + "\",\"TTHAI_S\":\"" + strTThaiS + "\",\"SLUONG\":" + i32SLuong.ToString().Trim() + ", \"LOAI_TTAC\":" + i32LoaiTT.ToString().Trim() + "}";
                string strIP_Per = ConfigurationManager.AppSettings["URI_PERFORMANCE"];
                //var baseAddress = "http://" + strIP + "/ServiceHDonPSinh-HDonPSinh-context-root/resources/serviceHDonPSinh/getMaSoGCSTHD";
                var baseAddress = "http://" + strIP_Per + "/api/hdon/getMaSoGCSTHD";
                var http = (HttpWebRequest)WebRequest.Create(new Uri(baseAddress));
                http.Accept = "application/json";
                http.ContentType = "application/json";
                http.Method = "POST";

                //string parsedContent = "{'MA_DVIQLY':'PC06AA','TEN_DANH_MUC':'D_SOGCS'}";
                string parsedContent = strInput;
                //ASCIIEncoding encoding = new ASCIIEncoding();
                Byte[] bytes = System.Text.Encoding.UTF8.GetBytes(parsedContent);
                //Byte[] bytes = encoding.GetBytes(parsedContent);

                Stream newStream = http.GetRequestStream();
                newStream.Write(bytes, 0, bytes.Length);
                newStream.Close();

                var response = http.GetResponse();

                var stream = response.GetResponseStream();
                var sr = new StreamReader(stream);
                var content = sr.ReadToEnd();
                ReturnObject obj = JsonConvert.DeserializeObject<ReturnObject>(content);
                if (obj.TYPE == "NODATA") return null;
                else if (obj.TYPE == "ERROR")
                    strError = obj.MESSAGE;
                else
                {
                    //Có dữ liệu
                    List<List<object>> objHangDoi = JsonConvert.DeserializeObject<List<List<object>>>(obj.MESSAGE);
                    return objHangDoi;
                }
                return null;
            }
            catch (Exception ex)
            {
                strError = ex.Message;
                return null;
            }
            finally
            {

            }
        }
        #endregion

    }
}
