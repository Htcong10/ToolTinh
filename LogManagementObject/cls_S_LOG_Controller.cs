using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using DbConnect.DB;

namespace LogManagementObject
{
    public class cls_S_LOG_Controller
    {
        #region   Atributes
        public List<cls_S_LOG_Info> lstInfo;

        private CMIS2 cmis2;
        public CMIS2 CMIS2
        {
            get
            {
                return cmis2;
            }
            set
            {
                cmis2 = value;
            }
        }
        #endregion

        #region   Constructor

        public cls_S_LOG_Controller()
        {
            lstInfo = new List<cls_S_LOG_Info>();
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
            try
            {
                var q = (from a in this.CMIS2.DB.S_LOG
                        orderby a.LOGID descending
                        select new
                        {
                           a.ASSEMBLYNAME,
                           a.BOOKID,
                           a.DETAIL,
                           a.LOGID,
                           a.METHODNAME,
                           a.NAMESPACE,
                           a.SUBDIVISIONID,
                           a.TIME
                        }).Take(intNumberLog);
                if(q == null || q.Count() == 0) return null;
                //if (intNumberLog > 0)
                //{
                //    q = q.ToList().Take(intNumberLog);
                //}
                //if (q == null || q.Take(1).Count() == 0) return null;
                DataSet ds = new DataSet();
                DataTable dt = BillingLibrary.BillingLibrary.LINQToDataTable(q);
                dt.TableName = "S_LOG";
                ds.Tables.Add(dt);
                ds.AcceptChanges();
                return ds;
            }
            catch
            {
                return null;
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
            try
            {
                var q = (from a in this.CMIS2.DB.S_LOG
                        where a.SUBDIVISIONID == strMaDViQLy
                        orderby a.LOGID descending
                        select new
                        {
                            a.ASSEMBLYNAME,
                            a.BOOKID,
                            a.DETAIL,
                            a.LOGID,
                            a.METHODNAME,
                            a.NAMESPACE,
                            a.SUBDIVISIONID,
                            a.TIME
                        }).Take(intNumberLog);
                if (q == null || q.Count() == 0) return null;
                //if (intNumberLog > 0)
                //{
                //    q = q.ToList().Take(intNumberLog);
                //}
                //if (q == null || q.Take(1).Count() == 0) return null;
                DataSet ds = new DataSet();
                DataTable dt = BillingLibrary.BillingLibrary.LINQToDataTable(q);
                dt.TableName = "S_LOG";
                ds.Tables.Add(dt);
                ds.AcceptChanges();
                return ds;
            }
            catch
            {
                return null;
            }
        }
        /// <summary>
        /// Lấy log theo số lượng bản ghi gần nhất và theo mã đơn vị quản lý + mã sổ ghi chỉ số
        /// </summary>
        /// <param name="strMaDViQLy">Mã đơn vị quản lý</param>
        /// <param name="strMaSoGCS">Mã sổ ghi chỉ số</param>   
        /// <param name="intNumberLog">Số bản ghi cần lấy</param>  
        /// <returns></returns>
        public DataSet getLogByMaSoGCS(string strMaDViQLy,string strMaSoGCS , int intNumberLog)
        {
            try
            {
                var q = (from a in this.CMIS2.DB.S_LOG
                         where a.SUBDIVISIONID == strMaDViQLy && a.BOOKID == strMaSoGCS
                         orderby a.LOGID descending
                         select new
                         {
                             a.ASSEMBLYNAME,
                             a.BOOKID,
                             a.DETAIL,
                             a.LOGID,
                             a.METHODNAME,
                             a.NAMESPACE,
                             a.SUBDIVISIONID,
                             a.TIME
                         }).ToList().Take(intNumberLog);
                if (q == null || q.Count() == 0) return null;
                //if (intNumberLog > 0)
                //{
                //    q = q.ToList().Take(intNumberLog);
                //}
                //if (q == null || q.Take(1).Count() == 0) return null;
                DataSet ds = new DataSet();
                DataTable dt = BillingLibrary.BillingLibrary.LINQToDataTable(q);
                dt.TableName = "S_LOG";
                ds.Tables.Add(dt);
                ds.AcceptChanges();
                return ds;
            }
            catch
            {
                return null;
            }
        }
        /// <summary>
        /// Insert một mảng đối tượng, sau khi insert xong submit luôn 
        /// </summary>
        /// <returns>True or False</returns>
        public bool InsertList()
        {
            if (lstInfo.Count == 0) return false;
            try
            {
                foreach (cls_S_LOG_Info info in lstInfo)
                {
                    S_LOG objSLog = new S_LOG
                    {
                        ASSEMBLYNAME = info.ASSEMBLYNAME,
                        BOOKID = info.BOOKID,
                        DETAIL = info.DETAIL,
                        LOGID = getMaxLogID(),
                        METHODNAME = info.METHODNAME,
                        NAMESPACE = info.NAMESPACE,
                        SUBDIVISIONID = info.SUBDIVISIONID,
                        TIME = info.TIME
                    };
                    this.CMIS2.DB.S_LOG.InsertOnSubmit(objSLog);
                }
                return true;
            }
            catch
            {
                return false;
            }
        }
        /// <summary>
        /// Insert một mảng đối tượng, sau khi insert xong không submit.
        /// </summary>
        /// <returns></returns>
        public bool InsertListNoSubmit()
        {
            if (lstInfo.Count == 0) return false;
            try
            {
                foreach (cls_S_LOG_Info info in lstInfo)
                {
                    S_LOG objSLog = new S_LOG
                    {
                        ASSEMBLYNAME = info.ASSEMBLYNAME,
                        BOOKID = info.BOOKID,
                        DETAIL = info.DETAIL,
                        LOGID = getMaxLogID(),
                        METHODNAME = info.METHODNAME,
                        NAMESPACE = info.NAMESPACE,
                        SUBDIVISIONID = info.SUBDIVISIONID,
                        TIME = info.TIME
                    };
                    this.CMIS2.DB.S_LOG.InsertOnSubmit(objSLog);
                }
                return true;
            }
            catch
            {                
                return false;
            }
        }        
        /// <summary>
        /// Lấy sequence của bảng S_LOG
        /// </summary>
        /// <returns></returns>
        protected long getMaxLogID()
        {
            try
            {                
                long lngSeqLog = this.CMIS2.DB.ExecuteCommand("SELECT SEQ_S_LOG.NEXTVAL FROM DUAL");
                return lngSeqLog;
            }
            catch (Exception ex)
            {
                return -1;
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
            try
            {
                S_LOG objLog = this.CMIS2.DB.S_LOG.Single(c => c.SUBDIVISIONID == strMaDViQLy && c.LOGID == lngLogID);
                if (objLog != null)
                {
                    this.CMIS2.DB.S_LOG.DeleteOnSubmit(objLog);
                }
                return true;
            }
            catch
            {                
                return false;
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
            if (lngFromID > lngToID)
            {
                long lngTG = lngFromID;
                lngFromID = lngToID;
                lngToID = lngTG;
            }
            var q = this.CMIS2.DB.S_LOG.Where(c => c.SUBDIVISIONID == strMaDViQLy && c.LOGID <= lngToID && c.LOGID >= lngFromID);
            if (q != null && q.Take(1).Count() > 0)
            {
                try
                {
                    this.CMIS2.DB.S_LOG.DeleteAllOnSubmit(q.ToList());
                    return true;
                }
                catch
                {                    
                    return false;
                }
            }
            return true;
        }
        /// <summary>
        /// Xóa log theo mã đơn vị quản lý và mã sổ ghi chỉ số
        /// </summary>
        /// <param name="strMaDViQLy"></param>
        /// <param name="strMaSoGCS"></param>
        /// <returns></returns>
        public bool DeleteByMaSoGCS(string strMaDViQLy, string strMaSoGCS)
        {
            var q = this.CMIS2.DB.S_LOG.Where(c => c.SUBDIVISIONID == strMaDViQLy && c.BOOKID == strMaSoGCS);
            if (q != null && q.Take(1).Count() > 0)
            {
                try
                {
                    this.CMIS2.DB.S_LOG.DeleteAllOnSubmit(q.ToList());
                    return true;
                }
                catch
                {                    
                    return false;
                }
            }
            return true;
        }
        #endregion

    }
}
