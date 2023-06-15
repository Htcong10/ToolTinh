using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using LogManagementObject;
using System.IO;
using DbConnect.DB;

namespace busLogManagement
{
    public class cls_DSachSo_THD
    {
        public DataSet getMaSoGCS()
        {
            CMIS2 db = new CMIS2();
            try
            {
                DataSet ds = new DataSet();
                string strError = "";
                db.DB.Sp_get_so_queue("Chưa tính", "Đang tính", out strError, out ds);
                if (strError.Trim().Length > 0 && strError!="OK") return null;
                if (ds == null || ds.Tables.Count == 0 || ds.Tables[0].Columns.Count <= 1 || ds.Tables[0].Rows.Count == 0) return null;
                //cls_HDN_DSACH_SOTHD_Controller obj_HDN_DSACH_SOTHD_Controller = new cls_HDN_DSACH_SOTHD_Controller();
                //var so = obj_HDN_DSACH_SOTHD_Controller.getMaSoGCS(db);
                //if (so == null || so.Count <= 0) return null;
                //var kq = from a in so
                //         select new
                //         {
                //             a.MA_DVIQLY,
                //             a.MA_SOGCS,
                //             a.TEN_DNHAP,
                //             a.KY,
                //             a.THANG,
                //             a.NAM,
                //             a.CURRENTLIBID,
                //             a.WORKFLOWID,
                //             a.TRANG_THAI,
                //             a.S_NAME,
                //             a.TIMESTAMP
                //         };
                //DataTable dt = BillingLibrary.BillingLibrary.LINQToDataTable(kq);
                
                //ds.Tables.Add(dt);
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
        public long CountMaSoGCS()
        {
            CMIS2 db = new CMIS2();
            try
            {
                cls_HDN_DSACH_SOTHD_Controller obj_HDN_DSACH_SOTHD_Controller = new cls_HDN_DSACH_SOTHD_Controller();
                long so = obj_HDN_DSACH_SOTHD_Controller.CountMaSoGCS(db);
                return so;
            }
            catch
            {
                return 0;
            }
            finally
            {
                db.ReleaseConnection();

            }
        }
        public bool InsertDSachSo(DataSet dsDSachMoi, ref string strErr)
        {
            CMIS2 db = new CMIS2();
            try
            {
                cls_HDN_DSACH_SOTHD_Controller obj_HDN_DSACH_SOTHD_Controller = new cls_HDN_DSACH_SOTHD_Controller();
                bool ok = obj_HDN_DSACH_SOTHD_Controller.InsertDSachSo(dsDSachMoi, db);
                return ok;
            }
            catch (Exception ex)
            {
                strErr= ex.ToString();
                return false;
            }
            finally            
            {
                db.ReleaseConnection();

            }
        }
        public bool DeleteMaSoGCS(string strMaDViQLy, string strMaSoGCS, Int16 i16Ky, Int16 i16Thang, Int16 i16Nam, string strTenDNhap, Int64 lngCurrentLibID, Int64 lngWorkflowID)
        {
            CMIS2 db = new CMIS2();
            try
            {
                cls_HDN_DSACH_SOTHD_Controller obj_HDN_DSACH_SOTHD_Controller = new cls_HDN_DSACH_SOTHD_Controller();
                string ok = obj_HDN_DSACH_SOTHD_Controller.DeleteMaSoGCS( strMaDViQLy,  strMaSoGCS,  i16Ky,  i16Thang,  i16Nam,  strTenDNhap,  lngCurrentLibID,  lngWorkflowID, db);
                if (ok.Trim().Length > 0)
                {
                    WriteLog(strMaDViQLy, strMaSoGCS, "LogManagementObject", "LogManagementObject.cls_HDN_DSACH_SOTHD_Controller.cs", "DeleteMaSoGCS", ok);
                    return false;
                }
                return true;
            }
            catch(Exception ex)
            {
                WriteLog(strMaDViQLy, strMaSoGCS, "busLogManagement", "busLogManagement.cls_DSachSo_THD", "DeleteMaSoGCS", ex.Message);
                return false;
            }
            finally
            {
                db.ReleaseConnection();

            }
        }
        public void WriteLog(string strMa_DViQLy, string strMa_SoGCS, string strAssemblyName, string strNamespace, string strMethodName, string strDetail)
        {
            LOGDATA dsLog = new LOGDATA();
            DataRow drLog;
            drLog = dsLog.Tables["S_LOG"].NewRow();
            drLog["SUBDIVISIONID"] = strMa_DViQLy;
            drLog["LOGID"] = 1;
            drLog["BOOKID"] = strMa_SoGCS;
            drLog["ASSEMBLYNAME"] = strAssemblyName;
            drLog["NAMESPACE"] = strNamespace;
            drLog["METHODNAME"] = strMethodName;
            drLog["TIME"] = System.DateTime.Now;
            if (strDetail.Length > 500)
            {
                strDetail = strDetail.Substring(0, 500);
            }
            drLog["DETAIL"] = strDetail;
            dsLog.Tables["S_LOG"].Rows.Add(drLog);
            dsLog.Tables["S_LOG"].AcceptChanges();

            cls_LogManagement log = new cls_LogManagement(dsLog);
            log.InsertList();
            //dsLog.Tables["S_LOG"].Rows.Clear();
        }
        public DataSet getSoGCS_Queue()
        {
            CMIS2 db = new CMIS2();
            try
            {
                cls_HDN_DSACH_SOTHD_Controller obj_HDN_DSACH_SOTHD_Controller = new cls_HDN_DSACH_SOTHD_Controller();
                var so = obj_HDN_DSACH_SOTHD_Controller.getSoGCS_Queue(db);
                if (so == null || so.Count <= 0) return null;
                var kq = from a in so
                         select new
                {
                    a.MA_DVIQLY,
                    a.MA_SOGCS,
                    a.TEN_DNHAP,
                    a.KY,
                    a.THANG,
                    a.NAM,
                    a.CURRENTLIBID,
                    a.WORKFLOWID,
                    a.TRANG_THAI,
                    a.S_NAME,
                    a.TIMESTAMP
                };
                DataTable dt = BillingLibrary.BillingLibrary.LINQToDataTable(kq);
                DataSet ds = new DataSet();
                ds.Tables.Add(dt);
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
    }
}
