using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using DbConnect.DB;
using CMISLibrary;

namespace LogManagementObject
{
    public class cls_HDN_DSACH_SOTHD_Controller
    {
        List<HDN_DSACH_SOTHD> lstInfor;
        public List<HDN_DSACH_SOTHD> getMaSoGCS(CMIS2 db)
        {
            try
            {
                var so = (from a in db.DB.HDN_DSACH_SOTHD
                          where a.TRANG_THAI == "Chưa tính"
                          select a).Take(1).ToList();
                if (so != null && so.Count > 0)
                {
                    //Cập nhật trạng thái đang tính cho sổ
                    HDN_DSACH_SOTHD obj = db.DB.HDN_DSACH_SOTHD.Single(c => c.MA_DVIQLY == so[0].MA_DVIQLY && c.MA_SOGCS == so[0].MA_SOGCS && c.KY == so[0].KY && c.THANG == so[0].THANG && c.NAM == so[0].NAM);
                    if (obj != null)
                        obj.TRANG_THAI = "Đang tính";
                    db.DB.SubmitChanges();
                    //HDN_DSACH_SOTHD obj = new HDN_DSACH_SOTHD();
                    //obj.MA_DVIQLY = so[0].MA_DVIQLY;
                    //obj.MA_SOGCS = so[0].MA_SOGCS;
                    //obj.KY = so[0].KY;
                    //obj.THANG = so[0].THANG;
                    //obj.NAM = so[0].NAM;
                    //obj.TEN_DNHAP = so[0].TEN_DNHAP;
                    //obj.CURRENTLIBID = so[0].CURRENTLIBID;
                    //obj.WORKFLOWID = so[0].WORKFLOWID;
                    //obj.TRANG_THAI = "Đang tính";

                    //db.DB.HDN_DSACH_SOTHD.UpdateOnSubmit(obj);
                    //db.DB.SubmitChanges(1);
                }
                /*new
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
                }).ToList();*/
                return so;
            }
            catch
            {
                return null;
            }
        }

        public long CountMaSoGCS(CMIS2 db)
        {
            try
            {
                long so = (from a in db.DB.HDN_DSACH_SOTHD
                           where a.TRANG_THAI == "Chưa tính"
                           select a).Count();
                /*new
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
                }).ToList();*/
                return so;
            }
            catch
            {
                return 0;
            }
        }
        public bool InsertDSachSo(DataSet dsDSachMoi, CMIS2 db)
        {
            try
            {
                List<HDN_DSACH_SOTHD> lst = new List<HDN_DSACH_SOTHD>();
                 string strError = "";
                 foreach (DataRow dr in dsDSachMoi.Tables[0].Rows)
                 {
                     HDN_DSACH_SOTHD info = new HDN_DSACH_SOTHD();
                     info = Utility.MapDatarowToObject<HDN_DSACH_SOTHD>(dr, ref strError);
                     if (strError != null && strError.Trim().Length > 0) return false;
                     info.TRANG_THAI = "Chưa tính";
                     lst.Add(info);
                 }
                db.DB.HDN_DSACH_SOTHD.InsertAllOnSubmit(lst);
                db.DB.SubmitChanges(1);
                return true;
            }
            catch
            {
                return false;
            }
        }
        public string DeleteMaSoGCS(string strMaDViQLy, string strMaSoGCS, Int16 i16Ky, Int16 i16Thang, Int16 i16Nam, string strTenDNhap, Int64 lngCurrentLibID, Int64 lngWorkflowID, CMIS2 db)
        {
            try
            {
                int ky = i16Ky;
                int thang = i16Thang;
                int nam = i16Nam;
                //HDN_DSACH_SOTHD info = new HDN_DSACH_SOTHD();
                var so = (from a in db.DB.HDN_DSACH_SOTHD
                          where a.MA_DVIQLY == strMaDViQLy
                          && a.MA_SOGCS == strMaSoGCS
                          && a.KY == ky
                          && a.THANG == thang
                          && a.NAM == nam
                          && a.TEN_DNHAP == strTenDNhap
                          && a.TRANG_THAI == "Đang tính"
                          && a.CURRENTLIBID == lngCurrentLibID
                          && a.WORKFLOWID == lngWorkflowID
                          select a).ToList();
                if (so != null && so.Count > 0)
                {
                    db.DB.HDN_DSACH_SOTHD.DeleteAllOnSubmit(so);
                    db.DB.SubmitChanges();
                }
                return "";
            }
            catch (Exception ex)
            {                
                return ex.Message;
            }
        }
        public List<HDN_DSACH_SOTHD> getSoGCS_Queue(CMIS2 db)
        {

            try
            {
                var so = (from a in db.DB.HDN_DSACH_SOTHD                          
                          select a).ToList();
                /*new
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
                }).ToList();*/
                return so;
            }
            catch
            {
                return null;
            }
        }
    }
}
