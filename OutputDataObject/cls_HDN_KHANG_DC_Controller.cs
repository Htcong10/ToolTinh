using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using DbConnect.DB;


namespace OutputDataObject
{
    public class cls_HDN_KHANG_DC_Controller
    {
        #region   Atributes

       
        private DataSet ds = new DataSet();
        private DataTable dt = new DataTable();
        private HDN_KHANG_DC info;
        private List<HDN_KHANG_DC> lstInfo;

        public List<HDN_KHANG_DC> LstInfo
        {
            get { return lstInfo; }
            set { lstInfo = value; }
        }

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

        public cls_HDN_KHANG_DC_Controller()
        {
            info = new HDN_KHANG_DC();
        }
        public cls_HDN_KHANG_DC_Controller(HDN_KHANG_DC Info)
        {
            this.info = Info;
        }

        #endregion

        #region   Properties

        public HDN_KHANG_DC pInfor
        {
            get { return info; }
            set { info = value; }
        }

        #endregion

        #region Method DungNT
        public DataTable get_HDN_KHANG_DC_ByID(string strMaDViQLy, long lngIDHDonDC)
        {
            try
            {
                var q = from a in this.CMIS2.DB.HDN_KHANG_DC
                        where a.MA_DVIQLY == strMaDViQLy && a.ID_HDON_DC == lngIDHDonDC
                        select new
                        {
                           a.DA_CAPNHAT,
                           a.DCHI_HDON,
                           a.DUONG_PHO,
                           a.ID_HDON_DC,
                           a.ID_HDONG,
                           a.MA_CNANG,
                           a.MA_DVIQLY,a
                           .MA_KHANG,
                           a.MA_KHTT,a
                           .MA_NHANG,
                           a.MASO_THUE,
                           a.NGAY_SUA,a
                           .NGAY_TAO,
                           a.NGUOI_SUA,
                           a.NGUOI_TAO,
                           a.SO_NHA,
                           a.TEN_KHANG,
                           a.TENHDON_KHANG,
                           a.TENTAT_KHANG,
                           a.TKHOAN_KHANG,
                           a.TLE_THUE
                        };
                DataTable dt = BillingLibrary.BillingLibrary.LINQToDataTable(q);
                dt.TableName = "HDN_KHANG_DC";
                dt.AcceptChanges();
                return dt;
            }
            catch
            {
                
                return null;
            }
        }
        public string InsertList()
        {
            try
            {
                this.CMIS2.DB.HDN_KHANG_DC.InsertAllOnSubmit(this.lstInfo);
                return "";
            }
            catch (Exception ex)
            {

                return "Lỗi HDN_KHANG_DC: " + ex.Message;
            }

        }
        public bool Delete(string strMaDViQLy, long lngIDHDonDC)
        {
            try
            {
                this.CMIS2.DB.HDN_KHANG_DC.DeleteAllOnSubmit(from a in this.CMIS2.DB.HDN_KHANG_DC where a.MA_DVIQLY == strMaDViQLy && a.ID_HDON_DC == lngIDHDonDC select a);
                return true;
            }
            catch
            {
                
                return false;
            }
        }
        #endregion

    }
}
