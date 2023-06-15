using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using DbConnect.DB;


namespace OutputDataObject
{
    public class cls_HDN_DIEMDO_DC_Controller
    {
        #region   Atributes

       
        private DataSet ds = new DataSet();
        private DataTable dt = new DataTable();
        private HDN_DIEMDO_DC info;
        private List<HDN_DIEMDO_DC> lstInfo;

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

        public cls_HDN_DIEMDO_DC_Controller()
        {
            info = new HDN_DIEMDO_DC();
            lstInfo = new List<HDN_DIEMDO_DC>();
        }
        public cls_HDN_DIEMDO_DC_Controller(HDN_DIEMDO_DC Info)
        {
            this.info = Info;
        }

        #endregion

        #region   Properties

        public List<HDN_DIEMDO_DC> LstInfo
        {
            get { return lstInfo; }
            set { lstInfo = value; }
        }
        public HDN_DIEMDO_DC pInfor
        {
            get { return info; }
            set { info = value; }
        }

        #endregion

        #region Method DungNT
        public DataTable get_HDN_DIEMDO_DC_ByID(string strMaDViQLy, long lngIDHDonDC)
        {
            try
            {
                var q = from a in this.CMIS2.DB.HDN_DIEMDO_DC
                        where a.MA_DVIQLY == strMaDViQLy && a.ID_HDON_DC == lngIDHDonDC
                        select new
                        {
                            a.DA_CAPNHAT,
                            a.DIA_CHI,
                            a.ID_HDON_DC,
                            a.ID_HDONG,
                            a.KIMUA_CSPK,
                            a.MA_CAPDA,
                            a.MA_CNANG,
                            a.MA_DDO,
                            a.MA_DVIQLY,
                            a.MA_KHANG,                            
                            a.NGAY_SUA,
                            a.NGAY_TAO,
                            a.NGUOI_SUA,
                            a.NGUOI_TAO,
                            a.SO_HO
                        };
                DataTable dt = BillingLibrary.BillingLibrary.LINQToDataTable(q);
                dt.TableName = "HDN_DIEMDO_DC";
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
                this.CMIS2.DB.HDN_DIEMDO_DC.InsertAllOnSubmit(this.lstInfo);
                return "";
            }
            catch (Exception ex)
            {
                
                return "Lỗi HDN_DIEMDO_DC: " + ex.Message;
            }

        }
        public bool Delete(string strMaDViQLy, long lngIDHDonDC)
        {
            try
            {
                this.CMIS2.DB.HDN_DIEMDO_DC.DeleteAllOnSubmit(from a in this.CMIS2.DB.HDN_DIEMDO_DC where a.MA_DVIQLY == strMaDViQLy && a.ID_HDON_DC == lngIDHDonDC select a);
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
