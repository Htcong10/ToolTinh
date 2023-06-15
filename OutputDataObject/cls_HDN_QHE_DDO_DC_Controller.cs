using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using DbConnect.DB;


namespace OutputDataObject
{
    public class cls_HDN_QHE_DDO_DC_Controller
    {
        #region   Atributes

       
        private DataSet ds = new DataSet();
        private DataTable dt = new DataTable();
        private HDN_QHE_DDO_DC info;
        private List<HDN_QHE_DDO_DC> lstInfo;

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

        public cls_HDN_QHE_DDO_DC_Controller()
        {
            info = new HDN_QHE_DDO_DC();
            lstInfo = new List<HDN_QHE_DDO_DC>();
        }
        public cls_HDN_QHE_DDO_DC_Controller(HDN_QHE_DDO_DC Info)
        {
            this.info = Info;
        }

        #endregion

        #region   Properties

        public List<HDN_QHE_DDO_DC> LstInfo
        {
            get { return lstInfo; }
            set { lstInfo = value; }
        }
        public HDN_QHE_DDO_DC pInfor
        {
            get { return info; }
            set { info = value; }
        }

        #endregion

        #region Method DungNT
        public DataTable get_HDN_QHE_DDO_DC_ByID(string strMaDViQLy, long lngIDHDonDC)
        {
            try
            {
                var q = from a in this.CMIS2.DB.HDN_QHE_DDO_DC
                        where a.MA_DVIQLY == strMaDViQLy && a.ID_HDON_DC == lngIDHDonDC
                        select new
                        {
                           a.DA_CAPNHAT,
                           a.ID_HDON_DC,
                           a.ID_QHE_DC,
                           a.KY,
                           a.KY_HOADON,
                           a.KY_P,
                           a.LOAI_QHE,
                           a.MA_CNANG,
                           a.MA_DDO_CHINH,
                           a.MA_DDO_PHU,
                           a.MA_DVIQLY,
                           a.NAM,
                           a.NGAY_SUA,
                           a.NGAY_TAO,
                           a.NGUOI_SUA,
                           a.NGUOI_TAO,
                           a.THANG,
                           a.TT_UUTIEN
                        };
                DataTable dt = BillingLibrary.BillingLibrary.LINQToDataTable(q);
                dt.TableName = "HDN_QHE_DDO_DC";
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
                //foreach (HDN_QHE_DDO_DC info in lstInfo)
                //{
                //    this.info.ID_QHE_DC = getMaxID();
                //}
                this.CMIS2.DB.HDN_QHE_DDO_DC.InsertAllOnSubmit(this.lstInfo);
                return "";
            }
            catch (Exception ex)
            {
                
                return "Lỗi khi Insert HDN_QHE_DDO_DC: " + ex.Message;
            }

        }
        public long getMaxID()
        {
            try
            {
                string str = "";
                long id = this.CMIS2.DB.ExecuteCommand("SELECT SEQ_HDG_QHE_DDO.NEXTVAL FROM DUAL", ref str);
                return id;
            }
            catch
            {
                
                return -1;
            }
        }

        public bool Delete(string strMaDViQLy, long lngIDHDonDC)
        {
            try
            {
                this.CMIS2.DB.HDN_QHE_DDO_DC.DeleteAllOnSubmit(from a in this.CMIS2.DB.HDN_QHE_DDO_DC where a.MA_DVIQLY == strMaDViQLy && a.ID_HDON_DC == lngIDHDonDC select a);
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
