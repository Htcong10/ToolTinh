using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using DbConnect.DB;

namespace OutputDataObject
{
    public class cls_HDN_CHISO_DC_Controller
    {
        #region   Atributes

       
        private DataSet ds = new DataSet();
        private DataTable dt = new DataTable();
        private HDN_CHISO_DC info;
        private List<HDN_CHISO_DC> lstInfo;

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

        public cls_HDN_CHISO_DC_Controller()
        {
            info = new HDN_CHISO_DC();
            lstInfo = new List<HDN_CHISO_DC>();
        }
        public cls_HDN_CHISO_DC_Controller(HDN_CHISO_DC Info)
        {
            this.info = Info;
        }

        #endregion

        #region   Properties

        public List<HDN_CHISO_DC> LstInfo
        {
            get { return lstInfo; }
            set { lstInfo = value; }
        }
        public HDN_CHISO_DC pInfor
        {
            get { return info; }
            set { info = value; }
        }

        #endregion

        #region Method DungNT
        public DataTable get_HDN_CHISO_DC_ByID(string strMaDViQLy, long lngIDHDonDC)
        {
            try
            {
                var q = from a in this.CMIS2.DB.HDN_CHISO_DC
                        where a.MA_DVIQLY == strMaDViQLy && a.ID_HDON_DC == lngIDHDonDC
                        select new
                        {
                            a.BCS,
                            a.CHISO_CU,
                            a.CHISO_MOI,
                            a.DA_CAPNHAT,
                            a.HS_NHAN,
                            a.ID_BCS_DC,
                            a.ID_CHISO,
                            a.ID_HDON_DC,
                            a.KY,
                            a.LOAI_CHISO,
                            a.MA_CNANG,
                            a.MA_CTO,
                            a.MA_DDO,
                            a.MA_DVIQLY,
                            a.MA_TTCTO,
                            a.NAM,
                            a.NGAY_CKY,
                            a.NGAY_DKY,
                            a.NGAY_SUA,
                            a.NGAY_TAO,
                            a.NGUOI_SUA,
                            a.NGUOI_TAO,
                            a.SAN_LUONG,
                            a.SLUONG_TRPHU,
                            a.SLUONG_TTIEP,
                            a.SO_CTO,
                            a.THANG
                        };
                DataTable dt = BillingLibrary.BillingLibrary.LINQToDataTable(q);
                dt.TableName = "HDN_CHISO_DC";
                dt.AcceptChanges();
                return dt;
            }
            catch
            {
                
                return null;
            }
        }
        public bool Delete(string strMaDViQLy, long lngIDHDonDC)
        {
            try
            {
                this.CMIS2.DB.HDN_CHISO_DC.DeleteAllOnSubmit(from a in this.CMIS2.DB.HDN_CHISO_DC where a.MA_DVIQLY == strMaDViQLy && a.ID_HDON_DC == lngIDHDonDC select a);
                return true;
            }
            catch
            {
                
                return false;
            }
        }
        public string InsertList()
        {
            try
            {
                //foreach (HDN_CHISO_DC info in lstInfo)
                //{
                //    this.info.ID_CHISO= getMaxID();
                //}
                this.CMIS2.DB.HDN_CHISO_DC.InsertAllOnSubmit(this.lstInfo);
                return "";
            }
            catch (Exception ex)
            {
                
                return "Lỗi khi Insert HDN_CHISO_DC: " + ex.Message;
            }

        }
        public long getMaxID()
        {
            try
            {
                string str = "";
                long id = this.CMIS2.DB.ExecuteCommand("SELECT SEQ_GCS_CHISO.NEXTVAL FROM DUAL", ref str);
                return id;
            }
            catch
            {
                
                return -1;
            }
        }
        #endregion
   

    }

}
