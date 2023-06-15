using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using DbConnect.DB;

namespace OutputDataObject
{
    public class cls_HDN_BCS_CTO_DC_Controller
    {
        #region   Atributes


        private DataSet ds = new DataSet();
        private DataTable dt = new DataTable();
        private HDN_BCS_CTO_DC info;
        private List<HDN_BCS_CTO_DC> lstInfo;

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

        public cls_HDN_BCS_CTO_DC_Controller()
        {
            info = new HDN_BCS_CTO_DC();
            lstInfo = new List<HDN_BCS_CTO_DC>();
        }
        public cls_HDN_BCS_CTO_DC_Controller(HDN_BCS_CTO_DC Info)
        {
            this.info = Info;
        }

        #endregion

        #region   Properties
        public List<HDN_BCS_CTO_DC> LstInfo
        {
            get { return lstInfo; }
            set { lstInfo = value; }
        }
        public HDN_BCS_CTO_DC pInfor
        {
            get { return info; }
            set { info = value; }
        }

        #endregion

        #region Method DungNT
        public DataTable get_HDN_BCS_CTO_DC_ByID(string strMaDViQLy, long lngIDHDonDC)
        {
            try
            {
                var q = from a in this.CMIS2.DB.HDN_BCS_CTO_DC
                        where a.MA_DVIQLY == strMaDViQLy && a.ID_HDON_DC == lngIDHDonDC
                        select new
                        {
                            a.BCS,
                            a.HS_NHAN,
                            a.ID_BCS_DC,
                            a.ID_HDON_DC,
                            a.MA_CNANG,
                            a.MA_CTO,
                            a.MA_DDO,
                            a.MA_DVIQLY,
                            a.NGAY_SUA,
                            a.NGAY_TAO,
                            a.NGUOI_SUA,
                            a.NGUOI_TAO,
                            a.SO_CTO
                        };
                DataTable dt = BillingLibrary.BillingLibrary.LINQToDataTable(q);
                dt.TableName = "HDN_BCS_CTO_DC";
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
                //foreach (HDN_BCS_CTO_DC info in lstInfo)
                //{
                //    info.ID_BCS_DC = getMaxID();
                //    //lstChiSo.Single(c => c.MA_DVIQLY == info.MA_DVIQLY && c.ID_HDON_DC == info.ID_HDON_DC && c.BCS == info.BCS).ID_BCS_DC = this.info.ID_BCS_DC;
                //}
                this.CMIS2.DB.HDN_BCS_CTO_DC.InsertAllOnSubmit(this.lstInfo);
                return "";
            }
            catch (Exception ex)
            {
                
                return "Lỗi HDN_BCS_CTO_DC: " + ex.Message;
            }

        }
        public long getMaxID()
        {
            try
            {
                string str = "";
                long id = this.CMIS2.DB.ExecuteCommand("SELECT SEQ_DD_BCS_CTO.NEXTVAL FROM DUAL", ref str);
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
                this.CMIS2.DB.HDN_BCS_CTO_DC.DeleteAllOnSubmit(from a in this.CMIS2.DB.HDN_BCS_CTO_DC where a.MA_DVIQLY == strMaDViQLy && a.ID_HDON_DC == lngIDHDonDC select a);
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
