using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using DbConnect.DB;
using System.Data;

namespace OutputDataObject
{
    public class cls_HDN_HDONCOSFI_DC_Controller
    {
        #region   Atributes

       
        private DataSet ds = new DataSet();
        private DataTable dt = new DataTable();
        private HDN_HDONCOSFI_DC info;
        private List<HDN_HDONCOSFI_DC> lstInfo;

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

        public cls_HDN_HDONCOSFI_DC_Controller()
        {
            info = new HDN_HDONCOSFI_DC();
            lstInfo=new List<HDN_HDONCOSFI_DC>();
        }
        public cls_HDN_HDONCOSFI_DC_Controller(HDN_HDONCOSFI_DC Info)
        {
            this.info = Info;
        }

        #endregion

        #region   Properties
        public List<HDN_HDONCOSFI_DC> LstInfo
        {
            get { return lstInfo; }
            set { lstInfo = value; }
        }
        public HDN_HDONCOSFI_DC pInfor
        {
            get { return info; }
            set { info = value; }
        }

        #endregion

        #region Method DungNT
        public bool Delete(string strMaDViQLy, long lngIDHDonDC)
        {
            try
            {
                this.CMIS2.DB.HDN_HDONCOSFI_DC.DeleteAllOnSubmit(from a in this.CMIS2.DB.HDN_HDONCOSFI_DC where a.MA_DVIQLY == strMaDViQLy && a.ID_HDON_DC == lngIDHDonDC select a);
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
                this.CMIS2.DB.HDN_HDONCOSFI_DC.InsertAllOnSubmit(lstInfo);
                return "";
            }
            catch (Exception ex)
            {
                
                return "Lỗi khi Insert HDN_HDONCOSFI_DC: " + ex.Message;
            }
        }
        public long getMaxID()
        {
            try
            {
                string str = "";
                long id = this.CMIS2.DB.ExecuteCommand("SELECT SEQ_HDN_HDONCOSFI.NEXTVAL FROM DUAL", ref str);
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
