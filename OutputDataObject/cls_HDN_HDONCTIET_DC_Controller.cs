using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using DbConnect.DB;

namespace OutputDataObject
{
    public class cls_HDN_HDONCTIET_DC_Controller
    {
        #region   Atributes

       
        private DataSet ds = new DataSet();
        private DataTable dt = new DataTable();
        private HDN_HDONCTIET_DC info;
        private List<HDN_HDONCTIET_DC> lstInfo;

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

        public cls_HDN_HDONCTIET_DC_Controller()
        {
            info = new HDN_HDONCTIET_DC();
            lstInfo = new List<HDN_HDONCTIET_DC>();
        }
        public cls_HDN_HDONCTIET_DC_Controller(HDN_HDONCTIET_DC Info)
        {
            this.info = Info;
        }

        #endregion

        #region   Properties
        public List<HDN_HDONCTIET_DC> LstInfo
        {
            get { return lstInfo; }
            set { lstInfo = value; }
        }
        public HDN_HDONCTIET_DC pInfor
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
                this.CMIS2.DB.HDN_HDONCTIET_DC.DeleteAllOnSubmit(from a in this.CMIS2.DB.HDN_HDONCTIET_DC where a.MA_DVIQLY == strMaDViQLy && a.ID_HDON_DC == lngIDHDonDC select a);
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
                //foreach (HDN_HDONCTIET_DC info in lstInfo)
                //{
                //    this.info.ID_HDONCTIET_DC= getMaxID();
                //}
                this.CMIS2.DB.HDN_HDONCTIET_DC.InsertAllOnSubmit(this.lstInfo);
                return "";
            }
            catch (Exception ex)
            {
                
                return "Lỗi khi Insert HDN_HDONCTIET_DC: " + ex.Message;
            }

        }
        public long getMaxID()
        {
            try
            {
                string str = "";
                long id = this.CMIS2.DB.ExecuteCommand("SELECT SEQ_HDN_HDONCTIET.NEXTVAL FROM DUAL", ref str);
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
