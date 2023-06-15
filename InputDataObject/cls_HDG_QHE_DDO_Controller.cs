using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using DbConnect.DB;
using System.Data;

namespace InputDataObject
{
    public class cls_HDG_QHE_DDO_Controller
    {
        #region   Atributes        
        private DataSet ds = new DataSet();
        private DataTable dt = new DataTable();
        private cls_HDG_QHE_DDO_Info info;

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

        public cls_HDG_QHE_DDO_Controller()
        {
            info = new cls_HDG_QHE_DDO_Info();
        }
        public cls_HDG_QHE_DDO_Controller(cls_HDG_QHE_DDO_Info Info)
        {
            this.info = Info;
        }

        #endregion

        #region   Properties

        public cls_HDG_QHE_DDO_Info pInfor
        {
            get { return info; }
            set { info = value; }
        }

        #endregion

        #region Method DũngNT
        public IEnumerable<HDG_QHE_DDO> getHDG_QHE_DDO_BQ(string strMaDViQLy, IEnumerable<string> strMaDDoCHinh, short ky, short thang, short nam)
        {
            try
            {
                var p = this.CMIS2.DB.HDG_QHE_DDO.Where(c => c.MA_DVIQLY == strMaDViQLy  && c.LOAI_QHE == "32" && c.NAM == nam && c.THANG == thang && c.KY == ky);
                var q = from a in p
                        where strMaDDoCHinh.Contains(a.MA_DDO_CHINH)
                        select a;  
                //if (q != null && q.Count() > 0) return q;
                return q;
            }
            catch
            {
                return null;
            }
        } 
        public IEnumerable<HDG_QHE_DDO> getHDG_QHE_DDO_TP(string strMaDViQLy,IEnumerable<string> strMaDDoCHinh, short ky, short thang, short nam)
        {
            try
            {
                var p = this.CMIS2.DB.HDG_QHE_DDO.Where(c => c.MA_DVIQLY == strMaDViQLy && c.LOAI_QHE!="40" && c.LOAI_QHE!="32" && c.NAM == nam && c.THANG == thang && c.KY == ky);
                var q = from a in p
                        where strMaDDoCHinh.Contains(a.MA_DDO_CHINH)
                        select a;  
                //if (q != null && q.Count() > 0) return q;
                return q;
            }
            catch
            {
                return null;
            }
        }        
        public IEnumerable<HDG_QHE_DDO> getQHeDDo_ForBilling(string strMaDViQLy, string strMaSoGCS, short ky, short thang, short nam)
        {
            try
            {
                var q = this.CMIS2.DB.HDG_QHE_DDO.Where(c => c.MA_DVIQLY == strMaDViQLy && (c.MA_SOGCS_CHINH == strMaSoGCS || c.MA_DDO_PHU == strMaSoGCS) && c.NAM == nam && c.THANG == thang && c.KY == ky);
                //if (q != null && q.Count() > 0) return q;
                return q;
            }
            catch
            {
                return null;
            }
        }        

        #endregion

    }
}
