using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using DbConnect.DB;

namespace InputDataObject
{
    public class cls_HDG_PTHUC_TTOAN_Controller
    {
        #region   Atributes
       
        private DataSet ds = new DataSet();
        private DataTable dt = new DataTable();
        private HDG_PTHUC_TTOAN info;

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

        public cls_HDG_PTHUC_TTOAN_Controller()
        {
            info = new HDG_PTHUC_TTOAN();
        }
        public cls_HDG_PTHUC_TTOAN_Controller(HDG_PTHUC_TTOAN Info)
        {
            this.info = Info;
        }

        #endregion

        #region   Properties

        public HDG_PTHUC_TTOAN pInfor
        {
            get { return info; }
            set { info = value; }
        }

        #endregion

        #region  Method DungNT
        public IEnumerable<HDG_PTHUC_TTOAN> getHDG_PTHUC_TTOAN_ForBilling(string strMaDViQLy, IEnumerable<string> strMaKH)
        {
            try
            {
                var p = from a in this.CMIS2.DB.HDG_PTHUC_TTOAN
                        where a.MA_DVIQLY == strMaDViQLy //&& strMaKH.Contains(a.MA_KHANG)
                        select a;
                var q = from a in p where strMaKH.Contains(a.MA_KHANG) select a;
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
