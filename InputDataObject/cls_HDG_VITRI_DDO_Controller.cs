using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using DbConnect.DB;
using System.Data;

namespace InputDataObject
{
    public class cls_HDG_VITRI_DDO_Controller
    {
        #region   Atributes
        private DataSet ds = new DataSet();
        private DataTable dt = new DataTable();
        private cls_HDG_VITRI_DDO_Info info;

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

        public cls_HDG_VITRI_DDO_Controller()
        {
            info = new cls_HDG_VITRI_DDO_Info();
        }
        public cls_HDG_VITRI_DDO_Controller(cls_HDG_VITRI_DDO_Info Info)
        {
            this.info = Info;
        }

        #endregion

        #region   Properties

        public cls_HDG_VITRI_DDO_Info pInfor
        {
            get { return info; }
            set { info = value; }
        }

        #endregion

        #region Method DungNT
        public IEnumerable<HDG_VITRI_DDO> getHDG_VITRI_DDO(string strMaDViQLy)
        {
            try
            {
                var q = this.CMIS2.DB.HDG_VITRI_DDO.Where(c => c.MA_DVIQLY == strMaDViQLy);
                if (q != null && q.Take(1).Count() > 0) return q;
                return null;
            }
            catch
            {
                return null;
            }
        }
        public IEnumerable<HDG_VITRI_DDO> getHDG_VITRI_DDO(string strMaDViQLy,IEnumerable<string> strMaDDo)
        {
            try
            {
                var p = this.CMIS2.DB.HDG_VITRI_DDO.Where(c => c.MA_DVIQLY == strMaDViQLy);//&& strMaDDo.Contains(c.MA_DDO));
                var q = from a in p where strMaDDo.Contains(a.MA_DDO) select a;
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
