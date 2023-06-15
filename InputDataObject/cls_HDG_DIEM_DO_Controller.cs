using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using DbConnect.DB;

namespace InputDataObject
{
    public class cls_HDG_DIEM_DO_Controller
    {
        #region   Atributes

        
        private cls_HDG_DIEM_DO_Info info;

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

        public cls_HDG_DIEM_DO_Controller()
        {
            info = new cls_HDG_DIEM_DO_Info();
        }
        public cls_HDG_DIEM_DO_Controller(cls_HDG_DIEM_DO_Info Info)
        {
            this.info = Info;
        }

        #endregion

        #region   Properties

        public cls_HDG_DIEM_DO_Info pInfor
        {
            get { return info; }
            set { info = value; }
        }

        #endregion

        #region Method DungNT
        public IEnumerable<HDG_DIEM_DO> getHDG_DIEM_DO(string strMaDViQLy)
        {
            try
            {
                var q = this.CMIS2.DB.HDG_DIEM_DO.Where(c=>c.MA_DVIQLY == strMaDViQLy);
                if (q != null && q.Take(1).Count() > 0) return q;
                return null;
            }
            catch
            {
                return null;
            }
        }
        public IEnumerable<HDG_DIEM_DO> getHDG_DIEM_DO(string strMaDViQLy, IEnumerable<string> strMaDDO)
        {
            try
            {
                var p = this.CMIS2.DB.HDG_DIEM_DO.Where(c =>c.MA_DVIQLY == strMaDViQLy);// && strMaDDO.Contains(c.MA_DDO));
                //int i = p.Count();
                var q = from a in p where strMaDDO.Contains(a.MA_DDO) select a;
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
