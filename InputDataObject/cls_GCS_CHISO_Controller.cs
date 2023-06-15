using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using DbConnect.DB;
using System.Data;

namespace InputDataObject
{
    public class cls_GCS_CHISO_Controller
    {
        #region   Atributes
        private DataSet ds = new DataSet();
        private DataTable dt = new DataTable();
        private cls_GCS_CHISO_Info info;

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

        public cls_GCS_CHISO_Controller()
        {
            info = new cls_GCS_CHISO_Info();
        }
        public cls_GCS_CHISO_Controller(cls_GCS_CHISO_Info Info)
        {
            this.info = Info;
        }

        #endregion

        #region   Properties

        public cls_GCS_CHISO_Info pInfor
        {
            get { return info; }
            set { info = value; }
        }

        #endregion

        #region Method DungNT
        public IEnumerable<GCS_CHISO> getGCS_CHISO_ForBilling(string strMaDViQLy, IEnumerable<string> strMaDDo, short ky, short thang, short nam)
        {
            try
            {
                var q = this.CMIS2.DB.GCS_CHISO.Where(c => c.MA_DVIQLY == strMaDViQLy && c.NAM == nam && c.THANG == thang && c.KY == ky && c.LOAI_CHISO != "DUP" && c.LOAI_CHISO != "DUP1");
                //if (q != null && q.Count() > 0)
                //{
                    var kq = from a in q
                             where strMaDDo.Contains(a.MA_DDO)
                             select a;
                    return kq;
                //}
                //return null;
            }
            catch
            {
                
                return null;
            }
        }
        #endregion

    }
}
