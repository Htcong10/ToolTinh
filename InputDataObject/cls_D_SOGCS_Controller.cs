using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using DbConnect.DB;
using System.Data;

namespace InputDataObject
{
    public class cls_D_SOGCS_Controller
    {
        #region   Atributes        
        private DataSet ds = new DataSet();
        private DataTable dt = new DataTable();
        private cls_D_SOGCS_Info info;
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

        public cls_D_SOGCS_Controller()
        {
            info = new cls_D_SOGCS_Info();
        }
        public cls_D_SOGCS_Controller(cls_D_SOGCS_Info Info)
        {
            this.info = Info;
        }

        #endregion

        #region   Properties

        public cls_D_SOGCS_Info pInfor
        {
            get { return info; }
            set { info = value; }
        }

        #endregion

        #region   Methods
        public IEnumerable<D_SOGCS> getD_SOGCS(string strMaDViQLy, IEnumerable<string> strMaSo)
        {
            try
            {
                var w = from p in this.CMIS2.DB.D_SOGCS.ToList()
                        where p.MA_DVIQLY == strMaDViQLy
                        //&& strMaSo.Contains(p.MA_SOGCS)
                        select p;
                var q = from a in w where strMaSo.Contains(a.MA_SOGCS) select a;
                return q;
            }
            catch
            {
                 return null;
            }
        }
        public IEnumerable<D_SOGCS> getD_SOGCS_ByMaSo(string strMaDViQLy, string strMaSo)
        {
            try
            {
                var q = from p in this.CMIS2.DB.D_SOGCS.ToList()
                        where p.MA_DVIQLY == strMaDViQLy
                        && p.MA_SOGCS== strMaSo
                        select p;
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
