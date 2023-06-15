using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using DbConnect.DB;
using System.Data;

namespace InputDataObject
{
    public class cls_D_CAP_DAP_Controller
    {
        #region   Atributes
        private DataSet ds = new DataSet();
        private DataTable dt = new DataTable();
        private cls_D_CAP_DAP_Info info;

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

        public cls_D_CAP_DAP_Controller()
        {
            info = new cls_D_CAP_DAP_Info();
        }
        public cls_D_CAP_DAP_Controller(cls_D_CAP_DAP_Info Info)
        {
            this.info = Info;
        }

        #endregion

        #region   Properties

        public cls_D_CAP_DAP_Info pInfor
        {
            get { return info; }
            set { info = value; }
        }

        #endregion

        #region   Methods

        public IEnumerable<D_CAP_DAP> select_D_CAP_DAP()
        {
            try
            {
                var q = from p in this.CMIS2.DB.D_CAP_DAP 
                        select p;
                return q;
            }
            catch
            {
                 return null;
            }
        }          
        #endregion

        #region Method DũngNT
        public DataTable getD_CAP_DAP()
        {
            try
            {
                var q = from a in this.CMIS2.DB.D_CAP_DAP
                        select new
                        {
                           a.MA_CAPDA,
                           a.MO_TA,
                           a.STT_HTHI,
                           a.TEN_CAPDA,
                           a.TENTAT_CAPDA,
                           a.TRANG_THAI
                        };
                if (q != null && q.Take(1).Count() > 0)
                {
                    dt = BillingLibrary.BillingLibrary.LINQToDataTable(q);                    
                    dt.TableName = "D_CAP_DAP";
                    dt.AcceptChanges();
                    return dt;
                }
                return null;
            }
            catch
            {
                return null;
            }
        }
        #endregion
    }
}
