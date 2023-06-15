using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using DbConnect.DB;

namespace InputDataObject
{
    public class cls_D_NHOM_NN_Controller
    {
        #region   Atributes       
        private DataSet ds = new DataSet();
        private DataTable dt = new DataTable();
        private cls_D_NHOM_NN_Info info;

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

        public cls_D_NHOM_NN_Controller()
        {
            info = new cls_D_NHOM_NN_Info();
        }
        public cls_D_NHOM_NN_Controller(cls_D_NHOM_NN_Info Info)
        {
            this.info = Info;
        }

        #endregion

        #region   Properties

        public cls_D_NHOM_NN_Info pInfor
        {
            get { return info; }
            set { info = value; }
        }

        #endregion

        #region Method DũngNT
        public DataTable getD_NHOM_NN()
        {
            try
            {
                var q = from a in this.CMIS2.DB.D_NHOM_NN
                        select new
                        {
                            a.MA_NHOMNN,
                            a.MO_TA,
                            a.STT_HTHI,
                            a.TEN_NHOMNN,
                            a.TEN_TAT,
                            a.TRANG_THAI
                        };
                if (q != null && q.Take(1).Count() > 0)
                {
                    dt = BillingLibrary.BillingLibrary.LINQToDataTable(q);
                    dt.TableName = "D_NHOM_NN";
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
