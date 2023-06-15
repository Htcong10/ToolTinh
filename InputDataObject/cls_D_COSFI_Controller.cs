using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using DbConnect.DB;

namespace InputDataObject
{
    public class cls_D_COSFI_Controller
    {
        #region   Atributes     
        private DataSet ds = new DataSet();
        private DataTable dt = new DataTable();
        private cls_D_COSFI_Info info;

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

        public cls_D_COSFI_Controller()
        {
            info = new cls_D_COSFI_Info();
        }
        public cls_D_COSFI_Controller(cls_D_COSFI_Info Info)
        {
            this.info = Info;
        }

        #endregion

        #region   Properties

        public cls_D_COSFI_Info pInfor
        {
            get { return info; }
            set { info = value; }
        }

        #endregion

        #region Method DũngNT
        public DataTable getD_COSFI()
        {
            try
            {
                var q = from a in this.CMIS2.DB.D_COSFI
                        select new
                        {
                           a.HS_COSFI,
                           a.KCOSFI,
                           a.MA_CNANG,
                           a.NGAY_ADUNG,
                           a.NGAY_SUA,
                           a.NGAY_TAO,
                           a.NGUOI_SUA,
                           a.NGUOI_TAO                           
                        };
                if (q != null && q.Take(1).Count() > 0)
                {
                    dt = BillingLibrary.BillingLibrary.LINQToDataTable(q);
                    dt.TableName = "D_COSFI";
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
