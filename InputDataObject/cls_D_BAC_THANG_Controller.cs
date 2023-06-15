using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using DbConnect.DB;

namespace InputDataObject
{
    public class cls_D_BAC_THANG_Controller
    {
        #region   Atributes       
        private DataSet ds = new DataSet();
        private DataTable dt = new DataTable();
        private cls_D_BAC_THANG_Info info;

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

        public cls_D_BAC_THANG_Controller()
        {
            info = new cls_D_BAC_THANG_Info();
        }
        public cls_D_BAC_THANG_Controller(cls_D_BAC_THANG_Info Info)
        {
            this.info = Info;
        }

        #endregion

        #region   Properties

        public cls_D_BAC_THANG_Info pInfor
        {
            get { return info; }
            set { info = value; }
        }

        #endregion

        #region Method DũngNT
        public DataTable getD_BAC_THANG()
        {
            try
            {
                var q = from a in this.CMIS2.DB.D_BAC_THANG
                        select new
                        {
                            a.BTHANG_ID,
                            a.DIEN_GIAI,
                            a.DINH_MUC,
                            a.DON_GIA,
                            a.GIANHOMNN_ID,
                            a.MA_NGIA,
                            a.MA_NHOMNN,
                            a.MA_NN,
                            a.MO_TA,
                            a.NGAY_HHLUC,
                            a.NGAY_HLUC,
                            a.STT_BTHANG                            
                        };
                if (q != null && q.Take(1).Count() > 0)
                {
                    dt = BillingLibrary.BillingLibrary.LINQToDataTable(q);
                    dt.TableName = "D_BAC_THANG";
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
