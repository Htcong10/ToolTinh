using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using DbConnect.DB;
using System.Data;

namespace InputDataObject
{
    public class cls_D_GIA_NHOMNN_Controller
    {
        #region   Atributes        
        private DataSet ds = new DataSet();
        private DataTable dt = new DataTable();
        private cls_D_GIA_NHOMNN_Info info;

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

        public cls_D_GIA_NHOMNN_Controller()
        {
            info = new cls_D_GIA_NHOMNN_Info();
        }
        public cls_D_GIA_NHOMNN_Controller(cls_D_GIA_NHOMNN_Info Info)
        {
            this.info = Info;
        }

        #endregion

        #region   Properties

        public cls_D_GIA_NHOMNN_Info pInfor
        {
            get { return info; }
            set { info = value; }
        }

        #endregion

        #region Method DũngNT
        public DataTable getD_GIA_NHOMNN()
        {
            try
            {
                var q = from a in this.CMIS2.DB.D_GIA_NHOMNN
                        select new
                        {
                           a.ID_GIANHOMNN,
                           a.BAC_THANG,
                           a.DON_GIA,
                           a.ID_GIACU,                           
                           a.KHOANG_DA,
                           a.LOAI_TIEN,
                           a.MA_NGIA,
                           a.MA_NHOMNN,
                           a.MOTA_GIA,
                           a.NGAY_ADUNG,
                           a.NGAY_HETHLUC,
                           a.SOTHUTU,
                           a.THOIGIAN_BDIEN                          
                        };
                if (q != null && q.Take(1).Count() > 0)
                {
                    dt = BillingLibrary.BillingLibrary.LINQToDataTable(q);
                    dt.TableName = "D_GIA_NHOMNN";
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
