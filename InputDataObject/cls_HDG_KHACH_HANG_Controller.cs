using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using DbConnect.DB;
using System.Data;

namespace InputDataObject
{
    public class cls_HDG_KHACH_HANG_Controller
    {
        #region   Atributes
        private DataSet ds = new DataSet();
        private DataTable dt = new DataTable();
        private cls_HDG_KHACH_HANG_Info info;

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

        public cls_HDG_KHACH_HANG_Controller()
        {
            info = new cls_HDG_KHACH_HANG_Info();
        }
        public cls_HDG_KHACH_HANG_Controller(cls_HDG_KHACH_HANG_Info Info)
        {
            this.info = Info;
        }

        #endregion

        #region   Properties

        public cls_HDG_KHACH_HANG_Info pInfor
        {
            get { return info; }
            set { info = value; }
        }

        #endregion

        #region  Method DungNT        
        public IEnumerable<HDG_KHACH_HANG> getHDG_KHACH_HANG_ForBilling(string strMaDViqLy, IEnumerable<string> strMaKH)
        {
            try
            {
                var w = from p in this.CMIS2.DB.HDG_KHACH_HANG
                        where p.MA_DVIQLY == strMaDViqLy
                        //&& strMaKH.Contains(p.MA_KHANG)
                        select p;
                var q = from a in w where strMaKH.Contains(a.MA_KHANG) select a;
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
