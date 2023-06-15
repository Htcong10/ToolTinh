using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using DbConnect.DB;

namespace OutputDataObject
{
    public class cls_HDG_HOP_DONG_Controller
    {
        #region   Atributes

       
        private DataSet ds = new DataSet();
        private DataTable dt = new DataTable();
        private HDG_HOP_DONG info;

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

        public cls_HDG_HOP_DONG_Controller()
        {
            info = new HDG_HOP_DONG();
        }
        public cls_HDG_HOP_DONG_Controller(HDG_HOP_DONG Info)
        {
            this.info = Info;
        }

        #endregion

        #region   Properties

        public HDG_HOP_DONG pInfor
        {
            get { return info; }
            set { info = value; }
        }

        #endregion

        #region Method DungNT
        public string getDCHI_TTOAN(string strMaDViQLy, long lngIDHDong, DateTime dtNgayTinhHD)
        {
            try
            {
                var q = from a in this.CMIS2.DB.HDG_HOP_DONG
                        where a.MA_DVIQLY == strMaDViQLy
                            && a.ID_HDONG == lngIDHDong
                            && a.NGAY_HLUC < dtNgayTinhHD
                            orderby a.NGAY_HLUC descending
                        select a;
                if (q != null && q.Take(1).Count() > 0)
                    return q.First().DCHI_TTOAN;
                else
                {
                    var p = from a in this.CMIS2.DB.HDG_HOP_DONG_LSU
                            where a.MA_DVIQLY == strMaDViQLy
                                && a.ID_HDONG == lngIDHDong
                                && a.NGAY_HLUC < dtNgayTinhHD
                            orderby a.NGAY_HLUC descending
                            select a;
                    if (p != null && p.Take(1).Count() > 0)
                        return p.First().DCHI_TTOAN;
                    else
                        return " ";
                }
            }
            catch
            {
                
                return " ";
            }
        }

        #endregion


    }
}
