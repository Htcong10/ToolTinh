using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using DbConnect.DB;
using System.Data;

namespace InputDataObject
{
    public class cls_HDG_DDO_SOGCS_Controller
    {
        #region   Atributes
        private DataSet ds = new DataSet();
        private DataTable dt = new DataTable();
        private cls_HDG_DDO_SOGCS_Info info;

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

        public cls_HDG_DDO_SOGCS_Controller()
        {
            info = new cls_HDG_DDO_SOGCS_Info();
        }
        public cls_HDG_DDO_SOGCS_Controller(cls_HDG_DDO_SOGCS_Info Info)
        {
            this.info = Info;
        }

        #endregion

        #region   Properties

        public cls_HDG_DDO_SOGCS_Info pInfor
        {
            get { return info; }
            set { info = value; }
        }

        #endregion

        #region Method DũngNT - Use for billing
        public IEnumerable<HDG_DDO_SOGCS> getHDG_DDO_SOGCS_ForBilling(string strMaDViQLy, string strMaSoGCS)
        {
            try
            {
                List<HDG_DDO_SOGCS> p = this.CMIS2.DB.HDG_DDO_SOGCS.Where(c => c.MA_DVIQLY == strMaDViQLy && c.MA_SOGCS == strMaSoGCS).ToList();
                List<HDG_DDO_SOGCS> q = new List<HDG_DDO_SOGCS>();
                foreach (var x in p)
                {
                    var temp = this.CMIS2.DB.HDG_DDO_SOGCS.Where(c => c.MA_DVIQLY == strMaDViQLy && c.MA_DDO == x.MA_DDO).ToList();
                    HDG_DDO_SOGCS obj = temp.Single(c => c.NGAY_HLUC == temp.Max(a => a.NGAY_HLUC));
                    if (obj.NGAY_HLUC == x.NGAY_HLUC) q.Add(obj);
                }               
                return q;
            }
            catch(Exception ex)
            {
                //System.Windows.Forms.MessageBox.Show(strMaSoGCS + "-" + strMaDViQLy + " - " + ex.Message);
                
                return null;
            }
        }
        public IEnumerable<HDG_DDO_SOGCS> getHDG_DDO_SOGCS(string strMaDViQLy,IEnumerable<string> strMaDDoPhu)
        {
            try
            {
                var p = this.CMIS2.DB.HDG_DDO_SOGCS.Where(c => c.MA_DVIQLY == strMaDViQLy);
                var q = from a in p where strMaDDoPhu.Contains(a.MA_DDO) select a;
                //if (q != null && q.Count() > 0)
                    return q;
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
