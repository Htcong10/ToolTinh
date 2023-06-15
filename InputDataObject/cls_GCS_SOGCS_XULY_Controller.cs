using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using DbConnect.DB;
using System.Data;

namespace InputDataObject
{
    public class cls_GCS_SOGCS_XULY_Controller
    {
        #region   Atributes
        private DataSet ds = new DataSet();
        private DataTable dt = new DataTable();
        private cls_GCS_SOGCS_XULY_Info info;

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

        public cls_GCS_SOGCS_XULY_Controller()
        {
            info = new cls_GCS_SOGCS_XULY_Info();
        }
        public cls_GCS_SOGCS_XULY_Controller(cls_GCS_SOGCS_XULY_Info Info)
        {
            this.info = Info;
        }

        #endregion

        #region   Properties

        public cls_GCS_SOGCS_XULY_Info pInfor
        {
            get { return info; }
            set { info = value; }
        }

        #endregion

        #region Method DungNT
        
        #endregion

    }
}
