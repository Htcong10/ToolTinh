using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using DbConnect.DB;

namespace InputDataObject
{
    public class cls_HDN_CHISO_DC_Controller
    {
        #region   Atributes
        private HDN_CHISO_DC info;

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
        public cls_HDN_CHISO_DC_Controller()
        {
            info = new HDN_CHISO_DC();
        }
        #endregion

        #region Method DungNT
        public long getMaxID()
        {
            try
            {
                string str = "";
                long id = this.CMIS2.DB.ExecuteCommand("SELECT SEQ_GCS_CHISO.NEXTVAL FROM DUAL", ref str);
                return id;
            }
            catch
            {
                
                return -1;
            }
        }
        #endregion


    }
}
