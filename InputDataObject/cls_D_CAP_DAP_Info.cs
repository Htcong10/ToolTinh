using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace InputDataObject
{
    public class cls_D_CAP_DAP_Info
    {
        #region string MA_CAPDA

        private string macapda;
        public string MA_CAPDA
        {
            get
            {
                return macapda;
            }
            set
            {
                macapda = value;
            }
        }

        #endregion

        #region string MO_TA

        private string mota;
        public string MO_TA
        {
            get
            {
                return mota;
            }
            set
            {
                mota = value;
            }
        }

        #endregion

        #region int STT_HTHI

        private int stththi;
        public int STT_HTHI
        {
            get
            {
                return stththi;
            }
            set
            {
                stththi = value;
            }
        }

        #endregion

        #region string TEN_CAPDA

        private string tencapda;
        public string TEN_CAPDA
        {
            get
            {
                return tencapda;
            }
            set
            {
                tencapda = value;
            }
        }

        #endregion

        #region string TENTAT_CAPDA

        private string tentatcapda;
        public string TENTAT_CAPDA
        {
            get
            {
                return tentatcapda;
            }
            set
            {
                tentatcapda = value;
            }
        }

        #endregion

        #region short TRANG_THAI

        private short trangthai;
        public short TRANG_THAI
        {
            get
            {
                return trangthai;
            }
            set
            {
                trangthai = value;
            }
        }

        #endregion

    }
}
