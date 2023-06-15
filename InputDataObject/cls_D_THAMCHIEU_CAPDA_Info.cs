using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace InputDataObject
{
    public class cls_D_THAMCHIEU_CAPDA_Info
    {
        #region string KHOANG_DA

        private string khoangda;
        public string KHOANG_DA
        {
            get
            {
                return khoangda;
            }
            set
            {
                khoangda = value;
            }
        }

        #endregion

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

        #region string MA_NHOMNN

        private string manhomnn;
        public string MA_NHOMNN
        {
            get
            {
                return manhomnn;
            }
            set
            {
                manhomnn = value;
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

        #region System.DateTime NGAY_ADUNG

        private System.DateTime ngayadung;
        public System.DateTime NGAY_ADUNG
        {
            get
            {
                return ngayadung;
            }
            set
            {
                ngayadung = value;
            }
        }

        #endregion

        #region System.DateTime? NGAY_HETHLUC

        private System.DateTime? ngayhethluc;
        public System.DateTime? NGAY_HETHLUC
        {
            get
            {
                return ngayhethluc;
            }
            set
            {
                ngayhethluc = value;
            }
        }

        #endregion

    }

}
