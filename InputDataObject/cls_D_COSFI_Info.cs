using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace InputDataObject
{
    public class cls_D_COSFI_Info
    {
        #region decimal HS_COSFI

        private decimal hscosfi;
        public decimal HS_COSFI
        {
            get
            {
                return hscosfi;
            }
            set
            {
                hscosfi = value;
            }
        }

        #endregion

        #region decimal KCOSFI

        private decimal kcosfi;
        public decimal KCOSFI
        {
            get
            {
                return kcosfi;
            }
            set
            {
                kcosfi = value;
            }
        }

        #endregion

        #region string MA_CNANG

        private string macnang;
        public string MA_CNANG
        {
            get
            {
                return macnang;
            }
            set
            {
                macnang = value;
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

        #region System.DateTime NGAY_SUA

        private System.DateTime ngaysua;
        public System.DateTime NGAY_SUA
        {
            get
            {
                return ngaysua;
            }
            set
            {
                ngaysua = value;
            }
        }

        #endregion

        #region System.DateTime NGAY_TAO

        private System.DateTime ngaytao;
        public System.DateTime NGAY_TAO
        {
            get
            {
                return ngaytao;
            }
            set
            {
                ngaytao = value;
            }
        }

        #endregion

        #region string NGUOI_SUA

        private string nguoisua;
        public string NGUOI_SUA
        {
            get
            {
                return nguoisua;
            }
            set
            {
                nguoisua = value;
            }
        }

        #endregion

        #region string NGUOI_TAO

        private string nguoitao;
        public string NGUOI_TAO
        {
            get
            {
                return nguoitao;
            }
            set
            {
                nguoitao = value;
            }
        }

        #endregion

    }

}
