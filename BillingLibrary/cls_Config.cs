using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace BillingLibrary
{
    public class cls_Config
    {
        //Ham khoi tao cac thong so cau hình ho tro Covid
        public bool isHTroThang = true;
        public short ky_htro = 1;
        
        public DateTime dtHLucHTroNSH = new DateTime(2020, 4, 16);
        public DateTime dtHLucHTroSH = new DateTime(2020, 5, 1);
        public DateTime dtHetHLucHTroNSH = new DateTime(2020, 7, 16);
        public DateTime dtHetHLucHTroSH = new DateTime(2020, 8, 1);
        public List<string> lstDTuongHTro = new List<string>() { "SHBT/A", "LTDL/A", "SXKB/A", "KDKB/A" };
        public List<string> lstDViHTro = new List<string>();
        public List<string> lstSoHTro = new List<string>();
        public List<string> lstTNamHTro = new List<string>() { "6/2021", "7/2021", "8/2021", "9/2021", "10/2021", "11/2021", "12/2021" };
        public List<string> lstTNamHTroSH = new List<string>() { "8/2021", "9/2021" };
        public DateTime dtGiaHTro = new DateTime(2021, 2, 1);
        public int iNumKH = 10;

        #region Tham số kiểm soát tỷ lệ thuế
        public DateTime dtBegin_8 = new DateTime(2022, 2, 1);
        public DateTime dtEnd_8 = new DateTime(2023, 12, 31);
        
        #endregion

    }
}
