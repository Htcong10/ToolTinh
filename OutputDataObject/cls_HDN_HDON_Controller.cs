using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using DbConnect.DB;
using System.Data.OracleClient;

namespace OutputDataObject
{
    public class cls_HDN_HDON_Controller
    {
        #region
        private DataSet ds = new DataSet();
        private DataTable dt = new DataTable();
        private HDN_HDON info;
        public List<HDN_HDON> lstHDon;
        //public CMIS3 DB;

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

        public cls_HDN_HDON_Controller()
        {
            info = new HDN_HDON();            
        }
        public cls_HDN_HDON_Controller(HDN_HDON Info)
        {
            this.info = Info;
        }

        #endregion

        #region   Properties

        public HDN_HDON pInfor
        {
            get { return info; }
            set { info = value; }
        }

        #endregion

        #region Method DungNT
        public string InsertHDN_HDON()
        {
            try
            {
                this.CMIS2.DB.HDN_HDON.InsertAllOnSubmit(lstHDon);
                return "";
            }
            catch (Exception e)
            {
                return e.Message;
            }
        }
        public long getID_HDON()
        {
            try
            {                
                //decimal result = 0;
                string strError = "";
                long lngID = this.CMIS2.DB.ExecuteCommand("select SEQ_HDN_HDON.nextval from dual", ref strError);
                //this.CMIS2.DB.Sp_getsequence("SEQ_HDN_HDON", out result);
                return lngID;
            }
            catch
            {
                return -1;
            }
        }        
        public IEnumerable<HDN_HDON> getHDonByMaSoGCS(string strmaDViQLy, string[] strMaSoGCS, Int16 i16Ky, Int16 i16Thang, Int16 i16Nam)
        {
            try
            {
                var hdon = from a in this.CMIS2.DB.HDN_HDON
                           where strMaSoGCS.Contains(a.MA_SOGCS) && a.MA_DVIQLY == strmaDViQLy && a.NAM == i16Nam && a.THANG == i16Thang && a.KY == i16Ky
                           select a;
                return hdon;
            }
            catch
            {
                return null;
            }
        }
        public IEnumerable<HDN_HDON> getHDonByMaSoGCS(string strmaDViQLy, string strMaSoGCS, Int16 i16Ky, Int16 i16Thang, Int16 i16Nam)
        {
            try
            {
                var hdon = from a in this.CMIS2.DB.HDN_HDON
                           where a.MA_DVIQLY == strmaDViQLy && a.NAM == i16Nam && a.THANG == i16Thang && a.KY == i16Ky
                           && a.MA_SOGCS==strMaSoGCS
                           select a;
                return hdon;
            }
            catch
            {
                return null;
            }
        }
        public IEnumerable<HDN_HDON> GetHDon(string strMa_DViQLy, string strMaSoGCS, Int16 i16Ky, Int16 i16Thang, Int16 i16Nam)
        {
            try
            {
                var hdon = from a in this.CMIS2.DB.HDN_HDON
                           where a.MA_DVIQLY == strMa_DViQLy && a.NAM == i16Nam && a.THANG == i16Thang && a.KY == i16Ky && a.MA_SOGCS == strMaSoGCS
                           select a;
                return hdon;
            }
            catch
            {
                return null;
            }
        }
        public IEnumerable<HDN_HDON> GetHDon_MaKHang(string strMa_DViQLy, string strMa_KHang, Int16 i16Ky, Int16 i16Thang, Int16 i16Nam)
        {
            try
            {
                var hdon = from a in this.CMIS2.DB.HDN_HDON
                           where a.MA_DVIQLY == strMa_DViQLy && a.NAM == i16Nam && a.THANG == i16Thang && a.KY == i16Ky && a.MA_KHANG == strMa_KHang
                           select a;
                return hdon;
            }
            catch
            {
                return null;
            }
        }
        public string DeleteHDN_HDON(IEnumerable<HDN_HDON> hdon)
        {
            try
            {
                List<HDN_HDON> lstDelete = new List<HDN_HDON>();
                foreach (HDN_HDON obj in hdon)
                {
                    HDN_HDON objDel = new HDN_HDON();
                    objDel.MA_DVIQLY = obj.MA_DVIQLY;
                    objDel.ID_HDON = obj.ID_HDON;
                    objDel.NAM = obj.NAM;
                    objDel.THANG = obj.THANG;
                    objDel.KY = obj.KY;
                    lstDelete.Add(objDel);
                }
                this.CMIS2.DB.HDN_HDON.DeleteAllOnSubmit(lstDelete);
                return "";
            }
            catch (Exception ex)
            {
                return "Lỗi khi xóa HDN_HDON: " + ex.Message;
            }
        }

        //Dũng NT bo sung insert S_NHATKY_DML theo yeu cau cua anh Xuan
        public long GetMaxSequence()
        {
            try
            {
                String strMessage = "";
                Int64 id = this.CMIS2.DB.ExecuteCommand("SELECT SEQ_S_NHATKY_DML.nextval FROM DUAL", ref strMessage);
                return id;
            }
            catch
            {
                return -1;
            }
        }
        public void InsertNhatKy(string MA_DVIQLY, string LOAI_THAOTAC, string LOAI_DTUONG, string MA_DTUONG, Int16 ky, Int16 thang, Int16 nam, string NOI_DUNG, string NGUOI_TAO, string MA_CNANG)
        {
            //Dùng trong Nhập sản lượng thương phẩm
            try
            {
                S_NHATKY_DML obj = new S_NHATKY_DML()
                {
                    ID_NHATKY = GetMaxSequence(),
                    KY = ky,
                    LOAI_DTUONG = LOAI_DTUONG,
                    LOAI_THAOTAC = LOAI_THAOTAC,
                    MA_CNANG = MA_CNANG,
                    MA_DTUONG = MA_DTUONG,
                    MA_DVIQLY = MA_DVIQLY,
                    NAM = nam,
                    NGAY_TAO = DateTime.Now,
                    NGUOI_TAO = NGUOI_TAO,
                    NOI_DUNG = NOI_DUNG,
                    THANG = thang
                };
                this.CMIS2.DB.S_NHATKY_DML.InsertOnSubmit(obj);                
            }
            catch
            {
                
            }
        }
        #endregion

    }
}
