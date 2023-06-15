using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using DbConnect.DB;
using System.Data;
using System.Data.Linq.SqlClient;

namespace OutputDataObject
{
    public class cls_HDN_HDON_DC_Controller
    {
        #region   Atributes

       
        private DataSet ds = new DataSet();
        private DataTable dt = new DataTable();
        private HDN_HDON_DC info;
        private List<HDN_HDON_DC> lstInfo;

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

        public cls_HDN_HDON_DC_Controller()
        {
            lstInfo = new List<HDN_HDON_DC>();
            info = new HDN_HDON_DC();
        }
        public cls_HDN_HDON_DC_Controller(HDN_HDON_DC Info)
        {
            this.info = Info;
        }

        #endregion

        #region   Properties
        public List<HDN_HDON_DC> LstInfo
        {
            get { return lstInfo; }
            set { lstInfo = value; }
        }
        public HDN_HDON_DC pInfor
        {
            get { return info; }
            set { info = value; }
        }

        #endregion

        #region Method DungNT
        public List<HDN_HDON_DC> get_HDN_HDON_DC_TT(string strMaDViQLy)
        {
            try
            {
                List<HDN_HDON_DC> lstResult = new List<HDN_HDON_DC>();
                var q = (from a in this.CMIS2.DB.HDN_HDON_DC
                         where a.MA_DVIQLY == strMaDViQLy
                         select a).ToList();
                foreach (HDN_HDON_DC obj in q)
                {
                    var p = from a in this.CMIS2.DB.HDN_HDON_TIEPNHAN
                            where a.MA_DVIQLY == obj.MA_DVIQLY && a.ID_HDON == obj.ID_HDON
                            select a;
                    if (p == null || p.Take(1).Count() == 0) lstResult.Add(obj);
                }
                return lstResult;
            }
            catch
            {
                return null;
            }
        }

        public List<HDN_HDON_DC> get_HDN_HDON_DC(string strMaDViQLy, long[] arrIDHDon)
        {
            try
            {

                List<HDN_HDON_DC> lstResult = (from a in this.CMIS2.DB.HDN_HDON_DC
                                               where a.MA_DVIQLY == strMaDViQLy && arrIDHDon.Contains(a.ID_HDON)
                                               select a).ToList();
                return lstResult;
            }
            catch
            {
                
                return new List<HDN_HDON_DC>();
            }
        }

        public DataTable getHDN_HDON_DC_ByID(string strMaDViQLy, long lngIDHDonDC)
        {
            try
            {               
                var q = from a in this.CMIS2.DB.HDN_HDON_DC
                        where a.MA_DVIQLY == strMaDViQLy && a.ID_HDON_DC == lngIDHDonDC
                        select new
                        {
                            a.CHI_TIET,
                            a.COSFI,
                            a.DA_CAPNHAT,
                            a.DCHI_KHANG,
                            a.DCHI_KHANGTT,
                            a.DIEN_TTHU,
                            a.ID_BBANPHANH,
                            a.ID_HDON,
                            a.ID_HDON_DC,
                            a.KCOSFI,
                            a.KIHIEU_SERY,
                            a.KY,
                            a.LOAI_DCHINH,
                            a.LOAI_HDON,
                            a.MA_CNANG,
                            a.MA_DVIQLY,
                            a.MA_HTTT,
                            a.MA_KHANG,
                            a.MA_KHANGTT,
                            a.MA_KVUC,
                            a.MA_NHANG,
                            a.MA_NVIN,
                            a.MA_NVPHANH,
                            a.MA_SOGCS,
                            a.MA_TO,
                            a.MASO_THUE,
                            a.NAM,
                            a.NGAY_CKY,
                            a.NGAY_DKY,
                            a.NGAY_IN,
                            a.NGAY_PHANH,
                            a.NGAY_SUA,
                            a.NGAY_TAO,
                            a.NGUOI_SUA,
                            a.NGUOI_TAO,
                            a.SO_CTO,
                            a.SO_HO,
                            a.SO_LANIN,
                            a.SO_SERY,
                            a.SO_TIEN,
                            a.STT,
                            a.STT_IN,
                            a.TEN_KHANG,
                            a.TEN_KHANGTT,
                            a.THANG,
                            a.TIEN_GTGT,
                            a.TKHOAN_KHANG,
                            a.TONG_TIEN,
                            a.TYLE_THUE,
                            a.DCHI_TTOAN,
                            a.LOAI_KHANG,
                            a.MANHOM_KHANG,
                            a.MA_LOAIDN,
                            a.MA_PTTT                           
                        };
                DataTable dt = BillingLibrary.BillingLibrary.LINQToDataTable(q);
                dt.TableName = "HDN_HDON_DC";
                dt.AcceptChanges();
                return dt;
            }
            catch
            {
                
                return null;
            }
        }

        public string Insert()
        {
            try
            {
                //this.info.ID_HDON_DC = getMaxID();
                this.CMIS2.DB.HDN_HDON_DC.InsertAllOnSubmit(this.lstInfo);
                return "";
            }
            catch (Exception ex)
            {
                
                return "Lỗi khi Insert HDN_HDON_DC: " + ex.Message;
            }

        }
        public long getMaxID()
        {
            try
            {
                string str = "";
                long id = this.CMIS2.DB.ExecuteCommand("SELECT SEQ_HDN_HDON.NEXTVAL FROM DUAL", ref str);
                return id;
            }
            catch 
            {
                
                return -1;
            }
        }
        public bool Delete(string strMaDViQLy, long lngIDHDonDC)
        {
            try
            {
                this.CMIS2.DB.HDN_HDON_DC.DeleteAllOnSubmit(from a in this.CMIS2.DB.HDN_HDON_DC where a.MA_DVIQLY == strMaDViQLy && a.ID_HDON_DC == lngIDHDonDC select a);
                return true;
            }
            catch
            {
                
                return false;
            }
        }
        //public string SubmitChange()
        //{
        //    try
        //    {
                
                
        //        return "";
        //    }
        //    catch (Exception e)
        //    {
                
        //        return e.Message;
        //    }
        //}
        #endregion
        /*
        #region   Methods

        public IEnumerable<HDN_HDON_DC> select_HDN_HDON_DC()
        {
            try
            {
                var q = from p in this.CMIS2.DB.HDN_HDON_DC select p;
                return q;
            }
            catch
            {
                 return null;
            }
        }
        public IEnumerable<HDN_HDON_DC> select_HDN_HDON_DC(string strMadviqly)
        {
            try
            {
                var q = from p in this.CMIS2.DB.HDN_HDON_DC where p.MA_DVIQLY == strMadviqly select p;
                return q;
            }
            catch
            {
                 return null;
            }
        }
        public bool update_HDN_HDON_DC()
        {
            try
            {
                HDN_HDON_DC objHDN_HDON_DC = this.CMIS2.DB.HDN_HDON_DC.Single(c => c.ID_HDON_DC == info.ID_HDON_DC && c.MA_DVIQLY == info.MA_DVIQLY);
                objHDN_HDON_DC.COSFI = info.COSFI;
                objHDN_HDON_DC.DCHI_KHANG = info.DCHI_KHANG;
                objHDN_HDON_DC.DCHI_KHANGTT = info.DCHI_KHANGTT;
                objHDN_HDON_DC.DIEN_TTHU = info.DIEN_TTHU;
                objHDN_HDON_DC.ID_BBANPHANH = info.ID_BBANPHANH;
                objHDN_HDON_DC.KCOSFI = info.KCOSFI;
                objHDN_HDON_DC.KY = info.KY;
                objHDN_HDON_DC.LOAI_DCHINH = info.LOAI_DCHINH;
                objHDN_HDON_DC.LOAI_HDON = info.LOAI_HDON;
                objHDN_HDON_DC.MA_CNANG = info.MA_CNANG;
                objHDN_HDON_DC.MA_KHANG = info.MA_KHANG;
                objHDN_HDON_DC.MA_KHANGTT = info.MA_KHANGTT;
                objHDN_HDON_DC.MA_KVUC = info.MA_KVUC;
                objHDN_HDON_DC.MA_NVIN = info.MA_NVIN;
                objHDN_HDON_DC.MA_NVPHANH = info.MA_NVPHANH;
                objHDN_HDON_DC.MA_SOGCS = info.MA_SOGCS;
                objHDN_HDON_DC.MA_TO = info.MA_TO;
                objHDN_HDON_DC.NAM = info.NAM;
                objHDN_HDON_DC.NGAY_CKY = info.NGAY_CKY;
                objHDN_HDON_DC.NGAY_DKY = info.NGAY_DKY;
                objHDN_HDON_DC.NGAY_IN = info.NGAY_IN;
                objHDN_HDON_DC.NGAY_PHANH = info.NGAY_PHANH;
                objHDN_HDON_DC.NGAY_TAO = info.NGAY_TAO;
                objHDN_HDON_DC.NGUOI_TAO = info.NGUOI_TAO;
                objHDN_HDON_DC.SO_CTO = info.SO_CTO;
                objHDN_HDON_DC.SO_HO = info.SO_HO;
                objHDN_HDON_DC.SO_LANIN = info.SO_LANIN;
                objHDN_HDON_DC.SO_TIEN = info.SO_TIEN;
                objHDN_HDON_DC.STT = info.STT;
                objHDN_HDON_DC.STT_IN = info.STT_IN;
                objHDN_HDON_DC.TEN_KHANG = info.TEN_KHANG;
                objHDN_HDON_DC.TEN_KHANGTT = info.TEN_KHANGTT;
                objHDN_HDON_DC.THANG = info.THANG;
                objHDN_HDON_DC.TIEN_GTGT = info.TIEN_GTGT;
                objHDN_HDON_DC.TONG_TIEN = info.TONG_TIEN;
                objHDN_HDON_DC.TYLE_THUE = info.TYLE_THUE;
                
                return true;
            }
            catch
            {
                 return false;
            }
        }
        #endregion

        #region Method NAMHN
        public IEnumerable<HDN_HDON_DC> select_HDN_HDON_DC(string strMadviqly, short? sKy, short? sThang, short? sNam, string strMaSo)
        {
            try
            {
                var q = from p in this.CMIS2.DB.HDN_HDON_DC
                        where p.MA_DVIQLY == strMadviqly
                        && (sKy != -1 && p.KY == sKy || sKy == -1)
                        && (sThang != -1 && p.THANG == sThang || sThang == -1)
                        && (sNam != -1 && p.NAM == sNam || sNam == -1)
                        && (strMaSo != " " && p.MA_SOGCS == strMaSo || strMaSo == " ")
                        select p;
                return q;
            }
            catch
            {
                 return null;
            }
        }
        public IEnumerable<HDN_HDON_DC> getHDon_KyThangNam()
        {
            try
            {

                var q = from p in this.CMIS2.DB.HDN_HDON_DC
                        where p.MA_DVIQLY == pInfor.MA_DVIQLY && p.ID_HDON_DC == pInfor.ID_HDON_DC
                              && p.KY == pInfor.KY && p.THANG == pInfor.THANG && p.NAM == pInfor.NAM
                        select p;
                return q;

            }
            catch
            {
                
                return null;
            }
        }
        public IEnumerable<HDN_HDON_DC> getHDN_HDon_DC()
        {
            try
            {
                var q = from p in this.CMIS2.DB.HDN_HDON_DC
                        where p.MA_DVIQLY == pInfor.MA_DVIQLY && p.KY == pInfor.KY
                              && p.THANG == pInfor.THANG && p.NAM == pInfor.NAM
                        select p;
                return q;
            }
            catch
            {
                
                return null;
            }
        }
        public IEnumerable<HDN_HDON_DC> select_HDN_HDON_DC(string strMadviqly, Int64? iMaHDon, short sKy, short sThang, short sNam, string strMaSoGCS, string strMaKHang)
        {
            try
            {
                var q = from p in this.CMIS2.DB.HDN_HDON_DC
                        where p.MA_DVIQLY == strMadviqly && p.KY == sKy && p.NAM == sNam && p.THANG == sThang
                        && (strMaSoGCS != " " && p.MA_SOGCS == strMaSoGCS || strMaSoGCS == " ")
                        && (iMaHDon != -1 && p.ID_HDON_DC == iMaHDon || iMaHDon == -1)
                        && (strMaKHang != " " && p.MA_KHANG == strMaKHang || strMaKHang == " ")
                        select p;
                return q;
            }
            catch
            {
                 return null;
            }
        }
        public bool update_HDN_HDon_BBanPH()
        {
            try
            {
                List<HDN_HDON_DC> lstHDN_Hdon = (from p in this.CMIS2.DB.HDN_HDON_DC
                                                 where p.MA_DVIQLY == pInfor.MA_DVIQLY && p.ID_BBANPHANH == pInfor.ID_BBANPHANH
                                                 select p).ToList();
                if (lstHDN_Hdon.Count > 0)
                {
                    foreach (HDN_HDON_DC objHDN_HDon in lstHDN_Hdon)
                    {
                        objHDN_HDon.MA_NVPHANH = pInfor.MA_NVPHANH;
                        objHDN_HDon.NGAY_PHANH = pInfor.NGAY_PHANH;
                    }
                }
                return true;
            }
            catch
            {
                
                return false;
            }
        }
        /// <summary>
        /// select trường hợp điều chỉnh số liệu KH
        /// </summary>
        /// <param name="strMaDVi"></param>
        /// <param name="sThang"></param>
        /// <param name="sNam"></param>
        /// <param name="strLoaiHD"></param>
        /// <returns></returns>
        public IEnumerable<HDN_HDON_DC> select_HDN_HDON_DC(string strMaDVi, short sThang, short sNam, string strLoaiHD)
        {
            var q = from p in this.CMIS2.DB.HDN_HDON_DC
                    where p.MA_DVIQLY == strMaDVi
                    && p.THANG == sThang && p.NAM == sNam
                    && p.DA_CAPNHAT == 0
                    && (strLoaiHD != " " && p.LOAI_HDON == strLoaiHD || strLoaiHD == " ")
                    select p;
            return q;
        }
        public bool delete_TTinPhanh_HDon(Int64[] arrHD)
        {
            try
            {
                if (arrHD.Length > 0)
                {
                    List<HDN_HDON_DC> lstHDN_Hdon = (from p in this.CMIS2.DB.HDN_HDON_DC
                                                     where p.MA_DVIQLY == pInfor.MA_DVIQLY && p.ID_BBANPHANH == pInfor.ID_BBANPHANH
                                                     select p).ToList();
                    lstHDN_Hdon = lstHDN_Hdon.Where(p => arrHD.Contains(p.ID_HDON_DC)).ToList();

                    if (lstHDN_Hdon.Count > 0)
                    {
                        foreach (HDN_HDON_DC objHDN_HDon in lstHDN_Hdon)
                        {
                            objHDN_HDon.MA_NVPHANH = pInfor.MA_NVPHANH;
                            objHDN_HDon.NGAY_PHANH = pInfor.NGAY_PHANH;
                            objHDN_HDon.ID_BBANPHANH = null;
                        }
                    }
                }
                return true;
            }
            catch
            {
                
                return false;
            }
        }
        public bool update_HDN_HDon_BBanPH(Int64[] arrHD)
        {
            try
            {
                List<HDN_HDON_DC> lstHDN_Hdon = (from p in this.CMIS2.DB.HDN_HDON_DC
                                                 where p.MA_DVIQLY == pInfor.MA_DVIQLY && p.ID_BBANPHANH == pInfor.ID_BBANPHANH
                                                 select p).ToList();
                if (arrHD.Length > 0)
                {
                    lstHDN_Hdon = lstHDN_Hdon.Where(p => arrHD.Contains(p.ID_HDON)).ToList();
                }
                if (lstHDN_Hdon.Count > 0)
                {
                    foreach (HDN_HDON_DC objHDN_HDon in lstHDN_Hdon)
                    {
                        objHDN_HDon.MA_NVPHANH = pInfor.MA_NVPHANH;
                        objHDN_HDon.NGAY_PHANH = pInfor.NGAY_PHANH;
                    }
                }
                return true;
            }
            catch
            {
                
                return false;
            }
        }
        public IEnumerable<HDN_HDON_DC> select_HDN_HDON_DC(string strMadviqly, short sKy, short sThang, short sNam)
        {
            try
            {
                var q = from p in this.CMIS2.DB.HDN_HDON_DC
                        where p.MA_DVIQLY == strMadviqly
                        && p.KY == sKy
                        && p.THANG == sThang
                        && p.NAM == sNam
                        select p;
                return q;
            }
            catch
            {
                 return null;
            }
        }
        public long insert_HDN_HDON_DC()
        {
            Int64 _ID = GetMaxSequenceByBranch();
            if (_ID == -1) return -1;
            try
            {
                HDN_HDON_DC objHDN_HDON_DC = new HDN_HDON_DC()
                {
                    CHI_TIET = info.CHI_TIET,
                    COSFI = info.COSFI,
                    DA_CAPNHAT = info.DA_CAPNHAT,
                    DCHI_KHANG = info.DCHI_KHANG,
                    DCHI_KHANGTT = info.DCHI_KHANGTT,
                    DIEN_TTHU = info.DIEN_TTHU,
                    ID_BBANPHANH = info.ID_BBANPHANH,
                    ID_HDON = info.ID_HDON,
                    ID_HDON_DC = _ID,
                    KCOSFI = info.KCOSFI,
                    KIHIEU_SERY = info.KIHIEU_SERY,
                    KY = info.KY,
                    LOAI_DCHINH = info.LOAI_DCHINH,
                    LOAI_HDON = info.LOAI_HDON,
                    MA_CNANG = info.MA_CNANG,
                    MA_DVIQLY = info.MA_DVIQLY,
                    MA_HTTT = info.MA_HTTT,
                    MA_KHANG = info.MA_KHANG,
                    MA_KHANGTT = info.MA_KHANGTT,
                    MA_KVUC = info.MA_KVUC,
                    MA_NHANG = info.MA_NHANG,
                    MA_NVIN = info.MA_NVIN,
                    MA_NVPHANH = info.MA_NVPHANH,
                    MA_SOGCS = info.MA_SOGCS,
                    MA_TO = info.MA_TO,
                    MASO_THUE = info.MASO_THUE,
                    NAM = info.NAM,
                    NGAY_CKY = info.NGAY_CKY,
                    NGAY_DKY = info.NGAY_DKY,
                    NGAY_IN = info.NGAY_IN,
                    NGAY_PHANH = info.NGAY_PHANH,
                    NGAY_SUA = info.NGAY_SUA,
                    NGAY_TAO = info.NGAY_TAO,
                    NGUOI_SUA = info.NGUOI_SUA,
                    NGUOI_TAO = info.NGUOI_TAO,
                    SO_CTO = info.SO_CTO,
                    SO_HO = info.SO_HO,
                    SO_LANIN = info.SO_LANIN,
                    SO_SERY = info.SO_SERY,
                    SO_TIEN = info.SO_TIEN,
                    STT = info.STT,
                    STT_IN = info.STT_IN,
                    TEN_KHANG = info.TEN_KHANG,
                    TEN_KHANGTT = info.TEN_KHANGTT,
                    THANG = info.THANG,
                    TIEN_GTGT = info.TIEN_GTGT,
                    TKHOAN_KHANG = info.TKHOAN_KHANG,
                    TONG_TIEN = info.TONG_TIEN,
                    TYLE_THUE = info.TYLE_THUE
                };
                this.CMIS2.DB.HDN_HDON_DC.InsertOnSubmit(objHDN_HDON_DC);
                return _ID;
            }
            catch
            {
                 return -1;
            }
        }
        public long GetMaxSequenceByBranch()
        {
            try
            {
                string strErr = "";
                long _ID = this.CMIS2.DB.ExecuteCommand("SELECT SEQ_HDN_HDON_DC.NEXTVAL FROM dual", ref strErr);
                if (strErr == "") return _ID;
                else return -1;
            }
            catch
            {
                 return -1;
            }
        }
        #endregion

        #region Methods Nghiatn
        public IEnumerable<HDN_HDON_DC> select_HDN_HDON_DC(string strMaDVi, string strMa_GCS, int iThang, int iNam, int iKy, string strLoaiHD)
        {
            var q = from p in this.CMIS2.DB.HDN_HDON_DC
                    where p.MA_DVIQLY == strMaDVi && p.MA_SOGCS == strMa_GCS
                    && p.THANG == iThang && p.NAM == iNam && p.KY == iKy
                    select p;
            return q;
        }
        #endregion Nghiatn
        */

        
    }
}
