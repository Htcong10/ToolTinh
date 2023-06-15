using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using DbConnect.DB;


namespace OutputDataObject
{
    public class cls_HDN_BBAN_APGIA_DC_Controller
    {
        #region   Atributes

       
        private DataSet ds = new DataSet();
        private DataTable dt = new DataTable();
        private HDN_BBAN_APGIA_DC info;
        private List<HDN_BBAN_APGIA_DC> lstInfo;

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

        public cls_HDN_BBAN_APGIA_DC_Controller()
        {
            lstInfo = new List<HDN_BBAN_APGIA_DC>();
            info = new HDN_BBAN_APGIA_DC();
        }
        public cls_HDN_BBAN_APGIA_DC_Controller(HDN_BBAN_APGIA_DC Info)
        {
            this.info = Info;
        }

        #endregion

        #region   Properties
        public List<HDN_BBAN_APGIA_DC> LstInfo
        {
            get { return lstInfo; }
            set { lstInfo = value; }
        }
        public HDN_BBAN_APGIA_DC pInfor
        {
            get { return info; }
            set { info = value; }
        }

        #endregion

        public DataTable get_HDN_BBAN_APGIA_DC_ByID(string strMaDViQLy, long lngIDHDonDC)
        {
            try
            {
                var q = (from a in this.CMIS2.DB.HDN_BBAN_APGIA_DC
                         join b in this.CMIS2.DB.D_THAMCHIEU_CAPDA
                         on new { a.MA_NHOMNN, a.MA_CAPDAP } equals new { b.MA_NHOMNN,MA_CAPDAP= b.MA_CAPDA }
                         where a.MA_DVIQLY == strMaDViQLy && a.ID_HDON_DC == lngIDHDonDC && b.NGAY_HETHLUC==null
                         select new
                         {
                             a.DA_CAPNHAT,
                             a.DINH_MUC,
                             a.ID_BBANAGIA,
                             a.ID_HDON_DC,
                             a.LOAI_BCS,
                             a.LOAI_DMUC,
                             a.MA_BBANAGIA,
                             a.MA_CAPDAP,
                             a.MA_CNANG,
                             a.MA_DDO,
                             a.MA_DVIQLY,
                             a.MA_NGIA,
                             a.MA_NHOMNN,
                             a.MA_NN,
                             a.NGAY_HLUC,
                             a.NGAY_SUA,
                             a.NGAY_TAO,
                             a.NGUOI_SUA,
                             a.NGUOI_TAO,
                             a.SO_HO,
                             a.SO_THUTU,
                             a.TGIAN_BDIEN,
                             b.KHOANG_DA,
                             b.NGAY_ADUNG                             
                         }).ToList();
                var p = from a in q
                        join b in this.CMIS2.DB.D_GIA_NHOMNN
                        on new { a.KHOANG_DA, a.MA_NHOMNN, a.MA_NGIA,THOIGIAN_BDIEN = a.TGIAN_BDIEN } 
                        equals new { b.KHOANG_DA, b.MA_NHOMNN, b.MA_NGIA, b.THOIGIAN_BDIEN }
                        where b.NGAY_HETHLUC == null
                        select new
                        {
                            a.DA_CAPNHAT,
                            a.DINH_MUC,
                            a.ID_BBANAGIA,
                            a.ID_HDON_DC,
                            a.LOAI_BCS,
                            a.LOAI_DMUC,
                            a.MA_BBANAGIA,
                            a.MA_CAPDAP,
                            a.MA_CNANG,
                            a.MA_DDO,
                            a.MA_DVIQLY,
                            a.MA_NGIA,
                            a.MA_NHOMNN,
                            a.MA_NN,
                            a.NGAY_HLUC,
                            a.NGAY_SUA,
                            a.NGAY_TAO,
                            a.NGUOI_SUA,
                            a.NGUOI_TAO,
                            a.SO_HO,
                            a.SO_THUTU,
                            a.TGIAN_BDIEN,
                            a.KHOANG_DA,
                            a.NGAY_ADUNG,
                            b.DON_GIA,
                            b.LOAI_TIEN,
                            b.MOTA_GIA
                        };
                DataTable dt = BillingLibrary.BillingLibrary.LINQToDataTable(p);
                dt.TableName = "HDN_BBAN_APGIA_DC";
                dt.AcceptChanges();
                return dt;
            }
            catch
            {
                
                return null;
            }
        }
        public bool Delete(string strMaDViQLy, long lngIDHDonDC)
        {
            try
            {
                this.CMIS2.DB.HDN_BBAN_APGIA_DC.DeleteAllOnSubmit(from a in this.CMIS2.DB.HDN_BBAN_APGIA_DC where a.MA_DVIQLY == strMaDViQLy && a.ID_HDON_DC == lngIDHDonDC select a);
                return true;
            }
            catch
            {
                
                return false;
            }
        }
        public string InsertList()
        {
            try
            {
                //foreach (HDN_BBAN_APGIA_DC info in lstInfo)
                //{
                //    this.info.ID_BBANAGIA = getMaxID();
                //}
                this.CMIS2.DB.HDN_BBAN_APGIA_DC.InsertAllOnSubmit(this.lstInfo);
                return "";
            }
            catch (Exception ex)
            {
                
                return "Lỗi khi Insert HDN_BBAN_APGIA_DC: "+ ex.Message;
            }

        }
        public long getMaxID()
        {
            try
            {
                string str = "";
                long id = this.CMIS2.DB.ExecuteCommand("SELECT SEQ_HDG_BBAN_APGIA.NEXTVAL FROM DUAL", ref str);
                return id;
            }
            catch
            {
                
                return -1;
            }
        }
    }
}
