using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using DbConnect.DB;
using System.Data.OracleClient;

namespace OutputDataObject
{
    public class cls_GCS_CHISO_Controller
    {
        #region   Atributes

        //private DataSet CMIS_Header = new CMISOutputParameter();
        private DataSet ds = new DataSet();
        private DataTable dt = new DataTable();
        private GCS_CHISO info;
        private List<GCS_CHISO> lstInfo;
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

        public cls_GCS_CHISO_Controller()
        {
            info = new GCS_CHISO();
            lstInfo = new List<GCS_CHISO>();           
        }
        public cls_GCS_CHISO_Controller(GCS_CHISO Info)
        {
            this.info = Info;
            lstInfo = new List<GCS_CHISO>();
        }

        #endregion

        #region   Properties
        public List<GCS_CHISO> LstInfo
        {
            get { return lstInfo; }
            set { lstInfo = value; }
        }

        public GCS_CHISO pInfor
        {
            get { return info; }
            set { info = value; }
        }

        #endregion

        #region Method DungNT
        public string UpdateList()
        {
            string ok = "";
            try
            {
                if (lstInfo.Count == 0) return "Lỗi update GCS_CHISO";
                List<GCS_CHISO> lstcs = new List<GCS_CHISO>();
                foreach (GCS_CHISO info in lstInfo)
                {
                    GCS_CHISO cs = new GCS_CHISO();
                    cs.MA_DVIQLY = info.MA_DVIQLY;
                    cs.ID_CHISO = info.ID_CHISO;
                    if (info.SAN_LUONG != null)
                    {
                        cs.SAN_LUONG = Math.Round(info.SAN_LUONG.Value, 0, MidpointRounding.AwayFromZero);
                    }
                    else
                    {
                        cs.SAN_LUONG = 0;
                    }

                    if (info.SLUONG_TTIEP != null)
                    {
                        cs.SLUONG_TTIEP = Math.Round(info.SLUONG_TTIEP.Value, 0, MidpointRounding.AwayFromZero);
                    }
                    else
                    {
                        cs.SLUONG_TTIEP = 0;
                    }

                    if (info.SLUONG_TRPHU != null)
                    {
                        cs.SLUONG_TRPHU = Math.Round(info.SLUONG_TRPHU.Value, 0, MidpointRounding.AwayFromZero);
                    }
                    else
                    {
                        cs.SLUONG_TRPHU = 0;
                    }

                    lstcs.Add(cs);
                    //try
                    //{
                    //    GCS_CHISO objGCS_CHISO = this.CMIS2.DB.GCS_CHISO.Single(c => c.MA_DVIQLY == info.MA_DVIQLY && c.ID_CHISO == info.ID_CHISO);
                    //    if (info.SAN_LUONG != null)
                    //        objGCS_CHISO.SAN_LUONG = Math.Round(info.SAN_LUONG.Value, 0, MidpointRounding.AwayFromZero);
                    //    if (info.SLUONG_TTIEP != null)
                    //       objGCS_CHISO.SLUONG_TTIEP = Math.Round(info.SLUONG_TTIEP.Value, 0, MidpointRounding.AwayFromZero);
                    //    if (info.SLUONG_TRPHU != null)
                    //        objGCS_CHISO.SLUONG_TRPHU = Math.Round(info.SLUONG_TRPHU.Value, 0, MidpointRounding.AwayFromZero);
                    //    this.CMIS2.DB.GCS_CHISO.UpdateOnSubmit(objGCS_CHISO);
                    //    //ok = true;
                    //}
                    //catch (Exception ex)
                    //{
                    //    ok = "Lỗi update GCS_CHISO: " + ex.Message;
                    //    break;
                    //}
                }

                this.CMIS2.DB.GCS_CHISO.UpdateAllOnSubmit(lstcs);
                return ok;
            }
            catch (Exception ex)
            {
                return "Lỗi update GCS_CHISO: " + ex.Message;
            }
        }
        public string UpdateSLTP(string strMaDViQLy, string[] arrMaDDo, Int16 i16Ky, Int16 i16Thang, Int16 i16Nam)
        {
            //Dùng khi hủy tính hóa đơn
            string ok = "";
            try
            {
                List<GCS_CHISO> lstUPDATE = new List<GCS_CHISO>();
                //Tìm danh sách điểm đo chính trong quan hệ trừ phụ thuộc sổ cần hủy
                //string[] ddoChinh = (from a in this.CMIS2.DB.HDG_QHE_DDO
                //                     where a.MA_DVIQLY == strMaDViQLy
                //                     && arrMaSoGCs.Contains(a.MA_SOGCS_CHINH)
                //                     && a.NAM == i16Nam
                //                     && a.THANG == i16Thang
                //                     && a.KY == i16Ky
                //                     && a.LOAI_QHE != "40"
                //                     select a.MA_DDO_CHINH).ToArray();
                while (arrMaDDo != null && arrMaDDo.Length > 0)
                {
                    string[] arrTem = arrMaDDo.Take(1000).ToArray();
                    var chiso = from a in this.CMIS2.DB.GCS_CHISO
                                where a.MA_DVIQLY == strMaDViQLy
                                     && arrTem.Contains(a.MA_DDO)
                                     && a.NAM == i16Nam
                                     && a.THANG == i16Thang
                                     && a.KY == i16Ky
                                     && a.LOAI_CHISO != "DUP"
                                     && a.LOAI_CHISO != "DUP1"
                                select a;
                    if (chiso != null && chiso.Take(1).Count() > 0)
                    {
                        foreach (var x in chiso)
                        {
                            GCS_CHISO objUp = new GCS_CHISO();
                            objUp.MA_DVIQLY = x.MA_DVIQLY;
                            objUp.ID_CHISO = x.ID_CHISO;
                            objUp.SLUONG_TRPHU = 0;
                            lstUPDATE.Add(objUp);
                            //x.SLUONG_TRPHU = 0;
                        }
                        //this.CMIS2.DB.GCS_CHISO.UpdateAllOnSubmit(chiso);
                    }
                    arrMaDDo = arrMaDDo.Skip(1000).ToArray();
                }
                if(lstUPDATE.Count>0)
                this.CMIS2.DB.GCS_CHISO.UpdateAllOnSubmit(lstUPDATE);

            }
            catch (Exception ex)
            {
                ok = "Lỗi update SLTP GCS_CHISO khi hủy tính HD: " + ex.Message;
            }
            return ok;
        }
        #endregion

        public string InsertList()
        {
            try
            {
                //foreach (HDN_QHE_DDO_DC info in lstInfo)
                //{
                //    this.info.ID_QHE_DC = getMaxID();
                //}
                this.CMIS2.DB.GCS_CHISO.InsertAllOnSubmit(this.lstInfo);
                return "";
            }
            catch (Exception ex)
            {
                
                return "Lỗi khi cập nhật chỉ số đầu kỳ sau: " + ex.Message;
            }

        }

        public List<GCS_CHISO> getChiSo(string strMa_DViQLy, string[] arrMa_DDo, Int16 ky, Int16 thang, Int16 nam)
        {
            try
            {
                List<GCS_CHISO> lstChiSo = new List<GCS_CHISO>();
                while (arrMa_DDo != null && arrMa_DDo.Length > 0)
                {
                    string[] arrTemp = arrMa_DDo.Take(1000).ToArray();
                    var q = from a in this.CMIS2.DB.GCS_CHISO
                            where a.MA_DVIQLY == strMa_DViQLy && arrTemp.Contains(a.MA_DDO) && a.KY == ky && a.THANG == thang && a.NAM == nam
                            select a;
                    if (q != null && q.Take(1).Count() > 0)
                    {
                        lstChiSo.AddRange(q);
                    }
                    arrMa_DDo = arrMa_DDo.Skip(1000).ToArray();
                }

                return lstChiSo;
            }
            catch
            {
                return null;
            }
        }

        public IEnumerable<GCS_LICHGCS> getSoGCS_DaTinhHoaDon(string strMaDViQLy, short ky, short thang, short nam)
        {
            try
            {
                string strTThaiSo = "THD|XNHD|IHD|GHD";
                var q = from a in this.CMIS2.DB.GCS_LICHGCS
                        where a.MA_DVIQLY == strMaDViQLy
                           && a.NAM == nam
                           && a.THANG == thang
                           && a.KY == ky
                           && strTThaiSo.Contains(a.TRANG_THAI)
                        select a;
                return q;
            }
            catch
            {
                return null;
            }
        }
    }
}
