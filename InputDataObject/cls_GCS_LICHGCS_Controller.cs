using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using DbConnect.DB;
using System.Data;

namespace InputDataObject
{
    public class cls_GCS_LICHGCS_Controller
    {
        #region   Atributes
        private DataSet ds = new DataSet();
        private DataTable dt = new DataTable();
        private cls_GCS_LICHGCS_Info info;
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

        public cls_GCS_LICHGCS_Controller()
        {
            info = new cls_GCS_LICHGCS_Info();
        }
        public cls_GCS_LICHGCS_Controller(cls_GCS_LICHGCS_Info Info)
        {
            this.info = Info;
        }

        #endregion

        #region   Properties

        public cls_GCS_LICHGCS_Info pInfor
        {
            get { return info; }
            set { info = value; }
        }

        #endregion

        #region Method DungNT
        public IEnumerable<GCS_LICHGCS> getGCS_LICHGCS_ForBilling(string strMaDViQLy, string strMaSoGCS, short ky, short thang, short nam)
        {
            try
            {
                var q = this.CMIS2.DB.GCS_LICHGCS.Where(c => c.MA_DVIQLY == strMaDViQLy && c.MA_SOGCS == strMaSoGCS && c.NAM == nam && c.THANG == thang && c.KY == ky);
                //if (q != null && q.Count() > 0) return q;
                return q;
            }
            catch
            {
                
                return null;
            }
        }
        public IEnumerable<GCS_LICHGCS> getGCS_LICHGCS_GT_ForBilling(string strMaDViQLy, IEnumerable<string> strMaSoGCS, short ky, short thang, short nam)
        {
            try
            {
                var p = this.CMIS2.DB.GCS_LICHGCS.Where(c => c.MA_DVIQLY == strMaDViQLy && c.NAM == nam && c.THANG == thang && c.KY == ky);
                var q = from a in p
                        where strMaSoGCS.Contains(a.MA_SOGCS)
                        select a;

                //if (q != null && q.Count() > 0)
                //{
                return q;
                //}
                //return null;
            }
            catch
            {
                
                return null;
            }
        }
        public DataSet getGCS_LICHGCS_ForCalculation(string strMaDViQLy, short ky, short thang, short nam)
        {
            try
            {
                DataSet ds = new DataSet();
                List<HDG_QHE_DDO> lstQHe = new List<HDG_QHE_DDO>();
                List<GCS_LICHGCS> lstLichPhu = new List<GCS_LICHGCS>();
                List<GCS_LICHGCS> lstLich_TP = new List<GCS_LICHGCS>();
                string[] arrso_tp = null;
                //Dũng NT sửa, bổ sung thêm đoạn lấy các sổ phụ TP, thông thường số lượng rất ít - CMIS-8717
                var so_tp = (from c in this.CMIS2.DB.D_SOGCS
                            where c.MA_DVIQLY == strMaDViQLy
                            && c.LOAI_SOGCS == "TP"
                            select new 
                            { c.MA_DVIQLY, 
                              c.MA_SOGCS 
                            }).ToList();
                if (so_tp != null && so_tp.Count > 0)
                {
                    arrso_tp = so_tp.Select(c => c.MA_SOGCS).ToArray();
                    
                }
                else
                {
                    //khởi tạo giá trị, tránh bị null
                    arrso_tp = new string[1];
                    arrso_tp[0] = "_____";
                }
                var temp = from c in this.CMIS2.DB.GCS_LICHGCS
                           where c.MA_DVIQLY == strMaDViQLy
                           && c.NAM == nam
                           && c.THANG == thang
                           && c.KY == ky
                           && c.TRANG_THAI == "XNCS"
                           //Những sổ TP không lấy lên để tính hóa đơn,chỉ lỗi khi sổ TP quá nhiều, trên 1000 bản ghi 
                           && (!arrso_tp.Contains(c.MA_SOGCS))
                           select c;
                var q = (from c in temp.ToList()
                         select new
                         {
                             CHON = false,
                             c.ID_LICHGCS,
                             c.MA_DVIQLY,
                             c.MA_SOGCS,
                             c.NAM,
                             c.THANG,
                             c.KY,
                             c.NGAY_CKY,
                             c.NGAY_DKY,
                             c.TRANG_THAI
                         }).ToList();
                if (q != null && q.Count > 0)
                {
                    string[] arrMaSoGCS = q.Select(c => c.MA_SOGCS).ToArray();
                    //Lấy các điểm đo phụ trong quan hệ nếu có
                    while (arrMaSoGCS != null && arrMaSoGCS.Length > 0)
                    {
                        string[] arrTemp = arrMaSoGCS.Take(1000).ToArray();
                        var w = this.CMIS2.DB.HDG_QHE_DDO.Where(c => c.MA_DVIQLY == strMaDViQLy && arrTemp.Contains(c.MA_SOGCS_CHINH) && c.MA_SOGCS_PHU != c.MA_SOGCS_CHINH && c.NAM == nam && c.THANG == thang && c.KY == ky).ToList();
                        lstQHe.AddRange(w);
                        arrMaSoGCS = arrMaSoGCS.Skip(1000).ToArray();
                    }
                    if (lstQHe != null && lstQHe.Count > 0)
                    {
                        //Lấy các lịch chưa tính hóa đơn của sổ phụ
                        string[] arrMaSoPhu = lstQHe.Select(c => c.MA_SOGCS_PHU).ToArray();
                        //Lấy các sổ TP mà là sổ phụ của các sổ chính truyền vào                        
                        arrso_tp = arrso_tp.Where(a => arrMaSoPhu.Contains(a)).ToArray();
                        //Loại bỏ sổ phụ TP trong danh sách sổ phụ
                        arrMaSoPhu = arrMaSoPhu.Except(arrso_tp).ToArray();
                        while (arrMaSoPhu != null && arrMaSoPhu.Length > 0)
                        {
                            string[] arrTemp = arrMaSoPhu.Take(1000).ToArray();
                            var p = this.CMIS2.DB.GCS_LICHGCS.Where(c => c.MA_DVIQLY == strMaDViQLy
                                                            && arrTemp.Contains(c.MA_SOGCS)
                                                            && c.NAM == nam
                                                            && c.THANG == thang
                                                            && !c.TRANG_THAI.Contains("HD")).ToList();

                            //var ddo = this.CMIS2.DB.HDG_DDO_SOGCS.Where(c => c.MA_DVIQLY == strMaDViQLy
                            //                                    && arrTemp.Contains(c.MA_SOGCS));

                            //var sogcs = from a in ddo
                            //            join b in (this.CMIS2.DB.GCS_CHISO.Where(c => c.MA_DVIQLY == strMaDViQLy                                                           
                            //                               && c.THANG == thang
                            //                               && c.NAM == nam
                            //                               && c.CHISO_CU != c.CHISO_MOI
                            //                               && (c.BCS != "SG" && c.BCS != "VC")))
                            //            on new { a.MA_DVIQLY, a.MA_DDO } equals new { b.MA_DVIQLY, b.MA_DDO }
                            //            select new
                            //                {
                            //                    a.MA_DVIQLY,
                            //                    a.MA_SOGCS,
                            //                };

                            //var sophu = from a in sogcs
                            //            join b in (this.CMIS2.DB.GCS_LICHGCS.Where(c => c.MA_DVIQLY == strMaDViQLy
                            //                                && arrTemp.Contains(c.MA_SOGCS)
                            //                                && c.NAM == nam
                            //                                && c.THANG == thang
                            //                                && !c.TRANG_THAI.Contains("HD")).ToList())
                            //            on new { a.MA_DVIQLY, a.MA_SOGCS } equals new { b.MA_DVIQLY, b.MA_SOGCS }
                            //            select b;

                            lstLichPhu.AddRange(p);
                            arrMaSoPhu = arrMaSoPhu.Skip(1000).ToArray();
                        }
                        // Lấy lịch của các sổ TP mà chưa ở trạng thái XNCS

                        var lichso_tp = (from c in this.CMIS2.DB.GCS_LICHGCS
                                         where c.MA_DVIQLY == strMaDViQLy
                                         && c.NAM == nam
                                         && c.THANG == thang
                                         && c.KY == ky
                                         && c.TRANG_THAI != "XNCS"
                                         && (!c.TRANG_THAI.Contains("HD"))                                         
                                         select c).ToList();
                        lichso_tp = lichso_tp.Where(c=>arrso_tp.Contains(c.MA_SOGCS)).ToList();
                        //Kết nối quan hệ và lịch
                        var qhe = from a in lstQHe
                                  join b in lstLichPhu on
                                  new {  MA_SOGCS = a.MA_SOGCS_PHU, KY = a.KY_P != null ? a.KY_P.Value : ky }
                                  equals new {b.MA_SOGCS, b.KY }
                                  select new
                                  {
                                      a.MA_DVIQLY,
                                      a.MA_SOGCS_CHINH,
                                      a.MA_SOGCS_PHU,
                                      a.KY,
                                      a.KY_P,
                                      a.NAM,
                                      a.THANG,
                                      a.TT_UUTIEN,
                                      a.LOAI_QHE,
                                      THD = false,   //Trạng thái đã tính hóa đơn của sổ phụ
                                      LOAI_SOGCS="KH"
                                  };
                        var qhe_TP = from a in lstQHe
                                     join b in lichso_tp on
                                  new { MA_SOGCS = a.MA_SOGCS_PHU, KY = a.KY_P != null ? a.KY_P.Value : ky }
                                  equals new { b.MA_SOGCS, b.KY }
                                  select new
                                  {
                                      a.MA_DVIQLY,
                                      a.MA_SOGCS_CHINH,
                                      a.MA_SOGCS_PHU,
                                      a.KY,
                                      a.KY_P,
                                      a.NAM,
                                      a.THANG,
                                      a.TT_UUTIEN,
                                      a.LOAI_QHE,
                                      THD = false,   //Trạng thái đã tính hóa đơn của sổ phụ
                                      LOAI_SOGCS = "TP"
                                  };
                        if (qhe_TP != null && qhe_TP.Count() > 0)
                        {
                            qhe = qhe.Union(qhe_TP);
                        }
                        DataTable dtQHe = BillingLibrary.BillingLibrary.LINQToDataTable(qhe);
                        dtQHe.TableName = "HDG_QHE_DDO";
                        ds.Tables.Add(dtQHe);
                    }
                    DataTable dt = BillingLibrary.BillingLibrary.LINQToDataTable(q);
                    dt.TableName = "GCS_LICHGCS";
                    ds.Tables.Add(dt);
                    ds.AcceptChanges();
                    return ds;
                }
                return null;
            }
            catch
            {
                
                return null;
            }
        }
        public DataSet getGCS_LICHGCS_ForCancel(string strMaDViQLy, short ky, short thang, short nam)
        {
            try
            {
                DataSet ds = new DataSet();
                List<HDG_QHE_DDO> lstQHe = new List<HDG_QHE_DDO>();
                List<GCS_LICHGCS> lstLichChinh = new List<GCS_LICHGCS>();
                var temp = from c in this.CMIS2.DB.GCS_LICHGCS
                           where c.MA_DVIQLY == strMaDViQLy
                           && c.NAM == nam
                           && c.THANG == thang
                           && c.KY == ky
                           && c.TRANG_THAI == "THD"
                           select c;
                var q = (from c in temp.ToList()
                         select new
                         {
                             CHON = false,
                             c.ID_LICHGCS,
                             c.MA_DVIQLY,
                             c.MA_SOGCS,
                             c.NAM,
                             c.THANG,
                             c.KY,
                             c.NGAY_CKY,
                             c.NGAY_DKY,
                             c.TRANG_THAI
                         }).ToList();
                if (q != null && q.Count > 0)
                {
                    string[] arrMaSoGCS = q.Select(c => c.MA_SOGCS).ToArray();
                    //Lấy các điểm đo phụ trong quan hệ nếu có
                    while (arrMaSoGCS != null && arrMaSoGCS.Length > 0)
                    {
                        string[] arrTemp = arrMaSoGCS.Take(1000).ToArray();
                        var w = this.CMIS2.DB.HDG_QHE_DDO.Where(c => c.MA_DVIQLY == strMaDViQLy && arrTemp.Contains(c.MA_SOGCS_PHU) && c.MA_SOGCS_PHU != c.MA_SOGCS_CHINH && c.NAM == nam && c.THANG == thang && c.KY_P == ky).ToList();
                        lstQHe.AddRange(w);
                        arrMaSoGCS = arrMaSoGCS.Skip(1000).ToArray();
                    }
                    if (lstQHe != null && lstQHe.Count > 0)
                    {
                        //Lấy các lịch chưa tính hóa đơn của sổ phụ
                        string[] arrMaSoChinh = lstQHe.Select(c => c.MA_SOGCS_CHINH).Distinct().ToArray();
                        while (arrMaSoChinh != null && arrMaSoChinh.Length > 0)
                        {
                            string[] arrTemp = arrMaSoChinh.Take(1000).ToArray();
                            var p = this.CMIS2.DB.GCS_LICHGCS.Where(c => c.MA_DVIQLY == strMaDViQLy
                                                            && arrTemp.Contains(c.MA_SOGCS)
                                                            && c.NAM == nam
                                                            && c.THANG == thang
                                                            && c.TRANG_THAI.Contains("HD")).ToList();
                            lstLichChinh.AddRange(p);
                            arrMaSoChinh = arrMaSoChinh.Skip(1000).ToArray();
                        }
                        //Kết nối quan hệ và lịch
                        var qhe = from a in lstQHe
                                  join b in lstLichChinh on
                                  new { MA_SOGCS = a.MA_SOGCS_CHINH, KY = a.KY != null ? a.KY.Value : ky }
                                  equals new { b.MA_SOGCS, b.KY }
                                  select new
                                  {
                                      a.MA_DVIQLY,
                                      a.MA_SOGCS_CHINH,
                                      a.MA_SOGCS_PHU,
                                      a.NAM,
                                      a.THANG,
                                      a.TT_UUTIEN,
                                      a.LOAI_QHE,
                                      THD = false   //Trạng thái hủy tính hóa đơn của sổ chính
                                  };
                        DataTable dtQHe = BillingLibrary.BillingLibrary.LINQToDataTable(qhe);
                        dtQHe.TableName = "HDG_QHE_DDO";
                        ds.Tables.Add(dtQHe);
                    }
                    DataTable dt = BillingLibrary.BillingLibrary.LINQToDataTable(q);
                    dt.TableName = "GCS_LICHGCS";
                    ds.Tables.Add(dt);
                    ds.AcceptChanges();
                    return ds;
                }
                return null;
            }
            catch
            {
                
                return null;
            }                       
        }
        #endregion        
    }
}
