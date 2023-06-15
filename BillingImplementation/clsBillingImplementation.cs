using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using busInputDataReading;
using busOutputDataWriting;
using busBillingCalculation;
using busBillingConfiguration;
using busLogManagement;
using System.Data;
using Newtonsoft.Json.Linq;
using System.Configuration;
using Quobject.SocketIoClientDotNet.Client;
using BillingLibrary;
using System.Diagnostics;
using System.Threading.Tasks;
using System.Net.Http;
using System.IO;

//Dungnt CheckIn 0903
namespace BillingImplementation 
{
    public class clsBillingImplementation :IDisposable
    {
        //
        DataSet dsCustomerData;
        static DataSet dsStaticCatalog;
        DataSet dsInvoiceData;
        DS_CALCULATIONTABLES dsCalculation;
        LOGDATA dsLog;
        cls_InputDataReading inputDataReading;
        cls_OutputDataWriting outputDataWriting;
        clsRawDataCalculation rawDataCalculation;
        clsDetailDataCalculation detailDataCalculation;
        clsPriceSpecification priceSpecification;
        clsInvoiceCalculation invoiceCalculation;
        cls_LogManagement log;
        cls_Config config = new cls_Config();
        private bool IsDisposed = false;
        //Ham khoi tao cac cau truc dataset
        public clsBillingImplementation()
        {
            dsCustomerData = new DataSet();
            dsInvoiceData = new DataSet();
            dsCalculation = new DS_CALCULATIONTABLES();
            dsLog = new LOGDATA();
            inputDataReading = new cls_InputDataReading();
            outputDataWriting = new cls_OutputDataWriting();
            rawDataCalculation = new clsRawDataCalculation();
            detailDataCalculation = new clsDetailDataCalculation();
            priceSpecification = new clsPriceSpecification();
            invoiceCalculation = new clsInvoiceCalculation();
        }
        ~clsBillingImplementation()
        {
            Dispose(false);
        }
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }
        protected virtual void Dispose(bool disposedStatus)
        {

            if (!IsDisposed)
            {
                IsDisposed = true;
                // Released unmanaged Resources
                if (disposedStatus)
                {
                    // Released managed Resources
                }
            }
        }
        // DũngNT sửa ngày 19-10-2009: Gán thêm 2 biến phân luồng từ UI => thêm 2 tham số đầu vào cho hàm
        // CMIS2
        //public string BillingImplementation(string strMa_DViQLy, string strMa_SoGCS, short ky, short thang, short nam, string strTen_DNhap, long lngCurrentLibID, long lngWorkflowID)
        //{
        //    if (ky == 1 && thang == 3 && nam == 2011)
        //    {
        //        //Ap dung cho ky 1 thang 3/2011, khong co lam tron ngang cho cac nhom bac thang SHBT, SHBB
        //        //Lay khoang chi so theo tung ngay cuoi ky
        //        //Lay don gia theo 2 bang gia khac nhau 
        //        return this.PriceChange1_Implementation(strMa_DViQLy, strMa_SoGCS, ky, thang, nam, strTen_DNhap, lngCurrentLibID, lngWorkflowID);
        //    }
        //    return this.CommonImplementation(strMa_DViQLy, strMa_SoGCS, ky, thang, nam, strTen_DNhap, lngCurrentLibID, lngWorkflowID);
        //}

        private string FilterHDG_BBAN_APGIA(ref DataSet dsCustomerData)
        {
            try
            {
                //Viet lai phan filter bien ban ap gia
                //Debug.WriteLine(DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff") + " FilterHDG_BBAN_APGIA Begin.");
                if (dsCustomerData.Tables.Contains("HDG_BBAN_APGIA") == false)
                    return "NotExistsHDG_BBAN_APGIA"; //khong ton tai bang HDG_BBAN_APGIA
                else
                    if (dsCustomerData.Tables["HDG_BBAN_APGIA"].Rows.Count == 0)
                    return "NoDataFoundInHDG_BBAN_APGIA"; //khong co du lieu trong bang HDG_BBAN_APGIA
                if (dsCustomerData.Tables.Contains("GCS_CHISO") == false)
                    return "NotExistsGCS_CHISO"; //khong ton tai bang GCS_CHISO
                else
                    if (dsCustomerData.Tables["GCS_CHISO"].Rows.Count == 0)
                    return "NoDataFoundInGCS_CHISO"; //khong co du lieu trong bang GCS_CHISO
                //Dũng NT hiệu chỉnh cho issue CMIS-6665
                if (dsCustomerData.Tables.Contains("HDG_DIEM_DO") == false)
                    return "NotExistsHDG_DIEM_DO"; //khong ton tai bang HDG_DIEM_DO
                else
                    if (dsCustomerData.Tables["HDG_DIEM_DO"].Rows.Count == 0)
                    return "NoDataFoundInHDG_DIEM_DO"; //khong co du lieu trong bang HDG_DIEM_DO
                DataView dw = new DataView(dsCustomerData.Tables["HDG_BBAN_APGIA"]);
                //DataView dw = dsCustomerData.Tables["HDG_BBAN_APGIA"].AsEnumerable().AsDataView(); 
                dw.Sort = "MA_DDO, NGAY_HLUC DESC";

                string strMa_DDo = "";
                DateTime ngay_dkymin, ngay_ckymax, ngay_hluc, ngay_hlucmin, ngay_hlucmax, ngay_hlucmaxSHBTN, dt3000;
                ngay_dkymin = DateTime.Parse("01/01/1900", new System.Globalization.CultureInfo("vi-VN"));
                ngay_ckymax = DateTime.Parse("01/01/1900", new System.Globalization.CultureInfo("vi-VN"));
                ngay_hluc = DateTime.Parse("01/01/1900", new System.Globalization.CultureInfo("vi-VN"));
                ngay_hlucmin = DateTime.Parse("01/01/3000", new System.Globalization.CultureInfo("vi-VN"));
                dt3000 = new DateTime(3000, 1, 1);
                DateTime? ngaySO_HO = null;
                DateTime? ngayLienTruoc = null;

                //Dũng NT hiệu chỉnh ngày 23/12/2012
                //Bổ sung thêm đoạn kiểm tra biên bản áp giá có phải là biên bản sinh ra do thay đổi số hộ không
                //Và sau đó tồn tại 1 biên bản áp giá khác ngay sau đó không.
                //Nếu có thì xóa luôn biên bản SOHO đó đi
                //Debug.WriteLine(DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff") + " FilterHDG_BBAN_APGIA Begin.");
                dw.RowFilter = "IS_SOHO>0";
                //Debug.WriteLine(DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff") + " FilterHDG_BBAN_APGIA RowFilter.");
                DataView dvAG = new DataView(dsCustomerData.Tables["HDG_BBAN_APGIA"]);
                if (dw != null && dw.Count > 0)
                {
                    foreach (DataRowView drw in dw)
                    {
                        //Là bản ghi thay đổi số hộ
                        //dwCS.RowFilter = "MA_DDO='" + drw["MA_DDO"].ToString().Trim() + "'";
                        var exists = dsCustomerData.Tables["GCS_CHISO"].Select("MA_DDO = '" + drw["MA_DDO"].ToString().Trim() + "' AND BCS IN ('KT','BT','CD','TD') AND LOAI_CHISO <> 'DUP'");
                        if (exists != null && exists.Length > 0)
                        {
                            DateTime ngay_dkyminTemp = DateTime.Parse(Convert.ToDateTime(dsCustomerData.Tables["GCS_CHISO"].Compute("MIN(NGAY_DKY)", "MA_DDO = '" + drw["MA_DDO"].ToString().Trim() + "' AND BCS IN ('KT','BT','CD','TD') AND LOAI_CHISO <> 'DUP'")).ToString("dd/MM/yyyy", new System.Globalization.CultureInfo("vi-VN")), new System.Globalization.CultureInfo("vi-VN"));
                            if (ngay_dkyminTemp >= Convert.ToDateTime(drw["NGAY_HLUC"]))
                                continue;
                        }
                        dvAG.RowFilter = "MA_DDO='" + drw["MA_DDO"].ToString().Trim() + "'";
                        if (dvAG != null && dvAG.Count > 0)
                        {
                            foreach (DataRowView drView in dvAG)
                            {
                                if (Convert.ToDateTime(drView["NGAY_HLUC"]) > Convert.ToDateTime(drw["NGAY_HLUC"]) && Convert.ToDecimal(drView["SO_HO"]) > 0)
                                {
                                    //Có bản ghi áp giá bậc thang mới hơn, xóa luôn bản ghi SOHO đó đi
                                    //Kiểm tra bản ghi áp giá mới phải là bậc thang thì mới (số hộ >0)
                                    drw.Row.Delete();
                                    break;
                                }
                            }
                        }
                        dvAG.RowFilter = "";
                    }
                }
                //Debug.WriteLine(DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff") + " FilterHDG_BBAN_APGIA IS_SOHO>0.");
                dw = new DataView(dsCustomerData.Tables["HDG_BBAN_APGIA"]);
                //dw = dsCustomerData.Tables["HDG_BBAN_APGIA"].AsEnumerable().AsDataView();
                dw.Sort = "MA_DDO, NGAY_HLUC DESC";
                //Kết thúc hiệu chỉnh
                //Dũng NT
                foreach (DataRowView drw in dw)
                {
                    //if (drw["MA_DDO"].ToString().Trim() == "PM05000001349001")
                    //{
                    //}
                    //Bổ sung thêm đoạn kiểm tra áp giá xuất hiện ở cả 2 bảng HDG_BBAN_APGIA và HDG_BBAN_APGIA_GT
                    //Chỉ xử lý với các điểm đo phụ trừ phụ của các điểm đo phụ ghép tổng của sổ chính
                    //Mã issue CMIS-6652
                    if (dsCustomerData.Tables.Contains("HDG_BBAN_APGIA_GT") && dsCustomerData.Tables["HDG_BBAN_APGIA_GT"].Rows.Count > 0)
                    {
                        //Kiểm tra điểm đo đang xét là điểm đo phụ trừ phụ
                        if (dsCustomerData.Tables.Contains("HDG_QHE_DDO") && dsCustomerData.Tables["HDG_QHE_DDO"].Rows.Count > 0)
                        {
                            DataRow[] existsTPhu = dsCustomerData.Tables["HDG_QHE_DDO"].Select("MA_DDO_PHU = '" + drw["MA_DDO"].ToString() + "' and LOAI_QHE not in ('40','32')");
                            if (existsTPhu != null && existsTPhu.Length > 0)
                            {
                                DateTime dtTemp = Convert.ToDateTime(drw["NGAY_HLUC"]);
                                Int64 idTemp = Convert.ToInt64(drw["ID_BBANAGIA"]);
                                //var existsAGiaGT = dsCustomerData.Tables["HDG_BBAN_APGIA_GT"].AsEnumerable().Where(c => c.Field<string>("MA_DDO") == drw["MA_DDO"].ToString()).ToArray();
                                DataRow[] existsAGiaGT = dsCustomerData.Tables["HDG_BBAN_APGIA_GT"].Select("MA_DDO = '" + drw["MA_DDO"].ToString().Trim() + "'");
                                if (existsAGiaGT != null && existsAGiaGT.Length > 0)
                                {
                                    foreach (var x in existsAGiaGT)
                                    {
                                        if (Convert.ToDateTime(x["NGAY_HLUC"]) == dtTemp && Convert.ToInt64(x["ID_BBANAGIA"]) == idTemp)
                                        {
                                            drw.Row.Delete();
                                            break;
                                        }
                                    }
                                }
                            }
                        }
                    }
                    //Kết thúc hiệu chỉnh


                    if (strMa_DDo != drw["MA_DDO"].ToString())
                    {
                        //Điểm đo chưa từng được duyệt dòng giá nào
                        //Khởi tạo lại giá trị ban đầu 2 biến ngày
                        ngaySO_HO = null;
                        ngayLienTruoc = null;
                        //ngay_dkymin = DateTime.Parse("01/01/3000", new System.Globalization.CultureInfo("vi-VN"));
                        //ngay_ckymax = DateTime.Parse("01/01/1900", new System.Globalization.CultureInfo("vi-VN"));
                        //ngay_hluc = DateTime.Parse("01/01/1900", new System.Globalization.CultureInfo("vi-VN"));
                        //ngay_hlucmin = DateTime.Parse("01/01/3000", new System.Globalization.CultureInfo("vi-VN"));
                        ngay_dkymin = new DateTime(3000, 1, 1);
                        ngay_ckymax = new DateTime(1900, 1, 1);
                        ngay_hluc = new DateTime(1900, 1, 1);
                        ngay_hlucmin = new DateTime(3000, 1, 1);

                        strMa_DDo = drw["MA_DDO"].ToString();
                        DataRow[] existsCS = dsCustomerData.Tables["GCS_CHISO"].Select("MA_DDO = '" + drw["MA_DDO"].ToString() + "' and BCS IN ('KT','BT','CD','TD') AND LOAI_CHISO <> 'DUP'");
                        if (existsCS == null || existsCS.Length <= 0) continue;
                        ngay_hluc = Convert.ToDateTime(drw["NGAY_HLUC"]);
                        //Tim ngay dau ky min va ngay cuoi ky max
                        //ngay_dkymin = DateTime.Parse(Convert.ToDateTime(dsCustomerData.Tables["GCS_CHISO"].Compute("MIN(NGAY_DKY)", "MA_DDO = '" + strMa_DDo + "' AND BCS IN ('KT','BT','CD','TD') AND LOAI_CHISO <> 'DUP'")).ToString("dd/MM/yyyy", new System.Globalization.CultureInfo("vi-VN")), new System.Globalization.CultureInfo("vi-VN"));
                        //ngay_ckymax = DateTime.Parse(Convert.ToDateTime(dsCustomerData.Tables["GCS_CHISO"].Compute("MAX(NGAY_CKY)", "MA_DDO = '" + strMa_DDo + "' AND BCS IN ('KT','BT','CD','TD') AND LOAI_CHISO <> 'DUP'")).ToString("dd/MM/yyyy", new System.Globalization.CultureInfo("vi-VN")), new System.Globalization.CultureInfo("vi-VN"));

                        //ngay_dkymin = dsCustomerData.Tables["GCS_CHISO"].AsEnumerable().Where(b => b.Field<string>("MA_DDO") == strMa_DDo && "KT;BT;CD;TD".Contains(b.Field<string>("BCS")) && b.Field<string>("BCS") != "DUP").Min(c => c.Field<DateTime>("NGAY_DKY"));
                        //ngay_ckymax = dsCustomerData.Tables["GCS_CHISO"].AsEnumerable().Where(b => b.Field<string>("MA_DDO") == strMa_DDo && "KT;BT;CD;TD".Contains(b.Field<string>("BCS")) && b.Field<string>("BCS") != "DUP").Max(c => c.Field<DateTime>("NGAY_CKY"));

                        //Foreach
                        foreach (DataRow dr in existsCS)
                        {
                            DateTime dtNgayDKy = Convert.ToDateTime(dr["NGAY_DKY"]);
                            DateTime dtNgayCKy = Convert.ToDateTime(dr["NGAY_CKY"]);
                            if (ngay_dkymin > dtNgayDKy) ngay_dkymin = dtNgayDKy;
                            if (ngay_ckymax < dtNgayCKy) ngay_ckymax = dtNgayCKy;
                        }

                        //Kiểm tra có trùng ngày hiệu lực với dòng điểm đo SOHO ko
                        var soho = dsCustomerData.Tables["HDG_DIEM_DO"].Select("MA_DDO = '" + drw["MA_DDO"].ToString() + "' and THAO_TACDL='SOHO'");
                        if (soho != null && soho.Length > 0)
                        {
                            //Kiểm tra ngày hiệu lực
                            DateTime ngayHLucDDo = Convert.ToDateTime(soho[0]["NGAY_HLUC"]);
                            if (ngay_hluc == ngayHLucDDo)
                            {
                                //dòng giá trùng bản ghi SOHO
                                ngaySO_HO = ngayHLucDDo;
                                continue;
                            }
                        }
                        if (ngay_ckymax < ngay_hluc)
                        {
                            drw.Row.Delete();
                            continue;
                        }
                        else
                        {
                            //Trong khoang gia
                            if (ngay_dkymin < ngay_hluc)
                            {
                                continue; //do nothing
                            }
                            else
                            {
                                if (ngay_dkymin == ngay_hluc)
                                {
                                    ngay_hlucmin = ngay_hluc;
                                }
                                else
                                {
                                    //if (ngay_hlucmin == DateTime.Parse("01/01/3000", new System.Globalization.CultureInfo("vi-VN")))
                                    if (ngay_hlucmin == dt3000)
                                    {
                                        ngay_hlucmin = ngay_hluc;
                                    }
                                    else
                                    {
                                        if (ngay_hluc >= ngay_hlucmin)
                                        {
                                            continue; //do nothing
                                        }
                                        else
                                        {
                                            drw.Row.Delete();
                                        }
                                    }
                                }
                            }
                        }
                    }
                    else
                    {
                        ngay_hluc = Convert.ToDateTime(drw["NGAY_HLUC"]);
                        if (ngaySO_HO != null)
                        {
                            if (ngay_hluc == ngaySO_HO.Value) continue;
                            if (ngayLienTruoc != null)
                            {
                                //Bản ghi trùng ngày liền trước, bỏ qua ko xét
                                if (ngay_hluc == ngayLienTruoc.Value) continue;
                            }
                            else
                            {
                                //Gán giá trị ngày liền trước SOHO
                                //
                                //So sánh ngày liền trước SOHO với ngày đầu kỳ min, ngày cuối kỳ max
                                if (ngay_ckymax < ngay_hluc)
                                {
                                    drw.Row.Delete();
                                    continue;
                                }
                                else
                                {
                                    //Trong khoang gia
                                    if (ngay_dkymin < ngay_hluc)
                                    {
                                        //Chưa phải ngày hiệu lực min
                                        ngayLienTruoc = ngay_hluc;
                                        continue; //do nothing
                                    }
                                    else
                                    {
                                        //Ngày liền trước SOHO cũng là ngày hiệu lực min luôn
                                        ngayLienTruoc = ngay_hluc;
                                        ngay_hlucmin = ngay_hluc;
                                        continue;
                                    }
                                }

                            }
                        }
                        if (ngay_ckymax < ngay_hluc)
                        {
                            drw.Row.Delete();
                            continue;
                        }
                        else
                        {
                            //Trong khoang gia
                            if (ngay_dkymin < ngay_hluc)
                            {
                                continue; //do nothing
                            }
                            else
                            {
                                if (ngay_dkymin == ngay_hluc)
                                {
                                    ngay_hlucmin = ngay_hluc;
                                }
                                else
                                {
                                    //if (ngay_hlucmin == DateTime.Parse("01/01/3000", new System.Globalization.CultureInfo("vi-VN")))
                                    if (ngay_hlucmin == dt3000)
                                    {
                                        ngay_hlucmin = ngay_hluc;
                                    }
                                    else
                                    {
                                        if (ngay_hluc >= ngay_hlucmin)
                                        {
                                            continue; //do nothing
                                        }
                                        else
                                        {
                                            drw.Row.Delete();
                                        }
                                    }
                                }
                            }
                        }
                    }

                }
                dw.Table.AcceptChanges();
                //Debug.WriteLine(DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff") + " FilterHDG_BBAN_APGIA ddo chính.");
                #region Xử lý riêng đoạn SHBT nhóm N, hiện tại không dùng nữa
                //Bo sung them doan xu ly SHBT nhom N cua bien ban ap gia moi nhat thi tinh ca ky
                //strMa_DDo = "";
                //ngay_hlucmax = DateTime.Parse("01/01/1900", new System.Globalization.CultureInfo("vi-VN"));
                //ngay_hlucmaxSHBTN = DateTime.Parse("01/01/1901", new System.Globalization.CultureInfo("vi-VN"));

                //foreach (DataRowView drw in dw)
                //{
                //    var existsCS = dsCustomerData.Tables["GCS_CHISO"].Select("MA_DDO = '" + drw["MA_DDO"].ToString() + "' and BCS IN ('KT','BT','CD','TD') AND LOAI_CHISO <> 'DUP'");
                //    if (existsCS == null || existsCS.Length <= 0) continue;
                //    if (strMa_DDo != drw["MA_DDO"].ToString())
                //    {
                //        strMa_DDo = drw["MA_DDO"].ToString();
                //        try
                //        {
                //            ngay_hlucmax = Convert.ToDateTime(dsCustomerData.Tables["HDG_BBAN_APGIA"].Compute("MAX(NGAY_HLUC)", "MA_DDO = '" + strMa_DDo + "'"));
                //            ngay_hlucmaxSHBTN = Convert.ToDateTime(dsCustomerData.Tables["HDG_BBAN_APGIA"].Compute("MAX(NGAY_HLUC)", "MA_DDO = '" + strMa_DDo + "' AND MA_NHOMNN = 'SHBT' AND MA_NGIA = 'N'"));
                //        }
                //        catch
                //        {
                //            ngay_hlucmax = DateTime.Parse("01/01/1900", new System.Globalization.CultureInfo("vi-VN"));
                //            ngay_hlucmaxSHBTN = DateTime.Parse("01/01/1901", new System.Globalization.CultureInfo("vi-VN"));
                //        }
                //        ngay_dkymin = DateTime.Parse(Convert.ToDateTime(dsCustomerData.Tables["GCS_CHISO"].Compute("MIN(NGAY_DKY)", "MA_DDO = '" + strMa_DDo + "' AND BCS IN ('KT','BT','CD','TD') AND LOAI_CHISO <> 'DUP'")).ToString("dd/MM/yyyy", new System.Globalization.CultureInfo("vi-VN")), new System.Globalization.CultureInfo("vi-VN"));
                //    }

                //    if (ngay_hlucmax == ngay_hlucmaxSHBTN)
                //    {
                //        //La diem do co bien ban ap gia moi nhat la SHBT nhom N
                //        if (Convert.ToDateTime(drw["NGAY_HLUC"]) == ngay_hlucmax)
                //        {
                //            //Day ngay hieu luc ap gia ve ngay dau ky
                //            drw["NGAY_HLUC"] = ngay_dkymin;
                //        }
                //        else
                //        {
                //            drw.Row.Delete();
                //        }
                //    }
                //}
                #endregion
                #region Tạo giả áp giá cho dòng CSC nếu chưa có
                //DataView dvCS = new DataView(dsCustomerData.Tables["GCS_CHISO"]);
                //dvCS.RowFilter = "LOAI_CHISO='CSC'";
                //dvCS.Sort = "MA_DDO, NGAY_CKY ASC";
                ////.Select("LOAI_CHISO='CSC'").OrderBy(c=>c.Field<string>("NGAY_CKY")).ToArray();
                //DateTime dtNgayCSC = new DateTime(1900, 1, 1);
                //DateTime dtNgayHLucCSC = new DateTime(1900, 1, 1);
                //DateTime dtNgayGiaClone = new DateTime(1900, 1, 1);
                //strMa_DDo = "";
                ////DataView dw = dsCustomerData.Tables["HDG_BBAN_APGIA"].AsEnumerable().AsDataView(); 
                //if (dvCS.Count > 0)
                //{
                //    foreach (DataRowView drCS in dvCS)
                //    {
                //        if (strMa_DDo == drCS["MA_DDO"].ToString().Trim() && dtNgayCSC == Convert.ToDateTime(drCS["NGAY_CKY"])) continue;

                //        dtNgayCSC = Convert.ToDateTime(drCS["NGAY_CKY"]);
                //        dtNgayHLucCSC = dtNgayCSC.AddDays(1);

                //        strMa_DDo = drCS["MA_DDO"].ToString().Trim();
                //        dw.RowFilter = "MA_DDO='" + strMa_DDo + "'";
                //        dw.Sort = "NGAY_HLUC DESC";
                //        bool isExitsCSC = false;
                //        foreach (DataRowView drvGia in dw)
                //        {
                //            DateTime dtNgayGia = Convert.ToDateTime(drvGia["NGAY_HLUC"]);
                //            if (dtNgayGia == dtNgayHLucCSC)
                //            {
                //                //Không thực hiện tiếp, đã có dòng giá tương ứng
                //                isExitsCSC = true;
                //                break;
                //            }
                //            if (dtNgayGia < dtNgayHLucCSC)
                //            {

                //                dtNgayGiaClone = dtNgayGia;
                //                break;
                //            }
                //        }
                //        if (!isExitsCSC)
                //        {
                //            foreach (DataRowView drvGia in dw)
                //            {
                //                DateTime dtNgayGia = Convert.ToDateTime(drvGia["NGAY_HLUC"]);
                //                if (dtNgayGia != dtNgayGiaClone) continue;
                //                DataRow drNew = dw.Table.NewRow();
                //                drNew.ItemArray = drvGia.Row.ItemArray;
                //                drNew["NGAY_HLUC"] = dtNgayHLucCSC;
                //                drNew["IS_SOHO"] = "0";
                //                dw.Table.Rows.Add(drNew);
                //            }
                //        }
                //    }
                //}


                #endregion
                //Debug.WriteLine(DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff") + " FilterHDG_BBAN_APGIA CSC.");

                if (dsCustomerData.Tables.Contains("HDG_BBAN_APGIA_GT") == false)
                {
                    return "";
                }
                if (dsCustomerData.Tables.Contains("GCS_CHISO_GT") == false)
                {
                    return "";
                }
                if (dsCustomerData.Tables.Contains("HDG_DIEM_DO_GT") == false)
                {
                    return "";
                }
                //Viet lai phan filter bien ban ap gia
                dw = new DataView(dsCustomerData.Tables["HDG_BBAN_APGIA_GT"]);
                dw.Sort = "MA_DDO, NGAY_HLUC DESC";
                strMa_DDo = "";
                ngay_dkymin = DateTime.Parse("01/01/1900", new System.Globalization.CultureInfo("vi-VN"));
                ngay_ckymax = DateTime.Parse("01/01/1900", new System.Globalization.CultureInfo("vi-VN"));
                ngay_hluc = DateTime.Parse("01/01/1900", new System.Globalization.CultureInfo("vi-VN"));
                ngay_hlucmin = DateTime.Parse("01/01/3000", new System.Globalization.CultureInfo("vi-VN"));
                //Dũng NT hiệu chỉnh ngày 23/12/2012
                //Bổ sung thêm đoạn kiểm tra biên bản áp giá có phải là biên bản sinh ra do thay đổi số hộ không
                //Và sau đó tồn tại 1 biên bản áp giá khác ngay sau đó không.
                //Nếu có thì xóa luôn biên bản SOHO đó đi
                dw.RowFilter = "IS_SOHO>0";
                dvAG = new DataView(dsCustomerData.Tables["HDG_BBAN_APGIA_GT"]);
                if (dw != null && dw.Count > 0)
                {
                    foreach (DataRowView drw in dw)
                    {
                        if (drw["MA_DDO"].ToString().Trim() == "PD07000079089006")
                        {

                        }
                        //Là bản ghi thay đổi số hộ
                        var exists = dsCustomerData.Tables["GCS_CHISO_GT"].Select("MA_DDO = '" + drw["MA_DDO"].ToString().Trim() + "' AND BCS IN ('KT','BT','CD','TD') AND LOAI_CHISO <> 'DUP'");
                        if (exists != null && exists.Length > 0)
                        {
                            DateTime ngay_dkyminTemp = DateTime.Parse(Convert.ToDateTime(dsCustomerData.Tables["GCS_CHISO_GT"].Compute("MIN(NGAY_DKY)", "MA_DDO = '" + drw["MA_DDO"].ToString().Trim() + "' AND BCS IN ('KT','BT','CD','TD') AND LOAI_CHISO <> 'DUP'")).ToString("dd/MM/yyyy", new System.Globalization.CultureInfo("vi-VN")), new System.Globalization.CultureInfo("vi-VN"));
                            if (ngay_dkyminTemp >= Convert.ToDateTime(drw["NGAY_HLUC"]))
                                continue;
                        }
                        dvAG.RowFilter = "MA_DDO='" + drw["MA_DDO"].ToString().Trim() + "'";
                        if (dvAG != null && dvAG.Count > 0)
                        {
                            foreach (DataRowView drView in dvAG)
                            {
                                if (Convert.ToDateTime(drView["NGAY_HLUC"]) > Convert.ToDateTime(drw["NGAY_HLUC"]) && Convert.ToDecimal(drView["SO_HO"]) > 0)
                                {
                                    //Có bản ghi áp giá mới hơn, xóa luôn bản ghi SOHO đó đi
                                    drw.Row.Delete();
                                    break;
                                }
                            }
                        }
                        dvAG.RowFilter = "";
                    }
                }
                dw = new DataView(dsCustomerData.Tables["HDG_BBAN_APGIA_GT"]);
                //Kết thúc hiệu chỉnh
                foreach (DataRowView drw in dw)
                {


                    if (strMa_DDo != drw["MA_DDO"].ToString())
                    {
                        ////
                        ngaySO_HO = null;
                        ngayLienTruoc = null;
                        //ngay_dkymin = DateTime.Parse("01/01/1900", new System.Globalization.CultureInfo("vi-VN"));
                        //ngay_ckymax = DateTime.Parse("01/01/1900", new System.Globalization.CultureInfo("vi-VN"));
                        //ngay_hluc = DateTime.Parse("01/01/1900", new System.Globalization.CultureInfo("vi-VN"));
                        //ngay_hlucmin = DateTime.Parse("01/01/3000", new System.Globalization.CultureInfo("vi-VN"));
                        ngay_dkymin = new DateTime(3000, 1, 1);
                        ngay_ckymax = new DateTime(1900, 1, 1);
                        ngay_hluc = new DateTime(1900, 1, 1);
                        ngay_hlucmin = new DateTime(3000, 1, 1);

                        strMa_DDo = drw["MA_DDO"].ToString();
                        ngay_hluc = Convert.ToDateTime(drw["NGAY_HLUC"]);
                        var existsCS_GT = dsCustomerData.Tables["GCS_CHISO_GT"].Select("MA_DDO = '" + drw["MA_DDO"].ToString() + "' and BCS IN ('KT','BT','CD','TD') AND LOAI_CHISO <> 'DUP'");
                        if (existsCS_GT == null || existsCS_GT.Length <= 0) continue;
                        //Tim ngay dau ky min va ngay cuoi ky max
                        //ngay_dkymin = DateTime.Parse(Convert.ToDateTime(dsCustomerData.Tables["GCS_CHISO_GT"].Compute("MIN(NGAY_DKY)", "MA_DDO = '" + strMa_DDo + "' AND BCS IN ('KT','BT','CD','TD') AND LOAI_CHISO <> 'DUP'")).ToString("dd/MM/yyyy", new System.Globalization.CultureInfo("vi-VN")), new System.Globalization.CultureInfo("vi-VN"));
                        //ngay_ckymax = DateTime.Parse(Convert.ToDateTime(dsCustomerData.Tables["GCS_CHISO_GT"].Compute("MAX(NGAY_CKY)", "MA_DDO = '" + strMa_DDo + "' AND BCS IN ('KT','BT','CD','TD') AND LOAI_CHISO <> 'DUP'")).ToString("dd/MM/yyyy", new System.Globalization.CultureInfo("vi-VN")), new System.Globalization.CultureInfo("vi-VN"));
                        foreach (DataRow dr in existsCS_GT)
                        {
                            DateTime dtNgayDKy = Convert.ToDateTime(dr["NGAY_DKY"]);
                            DateTime dtNgayCKy = Convert.ToDateTime(dr["NGAY_CKY"]);
                            if (ngay_dkymin > dtNgayDKy) ngay_dkymin = dtNgayDKy;
                            if (ngay_ckymax < dtNgayCKy) ngay_ckymax = dtNgayCKy;
                        }

                        //Kiểm tra có trùng ngày hiệu lực với dòng điểm đo SOHO ko
                        var soho = dsCustomerData.Tables["HDG_DIEM_DO_GT"].Select("MA_DDO = '" + drw["MA_DDO"].ToString() + "' and THAO_TACDL='SOHO'");
                        if (soho != null && soho.Length > 0)
                        {
                            //Kiểm tra ngày hiệu lực
                            DateTime ngayHLucDDo = Convert.ToDateTime(soho[0]["NGAY_HLUC"]);
                            if (ngay_hluc == ngayHLucDDo)
                            {
                                //dòng giá trùng bản ghi SOHO
                                ngaySO_HO = ngayHLucDDo;
                                continue;
                            }
                        }
                        if (ngay_ckymax < ngay_hluc)
                        {
                            drw.Row.Delete();
                            continue;
                        }
                        else
                        {
                            //Trong khoang gia
                            if (ngay_dkymin < ngay_hluc)
                            {
                                continue; //do nothing
                            }
                            else
                            {
                                if (ngay_dkymin == ngay_hluc)
                                {
                                    ngay_hlucmin = ngay_hluc;
                                }
                                else
                                {
                                    if (ngay_hlucmin == dt3000)
                                    {
                                        ngay_hlucmin = ngay_hluc;
                                    }
                                    else
                                    {
                                        if (ngay_hluc >= ngay_hlucmin)
                                        {
                                            continue; //do nothing
                                        }
                                        else
                                        {
                                            drw.Row.Delete();
                                        }
                                    }
                                }
                            }
                        }
                    }
                    else
                    {
                        ngay_hluc = Convert.ToDateTime(drw["NGAY_HLUC"]);
                        if (ngaySO_HO != null)
                        {
                            if (ngay_hluc == ngaySO_HO.Value) continue;
                            if (ngayLienTruoc != null)
                            {
                                //Bản ghi trùng ngày liền trước, bỏ qua ko xét
                                if (ngay_hluc == ngayLienTruoc.Value) continue;
                            }
                            else
                            {
                                //Gán giá trị ngày liền trước SOHO
                                ngayLienTruoc = ngay_hluc;
                                continue;
                            }
                        }
                        if (ngay_ckymax < ngay_hluc)
                        {
                            drw.Row.Delete();
                            continue;
                        }
                        else
                        {
                            //Trong khoang gia
                            if (ngay_dkymin < ngay_hluc)
                            {
                                continue; //do nothing
                            }
                            else
                            {
                                if (ngay_dkymin == ngay_hluc)
                                {
                                    ngay_hlucmin = ngay_hluc;
                                }
                                else
                                {
                                    if (ngay_hlucmin == dt3000)
                                    {
                                        ngay_hlucmin = ngay_hluc;
                                    }
                                    else
                                    {
                                        if (ngay_hluc >= ngay_hlucmin)
                                        {
                                            continue; //do nothing
                                        }
                                        else
                                        {
                                            drw.Row.Delete();
                                        }
                                    }
                                }
                            }
                        }
                    }
                }

                #region SHBT nhóm N cho Ghép tổng, bỏ luôn
                //foreach (DataRowView drw in dw)
                //{
                //    if (drw["MA_DDO"].ToString().Trim() == "PD07000079089006")
                //    {

                //    }
                //    var existsCS_GT = dsCustomerData.Tables["GCS_CHISO_GT"].Select("MA_DDO = '" + drw["MA_DDO"].ToString() + "' and BCS IN ('KT','BT','CD','TD') AND LOAI_CHISO <> 'DUP'");
                //    if (existsCS_GT == null || existsCS_GT.Length <= 0) continue;
                //    if (strMa_DDo != drw["MA_DDO"].ToString())
                //    {
                //        strMa_DDo = drw["MA_DDO"].ToString();
                //        try
                //        {
                //            ngay_hlucmax = Convert.ToDateTime(dsCustomerData.Tables["HDG_BBAN_APGIA_GT"].Compute("MAX(NGAY_HLUC)", "MA_DDO = '" + strMa_DDo + "'"));
                //            ngay_hlucmaxSHBTN = Convert.ToDateTime(dsCustomerData.Tables["HDG_BBAN_APGIA_GT"].Compute("MAX(NGAY_HLUC)", "MA_DDO = '" + strMa_DDo + "' AND MA_NHOMNN = 'SHBT' AND MA_NGIA = 'N'"));
                //        }
                //        catch
                //        {
                //            ngay_hlucmax = DateTime.Parse("01/01/1900", new System.Globalization.CultureInfo("vi-VN"));
                //            ngay_hlucmaxSHBTN = DateTime.Parse("01/01/1901", new System.Globalization.CultureInfo("vi-VN"));
                //        }
                //        ngay_dkymin = DateTime.Parse(Convert.ToDateTime(dsCustomerData.Tables["GCS_CHISO_GT"].Compute("MIN(NGAY_DKY)", "MA_DDO = '" + strMa_DDo + "' AND BCS IN ('KT','BT','CD','TD') AND LOAI_CHISO <> 'DUP'")).ToString("dd/MM/yyyy", new System.Globalization.CultureInfo("vi-VN")), new System.Globalization.CultureInfo("vi-VN"));
                //    }

                //    if (ngay_hlucmax == ngay_hlucmaxSHBTN)
                //    {
                //        //La diem do co bien ban ap gia moi nhat la SHBT nhom N
                //        if (Convert.ToDateTime(drw["NGAY_HLUC"]) == ngay_hlucmax)
                //        {
                //            //Day ngay hieu luc ap gia ve ngay dau ky
                //            drw["NGAY_HLUC"] = ngay_dkymin;
                //        }
                //        else
                //        {
                //            drw.Row.Delete();
                //        }
                //    }
                //}
                #endregion
                #region Tạo giả áp giá cho dòng CSC nếu chưa có
                // dvCS = new DataView(dsCustomerData.Tables["GCS_CHISO_GT"]);
                //dvCS.RowFilter = "LOAI_CHISO='CSC'";
                //dvCS.Sort = "MA_DDO, NGAY_CKY ASC";
                ////.Select("LOAI_CHISO='CSC'").OrderBy(c=>c.Field<string>("NGAY_CKY")).ToArray();
                // dtNgayCSC = new DateTime(1900, 1, 1);
                // dtNgayHLucCSC = new DateTime(1900, 1, 1);
                // dtNgayGiaClone = new DateTime(1900, 1, 1);
                //strMa_DDo = "";
                ////DataView dw = dsCustomerData.Tables["HDG_BBAN_APGIA"].AsEnumerable().AsDataView(); 
                //if (dvCS.Count > 0)
                //{
                //    foreach (DataRowView drCS in dvCS)
                //    {
                //        if (strMa_DDo == drCS["MA_DDO"].ToString().Trim() && dtNgayCSC == Convert.ToDateTime(drCS["NGAY_CKY"])) continue;

                //        dtNgayCSC = Convert.ToDateTime(drCS["NGAY_CKY"]);
                //        dtNgayHLucCSC = dtNgayCSC.AddDays(1);

                //        strMa_DDo = drCS["MA_DDO"].ToString().Trim();
                //        dw.RowFilter = "MA_DDO='" + strMa_DDo + "'";
                //        dw.Sort = "NGAY_HLUC DESC";
                //        bool isExitsCSC = false;
                //        foreach (DataRowView drvGia in dw)
                //        {
                //            DateTime dtNgayGia = Convert.ToDateTime(drvGia["NGAY_HLUC"]);
                //            if (dtNgayGia == dtNgayHLucCSC)
                //            {
                //                //Không thực hiện tiếp, đã có dòng giá tương ứng
                //                isExitsCSC = true;
                //                break;
                //            }
                //            if (dtNgayGia < dtNgayHLucCSC)
                //            {

                //                dtNgayGiaClone = dtNgayGia;
                //                break;
                //            }
                //        }
                //        if (!isExitsCSC)
                //        {
                //            foreach (DataRowView drvGia in dw)
                //            {
                //                DateTime dtNgayGia = Convert.ToDateTime(drvGia["NGAY_HLUC"]);
                //                if (dtNgayGia != dtNgayGiaClone) continue;
                //                DataRow drNew = dw.Table.NewRow();
                //                drNew.ItemArray = drvGia.Row.ItemArray;
                //                drNew["NGAY_HLUC"] = dtNgayHLucCSC;
                //                drNew["IS_SOHO"] = "0";
                //                dw.Table.Rows.Add(drNew);
                //            }
                //        }
                //    }
                //}


                #endregion

                return "";
            }
            catch (Exception ex)
            {
                return "Lỗi khi lọc thông tin áp giá:" + ex.Message;
            }
        }

        private string ChangeCCS(ref DataSet dsCustomerData, string strSrc, string strDesc, DataSet dsStaticCatalog)
        {
            try
            {
                string strTab = "GCS_CHISO;GCS_CHISO_GT;GCS_CHISO_TP;GCS_CHISO_BQ";
                List<string> lstTableName = strTab.Split(';').ToList();
                DataView dwCS = new DataView();
                DataView dwCS_DDo = new DataView();
                DateTime dtMinNgayDKy = new DateTime(1900, 1, 1), dtMaxNgayCKy = new DateTime(1900, 1, 1);
                if (!dsStaticCatalog.Tables.Contains("D_GIA_NHOMNN"))
                    return "NotExistsD_GIA_NHOMNN";
                if (!dsStaticCatalog.Tables.Contains("D_COSFI"))
                    return "NotExistsD_COSFI";
                DataView dwGia = new DataView(dsStaticCatalog.Tables["D_GIA_NHOMNN"]);
                dwGia.Sort = "NGAY_ADUNG DESC";
                DataView dwCosfi = new DataView(dsStaticCatalog.Tables["D_COSFI"]);
                dwCosfi.Sort = "NGAY_ADUNG DESC";
                //Nếu loại chỉ số cần đổi không thuộc 2 loại này thì không cho đổi
                if (!"CPK;CCS".Contains(strSrc)) return "";
                foreach (string strTableName in lstTableName)
                {
                    //reset biến ngày
                    dtMinNgayDKy = new DateTime(1900, 1, 1);
                    dtMaxNgayCKy = new DateTime(1900, 1, 1);
                    if (!dsCustomerData.Tables.Contains(strTableName)) continue;
                    dwCS = new DataView(dsCustomerData.Tables[strTableName]);
                    dwCS_DDo = new DataView(dsCustomerData.Tables[strTableName]);
                    dwCS.RowFilter = "LOAI_CS_GOC='" + strSrc + "'";
                    if (dwCS != null && dwCS.Count > 0)
                    {
                        foreach (DataRowView drv in dwCS)
                        {
                            dtMinNgayDKy = new DateTime(1900, 1, 1);
                            dtMaxNgayCKy = new DateTime(1900, 1, 1);
                            string strMaDDo = drv["MA_DDO"].ToString().Trim();
                            if (strSrc == "CPK")
                                dwCS_DDo.RowFilter = "MA_DDO='" + strMaDDo + "' and LOAI_CS_GOC='CCS'";
                            else
                                dwCS_DDo.RowFilter = "MA_DDO='" + strMaDDo + "' and LOAI_CS_GOC='CPK'";
                            if (dwCS_DDo != null && dwCS_DDo.Count > 0)
                            {
                                //Nếu có 2 loại chỉ số thì mới đổi
                                drv["LOAI_CHISO"] = strDesc;
                                //drv["ID_BCS"] = Convert.ToInt64(drv["ID_BCS"]) * iChinhIDBCS;
                                if (strDesc == "DDN")
                                {
                                    drv["MA_CTO"] = drv["MA_CTO"].ToString() + "_CPK";

                                }
                                else
                                {
                                    drv["MA_CTO"] = drv["MA_CTO"].ToString().Substring(0, drv["MA_CTO"].ToString().IndexOf("_CPK"));

                                }
                            }
                            else
                            {
                                dtMinNgayDKy = Convert.ToDateTime(dwCS_DDo.Table.Compute("MIN(NGAY_DKY)", "MA_DDO = '" + strMaDDo + "'"));
                                dtMaxNgayCKy = Convert.ToDateTime(dwCS_DDo.Table.Compute("MAX(NGAY_CKY)", "MA_DDO = '" + strMaDDo + "'"));
                                //Nếu chỉ có một loại chỉ số thì kiểm tra thêm điều kiện có đổi giá/CSPK bảng danh mục
                                if (strSrc == "CPK")
                                {
                                    //Kiểm tra bảng danh mục D_GIA_NHOMNN
                                    foreach (DataRowView drvTemp in dwGia)
                                    {
                                        DateTime dtTemp = Convert.ToDateTime(drvTemp["NGAY_ADUNG"]);
                                        if (dtMinNgayDKy < dtTemp && dtMaxNgayCKy >= dtTemp)
                                        {
                                            //Có đổi giá nhà nước trong kỳ, có đổi loại chỉ số
                                            drv["LOAI_CHISO"] = strDesc;
                                            //drv["ID_BCS"] = Convert.ToInt64(drv["ID_BCS"]) * iChinhIDBCS;
                                            if (strDesc == "DDN")
                                            {
                                                drv["MA_CTO"] = drv["MA_CTO"].ToString() + "_CPK";

                                            }
                                            else
                                            {
                                                drv["MA_CTO"] = drv["MA_CTO"].ToString().Substring(0, drv["MA_CTO"].ToString().IndexOf("_CPK"));
                                            }
                                            break;
                                        }
                                    }
                                }
                                else
                                {
                                    //Kiểm tra bảng danh mục D_COSFI
                                    foreach (DataRowView drvTemp in dwCosfi)
                                    {
                                        DateTime dtTemp = Convert.ToDateTime(drvTemp["NGAY_ADUNG"]);
                                        if (dtMinNgayDKy < dtTemp && dtMaxNgayCKy >= dtTemp)
                                        {
                                            //Có đổi CSPK trong kỳ, có đổi loại chỉ số
                                            drv["LOAI_CHISO"] = strDesc;
                                            break;
                                        }
                                    }
                                }
                            }
                        }
                    }

                }
                dsCustomerData.AcceptChanges();
                return "";
            }
            catch (Exception ex)
            {
                return "Lỗi khi thêm cột mới:" + ex.Message;
            }
        }
        //hàm thêm cột, sau bỏ đi vì sẽ lấy từ PKG
        private string AddColumn(ref DataSet dsCustomerData, DataSet dsStaticCatalog)
        {
            try
            {
                string strTab = "GCS_CHISO;GCS_CHISO_GT;GCS_CHISO_TP;GCS_CHISO_BQ";
                List<string> lstTableName = strTab.Split(';').ToList();
                List<DateTime> lstCPK = dsStaticCatalog.Tables["D_COSFI"].AsEnumerable().Select(c => c.Field<DateTime>("NGAY_ADUNG")).Distinct().ToList();
                List<DateTime> lstCCS = dsStaticCatalog.Tables["D_GIA_NHOMNN"].AsEnumerable().Select(c => c.Field<DateTime>("NGAY_ADUNG")).Distinct().ToList();
                DateTime dtNgayCK = new DateTime(1900, 1, 1);





                foreach (string strTableName in lstTableName)
                {
                    if (!dsCustomerData.Tables.Contains(strTableName)) continue;

                    if (!dsCustomerData.Tables[strTableName].Columns.Contains("LOAI_CS_GOC"))
                    {
                        dsCustomerData.Tables[strTableName].Columns.Add("LOAI_CS_GOC", typeof(string));
                        foreach (DataRow dr in dsCustomerData.Tables[strTableName].Rows)
                        {
                            if (dr["LOAI_CHISO"].ToString().Trim() == "CCS")
                            {
                                dtNgayCK = Convert.ToDateTime(dr["NGAY_CKY"]).Date.AddDays(1);
                                if (lstCCS.Contains(dtNgayCK))
                                {
                                    //CHốt đổi giá
                                    dr["LOAI_CS_GOC"] = dr["LOAI_CHISO"];
                                }
                                else if (lstCPK.Contains(dtNgayCK))
                                {
                                    //Chốt đổi CSPK
                                    dr["LOAI_CS_GOC"] = "CPK";
                                }
                                else
                                    return "Lỗi dòng chỉ số chốt của điểm đo " + dr["MA_DDO"].ToString() + ", không tồn tại danh mục tương ứng";
                                //dr["LOAI_CS_GOC"] = dr["LOAI_CHISO"];
                            }
                            else
                                dr["LOAI_CS_GOC"] = dr["LOAI_CHISO"];
                        }
                    }

                }
                dsCustomerData.AcceptChanges();
                return "";
            }
            catch (Exception ex)
            {
                return "Lỗi khi thêm cột mới:" + ex.Message;
            }
        }
        //CMIS2
        //private string CommonImplementation(string strMa_DViQLy, string strMa_SoGCS, short ky, short thang, short nam, string strTen_DNhap, long lngCurrentLibID, long lngWorkflowID)
        //{
        //    try
        //    {
        //        dsCustomerData = new DataSet();
        //        dsInvoiceData = new DataSet();
        //        dsCalculation = new DS_CALCULATIONTABLES();

        //        string strError = "";

        //        //Bat dau tinh hoa don
        //        WriteLog(strMa_DViQLy, strMa_SoGCS, "BillingImplementation", "BillingImplementation", "BillingImplementation", "Tính hoá đơn sổ " + strMa_SoGCS + " - kỳ " + ky + " tháng " + thang + "/" + nam);
        //        //Kiem tra so phu ghep tong
        //        strError = inputDataReading.checkPhuGhepTong(strMa_DViQLy, strMa_SoGCS, ky, thang, nam);
        //        if (strError != "" && strError != " ")
        //        {
        //            if (strError == "1")
        //            {
        //                WriteLog(strMa_DViQLy, strMa_SoGCS, "busInputDataReading", "busInputDataReading.cls_InputDataReading", "checkPhuGhepTong", "Kiểm tra sổ phụ ghép tổng.");
        //                strError = outputDataWriting.InsertInvoiceData_PGT(strMa_DViQLy, strMa_SoGCS, strTen_DNhap, lngCurrentLibID, lngWorkflowID, ky, thang, nam);
        //                if (strError != "")
        //                {
        //                    WriteLog(strMa_DViQLy, strMa_SoGCS, "busOutputDataWriting", "busOutputDataWriting.cls_OutputDataWriting", "InsertInvoiceData", "Cập nhật dữ liệu hoá đơn - " + strError);
        //                    return strError;
        //                }
        //                else
        //                {
        //                    WriteLog(strMa_DViQLy, strMa_SoGCS, "busOutputDataWriting", "busOutputDataWriting.cls_OutputDataWriting", "InsertInvoiceData", "Cập nhật dữ liệu hoá đơn");
        //                    WriteLog(strMa_DViQLy, strMa_SoGCS, "BillingImplementation", "BillingImplementation", "BillingImplementation", "Tính xong sổ " + strMa_SoGCS + " - hoá đơn kỳ " + ky + " tháng " + thang + "/" + nam);
        //                    return "";
        //                }
        //            }
        //            else if (strError == "2")
        //            {
        //                WriteLog(strMa_DViQLy, strMa_SoGCS, "busInputDataReading", "busInputDataReading.cls_InputDataReading", "checkPhuGhepTong", "Kiểm tra sổ phụ ghép tổng: Không tìm thấy dữ liệu trong HDG_DDO_SOGCS.");
        //                return strError;
        //            }
        //            else
        //            {
        //                WriteLog(strMa_DViQLy, strMa_SoGCS, "busInputDataReading", "busInputDataReading.cls_InputDataReading", "checkPhuGhepTong", "Kiểm tra sổ phụ ghép tổng - " + strError);
        //                return strError;
        //            }
        //        }
        //        else
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busInputDataReading", "busInputDataReading.cls_InputDataReading", "checkPhuGhepTong", "Kiểm tra sổ phụ ghép tổng");
        //        }
        //        //Doc du lieu dau vao
        //        strError = inputDataReading.getCustomerDataReading(ref dsCustomerData, strMa_DViQLy, strMa_SoGCS, ky, thang, nam);
        //        if (strError != "")
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busInputDataReading", "busInputDataReading.cls_InputDataReading", "getCustomerDataReading", "Đọc dữ liệu đầu vào - " + strError);
        //            return strError;
        //        }
        //        else
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busInputDataReading", "busInputDataReading.cls_InputDataReading", "getCustomerDataReading", "Đọc dữ liệu đầu vào");
        //        }
        //        //Loc bien ban ap gia, chi lay cac bien ban co lien quan den ky ghi chi so hien tai
        //        strError = this.FilterHDG_BBAN_APGIA(ref dsCustomerData);
        //        //decimal _tphu = dsCustomerData.Tables["HDG_QHE_DDO"].Select("MA_DDO_CHINH='PD05000093426001'").Length;
        //        if (strError != "")
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "BillingImplementation", "BillingImplementation.clsBillingImplementation.cs", "FilterHDG_BBAN_APGIA", "Lọc biên bản áp giá - " + strError);
        //            return strError;
        //        }
        //        else
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "BillingImplementation", "BillingImplementation.clsBillingImplementation.cs", "FilterHDG_BBAN_APGIA", "Lọc biên bản áp giá");
        //        }
        //        //Đổi loại chỉ số CPK sang DDN, sau bỏ đi
        //        strError = this.AddColumn(ref dsCustomerData, dsStaticCatalog);
        //        if (strError != "")
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "AddColumn", "Thêm cột mới - " + strError);
        //            return strError;
        //        }
        //        //Đổi loại chỉ số CPK sang DDN để check valid data ko báo lỗi dup chỉ số
        //        //strError = this.ChangeCCS(ref dsCustomerData, "CPK", "DDN", dsStaticCatalog);
        //        //if (strError != "")
        //        //{
        //        //    WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "AddColumn", "Tiền xử lý thông tin chỉ số - " + strError);
        //        //    return strError;
        //        //}
        //        //Kiem tra du lieu dau vao
        //        strError = inputDataReading.CheckValidData(strMa_DViQLy, this.dsCustomerData, clsBillingImplementation.dsStaticCatalog);
        //        if (strError != "")
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busInputDataReading", "busInputDataReading.cls_InputDataReading", "CheckValidData", "Kiểm tra dữ liệu đầu vào - " + strError);
        //            return strError;
        //        }
        //        else
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busInputDataReading", "busInputDataReading.cls_InputDataReading", "CheckValidData", "Kiểm tra dữ liệu đầu vào");
        //        }
        //        //var temp = dsCustomerData.Tables["HDG_BBAN_APGIA"].Select("MA_DDO = 'PC12LL0890486001'");
        //        //Tao cau truc du lieu dau ra
        //        dsInvoiceData = cls_OutputDataWriting.createDSInvoiceData();

        //        //Tinh san luong tho
        //        strError = rawDataCalculation.RawDataCalculation_1(this.dsCustomerData);
        //        if (strError != "")
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "RawDataCalculation_1", "Tính sản lượng thô - " + strError);
        //            return strError;
        //        }
        //        else
        //        {
        //            //Kiem tra neu khong con du lieu chi so thi thong bao ket thuc tinh hoa don luon
        //            if (this.dsCustomerData.Tables.Contains("GCS_CHISO") == true)
        //            {
        //                if (this.dsCustomerData.Tables["GCS_CHISO"].Rows.Count == 0)
        //                {
        //                    //Ket thuc tinh hoa don
        //                    WriteLog(strMa_DViQLy, strMa_SoGCS, "BillingImplementation", "BillingImplementation", "BillingImplementation", "Sổ không có dữ liệu chỉ số. Kết thúc tính hoá đơn của sổ " + strMa_SoGCS + " - kỳ " + ky + " tháng " + thang + "/" + nam);
        //                    return "";
        //                }
        //            }
        //        }

        //        strError = rawDataCalculation.RawDataCalculation_2(this.dsCustomerData);
        //        if (strError != "")
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "RawDataCalculation_2", "Tính sản lượng thô - " + strError);
        //            return strError;
        //        }

        //        strError = rawDataCalculation.RawDataCalculation_3(this.dsCustomerData);
        //        if (strError != "")
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "RawDataCalculation_3", "Tính sản lượng thô - " + strError);
        //            return strError;
        //        }
        //        //Đổi loại phản kháng về CCS, đổi CCS tạm sang DDN
        //        //strError = this.ChangeCCS(ref dsCustomerData, "CPK", "CCS", dsStaticCatalog);
        //        //if (strError != "")
        //        //{
        //        //    WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "AddColumn", "Tiền xử lý thông tin chỉ số - " + strError);
        //        //    return strError;
        //        //}
        //        strError = rawDataCalculation.RawDataCalculation_4(this.dsCustomerData, dsStaticCatalog);
        //        if (strError != "")
        //        {
        //            //Rieng phan tinh tru phu chi ghi log, khong can thoat
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "RawDataCalculation_4", "Tính sản lượng thô - " + strError);
        //            return strError;
        //        }
        //        //Đổi áp giá về DDN để tính cosfi
        //        //strError = this.ChangeCCS(ref dsCustomerData, "CCS", "DDN", dsStaticCatalog);
        //        //if (strError != "")
        //        //{
        //        //    WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "AddColumn", "Tiền xử lý thông tin chỉ số - " + strError);
        //        //    return strError;
        //        //}
        //        strError = rawDataCalculation.RawDataCalculation_5(this.dsCustomerData, this.dsInvoiceData, clsBillingImplementation.dsStaticCatalog, strTen_DNhap);
        //        if (strError != "")
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "RawDataCalculation_5", "Tính sản lượng thô - " + strError);
        //            return strError;
        //        }

        //        strError = rawDataCalculation.RawDataCalculation_6(this.dsCustomerData, this.dsInvoiceData, clsBillingImplementation.dsStaticCatalog, strTen_DNhap);
        //        if (strError != "")
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "RawDataCalculation_6", "Tính sản lượng thô - " + strError);
        //            return strError;
        //        }

        //        strError = rawDataCalculation.RawDataCalculation_7(this.dsCustomerData, this.dsInvoiceData, clsBillingImplementation.dsStaticCatalog, strTen_DNhap);
        //        if (strError != "")
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "RawDataCalculation_7", "Tính sản lượng thô - " + strError);
        //            return strError;
        //        }
        //        WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "RawDataCalculation_7", "Tính xong sản lượng thô");
        //        //đã tính xong cosfi, đổi CCS trả về CCS, CPK về DDN
        //        //strError = this.ChangeCCS(ref dsCustomerData, "CPK", "DDN", dsStaticCatalog);
        //        //if (strError != "")
        //        //{
        //        //    WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "AddColumn", "Tiền xử lý thông tin chỉ số - " + strError);
        //        //    return strError;
        //        //}
        //        //strError = this.ChangeCCS(ref dsCustomerData, "CCS", "CCS", dsStaticCatalog);
        //        //if (strError != "")
        //        //{
        //        //    WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "AddColumn", "Tiền xử lý thông tin chỉ số - " + strError);
        //        //    return strError;
        //        //}
        //        //Tinh san luong chi tiet
        //        strError = detailDataCalculation.DetailDataCalculation_11(this.dsCustomerData, ref this.dsCalculation, dsStaticCatalog);
        //        if (strError != "")
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_11", "Tính sản lượng chi tiết - " + strError);
        //            return strError;
        //        }

        //        strError = detailDataCalculation.DetailDataCalculation_2(this.dsCalculation, this.dsCustomerData, dsStaticCatalog);
        //        if (strError != "")
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_2", "Tính sản lượng chi tiết - " + strError);
        //            return strError;
        //        }
        //        //Xu ly cho KH tinh vat qua doi gia
        //        strError = detailDataCalculation.DetailDataCalculation_21(this.dsCalculation, this.dsCustomerData, dsStaticCatalog);
        //        if (strError != "")
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_21", "Tính sản lượng chi tiết trong kỳ đổi giá - " + strError);
        //            return strError;
        //        }

        //        strError = detailDataCalculation.DetailDataCalculation_3(this.dsCalculation, this.dsCustomerData, dsStaticCatalog);
        //        if (strError != "")
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_3", "Tính sản lượng chi tiết - " + strError);
        //            return strError;
        //        }

        //        strError = detailDataCalculation.DetailDataCalculation_40(this.dsCalculation, this.dsCustomerData, dsStaticCatalog, this.dsInvoiceData, strMa_DViQLy, strMa_SoGCS, ky, thang, nam);
        //        if (strError != "")
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_4", "Tính sản lượng chi tiết - " + strError);
        //            return strError;
        //        }
        //        WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_4", "Tính xong sản lượng chi tiết");

        //        strError = detailDataCalculation.DetailDataCalculation_41(this.dsCalculation, dsStaticCatalog, dsCustomerData);
        //        if (strError != "")
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_41", "Tính sản lượng chi tiết cho bán buôn trong kỳ đổi giá - " + strError);
        //            return strError;
        //        }
        //        WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_41", "Tính sản lượng chi tiết cho bán buôn trong kỳ đổi giá");

        //        strError = detailDataCalculation.DetailDataCalculation_42(this.dsCalculation);
        //        if (strError != "")
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_42", "Xử lý riêng cho bán buôn SHBB nhóm L trong tháng 03/2011 - " + strError);
        //            return strError;
        //        }
        //        WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_42", "Xử lý riêng cho bán buôn SHBB nhóm L trong tháng 03/2011");


        //        //Xac dinh don gia
        //        //strError = priceSpecification.PriceSpecification_1(this.dsCalculation, clsBillingImplementation.dsStaticCatalog, dsCustomerData);
        //        //Xu ly cho KH tinh qua doi gia
        //        //Ko làm tròn ngang cho nhóm bậc thang
        //        strError = priceSpecification.PriceSpecification_11(this.dsCalculation, clsBillingImplementation.dsStaticCatalog, dsCustomerData);
        //        if (strError != "")
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsPriceSpecification", "PriceSpecification_1", "Xử lý riêng cho giá bậc thang - " + strError);
        //            return strError;
        //        }

        //        //Xu ly lam tron ngang cho gia bac thang
        //        strError = priceSpecification.PriceSpecification_12(this.dsCalculation, clsBillingImplementation.dsStaticCatalog, dsCustomerData);
        //        if (strError != "")
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsPriceSpecification", "PriceSpecification_12", "Xử lý riêng cho giá bậc thang trong kỳ đổi giá - " + strError);
        //            return strError;
        //        }

        //        //strError = priceSpecification.PriceSpecification_2(this.dsCalculation, clsBillingImplementation.dsStaticCatalog);
        //        //Xu ly cho KH tinh qua doi gia
        //        strError = priceSpecification.PriceSpecification_21(this.dsCalculation, clsBillingImplementation.dsStaticCatalog, this.dsCustomerData);
        //        if (strError != "")
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsPriceSpecification", "PriceSpecification_2", "Xác định đơn giá - " + strError);
        //            return strError;
        //        }

        //        strError = priceSpecification.PriceSpecification_3(this.dsCalculation, this.dsCustomerData, this.dsInvoiceData, strTen_DNhap, strNgayGhi, ky, thang, nam);
        //        if (strError != "")
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsPriceSpecification", "PriceSpecification_3", "Xác định đơn giá - " + strError);
        //            return strError;
        //        }
        //        WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsPriceSpecification", "PriceSpecification_3", "Kết thúc xác định đơn giá");

        //        //dsCustomerData.WriteXml(@"C:\CustomerData.xml");
        //        //dsStaticCatalog.WriteXml(@"C:\StaticCatalog.xml");
        //        //dsCalculation.WriteXml(@"C:\CalculationData.xml");
        //        //dsInvoiceData.WriteXml(@"C:\InvoiceData.xml");

        //        //Tinh hoa don
        //        //Kiem tra tham so G_INCHUNGRIENG = 'C' thi se ra hoa don TC
        //        if (dsStaticCatalog.Tables.Contains("S_PARAMETER") == true)
        //        {
        //            if (dsStaticCatalog.Tables["S_PARAMETER"].Select("NAME = 'G_INCHUNGRIENG' AND (PRAVALUE = 'C' OR PRAVALUE = 'c')").Length > 0)
        //            {
        //                strError = invoiceCalculation.InvoiceCalculation_TC(ref this.dsInvoiceData, this.dsCustomerData, this.dsCalculation, strMa_DViQLy, strMa_SoGCS, ky, thang, nam, dsStaticCatalog);
        //            }
        //            else
        //            {
        //                strError = invoiceCalculation.InvoiceCalculation(ref this.dsInvoiceData, this.dsCustomerData, this.dsCalculation, strMa_DViQLy, strMa_SoGCS, ky, thang, nam, dsStaticCatalog);
        //            }
        //        }
        //        else
        //        {
        //            strError = invoiceCalculation.InvoiceCalculation(ref this.dsInvoiceData, this.dsCustomerData, this.dsCalculation, strMa_DViQLy, strMa_SoGCS, ky, thang, nam, dsStaticCatalog);
        //        }
        //        if (strError != "")
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsInvoiceCalculation", "InvoiceCalculation", "Tính hoá đơn - " + strError);
        //            return strError;
        //        }
        //        else
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsInvoiceCalculation", "InvoiceCalculation", "Tính xong hoá đơn");
        //        }


        //        //Cap nhat du lieu
        //        // DũngNT sửa ngày 19-10-2009: Gán thêm 2 biến phân luồng từ UI vào phương thức InsertInvoiceData, ban đầu đang fix cứng 65,8
        //        strError = outputDataWriting.InsertInvoiceData(this.dsInvoiceData, this.dsCustomerData, strMa_DViQLy, strMa_SoGCS, strTen_DNhap, lngCurrentLibID, lngWorkflowID, ky, thang, nam);
        //        if (strError != "")
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busOutputDataWriting", "busOutputDataWriting.cls_OutputDataWriting", "InsertInvoiceData", "Cập nhật dữ liệu hoá đơn - " + strError);
        //            return strError;
        //        }
        //        else
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busOutputDataWriting", "busOutputDataWriting.cls_OutputDataWriting", "InsertInvoiceData", "Cập nhật dữ liệu hoá đơn");
        //        }

        //        //Ket thuc tinh hoa don
        //        WriteLog(strMa_DViQLy, strMa_SoGCS, "BillingImplementation", "BillingImplementation", "BillingImplementation", "Tính xong sổ " + strMa_SoGCS + " - hoá đơn kỳ " + ky + " tháng " + thang + "/" + nam);

        //        return "";
        //    }
        //    catch (Exception ex)
        //    {
        //        //Ghi log vao dataset
        //        WriteLog(strMa_DViQLy, strMa_SoGCS, "BillingImplementation", "BillingImplementation", "BillingImplementation", "Lỗi khi tính hoá đơn: sổ " + strMa_SoGCS + " - hoá đơn kỳ " + ky + " tháng " + thang + "/" + nam);
        //        return ex.Message;
        //    }
        //    finally
        //    {
        //        dsCustomerData.Reset();
        //        dsInvoiceData.Reset();
        //        dsCalculation.Reset();
        //    }
        //}

        //Ap dung cho ky doi gia, khong co lam tron ngang cho cac nhom bac thang SHBT, SHBB
        //Lay khoang chi so theo tung ngay cuoi ky
        //Lay don gia theo 2 bang gia khac nhau        
        //private string PriceChange1_Implementation(string strMa_DViQLy, string strMa_SoGCS, short ky, short thang, short nam, string strTen_DNhap, long lngCurrentLibID, long lngWorkflowID)
        //{
        //    try
        //    {
        //        dsCustomerData = new DataSet();
        //        dsInvoiceData = new DataSet();
        //        dsCalculation = new DS_CALCULATIONTABLES();

        //        string strError = "";

        //        //Bat dau tinh hoa don
        //        WriteLog(strMa_DViQLy, strMa_SoGCS, "BillingImplementation", "BillingImplementation", "BillingImplementation", "Tính hoá đơn sổ " + strMa_SoGCS + " - kỳ " + ky + " tháng " + thang + "/" + nam);
        //        //Doc du lieu dau vao
        //        strError = inputDataReading.getCustomerDataReading(ref dsCustomerData, strMa_DViQLy, strMa_SoGCS, ky, thang, nam);
        //        if (strError != "")
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busInputDataReading", "busInputDataReading.cls_InputDataReading", "getCustomerDataReading", "Đọc dữ liệu đầu vào - " + strError);
        //            return strError;
        //        }
        //        else
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busInputDataReading", "busInputDataReading.cls_InputDataReading", "getCustomerDataReading", "Đọc dữ liệu đầu vào");
        //        }

        //        //Kiem tra du lieu dau vao
        //        strError = inputDataReading.CheckValidData(strMa_DViQLy, this.dsCustomerData, clsBillingImplementation.dsStaticCatalog);
        //        if (strError != "")
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busInputDataReading", "busInputDataReading.cls_InputDataReading", "CheckValidData", "Kiểm tra dữ liệu đầu vào - " + strError);
        //            return strError;
        //        }
        //        else
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busInputDataReading", "busInputDataReading.cls_InputDataReading", "CheckValidData", "Kiểm tra dữ liệu đầu vào");
        //        }

        //        //Loc bien ban ap gia, chi lay cac bien ban co lien quan den ky ghi chi so hien tai
        //        strError = this.FilterHDG_BBAN_APGIA(ref dsCustomerData);
        //        if (strError != "")
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "BillingImplementation", "BillingImplementation.clsBillingImplementation.cs", "FilterHDG_BBAN_APGIA", "Lọc biên bản áp giá - " + strError);
        //            return strError;
        //        }
        //        else
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "BillingImplementation", "BillingImplementation.clsBillingImplementation.cs", "FilterHDG_BBAN_APGIA", "Lọc biên bản áp giá");
        //        }

        //        //Tao cau truc du lieu dau ra
        //        dsInvoiceData = cls_OutputDataWriting.createDSInvoiceData();

        //        //Tinh san luong tho
        //        strError = rawDataCalculation.RawDataCalculation_1(this.dsCustomerData);
        //        if (strError != "")
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "RawDataCalculation_1", "Tính sản lượng thô - " + strError);
        //            return strError;
        //        }
        //        else
        //        {
        //            //Kiem tra neu khong con du lieu chi so thi thong bao ket thuc tinh hoa don luon
        //            if (this.dsCustomerData.Tables.Contains("GCS_CHISO") == true)
        //            {
        //                if (this.dsCustomerData.Tables["GCS_CHISO"].Rows.Count == 0)
        //                {
        //                    //Ket thuc tinh hoa don
        //                    WriteLog(strMa_DViQLy, strMa_SoGCS, "BillingImplementation", "BillingImplementation", "BillingImplementation", "Sổ không có dữ liệu chỉ số. Kết thúc tính hoá đơn của sổ " + strMa_SoGCS + " - kỳ " + ky + " tháng " + thang + "/" + nam);
        //                    return "";
        //                }
        //            }
        //        }

        //        strError = rawDataCalculation.RawDataCalculation_2(this.dsCustomerData);
        //        if (strError != "")
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "RawDataCalculation_2", "Tính sản lượng thô - " + strError);
        //            return strError;
        //        }

        //        strError = rawDataCalculation.RawDataCalculation_3(this.dsCustomerData);
        //        if (strError != "")
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "RawDataCalculation_3", "Tính sản lượng thô - " + strError);
        //            return strError;
        //        }

        //        strError = rawDataCalculation.RawDataCalculation_4(this.dsCustomerData, dsStaticCatalog);
        //        if (strError != "")
        //        {
        //            //Rieng phan tinh tru phu chi ghi log, khong can thoat
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "RawDataCalculation_4", "Tính sản lượng thô - " + strError);
        //            return strError;
        //        }

        //        strError = rawDataCalculation.RawDataCalculation_5(this.dsCustomerData, this.dsInvoiceData, clsBillingImplementation.dsStaticCatalog, strTen_DNhap);
        //        if (strError != "")
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "RawDataCalculation_5", "Tính sản lượng thô - " + strError);
        //            return strError;
        //        }

        //        strError = rawDataCalculation.RawDataCalculation_6(this.dsCustomerData, this.dsInvoiceData, clsBillingImplementation.dsStaticCatalog, strTen_DNhap);
        //        if (strError != "")
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "RawDataCalculation_6", "Tính sản lượng thô - " + strError);
        //            return strError;
        //        }

        //        strError = rawDataCalculation.RawDataCalculation_7(this.dsCustomerData, this.dsInvoiceData, clsBillingImplementation.dsStaticCatalog, strTen_DNhap);
        //        if (strError != "")
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "RawDataCalculation_7", "Tính sản lượng thô - " + strError);
        //            return strError;
        //        }
        //        WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "RawDataCalculation_7", "Tính xong sản lượng thô");

        //        //Tinh san luong chi tiet
        //        strError = detailDataCalculation.DetailDataCalculation_12(this.dsCustomerData, ref this.dsCalculation);
        //        if (strError != "")
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_12", "Tính sản lượng chi tiết trong kỳ đổi giá - " + strError);
        //            return strError;
        //        }

        //        strError = detailDataCalculation.DetailDataCalculation_2(this.dsCalculation, this.dsCustomerData, dsStaticCatalog);
        //        if (strError != "")
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_2", "Tính sản lượng chi tiết trong kỳ đổi giá - " + strError);
        //            return strError;
        //        }

        //        strError = detailDataCalculation.DetailDataCalculation_21(this.dsCalculation, this.dsCustomerData, dsStaticCatalog);
        //        if (strError != "")
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_21", "Tính sản lượng chi tiết trong kỳ đổi giá - " + strError);
        //            return strError;
        //        }

        //        strError = detailDataCalculation.DetailDataCalculation_3(this.dsCalculation, this.dsCustomerData, dsStaticCatalog);
        //        if (strError != "")
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_3", "Tính sản lượng chi tiết trong kỳ đổi giá - " + strError);
        //            return strError;
        //        }

        //        strError = detailDataCalculation.DetailDataCalculation_40(this.dsCalculation, this.dsCustomerData, dsStaticCatalog, this.dsInvoiceData, strMa_DViQLy, strMa_SoGCS, ky, thang, nam);
        //        if (strError != "")
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_4", "Tính sản lượng chi tiết trong kỳ đổi giá - " + strError);
        //            return strError;
        //        }
        //        WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_4", "Tính xong sản lượng chi tiết trong kỳ đổi giá");

        //        strError = detailDataCalculation.DetailDataCalculation_41(this.dsCalculation, dsStaticCatalog, dsCustomerData);
        //        if (strError != "")
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_41", "Tính sản lượng chi tiết cho bán buôn trong kỳ đổi giá - " + strError);
        //            return strError;
        //        }
        //        WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_41", "Tính sản lượng chi tiết cho bán buôn trong kỳ đổi giá");

        //        strError = detailDataCalculation.DetailDataCalculation_42(this.dsCalculation);
        //        if (strError != "")
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_42", "Xử lý riêng cho bán buôn SHBB nhóm L trong tháng 03/2011 - " + strError);
        //            return strError;
        //        }
        //        WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_42", "Xử lý riêng cho bán buôn SHBB nhóm L trong tháng 03/2011");

        //        //Xac dinh don gia
        //        strError = priceSpecification.PriceSpecification_11(this.dsCalculation, clsBillingImplementation.dsStaticCatalog, dsCustomerData);
        //        if (strError != "")
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsPriceSpecification", "PriceSpecification_11", "Xác định đơn giá trong kỳ đổi giá - " + strError);
        //            return strError;
        //        }

        //        strError = priceSpecification.PriceSpecification_21(this.dsCalculation, clsBillingImplementation.dsStaticCatalog, dsCustomerData);
        //        if (strError != "")
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsPriceSpecification", "PriceSpecification_21", "Xác định đơn giá trong kỳ đổi giá - " + strError);
        //            return strError;
        //        }

        //        strError = priceSpecification.PriceSpecification_3(this.dsCalculation, this.dsCustomerData, this.dsInvoiceData, strTen_DNhap);
        //        if (strError != "")
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsPriceSpecification", "PriceSpecification_3", "Xác định đơn giá trong kỳ đổi giá - " + strError);
        //            return strError;
        //        }
        //        WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsPriceSpecification", "PriceSpecification_3", "Kết thúc xác định đơn giá trong kỳ đổi giá");

        //        //dsCustomerData.WriteXml(@"C:\CustomerData.xml");
        //        //dsStaticCatalog.WriteXml(@"C:\StaticCatalog.xml");
        //        //dsCalculation.WriteXml(@"C:\CalculationData.xml");
        //        //dsInvoiceData.WriteXml(@"C:\InvoiceData.xml");

        //        //Tinh hoa don
        //        //Kiem tra tham so G_INCHUNGRIENG = 'C' thi se ra hoa don TC
        //        if (dsStaticCatalog.Tables.Contains("S_PARAMETER") == true)
        //        {
        //            if (dsStaticCatalog.Tables["S_PARAMETER"].Select("NAME = 'G_INCHUNGRIENG' AND (PRAVALUE = 'C' OR PRAVALUE = 'c')").Length > 0)
        //            {
        //                strError = invoiceCalculation.InvoiceCalculation_TC(ref this.dsInvoiceData, this.dsCustomerData, this.dsCalculation, strMa_DViQLy, strMa_SoGCS, ky, thang, nam, dsStaticCatalog);
        //            }
        //            else
        //            {
        //                strError = invoiceCalculation.InvoiceCalculation(ref this.dsInvoiceData, this.dsCustomerData, this.dsCalculation, strMa_DViQLy, strMa_SoGCS, ky, thang, nam, dsStaticCatalog);
        //            }
        //        }
        //        else
        //        {
        //            strError = invoiceCalculation.InvoiceCalculation(ref this.dsInvoiceData, this.dsCustomerData, this.dsCalculation, strMa_DViQLy, strMa_SoGCS, ky, thang, nam, dsStaticCatalog);
        //        }
        //        if (strError != "")
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsInvoiceCalculation", "InvoiceCalculation", "Tính hoá đơn - " + strError);
        //            return strError;
        //        }
        //        else
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsInvoiceCalculation", "InvoiceCalculation", "Tính xong hoá đơn");
        //        }


        //        //Cap nhat du lieu
        //        // DũngNT sửa ngày 19-10-2009: Gán thêm 2 biến phân luồng từ UI vào phương thức InsertInvoiceData, ban đầu đang fix cứng 65,8
        //        strError = outputDataWriting.InsertInvoiceData(this.dsInvoiceData, this.dsCustomerData, strMa_DViQLy, strMa_SoGCS, strTen_DNhap, lngCurrentLibID, lngWorkflowID, ky, thang, nam);
        //        if (strError != "")
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busOutputDataWriting", "busOutputDataWriting.cls_OutputDataWriting", "InsertInvoiceData", "Cập nhật dữ liệu hoá đơn - " + strError);
        //            return strError;
        //        }
        //        else
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busOutputDataWriting", "busOutputDataWriting.cls_OutputDataWriting", "InsertInvoiceData", "Cập nhật dữ liệu hoá đơn");
        //        }

        //        //Ket thuc tinh hoa don
        //        WriteLog(strMa_DViQLy, strMa_SoGCS, "BillingImplementation", "BillingImplementation", "BillingImplementation", "Tính xong sổ " + strMa_SoGCS + " - hoá đơn kỳ " + ky + " tháng " + thang + "/" + nam);

        //        return "";
        //    }
        //    catch (Exception ex)
        //    {
        //        //Ghi log vao dataset
        //        WriteLog(strMa_DViQLy, strMa_SoGCS, "BillingImplementation", "BillingImplementation", "BillingImplementation", "Lỗi khi tính hoá đơn: sổ " + strMa_SoGCS + " - hoá đơn kỳ " + ky + " tháng " + thang + "/" + nam);
        //        return ex.Message;
        //    }
        //    finally
        //    {
        //        dsCustomerData.Reset();
        //        dsInvoiceData.Reset();
        //        dsCalculation.Reset();
        //    }
        //}
        public static void SetStaticData()
        {
            clsBillingImplementation.dsStaticCatalog = null;
        }
        public static string GetStaticData()
        {
            LOGDATA dsLogData = new LOGDATA();
            DataRow drLog;
            cls_LogManagement log;

            try
            {
                string strError = "";

                cls_InputDataReading inputStatic = new cls_InputDataReading();

                if (clsBillingImplementation.dsStaticCatalog == null)
                {
                    clsBillingImplementation.dsStaticCatalog = new DataSet();
                }

                drLog = dsLogData.Tables["S_LOG"].NewRow();
                drLog["SUBDIVISIONID"] = "*";
                drLog["LOGID"] = 1;
                drLog["BOOKID"] = "*";
                drLog["ASSEMBLYNAME"] = "busInputDataReading";
                drLog["NAMESPACE"] = "busInputDataReading.cls_InputDataReading";
                drLog["METHODNAME"] = "getStaticCatalogDataReading";
                drLog["TIME"] = System.DateTime.Now;
                drLog["DETAIL"] = "Khởi tạo dữ liệu";
                dsLogData.Tables["S_LOG"].Rows.Add(drLog);

                //Doc du lieu danh muc - Chi lay danh muc khi chua doc lan nao
                if (clsBillingImplementation.dsStaticCatalog.Tables.Count == 0)
                {
                    strError = inputStatic.getStaticCatalogDataReading(ref clsBillingImplementation.dsStaticCatalog);
                    if (strError != "")
                    {
                        drLog = dsLogData.Tables["S_LOG"].NewRow();
                        drLog["SUBDIVISIONID"] = "*";
                        drLog["LOGID"] = 1;
                        drLog["BOOKID"] = "*";
                        drLog["ASSEMBLYNAME"] = "busInputDataReading";
                        drLog["NAMESPACE"] = "busInputDataReading.cls_InputDataReading";
                        drLog["METHODNAME"] = "getStaticCatalogDataReading";
                        drLog["TIME"] = System.DateTime.Now;
                        drLog["DETAIL"] = "Đọc dữ liệu danh mục - " + strError;
                        dsLogData.Tables["S_LOG"].Rows.Add(drLog);

                        log = new cls_LogManagement(dsLogData);
                        log.InsertList();
                        return strError;
                    }
                    else
                    {
                        drLog = dsLogData.Tables["S_LOG"].NewRow();
                        drLog["SUBDIVISIONID"] = "*";
                        drLog["LOGID"] = 1;
                        drLog["BOOKID"] = "*";
                        drLog["ASSEMBLYNAME"] = "busInputDataReading";
                        drLog["NAMESPACE"] = "busInputDataReading.cls_InputDataReading";
                        drLog["METHODNAME"] = "getStaticCatalogDataReading";
                        drLog["TIME"] = System.DateTime.Now;
                        drLog["DETAIL"] = "Đọc dữ liệu danh mục";
                        dsLogData.Tables["S_LOG"].Rows.Add(drLog);
                        log = new cls_LogManagement(dsLogData);
                        log.InsertList();
                    }
                }

                return "";
            }
            catch (Exception ex)
            {
                drLog = dsLogData.Tables["S_LOG"].NewRow();
                drLog["SUBDIVISIONID"] = "*";
                drLog["LOGID"] = 1;
                drLog["BOOKID"] = "*";
                drLog["ASSEMBLYNAME"] = "busInputDataReading";
                drLog["NAMESPACE"] = "busInputDataReading.cls_InputDataReading";
                drLog["METHODNAME"] = "getStaticCatalogDataReading";
                drLog["TIME"] = System.DateTime.Now;
                drLog["DETAIL"] = "Lỗi khi khởi tạo dữ liệu danh mục: " + ex.Message;
                dsLogData.Tables["S_LOG"].Rows.Add(drLog);

                log = new cls_LogManagement(dsLogData);
                log.InsertList();

                return "Lỗi khi khởi tạo dữ liệu danh mục: " + ex.Message;
            }
        }

        private void WriteLog(string strMa_DViQLy, string strMa_SoGCS, string strAssemblyName, string strNamespace, string strMethodName, string strDetail)
        {
            DataRow drLog;
            drLog = dsLog.Tables["S_LOG"].NewRow();
            drLog["SUBDIVISIONID"] = strMa_DViQLy;
            drLog["LOGID"] = 1;
            drLog["BOOKID"] = strMa_SoGCS;
            drLog["ASSEMBLYNAME"] = strAssemblyName;
            drLog["NAMESPACE"] = strNamespace;
            drLog["METHODNAME"] = strMethodName;
            drLog["TIME"] = System.DateTime.Now;
            if (strDetail.Length > 500)
            {
                strDetail = strDetail.Substring(0, 500);
            }
            drLog["DETAIL"] = strDetail;
            dsLog.Tables["S_LOG"].Rows.Add(drLog);
            dsLog.Tables["S_LOG"].AcceptChanges();

            log = new cls_LogManagement(dsLog);
            log.InsertList();
            dsLog.Tables["S_LOG"].Rows.Clear();
        }

        //DũngNT thêm ngày 07-01-2010: hàm tính hóa đơn điều chỉnh
        //CMIS2
        //public string BillingImplementation_1(DataSet dsCustomerData, Int16 i16Thang, Int16 i16Nam)
        //{
        //    if (dsCustomerData.Tables.Contains("HDN_HDON") == true)
        //    {
        //        Int16 ky = Convert.ToInt16(dsCustomerData.Tables["HDN_HDON"].Rows[0]["KY"]);
        //        Int16 thang = i16Thang;
        //        Int16 nam = i16Nam;
        //        if (ky == 1 && thang == 3 && nam == 2011)
        //        {
        //            return this.PriceChange1_Implementation_SS(dsCustomerData, i16Thang, i16Nam);
        //        }
        //        return this.CommonImplementation_SS(dsCustomerData, i16Thang, i16Nam);
        //    }
        //    else
        //        return "Khong ton tai du lieu trong HDN_HDON";
        //}

        //private string CommonImplementation_SS(DataSet dsCustomerData, Int16 i16Thang, Int16 i16Nam)
        //{
        //    string strMa_DViQLy = "";
        //    string strMa_SoGCS = "";
        //    Int16 ky = 0;
        //    Int16 thang = 0;
        //    Int16 nam = 0;
        //    string strTen_DNhap = "";
        //    try
        //    {
        //        dsCalculation = new DS_CALCULATIONTABLES();
        //        //dsCustomerData = new DataSet();
        //        dsInvoiceData = new DataSet();

        //        GetStaticData();
        //        log = new cls_LogManagement(dsLog);

        //        strMa_DViQLy = dsCustomerData.Tables["HDN_HDON"].Rows[0]["MA_DVIQLY"].ToString();
        //        strMa_SoGCS = dsCustomerData.Tables["HDN_HDON"].Rows[0]["MA_SOGCS"].ToString();
        //        ky = Convert.ToInt16(dsCustomerData.Tables["HDN_HDON"].Rows[0]["KY"]);
        //        thang = i16Thang;
        //        nam = i16Nam;
        //        strTen_DNhap = dsCustomerData.Tables["HDN_HDON"].Rows[0]["NGUOI_TAO"].ToString();
        //        string strError = "";

        //        //Bat dau tinh hoa don
        //        WriteLog(strMa_DViQLy, strMa_SoGCS, "BillingImplementation", "BillingImplementation", "BillingImplementation", "Tính hoá đơn sổ " + strMa_SoGCS + " - kỳ " + ky + " tháng " + thang + "/" + nam);

        //        //Loc bien ban ap gia, chi lay cac bien ban co lien quan den ky ghi chi so hien tai
        //        strError = this.FilterHDG_BBAN_APGIA(ref dsCustomerData);
        //        if (strError != "")
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "BillingImplementation", "BillingImplementation.clsBillingImplementation.cs", "FilterHDG_BBAN_APGIA", "Lọc biên bản áp giá - " + strError);
        //            return strError;
        //        }
        //        else
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "BillingImplementation", "BillingImplementation.clsBillingImplementation.cs", "FilterHDG_BBAN_APGIA", "Lọc biên bản áp giá");
        //        }
        //        //Đổi loại chỉ số CPK sang DDN, sau bỏ đi
        //        strError = this.AddColumn(ref dsCustomerData, dsStaticCatalog);
        //        if (strError != "")
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "AddColumn", "Thêm cột mới - " + strError);
        //            return strError;
        //        }
        //        //Đổi loại chỉ số CPK sang DDN để check valid data ko báo lỗi dup chỉ số
        //        //strError = this.ChangeCCS(ref dsCustomerData, "CPK", "DDN", clsBillingImplementation.dsStaticCatalog);
        //        //if (strError != "")
        //        //{
        //        //    WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "AddColumn", "Tiền xử lý thông tin chỉ số - " + strError);
        //        //    return strError;
        //        //}
        //        //Kiem tra du lieu dau vao
        //        strError = inputDataReading.CheckValidData(strMa_DViQLy, dsCustomerData, clsBillingImplementation.dsStaticCatalog);
        //        if (strError != "")
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busInputDataReading", "busInputDataReading.cls_InputDataReading", "CheckValidData", "Kiểm tra dữ liệu đầu vào - " + strError);
        //            return strError;
        //        }
        //        else
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busInputDataReading", "busInputDataReading.cls_InputDataReading", "CheckValidData", "Kiểm tra dữ liệu đầu vào");
        //        }
        //        //Tao cau truc du lieu dau ra
        //        dsInvoiceData = cls_OutputDataWriting.createDSInvoiceData();

        //        //Tinh san luong tho
        //        strError = rawDataCalculation.RawDataCalculation_1(dsCustomerData);
        //        if (strError != "")
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "RawDataCalculation_1", "Tính sản lượng thô - " + strError);
        //            return strError;
        //        }

        //        strError = rawDataCalculation.RawDataCalculation_2(dsCustomerData);
        //        if (strError != "")
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "RawDataCalculation_2", "Tính sản lượng thô - " + strError);
        //            return strError;
        //        }

        //        strError = rawDataCalculation.RawDataCalculation_3(dsCustomerData);
        //        if (strError != "")
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "RawDataCalculation_3", "Tính sản lượng thô - " + strError);
        //            return strError;
        //        }
        //        //Đổi loại phản kháng về CCS, đổi CCS tạm sang DDN
        //        //strError = this.ChangeCCS(ref dsCustomerData, "CPK", "CCS", dsStaticCatalog);
        //        //if (strError != "")
        //        //{
        //        //    WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "AddColumn", "Tiền xử lý thông tin chỉ số - " + strError);
        //        //    return strError;
        //        //}
        //        strError = rawDataCalculation.RawDataCalculation_4(dsCustomerData, dsStaticCatalog);
        //        if (strError != "")
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "RawDataCalculation_4", "Tính sản lượng thô - " + strError);
        //            return strError;
        //        }
        //        //Đổi áp giá về DDN để tính cosfi
        //        //strError = this.ChangeCCS(ref dsCustomerData, "CCS", "DDN", dsStaticCatalog);
        //        //if (strError != "")
        //        //{
        //        //    WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "AddColumn", "Tiền xử lý thông tin chỉ số - " + strError);
        //        //    return strError;
        //        //}
        //        strError = rawDataCalculation.RawDataCalculation_5(dsCustomerData, this.dsInvoiceData, clsBillingImplementation.dsStaticCatalog, strTen_DNhap);
        //        if (strError != "")
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "RawDataCalculation_5", "Tính sản lượng thô - " + strError);
        //            return strError;
        //        }

        //        strError = rawDataCalculation.RawDataCalculation_6(dsCustomerData, this.dsInvoiceData, clsBillingImplementation.dsStaticCatalog, strTen_DNhap);
        //        if (strError != "")
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "RawDataCalculation_6", "Tính sản lượng thô - " + strError);
        //            return strError;
        //        }

        //        strError = rawDataCalculation.RawDataCalculation_7(dsCustomerData, this.dsInvoiceData, clsBillingImplementation.dsStaticCatalog, strTen_DNhap);
        //        if (strError != "")
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "RawDataCalculation_7", "Tính sản lượng thô - " + strError);
        //            return strError;
        //        }
        //        WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "RawDataCalculation_7", "Tính xong sản lượng thô");
        //        //đã tính xong cosfi, đổi CCS trả về CCS, CPK về DDN
        //        //strError = this.ChangeCCS(ref dsCustomerData, "CPK", "DDN", dsStaticCatalog);
        //        //if (strError != "")
        //        //{
        //        //    WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "AddColumn", "Tiền xử lý thông tin chỉ số - " + strError);
        //        //    return strError;
        //        //}
        //        //strError = this.ChangeCCS(ref dsCustomerData, "CCS", "CCS", dsStaticCatalog);
        //        //if (strError != "")
        //        //{
        //        //    WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "AddColumn", "Tiền xử lý thông tin chỉ số - " + strError);
        //        //    return strError;
        //        //}
        //        //Tinh san luong chi tiet

        //        strError = detailDataCalculation.DetailDataCalculation_11(dsCustomerData, ref this.dsCalculation, dsStaticCatalog);
        //        if (strError != "")
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_11", "Tính sản lượng chi tiết - " + strError);
        //            return strError;
        //        }

        //        strError = detailDataCalculation.DetailDataCalculation_2(this.dsCalculation, dsCustomerData, dsStaticCatalog);
        //        if (strError != "")
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_2", "Tính sản lượng chi tiết - " + strError);
        //            return strError;
        //        }
        //        //Xu ly cho KH tinh vat qua doi gia
        //        strError = detailDataCalculation.DetailDataCalculation_21(this.dsCalculation, dsCustomerData, dsStaticCatalog);
        //        if (strError != "")
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_21", "Tính sản lượng chi tiết trong kỳ đổi giá - " + strError);
        //            return strError;
        //        }
        //        strError = detailDataCalculation.DetailDataCalculation_3(this.dsCalculation, dsCustomerData, dsStaticCatalog);
        //        if (strError != "")
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_3", "Tính sản lượng chi tiết - " + strError);
        //            return strError;
        //        }
        //        WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_3", "Tính xong sản lượng chi tiết");

        //        strError = detailDataCalculation.DetailDataCalculation_40(this.dsCalculation, dsCustomerData, dsStaticCatalog, dsInvoiceData, strMa_DViQLy, strMa_SoGCS, ky, thang, nam);
        //        if (strError != "")
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_4", "Tính sản lượng chi tiết - " + strError);
        //            return strError;
        //        }
        //        WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_4", "Tính xong sản lượng chi tiết");

        //        strError = detailDataCalculation.DetailDataCalculation_41(this.dsCalculation, dsStaticCatalog, dsCustomerData);
        //        if (strError != "")
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_41", "Tính sản lượng chi tiết cho bán buôn trong kỳ đổi giá - " + strError);
        //            return strError;
        //        }
        //        WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_41", "Tính sản lượng chi tiết cho bán buôn trong kỳ đổi giá");

        //        strError = detailDataCalculation.DetailDataCalculation_42(this.dsCalculation);
        //        if (strError != "")
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_42", "Xử lý riêng cho bán buôn SHBB nhóm L trong tháng 03/2011 - " + strError);
        //            return strError;
        //        }
        //        WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_42", "Xử lý riêng cho bán buôn SHBB nhóm L trong tháng 03/2011");


        //        //Xac dinh don gia
        //        //strError = priceSpecification.PriceSpecification_1(this.dsCalculation, clsBillingImplementation.dsStaticCatalog, dsCustomerData);
        //        //if (strError != "")
        //        //{
        //        //    WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsPriceSpecification", "PriceSpecification_1", "Xác định đơn giá - " + strError);
        //        //    return strError;
        //        //}

        //        //strError = priceSpecification.PriceSpecification_2(this.dsCalculation, clsBillingImplementation.dsStaticCatalog);
        //        //if (strError != "")
        //        //{
        //        //    WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsPriceSpecification", "PriceSpecification_2", "Xác định đơn giá - " + strError);
        //        //    return strError;
        //        //}
        //        //Xu ly cho KH tinh qua doi gia
        //        strError = priceSpecification.PriceSpecification_11(this.dsCalculation, clsBillingImplementation.dsStaticCatalog, dsCustomerData);
        //        if (strError != "")
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsPriceSpecification", "PriceSpecification_1", "Xác định đơn giá - " + strError);
        //            return strError;
        //        }

        //        //Xu ly lam tron ngang cho gia bac thang
        //        strError = priceSpecification.PriceSpecification_12(this.dsCalculation, clsBillingImplementation.dsStaticCatalog, dsCustomerData);
        //        if (strError != "")
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsPriceSpecification", "PriceSpecification_12", "Xử lý riêng cho giá bậc thang trong kỳ đổi giá - " + strError);
        //            return strError;
        //        }

        //        //strError = priceSpecification.PriceSpecification_2(this.dsCalculation, clsBillingImplementation.dsStaticCatalog);
        //        //Xu ly cho KH tinh qua doi gia
        //        strError = priceSpecification.PriceSpecification_21(this.dsCalculation, clsBillingImplementation.dsStaticCatalog, dsCustomerData);
        //        if (strError != "")
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsPriceSpecification", "PriceSpecification_2", "Xác định đơn giá - " + strError);
        //            return strError;
        //        }
        //        strError = priceSpecification.PriceSpecification_3(this.dsCalculation, dsCustomerData, this.dsInvoiceData, strTen_DNhap);
        //        if (strError != "")
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsPriceSpecification", "PriceSpecification_3", "Xác định đơn giá - " + strError);
        //            return strError;
        //        }
        //        WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsPriceSpecification", "PriceSpecification_3", "Kết thúc xác định đơn giá");
        //        //dsCustomerData.WriteXml(@"C:\CustomerData.xml");
        //        //dsStaticCatalog.WriteXml(@"C:\StaticCatalog.xml");
        //        //dsCalculation.WriteXml(@"C:\CalculationData.xml");
        //        //dsInvoiceData.WriteXml(@"C:\InvoiceData.xml");

        //        //Tinh hoa don
        //        //Kiem tra tham so G_INCHUNGRIENG = 'C' thi se ra hoa don TC
        //        if (dsStaticCatalog.Tables.Contains("S_PARAMETER") == true)
        //        {
        //            if (dsStaticCatalog.Tables["S_PARAMETER"].Select("NAME = 'G_INCHUNGRIENG' AND (PRAVALUE = 'C' OR PRAVALUE = 'c')").Length > 0)
        //            {
        //                strError = invoiceCalculation.InvoiceCalculation_TC(ref this.dsInvoiceData, dsCustomerData, this.dsCalculation, strMa_DViQLy, strMa_SoGCS, ky, thang, nam, dsStaticCatalog);
        //            }
        //            else
        //            {
        //                strError = invoiceCalculation.InvoiceCalculation(ref this.dsInvoiceData, dsCustomerData, this.dsCalculation, strMa_DViQLy, strMa_SoGCS, ky, thang, nam, dsStaticCatalog);
        //            }
        //        }
        //        else
        //        {
        //            strError = invoiceCalculation.InvoiceCalculation(ref this.dsInvoiceData, dsCustomerData, this.dsCalculation, strMa_DViQLy, strMa_SoGCS, ky, thang, nam, dsStaticCatalog);
        //        }

        //        if (strError != "")
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsInvoiceCalculation", "InvoiceCalculation", "Tính hoá đơn - " + strError);
        //            return strError;
        //        }
        //        else
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsInvoiceCalculation", "InvoiceCalculation", "Tính xong hoá đơn");
        //        }

        //        //Cap nhat du lieu
        //        strError = outputDataWriting.InsertInvoiceData_1(this.dsInvoiceData, dsCustomerData);
        //        if (strError != "")
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busOutputDataWriting", "busOutputDataWriting.cls_OutputDataWriting", "InsertInvoiceData", "Cập nhật dữ liệu hoá đơn - " + strError);
        //            return strError;
        //        }
        //        else
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busOutputDataWriting", "busOutputDataWriting.cls_OutputDataWriting", "InsertInvoiceData", "Cập nhật dữ liệu hoá đơn");
        //        }

        //        //Ket thuc tinh hoa don
        //        WriteLog(strMa_DViQLy, strMa_SoGCS, "BillingImplementation", "BillingImplementation", "BillingImplementation", "Tính xong sổ " + strMa_SoGCS + " - hoá đơn kỳ " + ky + " tháng " + thang + "/" + nam);

        //        return "";
        //    }
        //    catch (Exception ex)
        //    {
        //        //Ghi log vao dataset
        //        WriteLog(strMa_DViQLy, strMa_SoGCS, "BillingImplementation", "BillingImplementation", "BillingImplementation", "Lỗi khi tính hoá đơn: sổ " + strMa_SoGCS + " - hoá đơn kỳ " + ky + " tháng " + thang + "/" + nam);
        //        return ex.Message;
        //    }
        //    finally
        //    {
        //        dsCustomerData.Reset();
        //        dsInvoiceData.Reset();
        //        dsCalculation.Reset();
        //    }
        //}

        //public string BillingImplementation_TTNT(DataSet dsCustomerData, Int16 i16Thang, Int16 i16Nam, ref DataSet dsDanhMuc, ref DataSet dsResult)
        //{
        //    return CommonImplementation_TTNT(dsCustomerData, i16Thang, i16Nam, ref dsDanhMuc, ref dsResult);
        //}
        //private string CommonImplementation_TTNT(DataSet dsCustomerData, Int16 i16Thang, Int16 i16Nam, ref DataSet dsDanhMuc, ref DataSet dsResult)
        //{
        //    string strMa_DViQLy = "";
        //    string strMa_SoGCS = "";
        //    Int16 ky = 0;
        //    Int16 thang = 0;
        //    Int16 nam = 0;
        //    string strTen_DNhap = "";
        //    try
        //    {
        //        dsCalculation = new DS_CALCULATIONTABLES();
        //        //dsCustomerData = new DataSet();
        //        dsInvoiceData = new DataSet();
        //        //Xử lý tránh lấy danh mục nhiều lần trong khi tính hóa đơn
        //        if (dsDanhMuc == null || dsDanhMuc.Tables.Count <= 0 || dsDanhMuc.Tables[0].Rows.Count <= 0)
        //        {
        //            GetStaticData();
        //            dsDanhMuc = new DataSet();
        //            dsDanhMuc = clsBillingImplementation.dsStaticCatalog.Copy();
        //        }
        //        else
        //            clsBillingImplementation.dsStaticCatalog = dsDanhMuc;
        //        log = new cls_LogManagement(dsLog);

        //        strMa_DViQLy = dsCustomerData.Tables["HDN_HDON"].Rows[0]["MA_DVIQLY"].ToString();
        //        strMa_SoGCS = dsCustomerData.Tables["HDN_HDON"].Rows[0]["MA_SOGCS"].ToString();
        //        ky = Convert.ToInt16(dsCustomerData.Tables["HDN_HDON"].Rows[0]["KY"]);
        //        thang = i16Thang;
        //        nam = i16Nam;
        //        strTen_DNhap = dsCustomerData.Tables["HDN_HDON"].Rows[0]["NGUOI_TAO"].ToString();
        //        string strError = "";

        //        //Bat dau tinh hoa don
        //        WriteLog(strMa_DViQLy, strMa_SoGCS, "BillingImplementation", "BillingImplementation", "BillingImplementation", "Tính hoá đơn sổ " + strMa_SoGCS + " - kỳ " + ky + " tháng " + thang + "/" + nam);

        //        //Loc bien ban ap gia, chi lay cac bien ban co lien quan den ky ghi chi so hien tai
        //        strError = this.FilterHDG_BBAN_APGIA(ref dsCustomerData);
        //        if (strError != "")
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "BillingImplementation", "BillingImplementation.clsBillingImplementation.cs", "FilterHDG_BBAN_APGIA", "Lọc biên bản áp giá - " + strError);
        //            return strError;
        //        }
        //        else
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "BillingImplementation", "BillingImplementation.clsBillingImplementation.cs", "FilterHDG_BBAN_APGIA", "Lọc biên bản áp giá");
        //        }
        //        //Đổi loại chỉ số CPK sang DDN, sau bỏ đi
        //        strError = this.AddColumn(ref dsCustomerData, dsStaticCatalog);
        //        if (strError != "")
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "AddColumn", "Thêm cột mới - " + strError);
        //            return strError;
        //        }
        //        ////Đổi loại chỉ số CPK sang DDN để check valid data ko báo lỗi dup chỉ số
        //        //strError = this.ChangeCCS(ref dsCustomerData, "CPK", "DDN", clsBillingImplementation.dsStaticCatalog);
        //        //if (strError != "")
        //        //{
        //        //    WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "AddColumn", "Tiền xử lý thông tin chỉ số - " + strError);
        //        //    return strError;
        //        //}
        //        //Kiem tra du lieu dau vao
        //        strError = inputDataReading.CheckValidData(strMa_DViQLy, dsCustomerData, clsBillingImplementation.dsStaticCatalog);
        //        if (strError != "")
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busInputDataReading", "busInputDataReading.cls_InputDataReading", "CheckValidData", "Kiểm tra dữ liệu đầu vào - " + strError);
        //            return strError;
        //        }
        //        else
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busInputDataReading", "busInputDataReading.cls_InputDataReading", "CheckValidData", "Kiểm tra dữ liệu đầu vào");
        //        }
        //        //Tao cau truc du lieu dau ra
        //        dsInvoiceData = cls_OutputDataWriting.createDSInvoiceData();

        //        //Tinh san luong tho
        //        strError = rawDataCalculation.RawDataCalculation_1(dsCustomerData);
        //        if (strError != "")
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "RawDataCalculation_1", "Tính sản lượng thô - " + strError);
        //            return strError;
        //        }

        //        strError = rawDataCalculation.RawDataCalculation_2(dsCustomerData);
        //        if (strError != "")
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "RawDataCalculation_2", "Tính sản lượng thô - " + strError);
        //            return strError;
        //        }

        //        strError = rawDataCalculation.RawDataCalculation_3(dsCustomerData);
        //        if (strError != "")
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "RawDataCalculation_3", "Tính sản lượng thô - " + strError);
        //            return strError;
        //        }
        //        //Đổi loại phản kháng về CCS, đổi CCS tạm sang DDN
        //        //strError = this.ChangeCCS(ref dsCustomerData, "CPK", "CCS", dsStaticCatalog);
        //        //if (strError != "")
        //        //{
        //        //    WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "AddColumn", "Tiền xử lý thông tin chỉ số - " + strError);
        //        //    return strError;
        //        //}
        //        strError = rawDataCalculation.RawDataCalculation_4(dsCustomerData, dsStaticCatalog);
        //        if (strError != "")
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "RawDataCalculation_4", "Tính sản lượng thô - " + strError);
        //            return strError;
        //        }
        //        //Đổi áp giá về DDN để tính cosfi
        //        //strError = this.ChangeCCS(ref dsCustomerData, "CCS", "DDN", dsStaticCatalog);
        //        //if (strError != "")
        //        //{
        //        //    WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "AddColumn", "Tiền xử lý thông tin chỉ số - " + strError);
        //        //    return strError;
        //        //}
        //        strError = rawDataCalculation.RawDataCalculation_5(dsCustomerData, this.dsInvoiceData, clsBillingImplementation.dsStaticCatalog, strTen_DNhap);
        //        if (strError != "")
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "RawDataCalculation_5", "Tính sản lượng thô - " + strError);
        //            return strError;
        //        }

        //        strError = rawDataCalculation.RawDataCalculation_6(dsCustomerData, this.dsInvoiceData, clsBillingImplementation.dsStaticCatalog, strTen_DNhap);
        //        if (strError != "")
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "RawDataCalculation_6", "Tính sản lượng thô - " + strError);
        //            return strError;
        //        }

        //        strError = rawDataCalculation.RawDataCalculation_7(dsCustomerData, this.dsInvoiceData, clsBillingImplementation.dsStaticCatalog, strTen_DNhap);
        //        if (strError != "")
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "RawDataCalculation_7", "Tính sản lượng thô - " + strError);
        //            return strError;
        //        }
        //        WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "RawDataCalculation_7", "Tính xong sản lượng thô");
        //        //đã tính xong cosfi, đổi CCS trả về CCS, CPK về DDN
        //        //strError = this.ChangeCCS(ref dsCustomerData, "CPK", "DDN", dsStaticCatalog);
        //        //if (strError != "")
        //        //{
        //        //    WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "AddColumn", "Tiền xử lý thông tin chỉ số - " + strError);
        //        //    return strError;
        //        //}
        //        //strError = this.ChangeCCS(ref dsCustomerData, "CCS", "CCS", dsStaticCatalog);
        //        //if (strError != "")
        //        //{
        //        //    WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "AddColumn", "Tiền xử lý thông tin chỉ số - " + strError);
        //        //    return strError;
        //        //}
        //        //Tinh san luong chi tiet

        //        strError = detailDataCalculation.DetailDataCalculation_11(dsCustomerData, ref this.dsCalculation, dsStaticCatalog);
        //        if (strError != "")
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_11", "Tính sản lượng chi tiết - " + strError);
        //            return strError;
        //        }

        //        strError = detailDataCalculation.DetailDataCalculation_2(this.dsCalculation, dsCustomerData, dsStaticCatalog);
        //        if (strError != "")
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_2", "Tính sản lượng chi tiết - " + strError);
        //            return strError;
        //        }
        //        //Xu ly cho KH tinh vat qua doi gia
        //        strError = detailDataCalculation.DetailDataCalculation_21(this.dsCalculation, dsCustomerData, dsStaticCatalog);
        //        if (strError != "")
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_21", "Tính sản lượng chi tiết trong kỳ đổi giá - " + strError);
        //            return strError;
        //        }
        //        strError = detailDataCalculation.DetailDataCalculation_3(this.dsCalculation, dsCustomerData, dsStaticCatalog);
        //        if (strError != "")
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_3", "Tính sản lượng chi tiết - " + strError);
        //            return strError;
        //        }
        //        WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_3", "Tính xong sản lượng chi tiết");

        //        strError = detailDataCalculation.DetailDataCalculation_40(this.dsCalculation, dsCustomerData, dsStaticCatalog, dsInvoiceData, strMa_DViQLy, strMa_SoGCS, ky, thang, nam);
        //        if (strError != "")
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_4", "Tính sản lượng chi tiết - " + strError);
        //            return strError;
        //        }
        //        WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_4", "Tính xong sản lượng chi tiết");

        //        strError = detailDataCalculation.DetailDataCalculation_41(this.dsCalculation, dsStaticCatalog, dsCustomerData);
        //        if (strError != "")
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_41", "Tính sản lượng chi tiết cho bán buôn trong kỳ đổi giá - " + strError);
        //            return strError;
        //        }
        //        WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_41", "Tính sản lượng chi tiết cho bán buôn trong kỳ đổi giá");

        //        strError = detailDataCalculation.DetailDataCalculation_42(this.dsCalculation);
        //        if (strError != "")
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_42", "Xử lý riêng cho bán buôn SHBB nhóm L trong tháng 03/2011 - " + strError);
        //            return strError;
        //        }
        //        WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_42", "Xử lý riêng cho bán buôn SHBB nhóm L trong tháng 03/2011");


        //        //Xac dinh don gia
        //        //strError = priceSpecification.PriceSpecification_1(this.dsCalculation, clsBillingImplementation.dsStaticCatalog, dsCustomerData);
        //        //if (strError != "")
        //        //{
        //        //    WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsPriceSpecification", "PriceSpecification_1", "Xác định đơn giá - " + strError);
        //        //    return strError;
        //        //}

        //        //strError = priceSpecification.PriceSpecification_2(this.dsCalculation, clsBillingImplementation.dsStaticCatalog);
        //        //if (strError != "")
        //        //{
        //        //    WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsPriceSpecification", "PriceSpecification_2", "Xác định đơn giá - " + strError);
        //        //    return strError;
        //        //}
        //        //Xu ly cho KH tinh qua doi gia
        //        strError = priceSpecification.PriceSpecification_11(this.dsCalculation, clsBillingImplementation.dsStaticCatalog, dsCustomerData);
        //        if (strError != "")
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsPriceSpecification", "PriceSpecification_1", "Xác định đơn giá - " + strError);
        //            return strError;
        //        }

        //        //Xu ly lam tron ngang cho gia bac thang
        //        strError = priceSpecification.PriceSpecification_12(this.dsCalculation, clsBillingImplementation.dsStaticCatalog, dsCustomerData);
        //        if (strError != "")
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsPriceSpecification", "PriceSpecification_12", "Xử lý riêng cho giá bậc thang trong kỳ đổi giá - " + strError);
        //            return strError;
        //        }

        //        //strError = priceSpecification.PriceSpecification_2(this.dsCalculation, clsBillingImplementation.dsStaticCatalog);
        //        //Xu ly cho KH tinh qua doi gia
        //        strError = priceSpecification.PriceSpecification_21(this.dsCalculation, clsBillingImplementation.dsStaticCatalog, dsCustomerData);
        //        if (strError != "")
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsPriceSpecification", "PriceSpecification_2", "Xác định đơn giá - " + strError);
        //            return strError;
        //        }
        //        strError = priceSpecification.PriceSpecification_3(this.dsCalculation, dsCustomerData, this.dsInvoiceData, strTen_DNhap);
        //        if (strError != "")
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsPriceSpecification", "PriceSpecification_3", "Xác định đơn giá - " + strError);
        //            return strError;
        //        }
        //        WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsPriceSpecification", "PriceSpecification_3", "Kết thúc xác định đơn giá");
        //        //dsCustomerData.WriteXml(@"C:\CustomerData.xml");
        //        //dsStaticCatalog.WriteXml(@"C:\StaticCatalog.xml");
        //        //dsCalculation.WriteXml(@"C:\CalculationData.xml");
        //        //dsInvoiceData.WriteXml(@"C:\InvoiceData.xml");

        //        //Tinh hoa don
        //        //Kiem tra tham so G_INCHUNGRIENG = 'C' thi se ra hoa don TC
        //        if (dsStaticCatalog.Tables.Contains("S_PARAMETER") == true)
        //        {
        //            if (dsStaticCatalog.Tables["S_PARAMETER"].Select("NAME = 'G_INCHUNGRIENG' AND (PRAVALUE = 'C' OR PRAVALUE = 'c')").Length > 0)
        //            {
        //                strError = invoiceCalculation.InvoiceCalculation_TC(ref this.dsInvoiceData, dsCustomerData, this.dsCalculation, strMa_DViQLy, strMa_SoGCS, ky, thang, nam, dsStaticCatalog);
        //            }
        //            else
        //            {
        //                strError = invoiceCalculation.InvoiceCalculation(ref this.dsInvoiceData, dsCustomerData, this.dsCalculation, strMa_DViQLy, strMa_SoGCS, ky, thang, nam, dsStaticCatalog);
        //            }
        //        }
        //        else
        //        {
        //            strError = invoiceCalculation.InvoiceCalculation(ref this.dsInvoiceData, dsCustomerData, this.dsCalculation, strMa_DViQLy, strMa_SoGCS, ky, thang, nam, dsStaticCatalog);
        //        }

        //        if (strError != "")
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsInvoiceCalculation", "InvoiceCalculation", "Tính hoá đơn - " + strError);
        //            return strError;
        //        }
        //        else
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsInvoiceCalculation", "InvoiceCalculation", "Tính xong hoá đơn");
        //        }
        //        //Không cập nhật dữ liệu, trả về luôn
        //        dsResult = new DataSet();
        //        dsResult = this.dsInvoiceData.Copy();
        //        //Cap nhat du lieu
        //        //strError = outputDataWriting.InsertInvoiceData_1(this.dsInvoiceData, dsCustomerData);
        //        //if (strError != "")
        //        //{
        //        //    WriteLog(strMa_DViQLy, strMa_SoGCS, "busOutputDataWriting", "busOutputDataWriting.cls_OutputDataWriting", "InsertInvoiceData", "Cập nhật dữ liệu hoá đơn - " + strError);
        //        //    return strError;
        //        //}
        //        //else
        //        //{
        //        //    WriteLog(strMa_DViQLy, strMa_SoGCS, "busOutputDataWriting", "busOutputDataWriting.cls_OutputDataWriting", "InsertInvoiceData", "Cập nhật dữ liệu hoá đơn");
        //        //}

        //        ////Ket thuc tinh hoa don
        //        //WriteLog(strMa_DViQLy, strMa_SoGCS, "BillingImplementation", "BillingImplementation", "BillingImplementation", "Tính xong sổ " + strMa_SoGCS + " - hoá đơn kỳ " + ky + " tháng " + thang + "/" + nam);

        //        return "";
        //    }
        //    catch (Exception ex)
        //    {
        //        //Ghi log vao dataset
        //        WriteLog(strMa_DViQLy, strMa_SoGCS, "BillingImplementation", "BillingImplementation", "BillingImplementation", "Lỗi khi tính hoá đơn: sổ " + strMa_SoGCS + " - hoá đơn kỳ " + ky + " tháng " + thang + "/" + nam);
        //        return ex.Message;
        //    }
        //    finally
        //    {
        //        dsCustomerData.Reset();
        //        dsInvoiceData.Reset();
        //        dsCalculation.Reset();
        //    }
        //}

        //private string PriceChange1_Implementation_SS(DataSet dsCustomerData, Int16 i16Thang, Int16 i16Nam)
        //{
        //    string strMa_DViQLy = "";
        //    string strMa_SoGCS = "";
        //    Int16 ky = 0;
        //    Int16 thang = 0;
        //    Int16 nam = 0;
        //    string strTen_DNhap = "";
        //    try
        //    {
        //        dsCalculation = new DS_CALCULATIONTABLES();
        //        //dsCustomerData = new DataSet();
        //        dsInvoiceData = new DataSet();

        //        GetStaticData();
        //        log = new cls_LogManagement(dsLog);

        //        strMa_DViQLy = dsCustomerData.Tables["HDN_HDON"].Rows[0]["MA_DVIQLY"].ToString();
        //        strMa_SoGCS = dsCustomerData.Tables["HDN_HDON"].Rows[0]["MA_SOGCS"].ToString();
        //        ky = Convert.ToInt16(dsCustomerData.Tables["HDN_HDON"].Rows[0]["KY"]);
        //        thang = i16Thang;
        //        nam = i16Nam;
        //        strTen_DNhap = dsCustomerData.Tables["HDN_HDON"].Rows[0]["NGUOI_TAO"].ToString();
        //        string strError = "";

        //        //Bat dau tinh hoa don
        //        WriteLog(strMa_DViQLy, strMa_SoGCS, "BillingImplementation", "BillingImplementation", "BillingImplementation", "Tính hoá đơn sổ " + strMa_SoGCS + " - kỳ " + ky + " tháng " + thang + "/" + nam);

        //        //Tao cau truc du lieu dau ra
        //        dsInvoiceData = cls_OutputDataWriting.createDSInvoiceData();

        //        //Tinh san luong tho
        //        strError = rawDataCalculation.RawDataCalculation_1(dsCustomerData);
        //        if (strError != "")
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "RawDataCalculation_1", "Tính sản lượng thô - " + strError);
        //            return strError;
        //        }

        //        strError = rawDataCalculation.RawDataCalculation_2(dsCustomerData);
        //        if (strError != "")
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "RawDataCalculation_2", "Tính sản lượng thô - " + strError);
        //            return strError;
        //        }

        //        strError = rawDataCalculation.RawDataCalculation_3(dsCustomerData);
        //        if (strError != "")
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "RawDataCalculation_3", "Tính sản lượng thô - " + strError);
        //            return strError;
        //        }

        //        strError = rawDataCalculation.RawDataCalculation_4(dsCustomerData, dsStaticCatalog);
        //        if (strError != "")
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "RawDataCalculation_4", "Tính sản lượng thô - " + strError);
        //            return strError;
        //        }

        //        strError = rawDataCalculation.RawDataCalculation_5(dsCustomerData, this.dsInvoiceData, clsBillingImplementation.dsStaticCatalog, strTen_DNhap);
        //        if (strError != "")
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "RawDataCalculation_5", "Tính sản lượng thô - " + strError);
        //            return strError;
        //        }

        //        strError = rawDataCalculation.RawDataCalculation_6(dsCustomerData, this.dsInvoiceData, clsBillingImplementation.dsStaticCatalog, strTen_DNhap);
        //        if (strError != "")
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "RawDataCalculation_6", "Tính sản lượng thô - " + strError);
        //            return strError;
        //        }

        //        strError = rawDataCalculation.RawDataCalculation_7(dsCustomerData, this.dsInvoiceData, clsBillingImplementation.dsStaticCatalog, strTen_DNhap);
        //        if (strError != "")
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "RawDataCalculation_7", "Tính sản lượng thô - " + strError);
        //            return strError;
        //        }
        //        WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "RawDataCalculation_7", "Tính xong sản lượng thô");
        //        //Tinh san luong chi tiet
        //        strError = detailDataCalculation.DetailDataCalculation_12(dsCustomerData, ref this.dsCalculation);
        //        if (strError != "")
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_12", "Tính sản lượng chi tiết trong kỳ đổi giá - " + strError);
        //            return strError;
        //        }

        //        strError = detailDataCalculation.DetailDataCalculation_2(this.dsCalculation, dsCustomerData, dsStaticCatalog);
        //        if (strError != "")
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_2", "Tính sản lượng chi tiết trong kỳ đổi giá - " + strError);
        //            return strError;
        //        }

        //        strError = detailDataCalculation.DetailDataCalculation_21(this.dsCalculation, dsCustomerData, dsStaticCatalog);
        //        if (strError != "")
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_21", "Tính sản lượng chi tiết trong kỳ đổi giá - " + strError);
        //            return strError;
        //        }

        //        strError = detailDataCalculation.DetailDataCalculation_3(this.dsCalculation, dsCustomerData, dsStaticCatalog);
        //        if (strError != "")
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_3", "Tính sản lượng chi tiết trong kỳ đổi giá - " + strError);
        //            return strError;
        //        }
        //        WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_3", "Tính xong sản lượng chi tiết trong kỳ đổi giá");

        //        strError = detailDataCalculation.DetailDataCalculation_40(this.dsCalculation, this.dsCustomerData, dsStaticCatalog, dsInvoiceData, strMa_DViQLy, strMa_SoGCS, ky, thang, nam);
        //        if (strError != "")
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_4", "Tính sản lượng chi tiết trong kỳ đổi giá - " + strError);
        //            return strError;
        //        }
        //        WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_4", "Tính xong sản lượng chi tiết trong kỳ đổi giá");

        //        strError = detailDataCalculation.DetailDataCalculation_41(this.dsCalculation, dsStaticCatalog, dsCustomerData);
        //        if (strError != "")
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_41", "Tính sản lượng chi tiết cho bán buôn trong kỳ đổi giá - " + strError);
        //            return strError;
        //        }
        //        WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_41", "Tính sản lượng chi tiết cho bán buôn trong kỳ đổi giá");

        //        strError = detailDataCalculation.DetailDataCalculation_42(this.dsCalculation);
        //        if (strError != "")
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_42", "Xử lý riêng cho bán buôn SHBB nhóm L trong tháng 03/2011 - " + strError);
        //            return strError;
        //        }
        //        WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_42", "Xử lý riêng cho bán buôn SHBB nhóm L trong tháng 03/2011");


        //        //Xac dinh don gia
        //        strError = priceSpecification.PriceSpecification_11(this.dsCalculation, clsBillingImplementation.dsStaticCatalog, dsCustomerData);
        //        if (strError != "")
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsPriceSpecification", "PriceSpecification_1", "Xác định đơn giá - " + strError);
        //            return strError;
        //        }

        //        //Xu ly lam tron ngang cho gia bac thang
        //        strError = priceSpecification.PriceSpecification_12(this.dsCalculation, clsBillingImplementation.dsStaticCatalog, dsCustomerData);
        //        if (strError != "")
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsPriceSpecification", "PriceSpecification_12", "Xử lý riêng cho giá bậc thang trong kỳ đổi giá - " + strError);
        //            return strError;
        //        }

        //        strError = priceSpecification.PriceSpecification_21(this.dsCalculation, clsBillingImplementation.dsStaticCatalog, dsCustomerData);
        //        if (strError != "")
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsPriceSpecification", "PriceSpecification_2", "Xác định đơn giá - " + strError);
        //            return strError;
        //        }

        //        strError = priceSpecification.PriceSpecification_3(this.dsCalculation, dsCustomerData, this.dsInvoiceData, strTen_DNhap);
        //        if (strError != "")
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsPriceSpecification", "PriceSpecification_3", "Xác định đơn giá - " + strError);
        //            return strError;
        //        }
        //        WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsPriceSpecification", "PriceSpecification_3", "Kết thúc xác định đơn giá");
        //        //dsCustomerData.WriteXml(@"C:\CustomerData.xml");
        //        //dsStaticCatalog.WriteXml(@"C:\StaticCatalog.xml");
        //        //dsCalculation.WriteXml(@"C:\CalculationData.xml");
        //        //dsInvoiceData.WriteXml(@"C:\InvoiceData.xml");

        //        //Tinh hoa don
        //        //Kiem tra tham so G_INCHUNGRIENG = 'C' thi se ra hoa don TC
        //        if (dsStaticCatalog.Tables.Contains("S_PARAMETER") == true)
        //        {
        //            if (dsStaticCatalog.Tables["S_PARAMETER"].Select("NAME = 'G_INCHUNGRIENG' AND (PRAVALUE = 'C' OR PRAVALUE = 'c')").Length > 0)
        //            {
        //                strError = invoiceCalculation.InvoiceCalculation_TC(ref this.dsInvoiceData, dsCustomerData, this.dsCalculation, strMa_DViQLy, strMa_SoGCS, ky, thang, nam, dsStaticCatalog);
        //            }
        //            else
        //            {
        //                strError = invoiceCalculation.InvoiceCalculation(ref this.dsInvoiceData, dsCustomerData, this.dsCalculation, strMa_DViQLy, strMa_SoGCS, ky, thang, nam, dsStaticCatalog);
        //            }
        //        }
        //        else
        //        {
        //            strError = invoiceCalculation.InvoiceCalculation(ref this.dsInvoiceData, dsCustomerData, this.dsCalculation, strMa_DViQLy, strMa_SoGCS, ky, thang, nam, dsStaticCatalog);
        //        }

        //        if (strError != "")
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsInvoiceCalculation", "InvoiceCalculation", "Tính hoá đơn - " + strError);
        //            return strError;
        //        }
        //        else
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsInvoiceCalculation", "InvoiceCalculation", "Tính xong hoá đơn");
        //        }

        //        //Cap nhat du lieu
        //        strError = outputDataWriting.InsertInvoiceData_1(this.dsInvoiceData, dsCustomerData);
        //        if (strError != "")
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busOutputDataWriting", "busOutputDataWriting.cls_OutputDataWriting", "InsertInvoiceData", "Cập nhật dữ liệu hoá đơn - " + strError);
        //            return strError;
        //        }
        //        else
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busOutputDataWriting", "busOutputDataWriting.cls_OutputDataWriting", "InsertInvoiceData", "Cập nhật dữ liệu hoá đơn");
        //        }

        //        //Ket thuc tinh hoa don
        //        WriteLog(strMa_DViQLy, strMa_SoGCS, "BillingImplementation", "BillingImplementation", "BillingImplementation", "Tính xong sổ " + strMa_SoGCS + " - hoá đơn kỳ " + ky + " tháng " + thang + "/" + nam);

        //        return "";
        //    }
        //    catch (Exception ex)
        //    {
        //        //Ghi log vao dataset
        //        WriteLog(strMa_DViQLy, strMa_SoGCS, "BillingImplementation", "BillingImplementation", "BillingImplementation", "Lỗi khi tính hoá đơn: sổ " + strMa_SoGCS + " - hoá đơn kỳ " + ky + " tháng " + thang + "/" + nam);
        //        return ex.Message;
        //    }
        //    finally
        //    {
        //        dsCustomerData.Reset();
        //        dsInvoiceData.Reset();
        //        dsCalculation.Reset();
        //    }
        //}

        //Tinh va huy tinh hoa don le cho khach hang
        //CMIS2
        //private string CommonCustomerBilling(string strMa_DViQLy, string strMa_KHang, string strTen_DNhap, short ky, short thang, short nam)
        //{
        //    try
        //    {
        //        dsCustomerData = new DataSet();
        //        dsInvoiceData = new DataSet();
        //        dsCalculation = new DS_CALCULATIONTABLES();

        //        string strError = "";

        //        //Bat dau tinh hoa don
        //        //WriteLog(strMa_DViQLy, strMa_KHang.Substring(6, strMa_KHang.Length - 6) + "x", "BillingImplementation", "BillingImplementation", "BillingImplementation", "Tính hoá đơn khách hàng " + strMa_KHang + " - kỳ " + ky + " tháng " + thang + "/" + nam);
        //        //Doc du lieu dau vao
        //        strError = inputDataReading.getCustomerDataReading_2(ref dsCustomerData, strMa_DViQLy, strMa_KHang, ky, thang, nam);
        //        if (strError != "")
        //        {
        //            //WriteLog(strMa_DViQLy, strMa_KHang.Substring(6, strMa_KHang.Length - 6) + "x", "busInputDataReading", "busInputDataReading.cls_InputDataReading", "getCustomerDataReading", "Đọc dữ liệu đầu vào - " + strError);
        //            return "Đọc dữ liệu đầu vào - " + strError;
        //        }
        //        //else
        //        //{
        //        //    WriteLog(strMa_DViQLy, strMa_KHang.Substring(6, strMa_KHang.Length - 6) + "x", "busInputDataReading", "busInputDataReading.cls_InputDataReading", "getCustomerDataReading", "Đọc dữ liệu đầu vào");
        //        //}

        //        //Gan lai ma so ghi chi so chinh cua khach hang
        //        string strMa_SoGCS = "x";
        //        DataRow[] drQHe;
        //        if (dsCustomerData != null)
        //        {
        //            if (dsCustomerData.Tables.Contains("HDG_QHE_DDO") == true)
        //            {
        //                if (dsCustomerData.Tables["HDG_QHE_DDO"].Rows.Count != 0)
        //                {
        //                    //Co ton tai du lieu trong bang quan he, lay thong tin ma so la ma so chinh
        //                    drQHe = dsCustomerData.Tables["HDG_QHE_DDO"].Select("MA_DVIQLY = '" + strMa_DViQLy + "' AND MA_KHANG_CHINH = '" + strMa_KHang + "' AND LOAI_QHE = '40'");
        //                    if (drQHe.Length != 0)
        //                    {
        //                        strMa_SoGCS = Convert.ToString(drQHe[0]["MA_SOGCS_CHINH"]);
        //                    }
        //                }
        //            }
        //        }

        //        if (strMa_SoGCS == "x")
        //        {
        //            //Truong hop khong co trong quan he ghep tong, lay trong HDG_DDO_SOGCS
        //            strMa_SoGCS = Convert.ToString(dsCustomerData.Tables["HDG_DDO_SOGCS"].Rows[0]["MA_SOGCS"]);
        //        }

        //        //Kiem tra xem so nay da tinh hoa don trong ky hay chua
        //        strError = outputDataWriting.CheckExistInvoice(strMa_DViQLy, strMa_SoGCS, ky, thang, nam);
        //        if (strError != "")
        //        {
        //            //WriteLog(strMa_DViQLy, strMa_KHang.Substring(6, strMa_KHang.Length - 6) + "x", "busInputDataReading", "busInputDataReading.cls_InputDataReading", "getCustomerDataReading", "Đọc dữ liệu đầu vào - " + strError);
        //            return "Kiểm tra sổ đã tính hoá đơn - " + strError;
        //        }

        //        //Kiem tra du lieu dau vao
        //        strError = inputDataReading.CheckValidData(strMa_DViQLy, this.dsCustomerData, clsBillingImplementation.dsStaticCatalog);
        //        if (strError != "")
        //        {
        //            //WriteLog(strMa_DViQLy, strMa_KHang.Substring(6, strMa_KHang.Length - 6) + "x", "busInputDataReading", "busInputDataReading.cls_InputDataReading", "CheckValidData", "Kiểm tra dữ liệu đầu vào - " + strError);
        //            return "Kiểm tra dữ liệu đầu vào - " + strError;
        //        }
        //        //else
        //        //{
        //        //    WriteLog(strMa_DViQLy, strMa_KHang.Substring(6, strMa_KHang.Length - 6) + "x", "busInputDataReading", "busInputDataReading.cls_InputDataReading", "CheckValidData", "Kiểm tra dữ liệu đầu vào");
        //        //}

        //        //Tao cau truc du lieu dau ra
        //        dsInvoiceData = cls_OutputDataWriting.createDSInvoiceData();

        //        //Tinh san luong tho
        //        strError = rawDataCalculation.RawDataCalculation_1(this.dsCustomerData);
        //        if (strError != "")
        //        {
        //            //WriteLog(strMa_DViQLy, strMa_KHang.Substring(6, strMa_KHang.Length - 6) + "x", "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "RawDataCalculation_1", "Tính sản lượng thô - " + strError);
        //            return "Tính sản lượng thô 1 - " + strError;
        //        }
        //        else
        //        {
        //            //Kiem tra neu khong con du lieu chi so thi thong bao ket thuc tinh hoa don luon
        //            if (this.dsCustomerData.Tables.Contains("GCS_CHISO") == true)
        //            {
        //                if (this.dsCustomerData.Tables["GCS_CHISO"].Rows.Count == 0)
        //                {
        //                    //Ket thuc tinh hoa don
        //                    //WriteLog(strMa_DViQLy, strMa_KHang.Substring(6, strMa_KHang.Length - 6) + "x", "BillingImplementation", "BillingImplementation", "BillingImplementation", "Khách hàng không có dữ liệu chỉ số. Kết thúc tính hoá đơn của khách hàng " + strMa_KHang + " - kỳ " + ky + " tháng " + thang + "/" + nam);
        //                    return "Khách hàng không có dữ liệu chỉ số. Kết thúc tính hoá đơn của khách hàng " + strMa_KHang + " - kỳ " + ky + " tháng " + thang + "/" + nam;
        //                }
        //            }
        //        }

        //        strError = rawDataCalculation.RawDataCalculation_2(this.dsCustomerData);
        //        if (strError != "")
        //        {
        //            //WriteLog(strMa_DViQLy, strMa_KHang.Substring(6, strMa_KHang.Length - 6) + "x", "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "RawDataCalculation_2", "Tính sản lượng thô - " + strError);
        //            return "Tính sản lượng thô 2 - " + strError;
        //        }

        //        strError = rawDataCalculation.RawDataCalculation_3(this.dsCustomerData);
        //        if (strError != "")
        //        {
        //            //WriteLog(strMa_DViQLy, strMa_KHang.Substring(6, strMa_KHang.Length - 6) + "x", "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "RawDataCalculation_3", "Tính sản lượng thô - " + strError);
        //            return "Tính sản lượng thô 3 - " + strError;
        //        }

        //        strError = rawDataCalculation.RawDataCalculation_4(this.dsCustomerData, dsStaticCatalog);
        //        if (strError != "")
        //        {
        //            //Rieng phan tinh tru phu chi ghi log, khong can thoat
        //            //WriteLog(strMa_DViQLy, strMa_KHang.Substring(6, strMa_KHang.Length - 6) + "x", "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "RawDataCalculation_4", "Tính sản lượng thô - " + strError);
        //            return "Tính sản lượng thô 4 - " + strError;
        //        }

        //        strError = rawDataCalculation.RawDataCalculation_5(this.dsCustomerData, this.dsInvoiceData, clsBillingImplementation.dsStaticCatalog, strTen_DNhap);
        //        if (strError != "")
        //        {
        //            //WriteLog(strMa_DViQLy, strMa_KHang.Substring(6, strMa_KHang.Length - 6) + "x", "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "RawDataCalculation_5", "Tính sản lượng thô - " + strError);
        //            return "Tính sản lượng thô 5 - " + strError;
        //        }

        //        strError = rawDataCalculation.RawDataCalculation_6(this.dsCustomerData, this.dsInvoiceData, clsBillingImplementation.dsStaticCatalog, strTen_DNhap);
        //        if (strError != "")
        //        {
        //            //WriteLog(strMa_DViQLy, strMa_KHang.Substring(6, strMa_KHang.Length - 6) + "x", "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "RawDataCalculation_6", "Tính sản lượng thô - " + strError);
        //            return "Tính sản lượng thô 6 - " + strError;
        //        }

        //        strError = rawDataCalculation.RawDataCalculation_7(this.dsCustomerData, this.dsInvoiceData, clsBillingImplementation.dsStaticCatalog, strTen_DNhap);
        //        if (strError != "")
        //        {
        //            //WriteLog(strMa_DViQLy, strMa_KHang.Substring(6, strMa_KHang.Length - 6) + "x", "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "RawDataCalculation_7", "Tính sản lượng thô - " + strError);
        //            return "Tính sản lượng thô 7 - " + strError;
        //        }
        //        //WriteLog(strMa_DViQLy, strMa_KHang.Substring(6, strMa_KHang.Length - 6) + "x", "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "RawDataCalculation_7", "Tính xong sản lượng thô");

        //        //Tinh san luong chi tiet
        //        strError = detailDataCalculation.DetailDataCalculation_11(this.dsCustomerData, ref this.dsCalculation, dsStaticCatalog);
        //        if (strError != "")
        //        {
        //            //WriteLog(strMa_DViQLy, strMa_KHang.Substring(6, strMa_KHang.Length - 6) + "x", "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_11", "Tính sản lượng chi tiết - " + strError);
        //            return "Tính sản lượng chi tiết 1 - " + strError;
        //        }

        //        strError = detailDataCalculation.DetailDataCalculation_2(this.dsCalculation, this.dsCustomerData, dsStaticCatalog);
        //        if (strError != "")
        //        {
        //            //WriteLog(strMa_DViQLy, strMa_KHang.Substring(6, strMa_KHang.Length - 6) + "x", "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_2", "Tính sản lượng chi tiết - " + strError);
        //            return "Tính sản lượng chi tiết 2 - " + strError;
        //        }

        //        strError = detailDataCalculation.DetailDataCalculation_3(this.dsCalculation, this.dsCustomerData, dsStaticCatalog);
        //        if (strError != "")
        //        {
        //            //WriteLog(strMa_DViQLy, strMa_KHang.Substring(6, strMa_KHang.Length - 6) + "x", "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_3", "Tính sản lượng chi tiết - " + strError);
        //            return "Tính sản lượng chi tiết 3 - " + strError;
        //        }

        //        strError = detailDataCalculation.DetailDataCalculation_40(this.dsCalculation, this.dsCustomerData, dsStaticCatalog, dsInvoiceData, strMa_DViQLy, strMa_SoGCS, ky, thang, nam);
        //        if (strError != "")
        //        {
        //            //WriteLog(strMa_DViQLy, strMa_KHang.Substring(6, strMa_KHang.Length - 6) + "x", "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_4", "Tính sản lượng chi tiết - " + strError);
        //            return "Tính sản lượng chi tiết 4 - " + strError;
        //        }
        //        //WriteLog(strMa_DViQLy, strMa_KHang.Substring(6, strMa_KHang.Length - 6) + "x", "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_4", "Tính xong sản lượng chi tiết");

        //        //Xac dinh don gia
        //        strError = priceSpecification.PriceSpecification_11(this.dsCalculation, clsBillingImplementation.dsStaticCatalog, dsCustomerData);
        //        if (strError != "")
        //        {
        //            //WriteLog(strMa_DViQLy, strMa_KHang.Substring(6, strMa_KHang.Length - 6) + "x", "busBillingCalculation", "busBillingCalculation.clsPriceSpecification", "PriceSpecification_1", "Xác định đơn giá - " + strError);
        //            return "Xác định đơn giá 1 - " + strError;
        //        }

        //        //Xu ly lam tron ngang cho gia bac thang
        //        strError = priceSpecification.PriceSpecification_12(this.dsCalculation, clsBillingImplementation.dsStaticCatalog, dsCustomerData);
        //        if (strError != "")
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsPriceSpecification", "PriceSpecification_12", "Xử lý riêng cho giá bậc thang trong kỳ đổi giá - " + strError);
        //            return strError;
        //        }

        //        strError = priceSpecification.PriceSpecification_21(this.dsCalculation, clsBillingImplementation.dsStaticCatalog, dsCustomerData);
        //        if (strError != "")
        //        {
        //            //WriteLog(strMa_DViQLy, strMa_KHang.Substring(6, strMa_KHang.Length - 6) + "x", "busBillingCalculation", "busBillingCalculation.clsPriceSpecification", "PriceSpecification_2", "Xác định đơn giá - " + strError);
        //            return "Xác định đơn giá 2 - " + strError;
        //        }

        //        strError = priceSpecification.PriceSpecification_3(this.dsCalculation, this.dsCustomerData, this.dsInvoiceData, strTen_DNhap);
        //        if (strError != "")
        //        {
        //            //WriteLog(strMa_DViQLy, strMa_KHang.Substring(6, strMa_KHang.Length - 6) + "x", "busBillingCalculation", "busBillingCalculation.clsPriceSpecification", "PriceSpecification_3", "Xác định đơn giá - " + strError);
        //            return "Xác định đơn giá 3 - " + strError;
        //        }
        //        //WriteLog(strMa_DViQLy, strMa_KHang.Substring(6, strMa_KHang.Length - 6) + "x", "busBillingCalculation", "busBillingCalculation.clsPriceSpecification", "PriceSpecification_3", "Kết thúc xác định đơn giá");

        //        //dsCustomerData.WriteXml(@"C:\CustomerData.xml");
        //        //dsStaticCatalog.WriteXml(@"C:\StaticCatalog.xml");
        //        //dsCalculation.WriteXml(@"C:\CalculationData.xml");
        //        //dsInvoiceData.WriteXml(@"C:\InvoiceData.xml");

        //        //Tinh hoa don
        //        //Kiem tra tham so G_INCHUNGRIENG = 'C' thi se ra hoa don TC
        //        if (dsStaticCatalog.Tables.Contains("S_PARAMETER") == true)
        //        {
        //            if (dsStaticCatalog.Tables["S_PARAMETER"].Select("NAME = 'G_INCHUNGRIENG' AND (PRAVALUE = 'C' OR PRAVALUE = 'c')").Length > 0)
        //            {
        //                strError = invoiceCalculation.InvoiceCalculation_TC(ref this.dsInvoiceData, this.dsCustomerData, this.dsCalculation, strMa_DViQLy, strMa_SoGCS, ky, thang, nam, dsStaticCatalog);
        //            }
        //            else
        //            {
        //                strError = invoiceCalculation.InvoiceCalculation(ref this.dsInvoiceData, this.dsCustomerData, this.dsCalculation, strMa_DViQLy, strMa_SoGCS, ky, thang, nam, dsStaticCatalog);
        //            }
        //        }
        //        else
        //        {
        //            strError = invoiceCalculation.InvoiceCalculation(ref this.dsInvoiceData, this.dsCustomerData, this.dsCalculation, strMa_DViQLy, strMa_SoGCS, ky, thang, nam, dsStaticCatalog);
        //        }
        //        if (strError != "")
        //        {
        //            //WriteLog(strMa_DViQLy, strMa_KHang.Substring(6, strMa_KHang.Length - 6) + "x", "busBillingCalculation", "busBillingCalculation.clsInvoiceCalculation", "InvoiceCalculation", "Tính hoá đơn - " + strError);
        //            return "Tính hoá đơn - " + strError;
        //        }
        //        //else
        //        //{
        //        //    WriteLog(strMa_DViQLy, strMa_KHang.Substring(6, strMa_KHang.Length - 6) + "x", "busBillingCalculation", "busBillingCalculation.clsInvoiceCalculation", "InvoiceCalculation", "Tính xong hoá đơn");
        //        //}


        //        //Cap nhat du lieu
        //        strError = outputDataWriting.InsertInvoiceData_2(this.dsInvoiceData, this.dsCustomerData, strMa_DViQLy, strMa_SoGCS, ky, thang, nam);
        //        if (strError != "")
        //        {
        //            //WriteLog(strMa_DViQLy, strMa_KHang.Substring(6, strMa_KHang.Length - 6) + "x", "busOutputDataWriting", "busOutputDataWriting.cls_OutputDataWriting", "InsertInvoiceData", "Cập nhật dữ liệu hoá đơn - " + strError);
        //            return "Cập nhật dữ liệu hoá đơn - " + strError;
        //        }
        //        //else
        //        //{
        //        //    WriteLog(strMa_DViQLy, strMa_KHang.Substring(6, strMa_KHang.Length - 6) + "x", "busOutputDataWriting", "busOutputDataWriting.cls_OutputDataWriting", "InsertInvoiceData", "Cập nhật dữ liệu hoá đơn");
        //        //}

        //        //Ket thuc tinh hoa don
        //        //WriteLog(strMa_DViQLy, strMa_KHang.Substring(6, strMa_KHang.Length - 6) + "x", "BillingImplementation", "BillingImplementation", "BillingImplementation", "Tính xong hoá đơn: khách hàng " + strMa_KHang + " - kỳ " + ky + " tháng " + thang + "/" + nam);

        //        return "";
        //    }
        //    catch (Exception ex)
        //    {
        //        //Ghi log vao dataset
        //        WriteLog(strMa_DViQLy, strMa_KHang.Substring(6, strMa_KHang.Length - 6) + "x", "BillingImplementation", "BillingImplementation", "BillingImplementation", "Lỗi khi tính hoá đơn: khách hàng " + strMa_KHang + " - hoá đơn kỳ " + ky + " tháng " + thang + "/" + nam);
        //        return ex.Message;
        //    }
        //    finally
        //    {
        //        dsCustomerData.Reset();
        //        dsInvoiceData.Reset();
        //        dsCalculation.Reset();
        //    }
        //}

        //public string CustomerInvoiceBilling(string strMa_DViQLy, string strMa_KHang, string strTen_DNhap, short ky, short thang, short nam)
        //{
        //    return CommonCustomerBilling(strMa_DViQLy, strMa_KHang, strTen_DNhap, ky, thang, nam);
        //}

        public string DeleteCustomerInvoice(string strMaDViQLy, string strMa_KHang, short ky, short thang, short nam)
        {
            busOutputDataWriting.clsCancelInvoiceCalculation objCancelInvoiceCalculation = new clsCancelInvoiceCalculation();
            return objCancelInvoiceCalculation.DeleteCustomerInvoice(strMaDViQLy, strMa_KHang, ky, thang, nam);
        }

        //public string CommonImplementation_ForApp(DataTable dtGCS_CHISO, string strMa_DViQLy, string strMa_SoGCS, string strListMaKhang, string strListMaDDo, Int16 ky, Int16 thang, Int16 nam, ref DataSet dsDanhMuc, ref DataSet dsResult)
        //{
        //    DataSet dsCustomerData = new DataSet();
        //    string strTen_DNhap = "";
        //    try
        //    {
        //        dsCalculation = new DS_CALCULATIONTABLES();
        //        dsInvoiceData = new DataSet();
        //        //Xử lý tránh lấy danh mục nhiều lần trong khi tính hóa đơn
        //        if (dsDanhMuc == null || dsDanhMuc.Tables.Count <= 0 || dsDanhMuc.Tables[0].Rows.Count <= 0)
        //        {
        //            GetStaticData();
        //            dsDanhMuc = new DataSet();
        //            dsDanhMuc = clsBillingImplementation.dsStaticCatalog.Copy();
        //        }
        //        else
        //            clsBillingImplementation.dsStaticCatalog = dsDanhMuc;
        //        log = new cls_LogManagement(dsLog);

        //        strTen_DNhap = "Service";
        //        string strError = "";
        //        //Bat dau tinh hoa don
        //        WriteLog(strMa_DViQLy, strMa_SoGCS, "BillingImplementation", "BillingImplementation", "BillingImplementation", "Tính hoá đơn sổ " + strMa_SoGCS + " - kỳ " + ky + " tháng " + thang + "/" + nam);

        //        // Lấy dữ liệu khách hàng
        //        strError = inputDataReading.getCustomerData_For_App(ref dsCustomerData, strMa_DViQLy, strMa_SoGCS, strListMaKhang, strListMaDDo, ky, thang, nam);
        //        if (strError != "")
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busInputDataReading", "busInputDataReading.cls_InputDataReading", "getCustomerData_For_App", "Lấy dữ liệu khách hàng - " + strError);
        //            return strError;
        //        }
        //        else
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busInputDataReading", "busInputDataReading.cls_InputDataReading", "getCustomerData_For_App", "Lấy dữ liệu khách hàng.");

        //        }
        //        //Gán bảng GCS_CHISO

        //        DataTable dtGCS = dtGCS_CHISO.Copy();
        //        dtGCS.TableName = "GCS_CHISO";
        //        dsCustomerData.Tables.Add(dtGCS);
        //        //Loc bien ban ap gia, chi lay cac bien ban co lien quan den ky ghi chi so hien tai
        //        strError = this.FilterHDG_BBAN_APGIA(ref dsCustomerData);
        //        if (strError != "")
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "BillingImplementation", "BillingImplementation.clsBillingImplementation.cs", "FilterHDG_BBAN_APGIA", "Lọc biên bản áp giá - " + strError);
        //            return strError;
        //        }
        //        else
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "BillingImplementation", "BillingImplementation.clsBillingImplementation.cs", "FilterHDG_BBAN_APGIA", "Lọc biên bản áp giá");
        //        }
        //        strError = this.AddColumn(ref dsCustomerData, dsStaticCatalog);
        //        if (strError != "")
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "AddColumn", "Thêm cột mới - " + strError);
        //            return strError;
        //        }
        //        //Kiem tra du lieu dau vao
        //        strError = inputDataReading.CheckValidData(strMa_DViQLy, dsCustomerData, clsBillingImplementation.dsStaticCatalog);
        //        if (strError != "")
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busInputDataReading", "busInputDataReading.cls_InputDataReading", "CheckValidData", "Kiểm tra dữ liệu đầu vào - " + strError);
        //            return strError;
        //        }
        //        else
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busInputDataReading", "busInputDataReading.cls_InputDataReading", "CheckValidData", "Kiểm tra dữ liệu đầu vào");
        //        }
        //        //Tao cau truc du lieu dau ra
        //        dsInvoiceData = cls_OutputDataWriting.createDSInvoiceData();

        //        //Tinh san luong tho
        //        strError = rawDataCalculation.RawDataCalculation_1(dsCustomerData);
        //        if (strError != "")
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "RawDataCalculation_1", "Tính sản lượng thô - " + strError);
        //            return strError;
        //        }

        //        strError = rawDataCalculation.RawDataCalculation_2(dsCustomerData);
        //        if (strError != "")
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "RawDataCalculation_2", "Tính sản lượng thô - " + strError);
        //            return strError;
        //        }

        //        strError = rawDataCalculation.RawDataCalculation_3(dsCustomerData);
        //        if (strError != "")
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "RawDataCalculation_3", "Tính sản lượng thô - " + strError);
        //            return strError;
        //        }
        //        //strError = rawDataCalculation.RawDataCalculation_4(dsCustomerData, dsStaticCatalog);
        //        //if (strError != "")
        //        //{
        //        //    WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "RawDataCalculation_4", "Tính sản lượng thô - " + strError);
        //        //    return strError;
        //        //}

        //        strError = rawDataCalculation.RawDataCalculation_5(dsCustomerData, this.dsInvoiceData, clsBillingImplementation.dsStaticCatalog, strTen_DNhap);
        //        if (strError != "")
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "RawDataCalculation_5", "Tính sản lượng thô - " + strError);
        //            return strError;
        //        }

        //        strError = rawDataCalculation.RawDataCalculation_6(dsCustomerData, this.dsInvoiceData, clsBillingImplementation.dsStaticCatalog, strTen_DNhap);
        //        if (strError != "")
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "RawDataCalculation_6", "Tính sản lượng thô - " + strError);
        //            return strError;
        //        }

        //        //strError = rawDataCalculation.RawDataCalculation_7(dsCustomerData, this.dsInvoiceData, clsBillingImplementation.dsStaticCatalog, strTen_DNhap);
        //        //if (strError != "")
        //        //{
        //        //    WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "RawDataCalculation_7", "Tính sản lượng thô - " + strError);
        //        //    return strError;
        //        //}
        //        WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "RawDataCalculation_7", "Tính xong sản lượng thô");

        //        //Tinh san luong chi tiet

        //        strError = detailDataCalculation.DetailDataCalculation_11(dsCustomerData, ref this.dsCalculation, dsStaticCatalog);
        //        if (strError != "")
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_11", "Tính sản lượng chi tiết - " + strError);
        //            return strError;
        //        }

        //        strError = detailDataCalculation.DetailDataCalculation_2(this.dsCalculation, dsCustomerData, dsStaticCatalog);
        //        if (strError != "")
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_2", "Tính sản lượng chi tiết - " + strError);
        //            return strError;
        //        }
        //        //Xu ly cho KH tinh vat qua doi gia
        //        strError = detailDataCalculation.DetailDataCalculation_21(this.dsCalculation, dsCustomerData, dsStaticCatalog);
        //        if (strError != "")
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_21", "Tính sản lượng chi tiết trong kỳ đổi giá - " + strError);
        //            return strError;
        //        }
        //        strError = detailDataCalculation.DetailDataCalculation_3(this.dsCalculation, dsCustomerData, dsStaticCatalog);
        //        if (strError != "")
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_3", "Tính sản lượng chi tiết - " + strError);
        //            return strError;
        //        }
        //        WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_3", "Tính xong sản lượng chi tiết");

        //        strError = detailDataCalculation.DetailDataCalculation_40(this.dsCalculation, dsCustomerData, dsStaticCatalog, dsInvoiceData, strMa_DViQLy, strMa_SoGCS, ky, thang, nam);
        //        if (strError != "")
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_4", "Tính sản lượng chi tiết - " + strError);
        //            return strError;
        //        }
        //        WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_4", "Tính xong sản lượng chi tiết");

        //        strError = detailDataCalculation.DetailDataCalculation_41(this.dsCalculation, dsStaticCatalog, dsCustomerData);
        //        if (strError != "")
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_41", "Tính sản lượng chi tiết cho bán buôn trong kỳ đổi giá - " + strError);
        //            return strError;
        //        }
        //        WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_41", "Tính sản lượng chi tiết cho bán buôn trong kỳ đổi giá");

        //        strError = detailDataCalculation.DetailDataCalculation_42(this.dsCalculation);
        //        if (strError != "")
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_42", "Xử lý riêng cho bán buôn SHBB nhóm L trong tháng 03/2011 - " + strError);
        //            return strError;
        //        }
        //        WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_42", "Xử lý riêng cho bán buôn SHBB nhóm L trong tháng 03/2011");

        //        //Xu ly cho KH tinh qua doi gia
        //        strError = priceSpecification.PriceSpecification_11(this.dsCalculation, clsBillingImplementation.dsStaticCatalog, dsCustomerData);
        //        if (strError != "")
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsPriceSpecification", "PriceSpecification_1", "Xác định đơn giá - " + strError);
        //            return strError;
        //        }

        //        //Xu ly lam tron ngang cho gia bac thang
        //        strError = priceSpecification.PriceSpecification_12(this.dsCalculation, clsBillingImplementation.dsStaticCatalog, dsCustomerData);
        //        if (strError != "")
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsPriceSpecification", "PriceSpecification_12", "Xử lý riêng cho giá bậc thang trong kỳ đổi giá - " + strError);
        //            return strError;
        //        }

        //        //strError = priceSpecification.PriceSpecification_2(this.dsCalculation, clsBillingImplementation.dsStaticCatalog);
        //        //Xu ly cho KH tinh qua doi gia
        //        strError = priceSpecification.PriceSpecification_21(this.dsCalculation, clsBillingImplementation.dsStaticCatalog, dsCustomerData);
        //        if (strError != "")
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsPriceSpecification", "PriceSpecification_2", "Xác định đơn giá - " + strError);
        //            return strError;
        //        }
        //        strError = priceSpecification.PriceSpecification_3(this.dsCalculation, dsCustomerData, this.dsInvoiceData, strTen_DNhap);
        //        if (strError != "")
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsPriceSpecification", "PriceSpecification_3", "Xác định đơn giá - " + strError);
        //            return strError;
        //        }
        //        WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsPriceSpecification", "PriceSpecification_3", "Kết thúc xác định đơn giá");

        //        //Tinh hoa don
        //        //Kiem tra tham so G_INCHUNGRIENG = 'C' thi se ra hoa don TC
        //        if (dsStaticCatalog.Tables.Contains("S_PARAMETER") == true)
        //        {
        //            if (dsStaticCatalog.Tables["S_PARAMETER"].Select("NAME = 'G_INCHUNGRIENG' AND (PRAVALUE = 'C' OR PRAVALUE = 'c')").Length > 0)
        //            {
        //                strError = invoiceCalculation.InvoiceCalculation_TC(ref this.dsInvoiceData, dsCustomerData, this.dsCalculation, strMa_DViQLy, strMa_SoGCS, ky, thang, nam, dsStaticCatalog);
        //            }
        //            else
        //            {
        //                strError = invoiceCalculation.InvoiceCalculation(ref this.dsInvoiceData, dsCustomerData, this.dsCalculation, strMa_DViQLy, strMa_SoGCS, ky, thang, nam, dsStaticCatalog);
        //            }
        //        }
        //        else
        //        {
        //            strError = invoiceCalculation.InvoiceCalculation(ref this.dsInvoiceData, dsCustomerData, this.dsCalculation, strMa_DViQLy, strMa_SoGCS, ky, thang, nam, dsStaticCatalog);
        //        }

        //        if (strError != "")
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsInvoiceCalculation", "InvoiceCalculation", "Tính hoá đơn - " + strError);
        //            return strError;
        //        }
        //        else
        //        {
        //            WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsInvoiceCalculation", "InvoiceCalculation", "Tính xong hoá đơn");
        //        }
        //        //Không cập nhật dữ liệu, trả về luôn
        //        dsResult = new DataSet();
        //        dsResult = this.dsInvoiceData.Copy();

        //        return "";
        //    }
        //    catch (Exception ex)
        //    {
        //        //Ghi log vao dataset
        //        WriteLog(strMa_DViQLy, strMa_SoGCS, "BillingImplementation", "BillingImplementation", "BillingImplementation", "Lỗi khi tính hoá đơn: sổ " + strMa_SoGCS + " - hoá đơn kỳ " + ky + " tháng " + thang + "/" + nam);
        //        return ex.Message;
        //    }
        //    finally
        //    {
        //        dsCustomerData.Reset();
        //        dsInvoiceData.Reset();
        //        dsCalculation.Reset();
        //    }
        //}
        #region Tinh hoa don cmis 3
        public async Task<string> BillingImplementationPlus(string strMa_DViQLy, string strMa_SoGCS, short ky, short thang, short nam, string strTen_DNhap, long lngCurrentLibID, long lngWorkflowID, string strNgayGhi)
        {

            //DateTime dtHLuc = new DateTime(2020, 4, 1);

            //DateTime dtHetHLuc = new DateTime(2020, 8, 1);

            //DateTime dtNgayCKy = DateTime.ParseExact(strNgayGhi, "dd/MM/yyyy", System.Globalization.CultureInfo.InvariantCulture);
            if (config.isHTroThang && config.lstTNamHTro.Contains(thang + "/" + nam))
                return await this.CommonImplementationPlus_GTruAsync(strMa_DViQLy, strMa_SoGCS, ky, thang, nam, strTen_DNhap, lngCurrentLibID, lngWorkflowID, strNgayGhi);
            else
                return  await this.CommonImplementationPlus(strMa_DViQLy, strMa_SoGCS, ky, thang, nam, strTen_DNhap, lngCurrentLibID, lngWorkflowID, strNgayGhi);
        }
        private async Task<string> CommonImplementationPlus(string strMa_DViQLy, string strMa_SoGCS, short ky, short thang, short nam, string strTen_DNhap, long lngCurrentLibID, long lngWorkflowID, string strNgayGhi)
        {
            try
            {
                dsCustomerData = new DataSet();
                dsInvoiceData = new DataSet();

                dsCalculation = new DS_CALCULATIONTABLES();

                string strError = "";

                //Bat dau tinh hoa don
               
                WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "BillingImplementation", "BillingImplementation", "BillingImplementation", "Tính hoá đơn sổ " + strMa_SoGCS + " - kỳ " + ky + " tháng " + thang + "/" + nam);
                Console.WriteLine("Ra khỏi phần checkPhu : " + strMa_SoGCS + "lúc  " + DateTime.Now.ToString("dd/MM/yyyy HH:mm:ss:ffff"));
                //Kiem tra so phu ghep tong
                strError = await inputDataReading.checkPhuGhepTongPlusAsync(strMa_DViQLy, strMa_SoGCS, ky, thang, nam, strNgayGhi);
                strError = strError.Trim();
                if (strError != "" && strError != " ")
                {
                    if (strError == "1")
                    {
                        WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busInputDataReading", "busInputDataReading.cls_InputDataReading", "checkPhuGhepTong", "Kiểm tra sổ phụ ghép tổng.");
                        strError = outputDataWriting.InsertInvoiceData_PGT_Plus(strMa_DViQLy, strMa_SoGCS, strTen_DNhap, lngCurrentLibID, lngWorkflowID, ky, thang, nam);
                        if (strError != "")
                        {
                            WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busOutputDataWriting", "busOutputDataWriting.cls_OutputDataWriting", "InsertInvoiceData", "Cập nhật dữ liệu hoá đơn - " + strError);
                            return strError;
                        }
                        else
                        {
                            WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busOutputDataWriting", "busOutputDataWriting.cls_OutputDataWriting", "InsertInvoiceData", "Cập nhật dữ liệu hoá đơn");
                            WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "BillingImplementation", "BillingImplementation", "BillingImplementation", "Tính xong sổ " + strMa_SoGCS + " - hoá đơn kỳ " + ky + " tháng " + thang + "/" + nam);
                            return "";
                        }
                    }
                    else if (strError == "2")
                    {
                        WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busInputDataReading", "busInputDataReading.cls_InputDataReading", "checkPhuGhepTong", "Kiểm tra sổ phụ ghép tổng: Không tìm thấy dữ liệu trong HDG_DDO_SOGCS.");
                        return strError;
                    }
                    else
                    {
                        WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busInputDataReading", "busInputDataReading.cls_InputDataReading", "checkPhuGhepTong", "Kiểm tra sổ phụ ghép tổng - " + strError);
                        return strError;
                    }
                }
                else
                {
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busInputDataReading", "busInputDataReading.cls_InputDataReading", "checkPhuGhepTong", "Kiểm tra sổ phụ ghép tổng");
                }
                //Doc du lieu dau vao
                //strError = inputDataReading.getCustomerDataReadingPlus(ref dsCustomerData, strMa_DViQLy, strMa_SoGCS, ky, thang, nam, strNgayGhi);
                Console.WriteLine("Vô phần lấy dữ liệu : " + strMa_SoGCS + "lúc  " + DateTime.Now.ToString("dd/MM/yyyy HH:mm:ss:ffff"));

                TinhHDonModel dataTest = await inputDataReading.getCustomerDataReadingPlusAsync(strMa_DViQLy, strMa_SoGCS, ky, thang, nam, strNgayGhi);

                Console.WriteLine("Ra khỏi phần lấy dữ liệu : " + strMa_SoGCS + "lúc  " + DateTime.Now.ToString("dd/MM/yyyy HH:mm:ss:ffff"));
                dsCustomerData = dataTest.Data;
                strError = dataTest.Mesage;
                //dsCustomerData.WriteXml("D:/CustomerDataAPI.xml", XmlWriteMode.WriteSchema);
                //dsCustomerData = new DataSet();
                //dsCustomerData.ReadXml("D:/CustomerDataAPI.xml", XmlReadMode.ReadSchema);
                if (strError != "")
                {
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busInputDataReading", "busInputDataReading.cls_InputDataReading", "getCustomerDataReading", "Đọc dữ liệu đầu vào - " + strError);
                    return strError;
                }
                else
                {
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busInputDataReading", "busInputDataReading.cls_InputDataReading", "getCustomerDataReading", "Đọc dữ liệu đầu vào");
                }
                //Loc bien ban ap gia, chi lay cac bien ban co lien quan den ky ghi chi so hien tai
                strError = this.FilterHDG_BBAN_APGIA(ref dsCustomerData);
                //decimal _tphu = dsCustomerData.Tables["HDG_QHE_DDO"].Select("MA_DDO_CHINH='PD05000093426001'").Length;
                if (strError != "")
                {
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "BillingImplementation", "BillingImplementation.clsBillingImplementation.cs", "FilterHDG_BBAN_APGIA", "Lọc biên bản áp giá - " + strError);
                    return strError;
                }
                else
                {
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "BillingImplementation", "BillingImplementation.clsBillingImplementation.cs", "FilterHDG_BBAN_APGIA", "Lọc biên bản áp giá");
                }
                //Đổi loại chỉ số CPK sang DDN, sau bỏ đi
                strError = this.AddColumn(ref dsCustomerData, dsStaticCatalog);
                if (strError != "")
                {
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "AddColumn", "Thêm cột mới - " + strError);
                    return strError;
                }
                else
                {
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "AddColumn", "Thêm cột mới");
                }
                //Đổi loại chỉ số CPK sang DDN để check valid data ko báo lỗi dup chỉ số
                //strError = this.ChangeCCS(ref dsCustomerData, "CPK", "DDN", dsStaticCatalog);
                //if (strError != "")
                //{
                //    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "AddColumn", "Tiền xử lý thông tin chỉ số - " + strError);
                //    return strError;
                //}
                //Kiem tra du lieu dau vao
                strError = inputDataReading.CheckValidData(strMa_DViQLy, this.dsCustomerData, clsBillingImplementation.dsStaticCatalog);               
                if (strError != "")
                {
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busInputDataReading", "busInputDataReading.cls_InputDataReading", "CheckValidData", "Kiểm tra dữ liệu đầu vào - " + strError);
                    return strError;
                }
                else
                {
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busInputDataReading", "busInputDataReading.cls_InputDataReading", "CheckValidData", "Kiểm tra dữ liệu đầu vào");
                }
                //var temp = dsCustomerData.Tables["HDG_BBAN_APGIA"].Select("MA_DDO = 'PC12LL0890486001'");
                //Tao cau truc du lieu dau ra
                dsInvoiceData = cls_OutputDataWriting.createDSInvoiceData();

                //Tinh san luong tho
                strError = rawDataCalculation.RawDataCalculation_1(this.dsCustomerData);
                if (strError != "")
                {
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "RawDataCalculation_1", "Tính sản lượng thô - " + strError);
                    return strError;
                }
                else
                {
                    //Kiem tra neu khong con du lieu chi so thi thong bao ket thuc tinh hoa don luon
                    if (this.dsCustomerData.Tables.Contains("GCS_CHISO") == true)
                    {
                        if (this.dsCustomerData.Tables["GCS_CHISO"].Rows.Count == 0)
                        {
                            //Ket thuc tinh hoa don
                            WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "BillingImplementation", "BillingImplementation", "BillingImplementation", "Sổ không có dữ liệu chỉ số. Kết thúc tính hoá đơn của sổ " + strMa_SoGCS + " - kỳ " + ky + " tháng " + thang + "/" + nam);
                            return "";
                        }
                    }
                }

                strError = rawDataCalculation.RawDataCalculation_2(this.dsCustomerData);
                if (strError != "")
                {
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "RawDataCalculation_2", "Tính sản lượng thô - " + strError);
                    return strError;
                }

                strError = rawDataCalculation.RawDataCalculation_3(this.dsCustomerData);
                if (strError != "")
                {
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "RawDataCalculation_3", "Tính sản lượng thô - " + strError);
                    return strError;
                }
                //Đổi loại phản kháng về CCS, đổi CCS tạm sang DDN
                //strError = this.ChangeCCS(ref dsCustomerData, "CPK", "CCS", dsStaticCatalog);
                //if (strError != "")
                //{
                //    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "AddColumn", "Tiền xử lý thông tin chỉ số - " + strError);
                //    return strError;
                //}
                strError = rawDataCalculation.RawDataCalculation_4(this.dsCustomerData, dsStaticCatalog);
                if (strError != "")
                {
                    //Rieng phan tinh tru phu chi ghi log, khong can thoat
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "RawDataCalculation_4", "Tính sản lượng thô - " + strError);
                    return strError;
                }
                //Đổi áp giá về DDN để tính cosfi
                //strError = this.ChangeCCS(ref dsCustomerData, "CCS", "DDN", dsStaticCatalog);
                //if (strError != "")
                //{
                //    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "AddColumn", "Tiền xử lý thông tin chỉ số - " + strError);
                //    return strError;
                //}
                strError = rawDataCalculation.RawDataCalculation_5(this.dsCustomerData, this.dsInvoiceData, clsBillingImplementation.dsStaticCatalog, strTen_DNhap);
                if (strError != "")
                {
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "RawDataCalculation_5", "Tính sản lượng thô - " + strError);
                    return strError;
                }

                strError = rawDataCalculation.RawDataCalculation_6(this.dsCustomerData, this.dsInvoiceData, clsBillingImplementation.dsStaticCatalog, strTen_DNhap);
                if (strError != "")
                {
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "RawDataCalculation_6", "Tính sản lượng thô - " + strError);
                    return strError;
                }

                strError = rawDataCalculation.RawDataCalculation_7(this.dsCustomerData, this.dsInvoiceData, clsBillingImplementation.dsStaticCatalog, strTen_DNhap);
                if (strError != "")
                {
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "RawDataCalculation_7", "Tính sản lượng thô - " + strError);
                    return strError;
                }
                WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "RawDataCalculation_7", "Tính xong sản lượng thô");
                //đã tính xong cosfi, đổi CCS trả về CCS, CPK về DDN
                //strError = this.ChangeCCS(ref dsCustomerData, "CPK", "DDN", dsStaticCatalog);
                //if (strError != "")
                //{
                //    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "AddColumn", "Tiền xử lý thông tin chỉ số - " + strError);
                //    return strError;
                //}
                //strError = this.ChangeCCS(ref dsCustomerData, "CCS", "CCS", dsStaticCatalog);
                //if (strError != "")
                //{
                //    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "AddColumn", "Tiền xử lý thông tin chỉ số - " + strError);
                //    return strError;
                //}
                //Tinh san luong chi tiet

                strError = detailDataCalculation.DetailDataCalculation_11(this.dsCustomerData, ref this.dsCalculation, dsStaticCatalog);
                if (strError != "")
                {
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_11", "Tính sản lượng chi tiết - " + strError);
                    return strError;
                }
                //Debug.WriteLine(DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff") + " DetailDataCalculation_11");
                strError = detailDataCalculation.DetailDataCalculation_2(this.dsCalculation, this.dsCustomerData, dsStaticCatalog);
                if (strError != "")
                {
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_2", "Tính sản lượng chi tiết - " + strError);
                    return strError;
                }
                //Debug.WriteLine(DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff") + " DetailDataCalculation_2");
                //Xu ly cho KH tinh vat qua doi gia
                strError = detailDataCalculation.DetailDataCalculation_21(this.dsCalculation, this.dsCustomerData, dsStaticCatalog);
                if (strError != "")
                {
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_21", "Tính sản lượng chi tiết trong kỳ đổi giá - " + strError);
                    return strError;
                }
                //Debug.WriteLine(DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff") + " DetailDataCalculation_21");
                strError = detailDataCalculation.DetailDataCalculation_3(this.dsCalculation, this.dsCustomerData, dsStaticCatalog);
                if (strError != "")
                {
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_3", "Tính sản lượng chi tiết - " + strError);
                    return strError;
                }
                //Debug.WriteLine(DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff") + " DetailDataCalculation_3");
                strError = detailDataCalculation.DetailDataCalculation_40(this.dsCalculation, this.dsCustomerData, dsStaticCatalog, this.dsInvoiceData, strMa_DViQLy, strMa_SoGCS, ky, thang, nam);
                if (strError != "")
                {
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_4", "Tính sản lượng chi tiết - " + strError);
                    return strError;
                }
                WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_4", "Tính xong sản lượng chi tiết");

                strError = detailDataCalculation.DetailDataCalculation_41(this.dsCalculation, dsStaticCatalog, dsCustomerData);
                if (strError != "")
                {
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_41", "Tính sản lượng chi tiết cho bán buôn trong kỳ đổi giá - " + strError);
                    return strError;
                }
                WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_41", "Tính sản lượng chi tiết cho bán buôn trong kỳ đổi giá");

                //Bỏ kiểm tra trường hợp SHBB nhóm L, do từ sau 28/02/2011 không còn nhóm này nữa
                //strError = detailDataCalculation.DetailDataCalculation_42(this.dsCalculation);
                //if (strError != "")
                //{
                //    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_42", "Xử lý riêng cho bán buôn SHBB nhóm L trong tháng 03/2011 - " + strError);
                //    return strError;
                //}
                //WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_42", "Xử lý riêng cho bán buôn SHBB nhóm L trong tháng 03/2011");


                //Xac dinh don gia
                //strError = priceSpecification.PriceSpecification_1(this.dsCalculation, clsBillingImplementation.dsStaticCatalog, dsCustomerData);
                //Xu ly cho KH tinh qua doi gia
                //Ko làm tròn ngang cho nhóm bậc thang
                strError = priceSpecification.PriceSpecification_11(this.dsCalculation, clsBillingImplementation.dsStaticCatalog, dsCustomerData);
                if (strError != "")
                {
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsPriceSpecification", "PriceSpecification_1", "Xử lý riêng cho giá bậc thang - " + strError);
                    return strError;
                }

                //Xu ly lam tron ngang cho gia bac thang
                strError = priceSpecification.PriceSpecification_12(this.dsCalculation, clsBillingImplementation.dsStaticCatalog, dsCustomerData);
                if (strError != "")
                {
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsPriceSpecification", "PriceSpecification_12", "Xử lý riêng cho giá bậc thang trong kỳ đổi giá - " + strError);
                    return strError;
                }

                //strError = priceSpecification.PriceSpecification_2(this.dsCalculation, clsBillingImplementation.dsStaticCatalog);
                //Xu ly cho KH tinh qua doi gia
                strError = priceSpecification.PriceSpecification_21(this.dsCalculation, clsBillingImplementation.dsStaticCatalog, this.dsCustomerData);
                if (strError != "")
                {
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsPriceSpecification", "PriceSpecification_2", "Xác định đơn giá - " + strError);
                    return strError;
                }


                strError = priceSpecification.PriceSpecification_3(this.dsCalculation, this.dsCustomerData, this.dsInvoiceData, strTen_DNhap, strNgayGhi, ky, thang, nam); if (strError != "")
                {
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsPriceSpecification", "PriceSpecification_3", "Xác định đơn giá - " + strError);
                    return strError;
                }
                WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsPriceSpecification", "PriceSpecification_3", "Kết thúc xác định đơn giá");
                //bool isGTru = false;
                ////Chỉ tính lại số tiền khi G_COVID = 1
                //if (dsStaticCatalog.Tables.Contains("S_PARAMETER") == true)
                //{
                //    if (dsStaticCatalog.Tables["S_PARAMETER"].Select("NAME = 'G_COVID' AND PRAVALUE = '1'").Length > 0)
                //    {
                //        if(nam>=2020 && thang<=6 && thang>=4)
                //        {
                //            isGTru = true;
                //            strError = priceSpecification.PriceSpecification_4(this.dsCustomerData, this.dsInvoiceData);
                //            if (strError != "")
                //            {
                //                WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsPriceSpecification", "PriceSpecification_4", "Tính toán giảm trừ - " + strError);
                //                return strError;
                //            }
                //            WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsPriceSpecification", "PriceSpecification_4", "Kết thúc tính toán giảm trừ");
                //        }
                //    }                    
                //}

                //dsCustomerData.WriteXml(@"C:\CustomerData.xml");
                //dsStaticCatalog.WriteXml(@"C:\StaticCatalog.xml");
                //dsCalculation.WriteXml(@"C:\CalculationData.xml");
                //dsInvoiceData.WriteXml(@"C:\InvoiceData.xml");

                //Tinh hoa don
                //Kiem tra tham so G_INCHUNGRIENG = 'C' thi se ra hoa don TC
                if (dsStaticCatalog.Tables.Contains("S_PARAMETER") == true)
                {
                    if (dsStaticCatalog.Tables["S_PARAMETER"].Select("NAME = 'G_INCHUNGRIENG' AND (PRAVALUE = 'C' OR PRAVALUE = 'c')").Length > 0)
                    {
                        //if (isGTru)
                        //    strError = invoiceCalculation.InvoiceCalculation_TC_GTru(ref this.dsInvoiceData, this.dsCustomerData, this.dsCalculation, strMa_DViQLy, strMa_SoGCS, ky, thang, nam, dsStaticCatalog);
                        //else
                        strError = invoiceCalculation.InvoiceCalculation_TC(ref this.dsInvoiceData, this.dsCustomerData, this.dsCalculation, strMa_DViQLy, strMa_SoGCS, ky, thang, nam, dsStaticCatalog);
                    }
                    else
                    {
                        //if (isGTru)
                        //    strError = invoiceCalculation.InvoiceCalculation_GTru(ref this.dsInvoiceData, this.dsCustomerData, this.dsCalculation, strMa_DViQLy, strMa_SoGCS, ky, thang, nam, dsStaticCatalog);
                        //else
                        strError = invoiceCalculation.InvoiceCalculation_2022(ref this.dsInvoiceData, this.dsCustomerData, this.dsCalculation, strMa_DViQLy, strMa_SoGCS, ky, thang, nam, dsStaticCatalog);
                    }
                }
                else
                {
                    //if (isGTru)
                    //    strError = invoiceCalculation.InvoiceCalculation_GTru(ref this.dsInvoiceData, this.dsCustomerData, this.dsCalculation, strMa_DViQLy, strMa_SoGCS, ky, thang, nam, dsStaticCatalog);
                    //else
                    strError = invoiceCalculation.InvoiceCalculation_2022(ref this.dsInvoiceData, this.dsCustomerData, this.dsCalculation, strMa_DViQLy, strMa_SoGCS, ky, thang, nam, dsStaticCatalog);
                }
                if (strError != "")
                {
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsInvoiceCalculation", "InvoiceCalculation", "Tính hoá đơn - " + strError);
                    return strError;
                }
                else
                {
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsInvoiceCalculation", "InvoiceCalculation", "Tính xong hoá đơn");
                }


                //Cap nhat du lieu
                // DũngNT sửa ngày 19-10-2009: Gán thêm 2 biến phân luồng từ UI vào phương thức InsertInvoiceData, ban đầu đang fix cứng 65,8
                strError = outputDataWriting.InsertInvoiceDataPlus_2022(this.dsInvoiceData, this.dsCustomerData, strMa_DViQLy, strMa_SoGCS, strTen_DNhap, lngCurrentLibID, lngWorkflowID, ky, thang, nam);
                if (strError != "")
                {
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busOutputDataWriting", "busOutputDataWriting.cls_OutputDataWriting", "InsertInvoiceData", "Cập nhật dữ liệu hoá đơn - " + strError);
                    return strError;
                }
                else
                {
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busOutputDataWriting", "busOutputDataWriting.cls_OutputDataWriting", "InsertInvoiceData", "Cập nhật dữ liệu hoá đơn");
                }
               // inputDataReading.Dispose(); 
                //Ket thuc tinh hoa don
                WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "BillingImplementation", "BillingImplementation", "BillingImplementation", "Tính xong sổ " + strMa_SoGCS + " - hoá đơn kỳ " + ky + " tháng " + thang + "/" + nam);
                Console.WriteLine("Tính toán xong cho : " + strMa_SoGCS + "lúc  " + DateTime.Now.ToString("dd/MM/yyyy HH:mm:ss:ffff"));
                // Console.WriteLine("Kết thúc tính cho sổ : " + strMa_SoGCS +"  " + DateTime.Now.ToLongTimeString());
                return "";
            }
            catch (Exception ex)
            {
                //Ghi log vao dataset
                WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "BillingImplementation", "BillingImplementation", "BillingImplementation", "Lỗi khi tính hoá đơn: sổ " + strMa_SoGCS + " - hoá đơn kỳ " + ky + " tháng " + thang + "/" + nam);
                return ex.Message;
            }
            finally
            {
                dsCustomerData.Reset();
                dsInvoiceData.Reset();
                dsCalculation.Reset();
            }
        }
        #region Giam tru Covid-truong hop giam thang vao don gia
        private async Task<string> CommonImplementationPlus_GTruAsync(string strMa_DViQLy, string strMa_SoGCS, short ky, short thang, short nam, string strTen_DNhap, long lngCurrentLibID, long lngWorkflowID, string strNgayGhi)
        {
            try
            {
                dsCustomerData = new DataSet();
                dsInvoiceData = new DataSet();
                dsCalculation = new DS_CALCULATIONTABLES();

                string strError = "";

                //Bat dau tinh hoa don
                WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "BillingImplementation", "BillingImplementation", "BillingImplementation", "Tính hoá đơn sổ " + strMa_SoGCS + " - kỳ " + ky + " tháng " + thang + "/" + nam);
                //Kiem tra so phu ghep tong
                strError = await inputDataReading.checkPhuGhepTongPlusAsync(strMa_DViQLy, strMa_SoGCS, ky, thang, nam, strNgayGhi);
                if (strError != "" && strError != " ")
                {
                    if (strError == "1")
                    {
                        WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busInputDataReading", "busInputDataReading.cls_InputDataReading", "checkPhuGhepTong", "Kiểm tra sổ phụ ghép tổng.");
                        strError = outputDataWriting.InsertInvoiceData_PGT_Plus(strMa_DViQLy, strMa_SoGCS, strTen_DNhap, lngCurrentLibID, lngWorkflowID, ky, thang, nam);
                        if (strError != "")
                        {
                            WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busOutputDataWriting", "busOutputDataWriting.cls_OutputDataWriting", "InsertInvoiceData", "Cập nhật dữ liệu hoá đơn - " + strError);
                            return strError;
                        }
                        else
                        {
                            WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busOutputDataWriting", "busOutputDataWriting.cls_OutputDataWriting", "InsertInvoiceData", "Cập nhật dữ liệu hoá đơn");
                            WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "BillingImplementation", "BillingImplementation", "BillingImplementation", "Tính xong sổ " + strMa_SoGCS + " - hoá đơn kỳ " + ky + " tháng " + thang + "/" + nam);
                            return "";
                        }
                    }
                    else if (strError == "2")
                    {
                        WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busInputDataReading", "busInputDataReading.cls_InputDataReading", "checkPhuGhepTong", "Kiểm tra sổ phụ ghép tổng: Không tìm thấy dữ liệu trong HDG_DDO_SOGCS.");
                        return strError;
                    }
                    else
                    {
                        WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busInputDataReading", "busInputDataReading.cls_InputDataReading", "checkPhuGhepTong", "Kiểm tra sổ phụ ghép tổng - " + strError);
                        return strError;
                    }
                }
                else
                {
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busInputDataReading", "busInputDataReading.cls_InputDataReading", "checkPhuGhepTong", "Kiểm tra sổ phụ ghép tổng");
                }
                //Doc du lieu dau vao
                strError = inputDataReading.getCustomerDataReadingPlus(ref dsCustomerData, strMa_DViQLy, strMa_SoGCS, ky, thang, nam, strNgayGhi);
                //dsCustomerData.WriteXml("D:/CustomerDataAPI.xml", XmlWriteMode.WriteSchema);
                //dsCustomerData = new DataSet();
                //dsCustomerData.ReadXml("D:/CustomerDataAPI.xml", XmlReadMode.ReadSchema);
                if (strError != "")
                {
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busInputDataReading", "busInputDataReading.cls_InputDataReading", "getCustomerDataReading", "Đọc dữ liệu đầu vào - " + strError);
                    return strError;
                }
                else
                {
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busInputDataReading", "busInputDataReading.cls_InputDataReading", "getCustomerDataReading", "Đọc dữ liệu đầu vào");
                }
                //Loc bien ban ap gia, chi lay cac bien ban co lien quan den ky ghi chi so hien tai
                strError = this.FilterHDG_BBAN_APGIA(ref dsCustomerData);
                //decimal _tphu = dsCustomerData.Tables["HDG_QHE_DDO"].Select("MA_DDO_CHINH='PD05000093426001'").Length;
                if (strError != "")
                {
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "BillingImplementation", "BillingImplementation.clsBillingImplementation.cs", "FilterHDG_BBAN_APGIA", "Lọc biên bản áp giá - " + strError);
                    return strError;
                }
                else
                {
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "BillingImplementation", "BillingImplementation.clsBillingImplementation.cs", "FilterHDG_BBAN_APGIA", "Lọc biên bản áp giá");
                }
                //Đổi loại chỉ số CPK sang DDN, sau bỏ đi
                strError = this.AddColumn(ref dsCustomerData, dsStaticCatalog);
                if (strError != "")
                {
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "AddColumn", "Thêm cột mới - " + strError);
                    return strError;
                }
                //Đổi loại chỉ số CPK sang DDN để check valid data ko báo lỗi dup chỉ số
                //strError = this.ChangeCCS(ref dsCustomerData, "CPK", "DDN", dsStaticCatalog);
                //if (strError != "")
                //{
                //    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "AddColumn", "Tiền xử lý thông tin chỉ số - " + strError);
                //    return strError;
                //}
                //Kiem tra du lieu dau vao
                strError = inputDataReading.CheckValidData(strMa_DViQLy, this.dsCustomerData, clsBillingImplementation.dsStaticCatalog);
                if (strError != "")
                {
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busInputDataReading", "busInputDataReading.cls_InputDataReading", "CheckValidData", "Kiểm tra dữ liệu đầu vào - " + strError);
                    return strError;
                }
                else
                {
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busInputDataReading", "busInputDataReading.cls_InputDataReading", "CheckValidData", "Kiểm tra dữ liệu đầu vào");
                }
                //var temp = dsCustomerData.Tables["HDG_BBAN_APGIA"].Select("MA_DDO = 'PC12LL0890486001'");
                //Tao cau truc du lieu dau ra
                dsInvoiceData = cls_OutputDataWriting.createDSInvoiceData();
                DataSet dsInvoiceDataGC = cls_OutputDataWriting.createDSInvoiceData();
                DataSet dsCustomerDataGC = dsCustomerData.Copy();
                DataSet dsDMuc = dsStaticCatalog.Copy();
                //Tính toán số liệu hóa đơn giá mới
                strError = this.CommonCalculationNoInsert(strMa_DViQLy, strMa_SoGCS, ky, thang, nam, strTen_DNhap, lngCurrentLibID, lngWorkflowID, strNgayGhi, dsCustomerData, dsStaticCatalog, ref dsInvoiceData, false);
                if (strError != "")
                {
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "BillingImplementation", "BillingImplementation.CommonCalculationNoInsert", "CommonCalculationNoInsert", "Tính hóa đơn lần 1 - " + strError);
                    return strError;
                }
                else
                {
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "BillingImplementation", "BillingImplementation.CommonCalculationNoInsert", "CommonCalculationNoInsert", "Tính hóa đơn lần 1");
                }
                this.CommonCalculationNoInsert(strMa_DViQLy, strMa_SoGCS, ky, thang, nam, strTen_DNhap, lngCurrentLibID, lngWorkflowID, strNgayGhi, dsCustomerDataGC, dsDMuc, ref dsInvoiceDataGC, true);
                if (strError != "")
                {
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "BillingImplementation", "BillingImplementation.CommonCalculationNoInsert", "CommonCalculationNoInsert", "Tính hóa đơn lần 2 - " + strError);
                    return strError;
                }
                else
                {
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "BillingImplementation", "BillingImplementation.CommonCalculationNoInsert", "CommonCalculationNoInsert", "Tính hóa đơn lần 2");
                }



                //Cap nhat du lieu
                // DũngNT sửa ngày 19-10-2009: Gán thêm 2 biến phân luồng từ UI vào phương thức InsertInvoiceData, ban đầu đang fix cứng 65,8
                strError = outputDataWriting.InsertInvoiceDataPlus_Gtru(dsInvoiceData, dsInvoiceDataGC, dsCustomerData, strMa_DViQLy, strMa_SoGCS, strTen_DNhap, lngCurrentLibID, lngWorkflowID, ky, thang, nam, strNgayGhi, config);
                if (strError != "")
                {
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busOutputDataWriting", "busOutputDataWriting.cls_OutputDataWriting", "InsertInvoiceData", "Cập nhật dữ liệu hoá đơn - " + strError);
                    return strError;
                }
                else
                {
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busOutputDataWriting", "busOutputDataWriting.cls_OutputDataWriting", "InsertInvoiceData", "Cập nhật dữ liệu hoá đơn");
                }

                //Ket thuc tinh hoa don
                WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "BillingImplementation", "BillingImplementation", "BillingImplementation", "Tính xong sổ " + strMa_SoGCS + " - hoá đơn kỳ " + ky + " tháng " + thang + "/" + nam);

                return "";
            }
            catch (Exception ex)
            {
                //Ghi log vao dataset
                WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "BillingImplementation", "BillingImplementation", "BillingImplementation", "Lỗi khi tính hoá đơn: sổ " + strMa_SoGCS + " - hoá đơn kỳ " + ky + " tháng " + thang + "/" + nam);
                return ex.Message;
            }
            finally
            {
                dsCustomerData.Reset();
                dsInvoiceData.Reset();
                dsCalculation.Reset();
            }
        }

        private string CommonCalculationNoInsert(string strMa_DViQLy, string strMa_SoGCS, short ky, short thang, short nam, string strTen_DNhap, long lngCurrentLibID, long lngWorkflowID, string strNgayGhi, DataSet dsCustomerData, DataSet dsDMuc, ref DataSet dsInvoiceData, bool isGiaCu)
        {

            try
            {
                string strError = "";
                DateTime dtOld = config.dtGiaHTro;
                DateTime dtNew = new DateTime(2090, 1, 1);
                dsCalculation = new DS_CALCULATIONTABLES();
                //var temp = dsCustomerData.Tables["HDG_BBAN_APGIA"].Select("MA_DDO = 'PC12LL0890486001'");
                //Tao cau truc du lieu dau ra
                dsInvoiceData = cls_OutputDataWriting.createDSInvoiceData();
                if (isGiaCu)
                {

                    ChangeDate(ref dsDMuc, dtOld, dtNew);

                }
                //Bổ sung lấy danh sách sổ hỗ trợ
                if (dsDMuc.Tables.Contains("D_SO_HOTRO_COVID") && dsDMuc.Tables["D_SO_HOTRO_COVID"].Rows.Count > 0)
                {
                    config.lstDViHTro = new List<string>();
                    for (int i = 0; i < dsDMuc.Tables["D_SO_HOTRO_COVID"].Rows.Count; i++)
                    {
                        if (dsDMuc.Tables["D_SO_HOTRO_COVID"].Rows[i]["TRANG_THAI"].ToString().Trim() == "1" && dsDMuc.Tables["D_SO_HOTRO_COVID"].Rows[i]["MA_DVIQLY"].ToString().Trim() == strMa_DViQLy)
                            config.lstSoHTro.Add(dsDMuc.Tables["D_SO_HOTRO_COVID"].Rows[i]["MA_SOGCS"].ToString());
                    }
                }
                //Bổ sung lấy danh sách đơn vị hỗ trợ
                if (dsDMuc.Tables.Contains("D_DVI_HOTRO_COVID") && dsDMuc.Tables["D_DVI_HOTRO_COVID"].Rows.Count > 0)
                {
                    config.lstDViHTro = new List<string>();
                    for (int i = 0; i < dsDMuc.Tables["D_DVI_HOTRO_COVID"].Rows.Count; i++)
                    {
                        if (dsDMuc.Tables["D_DVI_HOTRO_COVID"].Rows[i]["TRANG_THAI"].ToString().Trim() == "1")
                            config.lstDViHTro.Add(dsDMuc.Tables["D_DVI_HOTRO_COVID"].Rows[i]["MA_DVIQLY"].ToString());
                    }
                }
                //Tinh san luong tho
                strError = rawDataCalculation.RawDataCalculation_1(dsCustomerData);
                if (strError != "")
                {
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "RawDataCalculation_1", "Tính sản lượng thô - " + strError);
                    return strError;
                }
                else
                {
                    //Kiem tra neu khong con du lieu chi so thi thong bao ket thuc tinh hoa don luon
                    if (this.dsCustomerData.Tables.Contains("GCS_CHISO") == true)
                    {
                        if (this.dsCustomerData.Tables["GCS_CHISO"].Rows.Count == 0)
                        {
                            //Ket thuc tinh hoa don
                            WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "BillingImplementation", "BillingImplementation", "BillingImplementation", "Sổ không có dữ liệu chỉ số. Kết thúc tính hoá đơn của sổ " + strMa_SoGCS + " - kỳ " + ky + " tháng " + thang + "/" + nam);
                            return "";
                        }
                    }
                }

                strError = rawDataCalculation.RawDataCalculation_2(dsCustomerData);
                if (strError != "")
                {
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "RawDataCalculation_2", "Tính sản lượng thô - " + strError);
                    return strError;
                }

                strError = rawDataCalculation.RawDataCalculation_3(dsCustomerData);
                if (strError != "")
                {
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "RawDataCalculation_3", "Tính sản lượng thô - " + strError);
                    return strError;
                }
                //Đổi loại phản kháng về CCS, đổi CCS tạm sang DDN
                //strError = this.ChangeCCS(ref dsCustomerData, "CPK", "CCS", dsStaticCatalog);
                //if (strError != "")
                //{
                //    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "AddColumn", "Tiền xử lý thông tin chỉ số - " + strError);
                //    return strError;
                //}
                strError = rawDataCalculation.RawDataCalculation_4(dsCustomerData, dsStaticCatalog);
                if (strError != "")
                {
                    //Rieng phan tinh tru phu chi ghi log, khong can thoat
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "RawDataCalculation_4", "Tính sản lượng thô - " + strError);
                    return strError;
                }
                //Đổi áp giá về DDN để tính cosfi
                //strError = this.ChangeCCS(ref dsCustomerData, "CCS", "DDN", dsStaticCatalog);
                //if (strError != "")
                //{
                //    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "AddColumn", "Tiền xử lý thông tin chỉ số - " + strError);
                //    return strError;
                //}
                strError = rawDataCalculation.RawDataCalculation_5(dsCustomerData, dsInvoiceData, dsDMuc, strTen_DNhap);
                if (strError != "")
                {
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "RawDataCalculation_5", "Tính sản lượng thô - " + strError);
                    return strError;
                }

                strError = rawDataCalculation.RawDataCalculation_6(dsCustomerData, dsInvoiceData, dsDMuc, strTen_DNhap);
                if (strError != "")
                {
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "RawDataCalculation_6", "Tính sản lượng thô - " + strError);
                    return strError;
                }

                strError = rawDataCalculation.RawDataCalculation_7(dsCustomerData, dsInvoiceData, dsDMuc, strTen_DNhap);
                if (strError != "")
                {
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "RawDataCalculation_7", "Tính sản lượng thô - " + strError);
                    return strError;
                }
                WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "RawDataCalculation_7", "Tính xong sản lượng thô");
                //đã tính xong cosfi, đổi CCS trả về CCS, CPK về DDN
                //strError = this.ChangeCCS(ref dsCustomerData, "CPK", "DDN", dsStaticCatalog);
                //if (strError != "")
                //{
                //    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "AddColumn", "Tiền xử lý thông tin chỉ số - " + strError);
                //    return strError;
                //}
                //strError = this.ChangeCCS(ref dsCustomerData, "CCS", "CCS", dsStaticCatalog);
                //if (strError != "")
                //{
                //    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "AddColumn", "Tiền xử lý thông tin chỉ số - " + strError);
                //    return strError;
                //}
                //Tinh san luong chi tiet
                strError = detailDataCalculation.DetailDataCalculation_11(dsCustomerData, ref dsCalculation, dsDMuc);
                if (strError != "")
                {
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_11", "Tính sản lượng chi tiết - " + strError);
                    return strError;
                }

                strError = detailDataCalculation.DetailDataCalculation_2(dsCalculation, dsCustomerData, dsDMuc);
                if (strError != "")
                {
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_2", "Tính sản lượng chi tiết - " + strError);
                    return strError;
                }
                //Xu ly cho KH tinh vat qua doi gia
                strError = detailDataCalculation.DetailDataCalculation_21(dsCalculation, dsCustomerData, dsDMuc);
                if (strError != "")
                {
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_21", "Tính sản lượng chi tiết trong kỳ đổi giá - " + strError);
                    return strError;
                }

                strError = detailDataCalculation.DetailDataCalculation_3(dsCalculation, dsCustomerData, dsDMuc);
                if (strError != "")
                {
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_3", "Tính sản lượng chi tiết - " + strError);
                    return strError;
                }

                strError = detailDataCalculation.DetailDataCalculation_40(dsCalculation, dsCustomerData, dsDMuc, dsInvoiceData, strMa_DViQLy, strMa_SoGCS, ky, thang, nam);
                if (strError != "")
                {
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_4", "Tính sản lượng chi tiết - " + strError);
                    return strError;
                }
                WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_4", "Tính xong sản lượng chi tiết");

                strError = detailDataCalculation.DetailDataCalculation_41(dsCalculation, dsDMuc, dsCustomerData);
                if (strError != "")
                {
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_41", "Tính sản lượng chi tiết cho bán buôn trong kỳ đổi giá - " + strError);
                    return strError;
                }
                WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_41", "Tính sản lượng chi tiết cho bán buôn trong kỳ đổi giá");

                strError = detailDataCalculation.DetailDataCalculation_42(dsCalculation);
                if (strError != "")
                {
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_42", "Xử lý riêng cho bán buôn SHBB nhóm L trong tháng 03/2011 - " + strError);
                    return strError;
                }
                WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_42", "Xử lý riêng cho bán buôn SHBB nhóm L trong tháng 03/2011");


                //Xac dinh don gia
                //strError = priceSpecification.PriceSpecification_1(this.dsCalculation, clsBillingImplementation.dsStaticCatalog, dsCustomerData);
                //Xu ly cho KH tinh qua doi gia
                //Ko làm tròn ngang cho nhóm bậc thang
                strError = priceSpecification.PriceSpecification_11(dsCalculation, dsDMuc, dsCustomerData);
                if (strError != "")
                {
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsPriceSpecification", "PriceSpecification_1", "Xử lý riêng cho giá bậc thang - " + strError);
                    return strError;
                }

                //Xu ly lam tron ngang cho gia bac thang
                strError = priceSpecification.PriceSpecification_12(dsCalculation, dsDMuc, dsCustomerData);
                if (strError != "")
                {
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsPriceSpecification", "PriceSpecification_12", "Xử lý riêng cho giá bậc thang trong kỳ đổi giá - " + strError);
                    return strError;
                }

                //strError = priceSpecification.PriceSpecification_2(this.dsCalculation, clsBillingImplementation.dsStaticCatalog);
                //Xu ly cho KH tinh qua doi gia
                strError = priceSpecification.PriceSpecification_21(dsCalculation, dsDMuc, this.dsCustomerData);
                if (strError != "")
                {
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsPriceSpecification", "PriceSpecification_2", "Xác định đơn giá - " + strError);
                    return strError;
                }

                strError = priceSpecification.PriceSpecification_3(dsCalculation, dsCustomerData, dsInvoiceData, strTen_DNhap, strNgayGhi, ky, thang, nam);
                if (strError != "")
                {
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsPriceSpecification", "PriceSpecification_3", "Xác định đơn giá - " + strError);
                    return strError;
                }
                WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsPriceSpecification", "PriceSpecification_3", "Kết thúc xác định đơn giá");
                //bool isGTru = false;
                //Chỉ tính lại số tiền khi G_COVID = 1
                //if (dsStaticCatalog.Tables.Contains("S_PARAMETER") == true)
                //{
                //    if (dsStaticCatalog.Tables["S_PARAMETER"].Select("NAME = 'G_COVID' AND PRAVALUE = '1'").Length > 0)
                //    {

                //isGTru = true;

                if (config.isHTroThang && config.lstTNamHTro.Contains(thang + "/" + nam))
                {
                    strError = priceSpecification.PriceSpecification_4(dsCustomerData, dsInvoiceData, strMa_SoGCS, strNgayGhi, config, isGiaCu);
                    if (strError != "")
                    {
                        WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsPriceSpecification", "PriceSpecification_4", "Tính toán giảm trừ - " + strError);
                        return strError;
                    }
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsPriceSpecification", "PriceSpecification_4", "Kết thúc tính toán giảm trừ");
                }

                //    }
                //}

                //dsCustomerData.WriteXml(@"C:\CustomerData.xml");
                //dsStaticCatalog.WriteXml(@"C:\StaticCatalog.xml");
                //dsCalculation.WriteXml(@"C:\CalculationData.xml");
                //dsInvoiceData.WriteXml(@"C:\InvoiceData.xml");

                //Tinh hoa don
                //Kiem tra tham so G_INCHUNGRIENG = 'C' thi se ra hoa don TC
                if (dsStaticCatalog.Tables.Contains("S_PARAMETER") == true)
                {
                    if (dsStaticCatalog.Tables["S_PARAMETER"].Select("NAME = 'G_INCHUNGRIENG' AND (PRAVALUE = 'C' OR PRAVALUE = 'c')").Length > 0)
                    {
                        if (config.isHTroThang && config.lstTNamHTro.Contains(thang + "/" + nam) && isGiaCu == false)
                            strError = invoiceCalculation.InvoiceCalculation_TC_GTru(ref dsInvoiceData, dsCustomerData, dsCalculation, strMa_DViQLy, strMa_SoGCS, ky, thang, nam, dsDMuc, config);
                        else
                            strError = invoiceCalculation.InvoiceCalculation_TC(ref dsInvoiceData, dsCustomerData, dsCalculation, strMa_DViQLy, strMa_SoGCS, ky, thang, nam, dsDMuc);
                    }
                    else
                    {
                        if (config.isHTroThang && config.lstTNamHTro.Contains(thang + "/" + nam) && isGiaCu == false)
                            strError = invoiceCalculation.InvoiceCalculation_GTru(ref dsInvoiceData, dsCustomerData, dsCalculation, strMa_DViQLy, strMa_SoGCS, ky, thang, nam, dsDMuc, config);
                        else
                            strError = invoiceCalculation.InvoiceCalculation(ref dsInvoiceData, dsCustomerData, dsCalculation, strMa_DViQLy, strMa_SoGCS, ky, thang, nam, dsDMuc);
                    }
                }
                else
                {
                    if (config.isHTroThang && config.lstTNamHTro.Contains(thang + "/" + nam) && isGiaCu == false)
                        strError = invoiceCalculation.InvoiceCalculation_GTru(ref dsInvoiceData, dsCustomerData, dsCalculation, strMa_DViQLy, strMa_SoGCS, ky, thang, nam, dsDMuc, config);
                    else
                        strError = invoiceCalculation.InvoiceCalculation(ref dsInvoiceData, dsCustomerData, dsCalculation, strMa_DViQLy, strMa_SoGCS, ky, thang, nam, dsDMuc);
                }
                if (strError != "")
                {
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsInvoiceCalculation", "InvoiceCalculation", "Tính hoá đơn - " + strError);
                    return strError;
                }
                else
                {
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsInvoiceCalculation", "InvoiceCalculation", "Tính xong hoá đơn");
                }



                return "";
            }
            catch (Exception ex)
            {
                //Ghi log vao dataset
                WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "BillingImplementation", "BillingImplementation", "BillingImplementation", "Lỗi khi tính hoá đơn: sổ " + strMa_SoGCS + " - hoá đơn kỳ " + ky + " tháng " + thang + "/" + nam);
                return ex.Message;
            }
            finally
            {
                //dsCustomerData.Reset();
                //dsInvoiceData.Reset();
                //dsCalculation.Reset();
            }
        }
        private string CommonImplementationKH_GTru(string strMa_DViQLy, string strMa_SoGCS, string strMaKHang, short ky, short thang, short nam, string strTen_DNhap, long lngCurrentLibID, long lngWorkflowID, string strNgayGhi)
        {
            try
            {
                dsCustomerData = new DataSet();
                dsInvoiceData = new DataSet();
                dsCalculation = new DS_CALCULATIONTABLES();

                string strError = "";

                //Bat dau tinh hoa don
                WriteLogPlus(strMa_DViQLy, strMaKHang, "BillingImplementation", "BillingImplementation", "BillingImplementation", "Tính hoá đơn cho khách hàng " + strMaKHang + " - kỳ " + ky + " tháng " + thang + "/" + nam);

                //Doc du lieu dau vao
                strError = inputDataReading.getCustomerDataReadingKH(ref dsCustomerData, strMa_DViQLy, strMa_SoGCS, strMaKHang, ky, thang, nam, strNgayGhi);
                //dsCustomerData.WriteXml("D:/TinhHoaDon.xml", XmlWriteMode.WriteSchema);
                if (strError != "")
                {
                    WriteLogPlus(strMa_DViQLy, strMaKHang, "busInputDataReading", "busInputDataReading.cls_InputDataReading", "getCustomerDataReading", "Đọc dữ liệu đầu vào - " + strError);
                    return strError;
                }
                else
                {
                    WriteLogPlus(strMa_DViQLy, strMaKHang, "busInputDataReading", "busInputDataReading.cls_InputDataReading", "getCustomerDataReading", "Đọc dữ liệu đầu vào");
                }
                //Loc bien ban ap gia, chi lay cac bien ban co lien quan den ky ghi chi so hien tai
                strError = this.FilterHDG_BBAN_APGIA(ref dsCustomerData);
                //decimal _tphu = dsCustomerData.Tables["HDG_QHE_DDO"].Select("MA_DDO_CHINH='PD05000093426001'").Length;
                if (strError != "")
                {
                    WriteLogPlus(strMa_DViQLy, strMaKHang, "BillingImplementation", "BillingImplementation.clsBillingImplementation.cs", "FilterHDG_BBAN_APGIA", "Lọc biên bản áp giá - " + strError);
                    return strError;
                }
                else
                {
                    WriteLogPlus(strMa_DViQLy, strMaKHang, "BillingImplementation", "BillingImplementation.clsBillingImplementation.cs", "FilterHDG_BBAN_APGIA", "Lọc biên bản áp giá");
                }
                //Đổi loại chỉ số CPK sang DDN, sau bỏ đi
                strError = this.AddColumn(ref dsCustomerData, dsStaticCatalog);
                if (strError != "")
                {
                    WriteLogPlus(strMa_DViQLy, strMaKHang, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "AddColumn", "Thêm cột mới - " + strError);
                    return strError;
                }

                //Kiem tra du lieu dau vao
                strError = inputDataReading.CheckValidData(strMa_DViQLy, this.dsCustomerData, clsBillingImplementation.dsStaticCatalog);
                if (strError != "")
                {
                    WriteLogPlus(strMa_DViQLy, strMaKHang, "busInputDataReading", "busInputDataReading.cls_InputDataReading", "CheckValidData", "Kiểm tra dữ liệu đầu vào - " + strError);
                    return strError;
                }
                else
                {
                    WriteLogPlus(strMa_DViQLy, strMaKHang, "busInputDataReading", "busInputDataReading.cls_InputDataReading", "CheckValidData", "Kiểm tra dữ liệu đầu vào");
                }
                //var temp = dsCustomerData.Tables["HDG_BBAN_APGIA"].Select("MA_DDO = 'PC12LL0890486001'");
                //Tao cau truc du lieu dau ra
                dsInvoiceData = cls_OutputDataWriting.createDSInvoiceData();
                DataSet dsInvoiceDataGC = cls_OutputDataWriting.createDSInvoiceData();
                DataSet dsCustomerDataGC = dsCustomerData.Copy();
                DataSet dsDMuc = dsStaticCatalog.Copy();

                //Tính toán số liệu hóa đơn giá mới
                strError = this.CommonCalculationNoInsert(strMa_DViQLy, strMa_SoGCS, ky, thang, nam, strTen_DNhap, lngCurrentLibID, lngWorkflowID, strNgayGhi, dsCustomerData, dsStaticCatalog, ref dsInvoiceData, false);
                if (strError != "")
                {
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "BillingImplementation", "BillingImplementation.CommonCalculationNoInsert", "CommonCalculationNoInsert", "Tính hóa đơn lần 1 - " + strError);
                    return strError;
                }
                else
                {
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "BillingImplementation", "BillingImplementation.CommonCalculationNoInsert", "CommonCalculationNoInsert", "Tính hóa đơn lần 1");
                }
                this.CommonCalculationNoInsert(strMa_DViQLy, strMa_SoGCS, ky, thang, nam, strTen_DNhap, lngCurrentLibID, lngWorkflowID, strNgayGhi, dsCustomerDataGC, dsDMuc, ref dsInvoiceDataGC, true);
                if (strError != "")
                {
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "BillingImplementation", "BillingImplementation.CommonCalculationNoInsert", "CommonCalculationNoInsert", "Tính hóa đơn lần 2 - " + strError);
                    return strError;
                }
                else
                {
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "BillingImplementation", "BillingImplementation.CommonCalculationNoInsert", "CommonCalculationNoInsert", "Tính hóa đơn lần 2");
                }

                //Cap nhat du lieu
                // DũngNT sửa ngày 19-10-2009: Gán thêm 2 biến phân luồng từ UI vào phương thức InsertInvoiceData, ban đầu đang fix cứng 65,8
                strError = outputDataWriting.InsertInvoiceDataKH_Gtru(dsInvoiceData, dsInvoiceDataGC, dsCustomerData, strMa_DViQLy, strMa_SoGCS, strTen_DNhap, lngCurrentLibID, lngWorkflowID, ky, thang, nam);
                if (strError != "")
                {
                    WriteLogPlus(strMa_DViQLy, strMaKHang, "busOutputDataWriting", "busOutputDataWriting.cls_OutputDataWriting", "InsertInvoiceData", "Cập nhật dữ liệu hoá đơn - " + strError);
                    return strError;
                }
                else
                {
                    WriteLogPlus(strMa_DViQLy, strMaKHang, "busOutputDataWriting", "busOutputDataWriting.cls_OutputDataWriting", "InsertInvoiceData", "Cập nhật dữ liệu hoá đơn");
                }

                //Ket thuc tinh hoa don
                WriteLogPlus(strMa_DViQLy, strMaKHang, "BillingImplementation", "BillingImplementation", "BillingImplementation", "Tính xong khách hàng " + strMaKHang + " - hoá đơn lẻ kỳ " + ky + " tháng " + thang + "/" + nam);

                return "";
            }
            catch (Exception ex)
            {
                //Ghi log vao dataset
                WriteLogPlus(strMa_DViQLy, strMaKHang, "BillingImplementation", "BillingImplementation", "BillingImplementation", "Lỗi khi tính hoá đơn: khách hàng " + strMaKHang + " - hoá đơn lẻ kỳ " + ky + " tháng " + thang + "/" + nam);
                return ex.Message;
            }
            finally
            {
                dsCustomerData.Reset();
                dsInvoiceData.Reset();
                dsCalculation.Reset();
            }
        }

        #endregion
        private void WriteLogPlus(string strMa_DViQLy, string strMa_SoGCS, string strAssemblyName, string strNamespace, string strMethodName, string strDetail)
        {
            //Debug.WriteLine(DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff") + strDetail);
            DataRow drLog;
            drLog = dsLog.Tables["S_LOG"].NewRow();
            drLog["SUBDIVISIONID"] = strMa_DViQLy;
            drLog["LOGID"] = 1;
            drLog["BOOKID"] = strMa_SoGCS;
            drLog["ASSEMBLYNAME"] = strAssemblyName;
            drLog["NAMESPACE"] = strNamespace;
            drLog["METHODNAME"] = strMethodName;
            drLog["TIME"] = System.DateTime.Now;
            if (strDetail.Length > 500)
            {
                strDetail = strDetail.Substring(0, 500);
            }
            drLog["DETAIL"] = strDetail;
            dsLog.Tables["S_LOG"].Rows.Add(drLog);
            dsLog.Tables["S_LOG"].AcceptChanges();

            log = new cls_LogManagement(dsLog);
            log.InsertListPlus(dsLog);
            dsLog.Tables["S_LOG"].Rows.Clear();
        }
        public static string GetStaticDataPlus()
        {
            LOGDATA dsLogData = new LOGDATA();
            DataRow drLog;
            cls_LogManagement log;

            try
            {
                string strError = "";

                cls_InputDataReading inputStatic = new cls_InputDataReading();

                if (clsBillingImplementation.dsStaticCatalog == null)
                {
                    clsBillingImplementation.dsStaticCatalog = new DataSet();
                }

                drLog = dsLogData.Tables["S_LOG"].NewRow();
                drLog["SUBDIVISIONID"] = "*";
                drLog["LOGID"] = 1;
                drLog["BOOKID"] = "*";
                drLog["ASSEMBLYNAME"] = "busInputDataReading";
                drLog["NAMESPACE"] = "busInputDataReading.cls_InputDataReading";
                drLog["METHODNAME"] = "getStaticCatalogDataReading";
                drLog["TIME"] = System.DateTime.Now;
                drLog["DETAIL"] = "Khởi tạo dữ liệu";
                dsLogData.Tables["S_LOG"].Rows.Add(drLog);

                //Doc du lieu danh muc - Chi lay danh muc khi chua doc lan nao
                if (clsBillingImplementation.dsStaticCatalog.Tables.Count == 0)
                {
                    strError = inputStatic.getStaticCatalogDataReadingPlus(ref clsBillingImplementation.dsStaticCatalog);
                    if (strError != "")
                    {
                        drLog = dsLogData.Tables["S_LOG"].NewRow();
                        drLog["SUBDIVISIONID"] = "*";
                        drLog["LOGID"] = 1;
                        drLog["BOOKID"] = "*";
                        drLog["ASSEMBLYNAME"] = "busInputDataReading";
                        drLog["NAMESPACE"] = "busInputDataReading.cls_InputDataReading";
                        drLog["METHODNAME"] = "getStaticCatalogDataReading";
                        drLog["TIME"] = System.DateTime.Now;
                        drLog["DETAIL"] = "Đọc dữ liệu danh mục - " + strError;
                        dsLogData.Tables["S_LOG"].Rows.Add(drLog);

                        log = new cls_LogManagement(dsLogData);
                        log.InsertListPlus(dsLogData);
                        return strError;
                    }
                    else
                    {
                        drLog = dsLogData.Tables["S_LOG"].NewRow();
                        drLog["SUBDIVISIONID"] = "*";
                        drLog["LOGID"] = 1;
                        drLog["BOOKID"] = "*";
                        drLog["ASSEMBLYNAME"] = "busInputDataReading";
                        drLog["NAMESPACE"] = "busInputDataReading.cls_InputDataReading";
                        drLog["METHODNAME"] = "getStaticCatalogDataReading";
                        drLog["TIME"] = System.DateTime.Now;
                        drLog["DETAIL"] = "Đọc dữ liệu danh mục";
                        dsLogData.Tables["S_LOG"].Rows.Add(drLog);
                        log = new cls_LogManagement(dsLogData);
                        log.InsertListPlus(dsLogData);
                    }
                }

                return "";
            }
            catch (Exception ex)
            {
                drLog = dsLogData.Tables["S_LOG"].NewRow();
                drLog["SUBDIVISIONID"] = "*";
                drLog["LOGID"] = 1;
                drLog["BOOKID"] = "*";
                drLog["ASSEMBLYNAME"] = "busInputDataReading";
                drLog["NAMESPACE"] = "busInputDataReading.cls_InputDataReading";
                drLog["METHODNAME"] = "getStaticCatalogDataReading";
                drLog["TIME"] = System.DateTime.Now;
                drLog["DETAIL"] = "Lỗi khi khởi tạo dữ liệu danh mục: " + ex.Message;
                dsLogData.Tables["S_LOG"].Rows.Add(drLog);

                log = new cls_LogManagement(dsLogData);
                log.InsertListPlus(dsLogData);

                return "Lỗi khi khởi tạo dữ liệu danh mục: " + ex.Message;
            }
        }
        public List<string> getMaSoGCSPlus(int i32LoaiTTac)
        {

            try
            {
                string strError = "";
                cls_LogManagement log = new cls_LogManagement();
                return log.getMaSoGCSPlus("Chưa tính", "Đang tính", i32LoaiTTac, ref strError);

            }
            catch
            {
                return null;
            }
            finally
            {


            }
        }
        public async Task<bool> DeleteMaSoGCSPlus(string strMaDViQLy, string strMaSoGCS, Int64 i64ID_HDON, Int16 i16Ky, Int16 i16Thang, Int16 i16Nam, string strTrangThai, int i32LoaiTTac)
        {
            try
            {
                cls_LogManagement log = new cls_LogManagement();
                string strError = "";
                bool ok = log.DeleteMaSoGCSPlus(strMaDViQLy, strMaSoGCS, i64ID_HDON, i16Ky, i16Thang, i16Nam, strTrangThai, i32LoaiTTac, ref strError);
                if (strError.Trim().Length > 0)
                {
                    WriteLogPlus(strMaDViQLy, strMaSoGCS, "LogManagementObject", "LogManagementObject.cls_HDN_DSACH_SOTHD_Controller.cs", "DeleteMaSoGCS", strError);
                    return false;
                }
                else
                    return true;

            }
            catch (Exception ex)
            {
                WriteLogPlus(strMaDViQLy, strMaSoGCS, "busLogManagement", "busLogManagement.cls_DSachSo_THD", "DeleteMaSoGCS", ex.Message);
                return false;
            }
            finally
            {

            }
        }
        public string BillingImplementationDCPlus(string strMaDViQLy, string strMaSoGCS, Int16 i16Ky, Int16 i16Thang, Int16 i16Nam, string strIDHDon)
        {
            try
            {
                DataSet dsCustomerData = new DataSet();
                string strError = "";

                cls_InputDataReading inputStatic = new cls_InputDataReading();

                dsCustomerData = inputStatic.GetDataDChinhPlus(strMaDViQLy, strMaSoGCS, i16Ky, i16Thang, i16Nam, strIDHDon, ref strError);

                if (strError.Trim().Length > 0)
                {
                    return strError;
                }
                if (dsCustomerData.Tables.Contains("LST_TIEN_TRINH") && dsCustomerData.Tables["LST_TIEN_TRINH"].Rows.Count > 0)
                {
                    //Gọi hàm tính mới
                    strError = CommonImplementation_KTGS(dsCustomerData, i16Thang, i16Nam);
                    if (strError.Trim().Length > 0)
                    {
                        return strError;
                    }
                }
                else
                {
                    //Gọi hàm tính cũ
                    strError = CommonImplementation_SS_Plus(dsCustomerData, i16Thang, i16Nam);
                    if (strError.Trim().Length > 0)
                    {
                        return strError;
                    }
                }

                return "";
            }
            catch (Exception ex)
            {
                return "Lỗi khi tính hóa đơn điều chỉnh: " + ex.Message;
            }
        }
        public void ChangeDate(ref DataSet dsDMuc, DateTime dtOld, DateTime dtNew)
        {
            DateTime ngayadung = new DateTime(1900, 1, 1);
            DateTime ngayhhluc = new DateTime(1900, 1, 1);
            foreach (DataRow dr in dsDMuc.Tables["D_GIA_NHOMNN"].Rows)
            {
                ngayadung = DateTime.Parse(Convert.ToDateTime(dr["NGAY_ADUNG"]).ToString("dd/MM/yyyy", new System.Globalization.CultureInfo("vi-VN")), new System.Globalization.CultureInfo("vi-VN"));

                if (ngayadung == dtOld)
                    dr["NGAY_ADUNG"] = dtNew;
                else if (dr["NGAY_HETHLUC"].ToString().Trim().Length > 0)
                {
                    ngayhhluc = DateTime.Parse(Convert.ToDateTime(dr["NGAY_HETHLUC"]).ToString("dd/MM/yyyy", new System.Globalization.CultureInfo("vi-VN")), new System.Globalization.CultureInfo("vi-VN"));
                    if (ngayhhluc == dtOld.AddDays(-1))
                        dr["NGAY_HETHLUC"] = dtNew.AddDays(-1);

                }

            }
            foreach (DataRow dr in dsDMuc.Tables["D_BAC_THANG"].Rows)
            {
                ngayadung = DateTime.Parse(Convert.ToDateTime(dr["NGAY_HLUC"]).ToString("dd/MM/yyyy", new System.Globalization.CultureInfo("vi-VN")), new System.Globalization.CultureInfo("vi-VN"));

                if (ngayadung == dtOld)
                    dr["NGAY_HLUC"] = dtNew;
                else if (dr["NGAY_HHLUC"].ToString().Trim().Length > 0)
                {
                    ngayhhluc = DateTime.Parse(Convert.ToDateTime(dr["NGAY_HHLUC"]).ToString("dd/MM/yyyy", new System.Globalization.CultureInfo("vi-VN")), new System.Globalization.CultureInfo("vi-VN"));
                    if (ngayhhluc == dtOld.AddDays(-1))
                        dr["NGAY_HHLUC"] = dtNew.AddDays(-1);

                }
            }
        }
        public string CommonImplementation_SS_Plus(DataSet dsCustomerData, Int16 i16Thang, Int16 i16Nam)
        {
            string strMa_DViQLy = "";
            string strMa_SoGCS = "";
            Int16 ky = 0;
            Int16 thang = 0;
            Int16 nam = 0;
            Int16 iLoaiKHang = 0;
            string strTen_DNhap = "";

            DataSet dsDMuc = null;
            DateTime dtHLucNSH = new DateTime(2020, 4, 16);
            DateTime dtHLucSH = new DateTime(2020, 5, 1);
            DateTime dtHetHLucNSH = new DateTime(2020, 7, 16);
            DateTime dtHetHLucSH = new DateTime(2020, 8, 1);
            DateTime dtOld = config.dtGiaHTro;
            DateTime dtNew = new DateTime(2090, 8, 1);
            Int16 thangps = 0, namps = 0;
            DateTime dtNgayCKy = new DateTime(1900, 1, 1);
            //const Int16 thangStart = 4, thangEnd = 6, namHLuc = 2020;

            try
            {
                dsCalculation = new DS_CALCULATIONTABLES();
                //dsCustomerData = new DataSet();
                dsInvoiceData = new DataSet();

                GetStaticDataPlus();


                log = new cls_LogManagement(dsLog);

                strMa_DViQLy = dsCustomerData.Tables["HDN_HDON"].Rows[0]["MA_DVIQLY"].ToString();
                strMa_SoGCS = dsCustomerData.Tables["HDN_HDON"].Rows[0]["MA_SOGCS"].ToString();
                ky = Convert.ToInt16(dsCustomerData.Tables["HDN_HDON"].Rows[0]["KY"]);
                thangps = Convert.ToInt16(dsCustomerData.Tables["HDN_HDON"].Rows[0]["THANG"]);
                namps = Convert.ToInt16(dsCustomerData.Tables["HDN_HDON"].Rows[0]["NAM"]);
                iLoaiKHang = Convert.ToInt16(dsCustomerData.Tables["HDN_HDON"].Rows[0]["LOAI_KHANG"]);
                dtNgayCKy = Convert.ToDateTime(dsCustomerData.Tables["HDN_HDON"].Rows[0]["NGAY_CKY"]);
                string strNgayGhiPS = dtNgayCKy.ToString("dd/MM/yyyy").Trim();
                //Xử lý danh mục
                //if (iLoaiKHang == 0)
                //{
                //    //SH
                //    if (dtNgayCKy.CompareTo(dtHLucSH) >= 0 && dtNgayCKy.CompareTo(dtHetHLucSH) < 0)
                //    {
                //        //KH SH trong khoảng ngày hiệu lực, tính hết giá mới
                //        dsDMuc = dsStaticCatalog;
                //    }
                //    else
                //    {
                //        //KH SH ngoài khoảng ngày hiệu lực, tính hết giá cũ
                //        dsDMuc = dsStaticCatalog.Copy();
                //        ChangeDate(ref dsDMuc, dtOld, dtNew);
                //    }


                //}
                //else
                //{
                //    //NSH




                if (config.isHTroThang && config.lstTNamHTro.Contains(thangps + "/" + namps))
                {
                    //Hóa đơn PS trong khoảng tháng hiệu lực, tính hết giá Covid
                    dsDMuc = dsStaticCatalog;
                }
                else
                {
                    //Hóa đơn PS ngoài khoảng tháng hiệu lực, tính hết giá 648
                    dsDMuc = dsStaticCatalog.Copy();
                    ChangeDate(ref dsDMuc, dtOld, dtNew);
                }
                //}
                //Bổ sung lấy danh sách sổ hỗ trợ
                if (dsDMuc.Tables.Contains("D_SO_HOTRO_COVID") && dsDMuc.Tables["D_SO_HOTRO_COVID"].Rows.Count > 0)
                {
                    config.lstDViHTro = new List<string>();
                    for (int i = 0; i < dsDMuc.Tables["D_SO_HOTRO_COVID"].Rows.Count; i++)
                    {
                        if (dsDMuc.Tables["D_SO_HOTRO_COVID"].Rows[i]["TRANG_THAI"].ToString().Trim() == "1")
                            config.lstSoHTro.Add(dsDMuc.Tables["D_SO_HOTRO_COVID"].Rows[i]["MA_SOGCS"].ToString());
                    }
                }
                //Bổ sung lấy danh sách đơn vị hỗ trợ
                if (dsDMuc.Tables.Contains("D_DVI_HOTRO_COVID") && dsDMuc.Tables["D_DVI_HOTRO_COVID"].Rows.Count > 0)
                {
                    config.lstDViHTro = new List<string>();
                    for (int i = 0; i < dsDMuc.Tables["D_DVI_HOTRO_COVID"].Rows.Count; i++)
                    {
                        if (dsDMuc.Tables["D_DVI_HOTRO_COVID"].Rows[i]["TRANG_THAI"].ToString().Trim() == "1")
                            config.lstDViHTro.Add(dsDMuc.Tables["D_DVI_HOTRO_COVID"].Rows[i]["MA_DVIQLY"].ToString());
                    }
                }

                thang = i16Thang;
                nam = i16Nam;
                strTen_DNhap = dsCustomerData.Tables["HDN_HDON"].Rows[0]["NGUOI_TAO"].ToString();
                string strError = "";

                //Bat dau tinh hoa don
                WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "BillingImplementation", "BillingImplementation", "BillingImplementation", "Tính hoá đơn sổ " + strMa_SoGCS + " - kỳ " + ky + " tháng " + thang + "/" + nam);

                //Loc bien ban ap gia, chi lay cac bien ban co lien quan den ky ghi chi so hien tai
                strError = this.FilterHDG_BBAN_APGIA(ref dsCustomerData);
                if (strError != "")
                {
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "BillingImplementation", "BillingImplementation.clsBillingImplementation.cs", "FilterHDG_BBAN_APGIA", "Lọc biên bản áp giá - " + strError);
                    return strError;
                }
                else
                {
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "BillingImplementation", "BillingImplementation.clsBillingImplementation.cs", "FilterHDG_BBAN_APGIA", "Lọc biên bản áp giá");
                }
                //Đổi loại chỉ số CPK sang DDN, sau bỏ đi
                strError = this.AddColumn(ref dsCustomerData, dsDMuc);
                if (strError != "")
                {
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "AddColumn", "Thêm cột mới - " + strError);
                    return strError;
                }
                //Kiem tra du lieu dau vao
                strError = inputDataReading.CheckValidData(strMa_DViQLy, dsCustomerData, dsDMuc);
                if (strError != "")
                {
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busInputDataReading", "busInputDataReading.cls_InputDataReading", "CheckValidData", "Kiểm tra dữ liệu đầu vào - " + strError);
                    return strError;
                }
                else
                {
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busInputDataReading", "busInputDataReading.cls_InputDataReading", "CheckValidData", "Kiểm tra dữ liệu đầu vào");
                }
                //Tao cau truc du lieu dau ra
                dsInvoiceData = cls_OutputDataWriting.createDSInvoiceData();

                //Tinh san luong tho
                strError = rawDataCalculation.RawDataCalculation_1(dsCustomerData);
                if (strError != "")
                {
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "RawDataCalculation_1", "Tính sản lượng thô - " + strError);
                    return strError;
                }

                strError = rawDataCalculation.RawDataCalculation_2(dsCustomerData);
                if (strError != "")
                {
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "RawDataCalculation_2", "Tính sản lượng thô - " + strError);
                    return strError;
                }

                strError = rawDataCalculation.RawDataCalculation_3(dsCustomerData);
                if (strError != "")
                {
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "RawDataCalculation_3", "Tính sản lượng thô - " + strError);
                    return strError;
                }

                strError = rawDataCalculation.RawDataCalculation_4(dsCustomerData, dsDMuc);
                if (strError != "")
                {
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "RawDataCalculation_4", "Tính sản lượng thô - " + strError);
                    return strError;
                }

                strError = rawDataCalculation.RawDataCalculation_5(dsCustomerData, this.dsInvoiceData, dsDMuc, strTen_DNhap);
                if (strError != "")
                {
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "RawDataCalculation_5", "Tính sản lượng thô - " + strError);
                    return strError;
                }

                strError = rawDataCalculation.RawDataCalculation_6(dsCustomerData, this.dsInvoiceData, dsDMuc, strTen_DNhap);
                if (strError != "")
                {
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "RawDataCalculation_6", "Tính sản lượng thô - " + strError);
                    return strError;
                }

                strError = rawDataCalculation.RawDataCalculation_7(dsCustomerData, this.dsInvoiceData, dsDMuc, strTen_DNhap);
                if (strError != "")
                {
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "RawDataCalculation_7", "Tính sản lượng thô - " + strError);
                    return strError;
                }
                WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "RawDataCalculation_7", "Tính xong sản lượng thô");
                //đã tính xong cosfi, đổi CCS trả về CCS, CPK về DDN
                //strError = this.ChangeCCS(ref dsCustomerData, "CPK", "DDN", dsDMuc);
                //if (strError != "")
                //{
                //    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "AddColumn", "Tiền xử lý thông tin chỉ số - " + strError);
                //    return strError;
                //}
                //strError = this.ChangeCCS(ref dsCustomerData, "CCS", "CCS", dsDMuc);
                //if (strError != "")
                //{
                //    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "AddColumn", "Tiền xử lý thông tin chỉ số - " + strError);
                //    return strError;
                //}
                //Tinh san luong chi tiet

                strError = detailDataCalculation.DetailDataCalculation_11(dsCustomerData, ref this.dsCalculation, dsDMuc);
                if (strError != "")
                {
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_11", "Tính sản lượng chi tiết - " + strError);
                    return strError;
                }

                strError = detailDataCalculation.DetailDataCalculation_2(this.dsCalculation, dsCustomerData, dsDMuc);
                if (strError != "")
                {
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_2", "Tính sản lượng chi tiết - " + strError);
                    return strError;
                }
                //Xu ly cho KH tinh vat qua doi gia
                strError = detailDataCalculation.DetailDataCalculation_21(this.dsCalculation, dsCustomerData, dsDMuc);
                if (strError != "")
                {
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_21", "Tính sản lượng chi tiết trong kỳ đổi giá - " + strError);
                    return strError;
                }
                strError = detailDataCalculation.DetailDataCalculation_3(this.dsCalculation, dsCustomerData, dsDMuc);
                if (strError != "")
                {
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_3", "Tính sản lượng chi tiết - " + strError);
                    return strError;
                }
                WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_3", "Tính xong sản lượng chi tiết");

                strError = detailDataCalculation.DetailDataCalculation_40(this.dsCalculation, dsCustomerData, dsDMuc, dsInvoiceData, strMa_DViQLy, strMa_SoGCS, ky, thang, nam);
                if (strError != "")
                {
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_4", "Tính sản lượng chi tiết - " + strError);
                    return strError;
                }
                WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_4", "Tính xong sản lượng chi tiết");

                strError = detailDataCalculation.DetailDataCalculation_41(this.dsCalculation, dsDMuc, dsCustomerData);
                if (strError != "")
                {
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_41", "Tính sản lượng chi tiết cho bán buôn trong kỳ đổi giá - " + strError);
                    return strError;
                }
                WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_41", "Tính sản lượng chi tiết cho bán buôn trong kỳ đổi giá");

                strError = detailDataCalculation.DetailDataCalculation_42(this.dsCalculation);
                if (strError != "")
                {
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_42", "Xử lý riêng cho bán buôn SHBB nhóm L trong tháng 03/2011 - " + strError);
                    return strError;
                }
                WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_42", "Xử lý riêng cho bán buôn SHBB nhóm L trong tháng 03/2011");



                //Xu ly cho KH tinh qua doi gia
                strError = priceSpecification.PriceSpecification_11(this.dsCalculation, dsDMuc, dsCustomerData);
                if (strError != "")
                {
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsPriceSpecification", "PriceSpecification_1", "Xác định đơn giá - " + strError);
                    return strError;
                }

                //Xu ly lam tron ngang cho gia bac thang
                strError = priceSpecification.PriceSpecification_12(this.dsCalculation, dsDMuc, dsCustomerData);
                if (strError != "")
                {
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsPriceSpecification", "PriceSpecification_12", "Xử lý riêng cho giá bậc thang trong kỳ đổi giá - " + strError);
                    return strError;
                }

                //strError = priceSpecification.PriceSpecification_2(this.dsCalculation, dsDMuc);
                //Xu ly cho KH tinh qua doi gia
                strError = priceSpecification.PriceSpecification_21(this.dsCalculation, dsDMuc, dsCustomerData);
                if (strError != "")
                {
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsPriceSpecification", "PriceSpecification_2", "Xác định đơn giá - " + strError);
                    return strError;
                }
                strError = priceSpecification.PriceSpecification_3(this.dsCalculation, dsCustomerData, this.dsInvoiceData, strTen_DNhap, strNgayGhiPS, ky, thang, nam);
                if (strError != "")
                {
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsPriceSpecification", "PriceSpecification_3", "Xác định đơn giá - " + strError);
                    return strError;
                }
                WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsPriceSpecification", "PriceSpecification_3", "Kết thúc xác định đơn giá");
                //dsCustomerData.WriteXml(@"C:\CustomerData.xml");
                //dsDMuc.WriteXml(@"C:\StaticCatalog.xml");
                //dsCalculation.WriteXml(@"C:\CalculationData.xml");
                //dsInvoiceData.WriteXml(@"C:\InvoiceData.xml");
                //if (config.isHTroThang && config.lstTNamHTro.Contains(thangps + "/" + namps))
                //{
                //    strError = priceSpecification.PriceSpecification_4(dsCustomerData, this.dsInvoiceData, strMa_SoGCS, strNgayGhiPS, config, false);
                //    if (strError != "")
                //    {
                //        WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsPriceSpecification", "PriceSpecification_4", "Tính toán giảm trừ - " + strError);
                //        return strError;
                //    }
                //    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsPriceSpecification", "PriceSpecification_4", "Kết thúc tính toán giảm trừ");
                //}
                //Tinh hoa don
                //Kiem tra tham so G_INCHUNGRIENG = 'C' thi se ra hoa don TC
                if (dsDMuc.Tables.Contains("S_PARAMETER") == true)
                {
                    if (dsDMuc.Tables["S_PARAMETER"].Select("NAME = 'G_INCHUNGRIENG' AND (PRAVALUE = 'C' OR PRAVALUE = 'c')").Length > 0)
                    {
                        strError = invoiceCalculation.InvoiceCalculation_TC(ref this.dsInvoiceData, dsCustomerData, this.dsCalculation, strMa_DViQLy, strMa_SoGCS, ky, thang, nam, dsDMuc);
                    }
                    else
                    {
                        strError = invoiceCalculation.InvoiceCalculation(ref this.dsInvoiceData, dsCustomerData, this.dsCalculation, strMa_DViQLy, strMa_SoGCS, ky, thang, nam, dsDMuc);
                    }
                }
                else
                {
                    strError = invoiceCalculation.InvoiceCalculation(ref this.dsInvoiceData, dsCustomerData, this.dsCalculation, strMa_DViQLy, strMa_SoGCS, ky, thang, nam, dsDMuc);
                }

                if (strError != "")
                {
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsInvoiceCalculation", "InvoiceCalculation", "Tính hoá đơn - " + strError);
                    return strError;
                }
                else
                {
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsInvoiceCalculation", "InvoiceCalculation", "Tính xong hoá đơn");
                }

                //Cap nhat du lieu
                strError = outputDataWriting.InsertInvoiceData_DC_Plus(this.dsInvoiceData, dsCustomerData);
                if (strError != "")
                {
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busOutputDataWriting", "busOutputDataWriting.cls_OutputDataWriting", "InsertInvoiceData", "Cập nhật dữ liệu hoá đơn - " + strError);
                    return strError;
                }
                else
                {
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busOutputDataWriting", "busOutputDataWriting.cls_OutputDataWriting", "InsertInvoiceData", "Cập nhật dữ liệu hoá đơn");
                }

                //Ket thuc tinh hoa don
                WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "BillingImplementation", "BillingImplementation", "BillingImplementation", "Tính xong sổ " + strMa_SoGCS + " - hoá đơn kỳ " + ky + " tháng " + thang + "/" + nam);

                return "";
            }
            catch (Exception ex)
            {
                //Ghi log vao dataset
                WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "BillingImplementation", "BillingImplementation", "BillingImplementation", "Lỗi khi tính hoá đơn: sổ " + strMa_SoGCS + " - hoá đơn kỳ " + ky + " tháng " + thang + "/" + nam);
                return ex.Message;
            }
            finally
            {
                dsCustomerData.Reset();
                dsInvoiceData.Reset();
                dsCalculation.Reset();
            }
        }
        public string PushMessage(string strMaDViQLy, string strMaSoGCS, string strMess, string strTenDNhap, short thang, short nam, int i32LoaiTBao, ref JObject jout)
        {
            try
            {
                //Ghi vào CSDL, làm sau vì chưa có DB
                jout = JObject.FromObject(new
                {
                    type = "notify",
                    NDUNG_TBAO = strMess,
                    MA_DVIQLY = strMaDViQLy,
                    MA_DTUONG = strMaSoGCS,
                    NAM = nam,
                    THANG = thang,
                    LOAI_TBAO = i32LoaiTBao,
                    USER_NAME = strTenDNhap,
                    TRANG_THAI = 0
                });
                string strError = outputDataWriting.writeMessage(jout);

                return strError;
            }
            catch (Exception ex)
            {
                return ex.Message;
            }
        }
        public string BillingImplementationKH(string strMa_DViQLy, string strMaSoGCS, string strMaKHang, short ky, short thang, short nam, string strTen_DNhap, long lngCurrentLibID, long lngWorkflowID, string strNgayGhi)
        {
            DateTime dtHLuc = new DateTime(2020, 4, 1);

            DateTime dtHetHLuc = new DateTime(2020, 8, 1);

            DateTime dtNgayCKy = DateTime.ParseExact(strNgayGhi, "dd/MM/yyyy", System.Globalization.CultureInfo.InvariantCulture);
            if (dtNgayCKy.CompareTo(dtHLuc) >= 0 && dtNgayCKy.CompareTo(dtHetHLuc) < 0)
                return this.CommonImplementationKH_GTru(strMa_DViQLy, strMaSoGCS, strMaKHang, ky, thang, nam, strTen_DNhap, lngCurrentLibID, lngWorkflowID, strNgayGhi);
            else
                return this.CommonImplementationKH(strMa_DViQLy, strMaSoGCS, strMaKHang, ky, thang, nam, strTen_DNhap, lngCurrentLibID, lngWorkflowID, strNgayGhi);
        }

        private string CommonImplementationKH(string strMa_DViQLy, string strMa_SoGCS, string strMaKHang, short ky, short thang, short nam, string strTen_DNhap, long lngCurrentLibID, long lngWorkflowID, string strNgayGhi)
        {
            try
            {
                dsCustomerData = new DataSet();
                dsInvoiceData = new DataSet();
                DataSet dsInvoiceDataDCN = new DataSet();
                dsCalculation = new DS_CALCULATIONTABLES();

                string strError = "";

                //Bat dau tinh hoa don
                WriteLogPlus(strMa_DViQLy, strMaKHang, "BillingImplementation", "BillingImplementation", "BillingImplementation", "Tính hoá đơn cho khách hàng " + strMaKHang + " - kỳ " + ky + " tháng " + thang + "/" + nam);
                
                //Doc du lieu dau vao
                strError = inputDataReading.getCustomerDataReadingKH(ref dsCustomerData, strMa_DViQLy, strMa_SoGCS, strMaKHang, ky, thang, nam, strNgayGhi);
                //dsCustomerData.WriteXml("D:/TinhHoaDon.xml", XmlWriteMode.WriteSchema);
                if (strError != "")
                {
                    WriteLogPlus(strMa_DViQLy, strMaKHang, "busInputDataReading", "busInputDataReading.cls_InputDataReading", "getCustomerDataReading", "Đọc dữ liệu đầu vào - " + strError);
                    return strError;
                }
                else
                {
                    WriteLogPlus(strMa_DViQLy, strMaKHang, "busInputDataReading", "busInputDataReading.cls_InputDataReading", "getCustomerDataReading", "Đọc dữ liệu đầu vào");
                }
                //Loc bien ban ap gia, chi lay cac bien ban co lien quan den ky ghi chi so hien tai
                strError = this.FilterHDG_BBAN_APGIA(ref dsCustomerData);
                //decimal _tphu = dsCustomerData.Tables["HDG_QHE_DDO"].Select("MA_DDO_CHINH='PD05000093426001'").Length;
                if (strError != "")
                {
                    WriteLogPlus(strMa_DViQLy, strMaKHang, "BillingImplementation", "BillingImplementation.clsBillingImplementation.cs", "FilterHDG_BBAN_APGIA", "Lọc biên bản áp giá - " + strError);
                    return strError;
                }
                else
                {
                    WriteLogPlus(strMa_DViQLy, strMaKHang, "BillingImplementation", "BillingImplementation.clsBillingImplementation.cs", "FilterHDG_BBAN_APGIA", "Lọc biên bản áp giá");
                }
                //Đổi loại chỉ số CPK sang DDN, sau bỏ đi
                strError = this.AddColumn(ref dsCustomerData, dsStaticCatalog);
                if (strError != "")
                {
                    WriteLogPlus(strMa_DViQLy, strMaKHang, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "AddColumn", "Thêm cột mới - " + strError);
                    return strError;
                }
                //Đổi loại chỉ số CPK sang DDN để check valid data ko báo lỗi dup chỉ số
                //strError = this.ChangeCCS(ref dsCustomerData, "CPK", "DDN", dsStaticCatalog);
                //if (strError != "")
                //{
                //    WriteLogPlus(strMa_DViQLy, strMaKHang, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "AddColumn", "Tiền xử lý thông tin chỉ số - " + strError);
                //    return strError;
                //}
                //Kiem tra du lieu dau vao
                strError = inputDataReading.CheckValidData(strMa_DViQLy, this.dsCustomerData, clsBillingImplementation.dsStaticCatalog);
                if (strError != "")
                {
                    WriteLogPlus(strMa_DViQLy, strMaKHang, "busInputDataReading", "busInputDataReading.cls_InputDataReading", "CheckValidData", "Kiểm tra dữ liệu đầu vào - " + strError);
                    return strError;
                }
                else
                {
                    WriteLogPlus(strMa_DViQLy, strMaKHang, "busInputDataReading", "busInputDataReading.cls_InputDataReading", "CheckValidData", "Kiểm tra dữ liệu đầu vào");
                }
                //var temp = dsCustomerData.Tables["HDG_BBAN_APGIA"].Select("MA_DDO = 'PC12LL0890486001'");
                //Tao cau truc du lieu dau ra
                dsInvoiceData = cls_OutputDataWriting.createDSInvoiceData();

                strError = this.CommonCalculationNoInsert_DCN(strMa_DViQLy, strMa_SoGCS, strNgayGhi, ky, thang, nam, strTen_DNhap, lngCurrentLibID, lngWorkflowID, dsCustomerData, clsBillingImplementation.dsStaticCatalog, ref dsInvoiceData);
                if (strError != "")
                {
                    //WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "BillingImplementation", "BillingImplementation.CommonCalculationNoInsert_DCN", "InsertInvoiceData", "Cập nhật dữ liệu hoá đơn - " + strError);
                    return strError;
                }

                if (thang == 12 && nam <= 2025 && nam >= 2022 && dsCustomerData.Tables.Contains("GCS_CSO_DCN") && dsCustomerData.Tables["GCS_CSO_DCN"].Rows.Count > 0)
                {
                    //tính thêm 1 lần nữa
                    dsInvoiceDataDCN = cls_OutputDataWriting.createDSInvoiceData();
                    #region Đổi tên bảng GCS_CHISO
                    dsCustomerData.Tables["GCS_CHISO"].TableName = "GCS_CHISO_DDK";
                    dsCustomerData.Tables["GCS_CHISO_GT"].TableName = "GCS_CHISO_DDK_GT";
                    dsCustomerData.Tables["GCS_CHISO_TP"].TableName = "GCS_CHISO_DDK_TP";
                    dsCustomerData.Tables["GCS_CHISO_BQ"].TableName = "GCS_CHISO_DDK_BQ";
                    dsCustomerData.Tables["GCS_CSO_DCN"].TableName = "GCS_CHISO";
                    dsCustomerData.Tables["GCS_CSO_DCN_GT"].TableName = "GCS_CHISO_GT";
                    dsCustomerData.Tables["GCS_CSO_DCN_TP"].TableName = "GCS_CHISO_TP";
                    dsCustomerData.Tables["GCS_CSO_DCN_BQ"].TableName = "GCS_CHISO_BQ";
                    #endregion
                    strError = this.AddColumn(ref dsCustomerData, dsStaticCatalog);
                    if (strError != "")
                    {
                        WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "AddColumn", "Thêm cột mới DCN - " + strError);
                        return strError;
                    }
                    strError = this.CommonCalculationNoInsert_DCN(strMa_DViQLy, strMa_SoGCS, strNgayGhi, ky, thang, nam, strTen_DNhap, lngCurrentLibID, lngWorkflowID, dsCustomerData, clsBillingImplementation.dsStaticCatalog, ref dsInvoiceDataDCN);
                    if (strError != "")
                    {
                        return strError;
                    }
                    strError = outputDataWriting.InsertInvoiceDataKH_DCN(this.dsInvoiceData, dsInvoiceDataDCN, this.dsCustomerData, strMa_DViQLy, strMa_SoGCS, strTen_DNhap, lngCurrentLibID, lngWorkflowID, ky, thang, nam);
                }
                else
                    //Cap nhat du lieu
                    strError = outputDataWriting.InsertInvoiceDataKH(this.dsInvoiceData, this.dsCustomerData, strMa_DViQLy, strMa_SoGCS, strTen_DNhap, lngCurrentLibID, lngWorkflowID, ky, thang, nam);
                if (strError != "")
                {
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busOutputDataWriting", "busOutputDataWriting.cls_OutputDataWriting", "InsertInvoiceData", "Cập nhật dữ liệu hoá đơn - " + strError);
                    return strError;
                }
                else
                {
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busOutputDataWriting", "busOutputDataWriting.cls_OutputDataWriting", "InsertInvoiceData", "Cập nhật dữ liệu hoá đơn");

                }
                //Ket thuc tinh hoa
                WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "BillingImplementation", "BillingImplementation", "BillingImplementation", "Tính xong sổ " + strMa_SoGCS + " - hoá đơn kỳ " + ky + " tháng " + thang + "/" + nam);

                return "";
                
            }
            catch (Exception ex)
            {
                //Ghi log vao dataset
                WriteLogPlus(strMa_DViQLy, strMaKHang, "BillingImplementation", "BillingImplementation", "BillingImplementation", "Lỗi khi tính hoá đơn: khách hàng " + strMaKHang + " - hoá đơn lẻ kỳ " + ky + " tháng " + thang + "/" + nam);
                return ex.Message;
            }
            finally
            {
                dsCustomerData.Reset();
                dsInvoiceData.Reset();
                dsCalculation.Reset();
            }
        }
        public string CommonImplementation_API(DataSet dsDanhMuc, ref DataSet dsResult, DataSet dsCustomerData)
        {
            //dsCustomerData = dsData;
            //DataSet dsCustomerData = new DataSet();
            DateTime d1 = DateTime.Now;
            string strTen_DNhap = "";
            try
            {
                dsCalculation = new DS_CALCULATIONTABLES();
                dsInvoiceData = new DataSet();

                //dsDanhMuc = dsDanhMuc;
                //log = new cls_LogManagement(dsLog);

                strTen_DNhap = "Service";
                string strMa_DViQLy = "PD0600";
                string strMa_SoGCS = "SO_SHBB1";
                short ky = 1;
                short thang = Convert.ToInt16(dsCustomerData.Tables["GCS_CHISO"].Rows[0]["THANG"]);
                short nam = Convert.ToInt16(dsCustomerData.Tables["GCS_CHISO"].Rows[0]["NAM"]);
                DateTime dtNgayCKy = Convert.ToDateTime(dsCustomerData.Tables["GCS_CHISO"].Rows[0]["NGAY_CKY"]);
                string strNgayGhi = dtNgayCKy.ToString("dd/MM/yyyy");
                string strError = "";
                //Bat dau tinh hoa don
                DateTime dt1 = DateTime.Now;

                //Loc bien ban ap gia, chi lay cac bien ban co lien quan den ky ghi chi so hien tai
                strError = this.FilterHDG_BBAN_APGIA(ref dsCustomerData);
                if (strError != "")
                {
                    //WriteLog(strMa_DViQLy, strMa_SoGCS, "BillingImplementation", "BillingImplementation.clsBillingImplementation.cs", "FilterHDG_BBAN_APGIA", "Lọc biên bản áp giá - " + strError);
                    return strError;
                }
                else
                {
                    //WriteLog(strMa_DViQLy, strMa_SoGCS, "BillingImplementation", "BillingImplementation.clsBillingImplementation.cs", "FilterHDG_BBAN_APGIA", "Lọc biên bản áp giá");
                }
                strError = this.AddColumn(ref dsCustomerData, dsDanhMuc);
                if (strError != "")
                {
                    //WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "AddColumn", "Thêm cột mới - " + strError);
                    return strError;
                }
                DateTime dt2 = DateTime.Now;
                ////Kiem tra du lieu dau vao
                //strError = inputDataReading.CheckValidData(strMa_DViQLy, dsCustomerData, dsDanhMuc);
                //if (strError != "")
                //{
                //    //WriteLog(strMa_DViQLy, strMa_SoGCS, "busInputDataReading", "busInputDataReading.cls_InputDataReading", "CheckValidData", "Kiểm tra dữ liệu đầu vào - " + strError);
                //    return strError;
                //}
                //else
                //{
                //    //WriteLog(strMa_DViQLy, strMa_SoGCS, "busInputDataReading", "busInputDataReading.cls_InputDataReading", "CheckValidData", "Kiểm tra dữ liệu đầu vào");
                //}
                //Tao cau truc du lieu dau ra
                dsInvoiceData = cls_OutputDataWriting.createDSInvoiceData();

                //Tinh san luong tho
                strError = rawDataCalculation.RawDataCalculation_1(dsCustomerData);
                if (strError != "")
                {
                    //WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "RawDataCalculation_1", "Tính sản lượng thô - " + strError);
                    return strError;
                }

                strError = rawDataCalculation.RawDataCalculation_2(dsCustomerData);
                if (strError != "")
                {
                    //WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "RawDataCalculation_2", "Tính sản lượng thô - " + strError);
                    return strError;
                }

                strError = rawDataCalculation.RawDataCalculation_3(dsCustomerData);
                if (strError != "")
                {
                    //WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "RawDataCalculation_3", "Tính sản lượng thô - " + strError);
                    return strError;
                }
                strError = rawDataCalculation.RawDataCalculation_4(dsCustomerData, dsDanhMuc);
                if (strError != "")
                {
                    //WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "RawDataCalculation_4", "Tính sản lượng thô - " + strError);
                    return strError;
                }

                strError = rawDataCalculation.RawDataCalculation_5(dsCustomerData, this.dsInvoiceData, dsDanhMuc, strTen_DNhap);
                if (strError != "")
                {
                    //WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "RawDataCalculation_5", "Tính sản lượng thô - " + strError);
                    return strError;
                }

                strError = rawDataCalculation.RawDataCalculation_6(dsCustomerData, this.dsInvoiceData, dsDanhMuc, strTen_DNhap);
                if (strError != "")
                {
                    //WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "RawDataCalculation_6", "Tính sản lượng thô - " + strError);
                    return strError;
                }

                //strError = rawDataCalculation.RawDataCalculation_7(dsCustomerData, this.dsInvoiceData, dsDanhMuc, strTen_DNhap);
                //if (strError != "")
                //{
                //    WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "RawDataCalculation_7", "Tính sản lượng thô - " + strError);
                //    return strError;
                //}
                //WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "RawDataCalculation_7", "Tính xong sản lượng thô");

                //Tinh san luong chi tiet
                DateTime dt3 = DateTime.Now;
                strError = detailDataCalculation.DetailDataCalculation_11(dsCustomerData, ref this.dsCalculation, dsDanhMuc);
                if (strError != "")
                {
                    //WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_11", "Tính sản lượng chi tiết - " + strError);
                    return strError;
                }
                DateTime dt31 = DateTime.Now;
                strError = detailDataCalculation.DetailDataCalculation_2(this.dsCalculation, dsCustomerData, dsDanhMuc);
                if (strError != "")
                {
                    //WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_2", "Tính sản lượng chi tiết - " + strError);
                    return strError;
                }
                DateTime dt32 = DateTime.Now;
                //Xu ly cho KH tinh vat qua doi gia
                strError = detailDataCalculation.DetailDataCalculation_21(this.dsCalculation, dsCustomerData, dsDanhMuc);
                if (strError != "")
                {
                    //WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_21", "Tính sản lượng chi tiết trong kỳ đổi giá - " + strError);
                    return strError;
                }
                DateTime dt33 = DateTime.Now;
                strError = detailDataCalculation.DetailDataCalculation_3(this.dsCalculation, dsCustomerData, dsDanhMuc);
                if (strError != "")
                {
                    //WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_3", "Tính sản lượng chi tiết - " + strError);
                    return strError;
                }
                DateTime dt34 = DateTime.Now;
                //WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_3", "Tính xong sản lượng chi tiết");

                strError = detailDataCalculation.DetailDataCalculation_40(this.dsCalculation, dsCustomerData, dsDanhMuc, dsInvoiceData, strMa_DViQLy, strMa_SoGCS, ky, thang, nam);
                if (strError != "")
                {
                    //WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_4", "Tính sản lượng chi tiết - " + strError);
                    return strError;
                }
                //WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_4", "Tính xong sản lượng chi tiết");
                DateTime dt35 = DateTime.Now;
                strError = detailDataCalculation.DetailDataCalculation_41(this.dsCalculation, dsDanhMuc, dsCustomerData);
                if (strError != "")
                {
                    //WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_41", "Tính sản lượng chi tiết cho bán buôn trong kỳ đổi giá - " + strError);
                    return strError;
                }
                //WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_41", "Tính sản lượng chi tiết cho bán buôn trong kỳ đổi giá");
                DateTime dt36 = DateTime.Now;
                strError = detailDataCalculation.DetailDataCalculation_42(this.dsCalculation);
                if (strError != "")
                {
                    //WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_42", "Xử lý riêng cho bán buôn SHBB nhóm L trong tháng 03/2011 - " + strError);
                    return strError;
                }
                //WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_42", "Xử lý riêng cho bán buôn SHBB nhóm L trong tháng 03/2011");
                DateTime dt4 = DateTime.Now;
                //Xu ly cho KH tinh qua doi gia
                strError = priceSpecification.PriceSpecification_11(this.dsCalculation, dsDanhMuc, dsCustomerData);
                if (strError != "")
                {
                    //WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsPriceSpecification", "PriceSpecification_1", "Xác định đơn giá - " + strError);
                    return strError;
                }
                DateTime dt41 = DateTime.Now;
                //Xu ly lam tron ngang cho gia bac thang
                strError = priceSpecification.PriceSpecification_12(this.dsCalculation, dsDanhMuc, dsCustomerData);
                if (strError != "")
                {
                    //WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsPriceSpecification", "PriceSpecification_12", "Xử lý riêng cho giá bậc thang trong kỳ đổi giá - " + strError);
                    return strError;
                }
                DateTime dt42 = DateTime.Now;
                //strError = priceSpecification.PriceSpecification_2(this.dsCalculation, dsDanhMuc);
                //Xu ly cho KH tinh qua doi gia
                strError = priceSpecification.PriceSpecification_21(this.dsCalculation, dsDanhMuc, dsCustomerData);
                if (strError != "")
                {
                    // WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsPriceSpecification", "PriceSpecification_2", "Xác định đơn giá - " + strError);
                    return strError;
                }
                DateTime dt43 = DateTime.Now;
                strError = priceSpecification.PriceSpecification_3(this.dsCalculation, dsCustomerData, this.dsInvoiceData, strTen_DNhap, strNgayGhi, ky, thang, nam);
                if (strError != "")
                {
                    //WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsPriceSpecification", "PriceSpecification_3", "Xác định đơn giá - " + strError);
                    return strError;
                }
                //WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsPriceSpecification", "PriceSpecification_3", "Kết thúc xác định đơn giá");
                DateTime dt5 = DateTime.Now;
                //Tinh hoa don
                //Kiem tra tham so G_INCHUNGRIENG = 'C' thi se ra hoa don TC
                //if (dsDanhMuc.Tables.Contains("S_PARAMETER") == true)
                //{
                //    if (dsDanhMuc.Tables["S_PARAMETER"].Select("NAME = 'G_INCHUNGRIENG' AND (PRAVALUE = 'C' OR PRAVALUE = 'c')").Length > 0)
                //    {
                //        strError = invoiceCalculation.InvoiceCalculation_TC(ref this.dsInvoiceData, dsCustomerData, this.dsCalculation, strMa_DViQLy, strMa_SoGCS, ky, thang, nam, dsDanhMuc);
                //    }
                //    else
                //    {
                //        strError = invoiceCalculation.InvoiceCalculation(ref this.dsInvoiceData, dsCustomerData, this.dsCalculation, strMa_DViQLy, strMa_SoGCS, ky, thang, nam, dsDanhMuc);
                //    }
                //}
                //else
                //{
                strError = invoiceCalculation.InvoiceCalculation(ref this.dsInvoiceData, dsCustomerData, this.dsCalculation, strMa_DViQLy, strMa_SoGCS, ky, thang, nam, dsDanhMuc);
                //}
                DateTime dt6 = DateTime.Now;
                if (strError != "")
                {
                    //WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsInvoiceCalculation", "InvoiceCalculation", "Tính hoá đơn - " + strError);
                    return strError;
                }
                else
                {
                    //WriteLog(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsInvoiceCalculation", "InvoiceCalculation", "Tính xong hoá đơn");
                }
                //Không cập nhật dữ liệu, trả về luôn
                dsResult = new DataSet();
                dsResult = this.dsInvoiceData.Copy();
                DateTime dt7 = DateTime.Now;
                return "";
            }
            catch (Exception ex)
            {
                //Ghi log vao dataset
                //WriteLog(strMa_DViQLy, strMa_SoGCS, "BillingImplementation", "BillingImplementation", "BillingImplementation", "Lỗi khi tính hoá đơn: sổ " + strMa_SoGCS + " - hoá đơn kỳ " + ky + " tháng " + thang + "/" + nam);
                return ex.Message;
            }
            finally
            {
                //dsCustomerData.Reset();
                dsInvoiceData.Reset();
                dsCalculation.Reset();
            }
        }
        public string CommonImplementation_KTGS(DataSet dsCustomerData, Int16 i16Thang, Int16 i16Nam)
        {
            string strMa_DViQLy = "";
            string strMa_SoGCS = "";
            Int16 ky = 0;
            Int16 thang = 0;
            Int16 nam = 0;
            Int16 iLoaiKHang = 0;
            string strTen_DNhap = "";

            DataSet dsDMuc = null;
            DateTime dtHLucNSH = new DateTime(2020, 4, 16);
            DateTime dtHLucSH = new DateTime(2020, 5, 1);
            DateTime dtHetHLucNSH = new DateTime(2020, 7, 16);
            DateTime dtHetHLucSH = new DateTime(2020, 8, 1);
            DateTime dtOld = config.dtGiaHTro;
            DateTime dtNew = new DateTime(2090, 8, 1);
            Int16 thangps = 0, namps = 0;
            DateTime dtNgayCKy = new DateTime(1900, 1, 1);
            //tách theo từng kỳ tháng năm của chỉ số
            List<string> lstKTN = new List<string>();
            List<DataTable> lstCSo = new List<DataTable>();
            //List<DataTable> lstCTiet = new List<DataTable>();
            //Lưu lại bảng HDG_BBAN_APGIA ban đâu
            DataTable dtHDG_BBAN_APGIA = dsCustomerData.Tables["HDG_BBAN_APGIA"].Copy();
            dtHDG_BBAN_APGIA.TableName = "HDG_BBAN_APGIA";
            DataTable DTGCS_CHISO = dsCustomerData.Tables["GCS_CHISO"].Copy();
            DTGCS_CHISO.TableName = "GCS_CHISO";
            DataView dvCS = new DataView(dsCustomerData.Tables["GCS_CHISO"]);
            dvCS.Sort = "NAM ASC, THANG ASC, KY ASC";
            //DataView dvCT = new DataView(dsCustomerData.Tables["HDN_HDONCTIET"]);
            //dvCT.Sort = "NAM ASC, THANG ASC, KY ASC";
            int j = -1;
            foreach (DataRowView dr in dvCS)
            {
                string strKTN = dr["KY"].ToString() + "-" + dr["THANG"].ToString() + "-" + dr["NAM"].ToString();
                //if (!lstKTN.Contains(strKTN))
                //{
                //    lstKTN.Add(strKTN);
                //    j++;
                //}

                if (lstCSo.Count > 0 && lstCSo[j] != null && lstCSo[j].TableName == strKTN)
                {
                    //DataRow drCS = dsCSo.Tables[strKTN].NewRow();
                    //drCS.ItemArray = dr.ItemArray;
                    lstCSo[j].ImportRow(dr.Row);
                }
                else
                {
                    j++;
                    DataTable dt = dsCustomerData.Tables["GCS_CHISO"].Clone();
                    dt.TableName = strKTN;
                    lstCSo.Add(dt);
                    lstCSo[j].ImportRow(dr.Row);
                    ////Clone luôn bảng HDN_HDONCTIET tại đây
                    //DataTable dtCT = dsCustomerData.Tables["HDN_HDONCTIET"].Clone();
                    //dtCT.TableName = strKTN;
                    //lstCTiet.Add(dtCT);

                }
            }
            //j = 0;
            //foreach (DataRowView dr in dvCT)
            //{
            //    string strKTN = dr["KY"].ToString() + "-" + dr["THANG"].ToString() + "-" + dr["NAM"].ToString();

            //    if (lstCTiet[j].TableName != strKTN)
            //        j++;
            //    lstCTiet[j].ImportRow(dr.Row);
            //}


            try
            {
                DataSet dsCalculationAll = new DS_CALCULATIONTABLES();
                //dsCustomerData = new DataSet();
                DataSet dsInvoiceDataAll = new DataSet();

                GetStaticDataPlus();


                log = new cls_LogManagement(dsLog);

                strMa_DViQLy = dsCustomerData.Tables["HDN_HDON"].Rows[0]["MA_DVIQLY"].ToString();
                strMa_SoGCS = dsCustomerData.Tables["HDN_HDON"].Rows[0]["MA_SOGCS"].ToString();
                ky = Convert.ToInt16(dsCustomerData.Tables["HDN_HDON"].Rows[0]["KY"]);
                thangps = Convert.ToInt16(dsCustomerData.Tables["HDN_HDON"].Rows[0]["THANG"]);
                namps = Convert.ToInt16(dsCustomerData.Tables["HDN_HDON"].Rows[0]["NAM"]);
                iLoaiKHang = Convert.ToInt16(dsCustomerData.Tables["HDN_HDON"].Rows[0]["LOAI_KHANG"]);
                dtNgayCKy = Convert.ToDateTime(dsCustomerData.Tables["HDN_HDON"].Rows[0]["NGAY_CKY"]);
                string strNgayGhiPS = dtNgayCKy.ToString("dd/MM/yyyy");

                //Hóa đơn KTRAGSMBD, tính hết giá 648, loại bỏ các nhóm giá 1/1/2020 và 1/2/2021
                dsDMuc = dsStaticCatalog.Copy();
                ChangeDate(ref dsDMuc, dtOld, dtNew);
                dtOld = new DateTime(2020, 1, 1);
                ChangeDate(ref dsDMuc, dtOld, dtNew);


                thang = i16Thang;
                nam = i16Nam;
                strTen_DNhap = dsCustomerData.Tables["HDN_HDON"].Rows[0]["NGUOI_TAO"].ToString();
                string strError = "";

                //Bat dau tinh hoa don
                WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "BillingImplementation", "BillingImplementation", "BillingImplementation", "Tính hoá đơn sổ " + strMa_SoGCS + " - kỳ " + ky + " tháng " + thang + "/" + nam);

                for (int i = 0; i < lstCSo.Count; i++)
                {
                    dsCustomerData.Tables.Remove("GCS_CHISO");
                    //dsCustomerData.Tables.Remove("HDN_HDONCTIET");
                    dsCustomerData.Tables.Remove("HDG_BBAN_APGIA");
                    lstCSo[i].TableName = "GCS_CHISO";
                    //lstCTiet[i].TableName = "HDN_HDONCTIET";
                    dsCustomerData.Tables.Add(lstCSo[i]);
                    //dsCustomerData.Tables.Add(lstCTiet[i]);
                    DataTable dtAG = dtHDG_BBAN_APGIA.Copy();
                    dtAG.TableName = "HDG_BBAN_APGIA";
                    dsCustomerData.Tables.Add(dtAG);
                    #region Tính toán chi tiết

                    dsInvoiceData = new DataSet();
                    dsCalculation = new DS_CALCULATIONTABLES();

                    //Loc bien ban ap gia, chi lay cac bien ban co lien quan den ky ghi chi so hien tai
                    strError = this.FilterHDG_BBAN_APGIA(ref dsCustomerData);
                    if (strError != "")
                    {
                        WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "BillingImplementation", "BillingImplementation.clsBillingImplementation.cs", "FilterHDG_BBAN_APGIA", "Lọc biên bản áp giá - " + strError);
                        return strError;
                    }
                    else
                    {
                        WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "BillingImplementation", "BillingImplementation.clsBillingImplementation.cs", "FilterHDG_BBAN_APGIA", "Lọc biên bản áp giá");
                    }
                    //Đổi loại chỉ số CPK sang DDN, sau bỏ đi
                    strError = this.AddColumn(ref dsCustomerData, dsDMuc);
                    if (strError != "")
                    {
                        WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "AddColumn", "Thêm cột mới - " + strError);
                        return strError;
                    }



                    //Kiem tra du lieu dau vao
                    strError = inputDataReading.CheckValidData(strMa_DViQLy, dsCustomerData, dsDMuc);
                    if (strError != "")
                    {
                        WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busInputDataReading", "busInputDataReading.cls_InputDataReading", "CheckValidData", "Kiểm tra dữ liệu đầu vào - " + strError);
                        return strError;
                    }
                    else
                    {
                        WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busInputDataReading", "busInputDataReading.cls_InputDataReading", "CheckValidData", "Kiểm tra dữ liệu đầu vào");
                    }
                    //Tao cau truc du lieu dau ra
                    dsInvoiceData = cls_OutputDataWriting.createDSInvoiceData();

                    //Tinh san luong tho
                    strError = rawDataCalculation.RawDataCalculation_1(dsCustomerData);
                    if (strError != "")
                    {
                        WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "RawDataCalculation_1", "Tính sản lượng thô - " + strError);
                        return strError;
                    }

                    strError = rawDataCalculation.RawDataCalculation_2(dsCustomerData);
                    if (strError != "")
                    {
                        WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "RawDataCalculation_2", "Tính sản lượng thô - " + strError);
                        return strError;
                    }

                    strError = rawDataCalculation.RawDataCalculation_3(dsCustomerData);
                    if (strError != "")
                    {
                        WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "RawDataCalculation_3", "Tính sản lượng thô - " + strError);
                        return strError;
                    }

                    strError = rawDataCalculation.RawDataCalculation_4(dsCustomerData, dsDMuc);
                    if (strError != "")
                    {
                        WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "RawDataCalculation_4", "Tính sản lượng thô - " + strError);
                        return strError;
                    }

                    strError = rawDataCalculation.RawDataCalculation_5(dsCustomerData, this.dsInvoiceData, dsDMuc, strTen_DNhap);
                    if (strError != "")
                    {
                        WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "RawDataCalculation_5", "Tính sản lượng thô - " + strError);
                        return strError;
                    }

                    strError = rawDataCalculation.RawDataCalculation_6(dsCustomerData, this.dsInvoiceData, dsDMuc, strTen_DNhap);
                    if (strError != "")
                    {
                        WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "RawDataCalculation_6", "Tính sản lượng thô - " + strError);
                        return strError;
                    }

                    strError = rawDataCalculation.RawDataCalculation_7(dsCustomerData, this.dsInvoiceData, dsDMuc, strTen_DNhap);
                    if (strError != "")
                    {
                        WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "RawDataCalculation_7", "Tính sản lượng thô - " + strError);
                        return strError;
                    }
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "RawDataCalculation_7", "Tính xong sản lượng thô");

                    //Tinh san luong chi tiet

                    strError = detailDataCalculation.DetailDataCalculation_11(dsCustomerData, ref this.dsCalculation, dsDMuc);
                    if (strError != "")
                    {
                        WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_11", "Tính sản lượng chi tiết - " + strError);
                        return strError;
                    }

                    strError = detailDataCalculation.DetailDataCalculation_2(this.dsCalculation, dsCustomerData, dsDMuc);
                    if (strError != "")
                    {
                        WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_2", "Tính sản lượng chi tiết - " + strError);
                        return strError;
                    }
                    //Xu ly cho KH tinh vat qua doi gia
                    strError = detailDataCalculation.DetailDataCalculation_21(this.dsCalculation, dsCustomerData, dsDMuc);
                    if (strError != "")
                    {
                        WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_21", "Tính sản lượng chi tiết trong kỳ đổi giá - " + strError);
                        return strError;
                    }
                    strError = detailDataCalculation.DetailDataCalculation_3(this.dsCalculation, dsCustomerData, dsDMuc);
                    if (strError != "")
                    {
                        WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_3", "Tính sản lượng chi tiết - " + strError);
                        return strError;
                    }
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_3", "Tính xong sản lượng chi tiết");

                    strError = detailDataCalculation.DetailDataCalculation_40(this.dsCalculation, dsCustomerData, dsDMuc, dsInvoiceData, strMa_DViQLy, strMa_SoGCS, ky, thang, nam);
                    if (strError != "")
                    {
                        WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_4", "Tính sản lượng chi tiết - " + strError);
                        return strError;
                    }
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_4", "Tính xong sản lượng chi tiết");

                    strError = detailDataCalculation.DetailDataCalculation_41(this.dsCalculation, dsDMuc, dsCustomerData);
                    if (strError != "")
                    {
                        WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_41", "Tính sản lượng chi tiết cho bán buôn trong kỳ đổi giá - " + strError);
                        return strError;
                    }
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_41", "Tính sản lượng chi tiết cho bán buôn trong kỳ đổi giá");

                    strError = detailDataCalculation.DetailDataCalculation_42(this.dsCalculation);
                    if (strError != "")
                    {
                        WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_42", "Xử lý riêng cho bán buôn SHBB nhóm L trong tháng 03/2011 - " + strError);
                        return strError;
                    }
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_42", "Xử lý riêng cho bán buôn SHBB nhóm L trong tháng 03/2011");



                    //Xu ly cho KH tinh qua doi gia
                    strError = priceSpecification.PriceSpecification_11(this.dsCalculation, dsDMuc, dsCustomerData);
                    if (strError != "")
                    {
                        WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsPriceSpecification", "PriceSpecification_1", "Xác định đơn giá - " + strError);
                        return strError;
                    }

                    //Xu ly lam tron ngang cho gia bac thang
                    strError = priceSpecification.PriceSpecification_12(this.dsCalculation, dsDMuc, dsCustomerData);
                    if (strError != "")
                    {
                        WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsPriceSpecification", "PriceSpecification_12", "Xử lý riêng cho giá bậc thang trong kỳ đổi giá - " + strError);
                        return strError;
                    }

                    //strError = priceSpecification.PriceSpecification_2(this.dsCalculation, dsDMuc);
                    //Xu ly cho KH tinh qua doi gia
                    strError = priceSpecification.PriceSpecification_21(this.dsCalculation, dsDMuc, dsCustomerData);
                    if (strError != "")
                    {
                        WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsPriceSpecification", "PriceSpecification_2", "Xác định đơn giá - " + strError);
                        return strError;
                    }
                    strError = priceSpecification.PriceSpecification_3(this.dsCalculation, dsCustomerData, this.dsInvoiceData, strTen_DNhap, strNgayGhiPS, ky, thang, nam);
                    if (strError != "")
                    {
                        WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsPriceSpecification", "PriceSpecification_3", "Xác định đơn giá - " + strError);
                        return strError;
                    }
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsPriceSpecification", "PriceSpecification_3", "Kết thúc xác định đơn giá");
                    #endregion
                    foreach (DataTable dt in dsCalculation.Tables)
                    {
                        if (dsCalculationAll.Tables.Count > 0 && dsCalculationAll.Tables[dt.TableName] != null)
                        {
                            dsCalculationAll.Tables[dt.TableName].Merge(dt, false, MissingSchemaAction.Add);
                        }
                        else
                        {
                            dsCalculationAll.Tables.Add(dt.Copy());
                        }
                    }
                    foreach (DataTable dt in dsInvoiceData.Tables)
                    {
                        if (dsInvoiceDataAll.Tables.Count > 0 && dsInvoiceDataAll.Tables[dt.TableName] != null)
                        {
                            dsInvoiceDataAll.Tables[dt.TableName].Merge(dt, false, MissingSchemaAction.Add);
                        }
                        else
                        {
                            dsInvoiceDataAll.Tables.Add(dt.Copy());
                        }
                    }
                }
                if (!dsInvoiceDataAll.Tables["HDN_HDONCOSFI"].Columns.Contains("NOI_DUNG"))
                    dsInvoiceDataAll.Tables["HDN_HDONCOSFI"].Columns.Add("NOI_DUNG");
                if (!dsInvoiceDataAll.Tables["HDN_HDONCTIET"].Columns.Contains("NOI_DUNG"))
                    dsInvoiceDataAll.Tables["HDN_HDONCTIET"].Columns.Add("NOI_DUNG");
                //Duyệt bổ sung thêm dữ liệu kỳ tháng năm tính toán cho cột nội dung
                foreach (DataRow dr in dsInvoiceDataAll.Tables["HDN_HDONCTIET"].Rows)
                {
                    dr["NOI_DUNG"] = dr["KY"] + "-" + dr["THANG"] + "-" + dr["NAM"];
                    dr["KY"] = ky;
                    dr["THANG"] = thang;
                    dr["NAM"] = nam;
                }
                foreach (DataRow dr in dsInvoiceDataAll.Tables["HDN_HDONCOSFI"].Rows)
                {
                    dr["NOI_DUNG"] = dr["KY"] + "-" + dr["THANG"] + "-" + dr["NAM"];
                    dr["KY"] = ky;
                    dr["THANG"] = thang;
                    dr["NAM"] = nam;
                }
                //Trả lại bảng GCS_CHISO
                dsCustomerData.Tables.Remove("GCS_CHISO");

                dsCustomerData.Tables.Add(DTGCS_CHISO);
                //dsCustomerData.WriteXml(@"C:\CustomerData.xml");
                //dsDMuc.WriteXml(@"C:\StaticCatalog.xml");
                //dsCalculation.WriteXml(@"C:\CalculationData.xml");
                //dsInvoiceData.WriteXml(@"C:\InvoiceData.xml");

                //Tinh hoa don
                //Kiem tra tham so G_INCHUNGRIENG = 'C' thi se ra hoa don TC
                if (dsDMuc.Tables.Contains("S_PARAMETER") == true)
                {
                    if (dsDMuc.Tables["S_PARAMETER"].Select("NAME = 'G_INCHUNGRIENG' AND (PRAVALUE = 'C' OR PRAVALUE = 'c')").Length > 0)
                    {
                        strError = invoiceCalculation.InvoiceCalculation_TC(ref dsInvoiceDataAll, dsCustomerData, dsCalculationAll, strMa_DViQLy, strMa_SoGCS, ky, thang, nam, dsDMuc);
                    }
                    else
                    {
                        strError = invoiceCalculation.InvoiceCalculation(ref dsInvoiceDataAll, dsCustomerData, dsCalculationAll, strMa_DViQLy, strMa_SoGCS, ky, thang, nam, dsDMuc);
                    }
                }
                else
                {
                    strError = invoiceCalculation.InvoiceCalculation(ref dsInvoiceDataAll, dsCustomerData, dsCalculationAll, strMa_DViQLy, strMa_SoGCS, ky, thang, nam, dsDMuc);
                }

                if (strError != "")
                {
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsInvoiceCalculation", "InvoiceCalculation", "Tính hoá đơn - " + strError);
                    return strError;
                }
                else
                {
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsInvoiceCalculation", "InvoiceCalculation", "Tính xong hoá đơn");
                }

                //Cap nhat du lieu
                strError = outputDataWriting.InsertInvoiceData_DC_Plus(dsInvoiceDataAll, dsCustomerData);
                if (strError != "")
                {
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busOutputDataWriting", "busOutputDataWriting.cls_OutputDataWriting", "InsertInvoiceData", "Cập nhật dữ liệu hoá đơn - " + strError);
                    return strError;
                }
                else
                {
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busOutputDataWriting", "busOutputDataWriting.cls_OutputDataWriting", "InsertInvoiceData", "Cập nhật dữ liệu hoá đơn");
                }

                //Ket thuc tinh hoa don
                WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "BillingImplementation", "BillingImplementation", "BillingImplementation", "Tính xong sổ " + strMa_SoGCS + " - hoá đơn kỳ " + ky + " tháng " + thang + "/" + nam);

                return "";
            }
            catch (Exception ex)
            {
                //Ghi log vao dataset
                WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "BillingImplementation", "BillingImplementation", "BillingImplementation", "Lỗi khi tính hoá đơn: sổ " + strMa_SoGCS + " - hoá đơn kỳ " + ky + " tháng " + thang + "/" + nam);
                return ex.Message;
            }
            finally
            {
                dsCustomerData.Reset();
                dsInvoiceData.Reset();
                dsCalculation.Reset();
            }
        }
        public object CommonImplementation_KTGS_API(DataSet dsDanhMuc, ref DataSet dsResult, DataSet dsCustomerData, ref string strError)
        {
            string strMa_DViQLy = "";
            string strMa_SoGCS = "";
            Int16 ky = 0;
            Int16 thang = 0;
            Int16 nam = 0;
            Int16 iLoaiKHang = 0;
            string strTen_DNhap = "";

            DataSet dsDMuc = dsDanhMuc;
            //DateTime dtHLucNSH = new DateTime(2020, 4, 16);
            //DateTime dtHLucSH = new DateTime(2020, 5, 1);
            //DateTime dtHetHLucNSH = new DateTime(2020, 7, 16);
            //DateTime dtHetHLucSH = new DateTime(2020, 8, 1);
            DateTime dtOld = config.dtGiaHTro;
            DateTime dtNew = new DateTime(2090, 8, 1);
            Int16 thangps = 0, namps = 0;
            DateTime dtNgayCKy = new DateTime(1900, 1, 1);
            //tách theo từng kỳ tháng năm của chỉ số
            List<string> lstKTN = new List<string>();
            List<DataTable> lstCSo = new List<DataTable>();
            //List<DataTable> lstCTiet = new List<DataTable>();
            //Lưu lại bảng HDG_BBAN_APGIA ban đâu
            DataTable dtHDG_BBAN_APGIA = dsCustomerData.Tables["HDG_BBAN_APGIA"].Copy();
            dtHDG_BBAN_APGIA.TableName = "HDG_BBAN_APGIA";
            DataTable DTGCS_CHISO = dsCustomerData.Tables["GCS_CHISO"].Copy();
            DTGCS_CHISO.TableName = "GCS_CHISO";
            DataView dvCS = new DataView(dsCustomerData.Tables["GCS_CHISO"]);
            dvCS.Sort = "NAM ASC, THANG ASC, KY ASC";
            //DataView dvCT = new DataView(dsCustomerData.Tables["HDN_HDONCTIET"]);
            //dvCT.Sort = "NAM ASC, THANG ASC, KY ASC";
            int j = -1;
            foreach (DataRowView dr in dvCS)
            {
                string strKTN = dr["KY"].ToString() + "-" + dr["THANG"].ToString() + "-" + dr["NAM"].ToString();
                if (lstCSo.Count > 0 && lstCSo[j] != null && lstCSo[j].TableName == strKTN)
                {
                    lstCSo[j].ImportRow(dr.Row);
                }
                else
                {
                    j++;
                    DataTable dt = dsCustomerData.Tables["GCS_CHISO"].Clone();
                    dt.TableName = strKTN;
                    lstCSo.Add(dt);
                    lstCSo[j].ImportRow(dr.Row);
                }
            }



            try
            {
                DataSet dsCalculationAll = new DS_CALCULATIONTABLES();
                //dsCustomerData = new DataSet();
                DataSet dsInvoiceDataAll = new DataSet();




                log = new cls_LogManagement(dsLog);

                strMa_DViQLy = dsCustomerData.Tables["HDN_HDON"].Rows[0]["MA_DVIQLY"].ToString();
                strMa_SoGCS = dsCustomerData.Tables["HDN_HDON"].Rows[0]["MA_SOGCS"].ToString();
                ky = Convert.ToInt16(dsCustomerData.Tables["HDN_HDON"].Rows[0]["KY"]);
                thangps = Convert.ToInt16(dsCustomerData.Tables["HDN_HDON"].Rows[0]["THANG"]);
                namps = Convert.ToInt16(dsCustomerData.Tables["HDN_HDON"].Rows[0]["NAM"]);
                //iLoaiKHang = Convert.ToInt16(dsCustomerData.Tables["HDN_HDON"].Rows[0]["LOAI_KHANG"]);
                dtNgayCKy = Convert.ToDateTime(dsCustomerData.Tables["HDN_HDON"].Rows[0]["NGAY_CKY"]);
                string strNgayGhiPS = dtNgayCKy.ToString("dd/MM/yyyy");

                //Hóa đơn KTRAGSMBD, tính hết giá 648, loại bỏ các nhóm giá 1/1/2020 và 1/2/2021
                //dsDMuc = dsStaticCatalog.Copy();
                //ChangeDate(ref dsDMuc, dtOld, dtNew);
                //dtOld = new DateTime(2020, 1, 1);
                //ChangeDate(ref dsDMuc, dtOld, dtNew);


                thang = Convert.ToInt16(dsCustomerData.Tables["HDN_BBAN_DCHINH"].Rows[0]["THANG_DC"]);
                nam = Convert.ToInt16(dsCustomerData.Tables["HDN_BBAN_DCHINH"].Rows[0]["NAM_DC"]);
                strTen_DNhap = dsCustomerData.Tables["HDN_BBAN_DCHINH"].Rows[0]["NGUOI_TAO"].ToString();
                //string strError = "";

                //Bat dau tinh hoa don
                WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "BillingImplementation", "BillingImplementation", "BillingImplementation", "Tính hoá đơn sổ " + strMa_SoGCS + " - kỳ " + ky + " tháng " + thang + "/" + nam);

                for (int i = 0; i < lstCSo.Count; i++)
                {
                    dsCustomerData.Tables.Remove("GCS_CHISO");
                    //dsCustomerData.Tables.Remove("HDN_HDONCTIET");
                    dsCustomerData.Tables.Remove("HDG_BBAN_APGIA");
                    lstCSo[i].TableName = "GCS_CHISO";
                    //lstCTiet[i].TableName = "HDN_HDONCTIET";
                    dsCustomerData.Tables.Add(lstCSo[i]);
                    //dsCustomerData.Tables.Add(lstCTiet[i]);
                    DataTable dtAG = dtHDG_BBAN_APGIA.Copy();
                    dtAG.TableName = "HDG_BBAN_APGIA";
                    dsCustomerData.Tables.Add(dtAG);
                    #region Tính toán chi tiết

                    dsInvoiceData = new DataSet();
                    dsCalculation = new DS_CALCULATIONTABLES();

                    //Loc bien ban ap gia, chi lay cac bien ban co lien quan den ky ghi chi so hien tai
                    strError = this.FilterHDG_BBAN_APGIA(ref dsCustomerData);
                    if (strError != "")
                    {
                        WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "BillingImplementation", "BillingImplementation.clsBillingImplementation.cs", "FilterHDG_BBAN_APGIA", "Lọc biên bản áp giá - " + strError);
                        return strError;
                    }
                    else
                    {
                        WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "BillingImplementation", "BillingImplementation.clsBillingImplementation.cs", "FilterHDG_BBAN_APGIA", "Lọc biên bản áp giá");
                    }
                    //Đổi loại chỉ số CPK sang DDN, sau bỏ đi
                    strError = this.AddColumn(ref dsCustomerData, dsDMuc);
                    if (strError != "")
                    {
                        WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "AddColumn", "Thêm cột mới - " + strError);
                        return strError;
                    }



                    //Kiem tra du lieu dau vao
                    strError = inputDataReading.CheckValidData(strMa_DViQLy, dsCustomerData, dsDMuc);
                    if (strError != "")
                    {
                        WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busInputDataReading", "busInputDataReading.cls_InputDataReading", "CheckValidData", "Kiểm tra dữ liệu đầu vào - " + strError);
                        return strError;
                    }
                    else
                    {
                        WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busInputDataReading", "busInputDataReading.cls_InputDataReading", "CheckValidData", "Kiểm tra dữ liệu đầu vào");
                    }
                    //Tao cau truc du lieu dau ra
                    dsInvoiceData = cls_OutputDataWriting.createDSInvoiceData();

                    //Tinh san luong tho
                    strError = rawDataCalculation.RawDataCalculation_1(dsCustomerData);
                    if (strError != "")
                    {
                        WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "RawDataCalculation_1", "Tính sản lượng thô - " + strError);
                        return strError;
                    }

                    strError = rawDataCalculation.RawDataCalculation_2(dsCustomerData);
                    if (strError != "")
                    {
                        WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "RawDataCalculation_2", "Tính sản lượng thô - " + strError);
                        return strError;
                    }

                    strError = rawDataCalculation.RawDataCalculation_3(dsCustomerData);
                    if (strError != "")
                    {
                        WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "RawDataCalculation_3", "Tính sản lượng thô - " + strError);
                        return strError;
                    }

                    strError = rawDataCalculation.RawDataCalculation_4(dsCustomerData, dsDMuc);
                    if (strError != "")
                    {
                        WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "RawDataCalculation_4", "Tính sản lượng thô - " + strError);
                        return strError;
                    }

                    strError = rawDataCalculation.RawDataCalculation_5(dsCustomerData, this.dsInvoiceData, dsDMuc, strTen_DNhap);
                    if (strError != "")
                    {
                        WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "RawDataCalculation_5", "Tính sản lượng thô - " + strError);
                        return strError;
                    }

                    strError = rawDataCalculation.RawDataCalculation_6(dsCustomerData, this.dsInvoiceData, dsDMuc, strTen_DNhap);
                    if (strError != "")
                    {
                        WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "RawDataCalculation_6", "Tính sản lượng thô - " + strError);
                        return strError;
                    }

                    strError = rawDataCalculation.RawDataCalculation_7(dsCustomerData, this.dsInvoiceData, dsDMuc, strTen_DNhap);
                    if (strError != "")
                    {
                        WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "RawDataCalculation_7", "Tính sản lượng thô - " + strError);
                        return strError;
                    }
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "RawDataCalculation_7", "Tính xong sản lượng thô");

                    //Tinh san luong chi tiet

                    strError = detailDataCalculation.DetailDataCalculation_11(dsCustomerData, ref this.dsCalculation, dsDMuc);
                    if (strError != "")
                    {
                        WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_11", "Tính sản lượng chi tiết - " + strError);
                        return strError;
                    }

                    strError = detailDataCalculation.DetailDataCalculation_2(this.dsCalculation, dsCustomerData, dsDMuc);
                    if (strError != "")
                    {
                        WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_2", "Tính sản lượng chi tiết - " + strError);
                        return strError;
                    }
                    //Xu ly cho KH tinh vat qua doi gia
                    strError = detailDataCalculation.DetailDataCalculation_21(this.dsCalculation, dsCustomerData, dsDMuc);
                    if (strError != "")
                    {
                        WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_21", "Tính sản lượng chi tiết trong kỳ đổi giá - " + strError);
                        return strError;
                    }
                    strError = detailDataCalculation.DetailDataCalculation_3(this.dsCalculation, dsCustomerData, dsDMuc);
                    if (strError != "")
                    {
                        WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_3", "Tính sản lượng chi tiết - " + strError);
                        return strError;
                    }
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_3", "Tính xong sản lượng chi tiết");

                    strError = detailDataCalculation.DetailDataCalculation_40(this.dsCalculation, dsCustomerData, dsDMuc, dsInvoiceData, strMa_DViQLy, strMa_SoGCS, ky, thang, nam);
                    if (strError != "")
                    {
                        WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_4", "Tính sản lượng chi tiết - " + strError);
                        return strError;
                    }
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_4", "Tính xong sản lượng chi tiết");

                    strError = detailDataCalculation.DetailDataCalculation_41(this.dsCalculation, dsDMuc, dsCustomerData);
                    if (strError != "")
                    {
                        WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_41", "Tính sản lượng chi tiết cho bán buôn trong kỳ đổi giá - " + strError);
                        return strError;
                    }
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_41", "Tính sản lượng chi tiết cho bán buôn trong kỳ đổi giá");

                    strError = detailDataCalculation.DetailDataCalculation_42(this.dsCalculation);
                    if (strError != "")
                    {
                        WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_42", "Xử lý riêng cho bán buôn SHBB nhóm L trong tháng 03/2011 - " + strError);
                        return strError;
                    }
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_42", "Xử lý riêng cho bán buôn SHBB nhóm L trong tháng 03/2011");



                    //Xu ly cho KH tinh qua doi gia
                    strError = priceSpecification.PriceSpecification_11(this.dsCalculation, dsDMuc, dsCustomerData);
                    if (strError != "")
                    {
                        WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsPriceSpecification", "PriceSpecification_1", "Xác định đơn giá - " + strError);
                        return strError;
                    }

                    //Xu ly lam tron ngang cho gia bac thang
                    strError = priceSpecification.PriceSpecification_12(this.dsCalculation, dsDMuc, dsCustomerData);
                    if (strError != "")
                    {
                        WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsPriceSpecification", "PriceSpecification_12", "Xử lý riêng cho giá bậc thang trong kỳ đổi giá - " + strError);
                        return strError;
                    }

                    //strError = priceSpecification.PriceSpecification_2(this.dsCalculation, dsDMuc);
                    //Xu ly cho KH tinh qua doi gia
                    strError = priceSpecification.PriceSpecification_21(this.dsCalculation, dsDMuc, dsCustomerData);
                    if (strError != "")
                    {
                        WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsPriceSpecification", "PriceSpecification_2", "Xác định đơn giá - " + strError);
                        return strError;
                    }
                    strError = priceSpecification.PriceSpecification_3(this.dsCalculation, dsCustomerData, this.dsInvoiceData, strTen_DNhap, strNgayGhiPS, ky, thang, nam);
                    if (strError != "")
                    {
                        WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsPriceSpecification", "PriceSpecification_3", "Xác định đơn giá - " + strError);
                        return strError;
                    }
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsPriceSpecification", "PriceSpecification_3", "Kết thúc xác định đơn giá");
                    #endregion
                    foreach (DataTable dt in dsCalculation.Tables)
                    {
                        if (dsCalculationAll.Tables.Count > 0 && dsCalculationAll.Tables[dt.TableName] != null)
                        {
                            dsCalculationAll.Tables[dt.TableName].Merge(dt, false, MissingSchemaAction.Add);
                        }
                        else
                        {
                            dsCalculationAll.Tables.Add(dt.Copy());
                        }
                    }
                    foreach (DataTable dt in dsInvoiceData.Tables)
                    {
                        if (dsInvoiceDataAll.Tables.Count > 0 && dsInvoiceDataAll.Tables[dt.TableName] != null)
                        {
                            dsInvoiceDataAll.Tables[dt.TableName].Merge(dt, false, MissingSchemaAction.Add);
                        }
                        else
                        {
                            dsInvoiceDataAll.Tables.Add(dt.Copy());
                        }
                    }
                }
                if (!dsInvoiceDataAll.Tables["HDN_HDONCOSFI"].Columns.Contains("NOI_DUNG"))
                    dsInvoiceDataAll.Tables["HDN_HDONCOSFI"].Columns.Add("NOI_DUNG");
                if (!dsInvoiceDataAll.Tables["HDN_HDONCTIET"].Columns.Contains("NOI_DUNG"))
                    dsInvoiceDataAll.Tables["HDN_HDONCTIET"].Columns.Add("NOI_DUNG");
                //Duyệt bổ sung thêm dữ liệu kỳ tháng năm tính toán cho cột nội dung
                foreach (DataRow dr in dsInvoiceDataAll.Tables["HDN_HDONCTIET"].Rows)
                {
                    dr["NOI_DUNG"] = dr["KY"] + "-" + dr["THANG"] + "-" + dr["NAM"];
                    dr["KY"] = ky;
                    dr["THANG"] = thang;
                    dr["NAM"] = nam;
                }
                foreach (DataRow dr in dsInvoiceDataAll.Tables["HDN_HDONCOSFI"].Rows)
                {
                    dr["NOI_DUNG"] = dr["KY"] + "-" + dr["THANG"] + "-" + dr["NAM"];
                    dr["KY"] = ky;
                    dr["THANG"] = thang;
                    dr["NAM"] = nam;
                }
                //Trả lại bảng GCS_CHISO
                dsCustomerData.Tables.Remove("GCS_CHISO");

                dsCustomerData.Tables.Add(DTGCS_CHISO);
                //dsCustomerData.WriteXml(@"C:\CustomerData.xml");
                //dsDMuc.WriteXml(@"C:\StaticCatalog.xml");
                //dsCalculation.WriteXml(@"C:\CalculationData.xml");
                //dsInvoiceData.WriteXml(@"C:\InvoiceData.xml");

                //Tinh hoa don
                //Kiem tra tham so G_INCHUNGRIENG = 'C' thi se ra hoa don TC
                if (dsDMuc.Tables.Contains("S_PARAMETER") == true)
                {
                    if (dsDMuc.Tables["S_PARAMETER"].Select("NAME = 'G_INCHUNGRIENG' AND (PRAVALUE = 'C' OR PRAVALUE = 'c')").Length > 0)
                    {
                        strError = invoiceCalculation.InvoiceCalculation_TC(ref dsInvoiceDataAll, dsCustomerData, dsCalculationAll, strMa_DViQLy, strMa_SoGCS, ky, thang, nam, dsDMuc);
                    }
                    else
                    {
                        strError = invoiceCalculation.InvoiceCalculation(ref dsInvoiceDataAll, dsCustomerData, dsCalculationAll, strMa_DViQLy, strMa_SoGCS, ky, thang, nam, dsDMuc);
                    }
                }
                else
                {
                    strError = invoiceCalculation.InvoiceCalculation(ref dsInvoiceDataAll, dsCustomerData, dsCalculationAll, strMa_DViQLy, strMa_SoGCS, ky, thang, nam, dsDMuc);
                }

                if (strError != "")
                {
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsInvoiceCalculation", "InvoiceCalculation", "Tính hoá đơn - " + strError);
                    return strError;
                }
                else
                {
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsInvoiceCalculation", "InvoiceCalculation", "Tính xong hoá đơn");
                }
                //Cap nhat du lieu
                object objResult = outputDataWriting.InsertInvoiceData_DC_Plus_Mobile(dsInvoiceDataAll, dsCustomerData, ref strError);
                if (strError != "")
                {
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busOutputDataWriting", "busOutputDataWriting.cls_OutputDataWriting", "InsertInvoiceData", "Cập nhật dữ liệu hoá đơn - " + strError);
                    return null;
                }
                else
                {
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busOutputDataWriting", "busOutputDataWriting.cls_OutputDataWriting", "InsertInvoiceData", "Cập nhật dữ liệu hoá đơn");
                }

                //Ket thuc tinh hoa don
                WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "BillingImplementation", "BillingImplementation", "BillingImplementation", "Tính xong sổ " + strMa_SoGCS + " - hoá đơn kỳ " + ky + " tháng " + thang + "/" + nam);


                //Không cập nhật dữ liệu, trả về luôn
                //dsResult = new DataSet();
                //dsResult = dsInvoiceDataAll.Copy();
                //DateTime dt7 = DateTime.Now;

                return objResult;
            }
            catch (Exception ex)
            {
                //Ghi log vao dataset
                WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "BillingImplementation", "BillingImplementation", "BillingImplementation", "Lỗi khi tính hoá đơn: sổ " + strMa_SoGCS + " - hoá đơn kỳ " + ky + " tháng " + thang + "/" + nam);
                strError = ex.Message;
                return null;
            }
            finally
            {
                dsCustomerData.Reset();
                dsInvoiceData.Reset();
                dsCalculation.Reset();
            }
        }
        #endregion
        public string getDataXML(string strMa_DViQLy, string strMa_SoGCS, short ky, short thang, short nam, string strTen_DNhap, long lngCurrentLibID, long lngWorkflowID, string strNgayGhi)
        {
            try
            {
                DataSet dsCustomerData = new DataSet();
                string strError = inputDataReading.getCustomerDataReadingPlus(ref dsCustomerData, strMa_DViQLy, strMa_SoGCS, ky, thang, nam, strNgayGhi);
                dsCustomerData.WriteXml("D:/CustomerDataAPI.xml", XmlWriteMode.WriteSchema);
                return "Success";
            }
            catch (Exception ex)
            {
                return ex.ToString();
            }
        }
        #region CMIS 2022
        public DataSet getMaSoGCSPlusList(int i32LoaiTTac, int i32SLuong, ref string strError)
        {

            try
            {
                //string strError = "";
                cls_LogManagement log = new cls_LogManagement();
                return log.getMaSoGCSPlusList("Chưa tính", "Đang tính", i32LoaiTTac, i32SLuong, ref strError);
            }
            catch (Exception ex)
            {
                strError = ex.Message;
                return null;
            }
            finally
            {


            }
        }
        public List<List<object>> getMaSoGCSPlusAPI(int i32LoaiTTac,string maDviQly, int i32SLuong, ref string strError)
        {

            try
            {
                //string strError = "";
                cls_LogManagement log = new cls_LogManagement();
                return log.getMaSoGCSPlusListAPI("Chưa tính", "Đang tính", maDviQly, i32LoaiTTac, i32SLuong, ref strError);
            }
            catch (Exception ex)
            {
                strError = ex.Message;
                return null;
            }
            finally
            {


            }
        }
        private string CommonCalculationNoInsert_DCN(string strMa_DViQLy, string strMa_SoGCS, string strNgayGhi, short ky, short thang, short nam, string strTen_DNhap, long lngCurrentLibID, long lngWorkflowID, DataSet dsCustomerData, DataSet dsDMuc, ref DataSet dsInvoiceData)
        {

            try
            {
                string strError = "";
                dsCalculation = new DS_CALCULATIONTABLES();

                //Tao cau truc du lieu dau ra
                dsInvoiceData = cls_OutputDataWriting.createDSInvoiceData();


                //Tinh san luong tho
                strError = rawDataCalculation.RawDataCalculation_1(dsCustomerData);
                if (strError != "")
                {
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "RawDataCalculation_1", "Tính sản lượng thô - " + strError);
                    return strError;
                }
                else
                {
                    //Kiem tra neu khong con du lieu chi so thi thong bao ket thuc tinh hoa don luon
                    if (this.dsCustomerData.Tables.Contains("GCS_CHISO") == true)
                    {
                        if (this.dsCustomerData.Tables["GCS_CHISO"].Rows.Count == 0)
                        {
                            //Ket thuc tinh hoa don
                            WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "BillingImplementation", "BillingImplementation", "BillingImplementation", "Sổ không có dữ liệu chỉ số. Kết thúc tính hoá đơn của sổ " + strMa_SoGCS + " - kỳ " + ky + " tháng " + thang + "/" + nam);
                            return "";
                        }
                    }
                }

                strError = rawDataCalculation.RawDataCalculation_2(dsCustomerData);
                if (strError != "")
                {
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "RawDataCalculation_2", "Tính sản lượng thô - " + strError);
                    return strError;
                }

                strError = rawDataCalculation.RawDataCalculation_3(dsCustomerData);
                if (strError != "")
                {
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "RawDataCalculation_3", "Tính sản lượng thô - " + strError);
                    return strError;
                }

                strError = rawDataCalculation.RawDataCalculation_4(dsCustomerData, dsStaticCatalog);
                if (strError != "")
                {
                    //Rieng phan tinh tru phu chi ghi log, khong can thoat
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "RawDataCalculation_4", "Tính sản lượng thô - " + strError);
                    return strError;
                }

                strError = rawDataCalculation.RawDataCalculation_5(dsCustomerData, dsInvoiceData, dsDMuc, strTen_DNhap);
                if (strError != "")
                {
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "RawDataCalculation_5", "Tính sản lượng thô - " + strError);
                    return strError;
                }

                strError = rawDataCalculation.RawDataCalculation_6(dsCustomerData, dsInvoiceData, dsDMuc, strTen_DNhap);
                if (strError != "")
                {
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "RawDataCalculation_6", "Tính sản lượng thô - " + strError);
                    return strError;
                }

                strError = rawDataCalculation.RawDataCalculation_7(dsCustomerData, dsInvoiceData, dsDMuc, strTen_DNhap);
                if (strError != "")
                {
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "RawDataCalculation_7", "Tính sản lượng thô - " + strError);
                    return strError;
                }
                WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "RawDataCalculation_7", "Tính xong sản lượng thô");

                //Tinh san luong chi tiet
                strError = detailDataCalculation.DetailDataCalculation_11(dsCustomerData, ref dsCalculation, dsDMuc);
                if (strError != "")
                {
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_11", "Tính sản lượng chi tiết - " + strError);
                    return strError;
                }

                strError = detailDataCalculation.DetailDataCalculation_2(dsCalculation, dsCustomerData, dsDMuc);
                if (strError != "")
                {
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_2", "Tính sản lượng chi tiết - " + strError);
                    return strError;
                }
                //Xu ly cho KH tinh vat qua doi gia
                strError = detailDataCalculation.DetailDataCalculation_21(dsCalculation, dsCustomerData, dsDMuc);
                if (strError != "")
                {
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_21", "Tính sản lượng chi tiết trong kỳ đổi giá - " + strError);
                    return strError;
                }

                strError = detailDataCalculation.DetailDataCalculation_3(dsCalculation, dsCustomerData, dsDMuc);
                if (strError != "")
                {
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_3", "Tính sản lượng chi tiết - " + strError);
                    return strError;
                }

                strError = detailDataCalculation.DetailDataCalculation_40(dsCalculation, dsCustomerData, dsDMuc, dsInvoiceData, strMa_DViQLy, strMa_SoGCS, ky, thang, nam);
                if (strError != "")
                {
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_4", "Tính sản lượng chi tiết - " + strError);
                    return strError;
                }
                WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_4", "Tính xong sản lượng chi tiết");

                strError = detailDataCalculation.DetailDataCalculation_41(dsCalculation, dsDMuc, dsCustomerData);
                if (strError != "")
                {
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_41", "Tính sản lượng chi tiết cho bán buôn trong kỳ đổi giá - " + strError);
                    return strError;
                }
                WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsDetailDataCalculation", "DetailDataCalculation_41", "Tính sản lượng chi tiết cho bán buôn trong kỳ đổi giá");


                //Xac dinh don gia
                //strError = priceSpecification.PriceSpecification_1(this.dsCalculation, clsBillingImplementation.dsStaticCatalog, dsCustomerData);
                //Xu ly cho KH tinh qua doi gia
                //Ko làm tròn ngang cho nhóm bậc thang
                strError = priceSpecification.PriceSpecification_11(dsCalculation, dsDMuc, dsCustomerData);
                if (strError != "")
                {
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsPriceSpecification", "PriceSpecification_1", "Xử lý riêng cho giá bậc thang - " + strError);
                    return strError;
                }

                //Xu ly lam tron ngang cho gia bac thang
                strError = priceSpecification.PriceSpecification_12(dsCalculation, dsDMuc, dsCustomerData);
                if (strError != "")
                {
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsPriceSpecification", "PriceSpecification_12", "Xử lý riêng cho giá bậc thang trong kỳ đổi giá - " + strError);
                    return strError;
                }

                //strError = priceSpecification.PriceSpecification_2(this.dsCalculation, clsBillingImplementation.dsStaticCatalog);
                //Xu ly cho KH tinh qua doi gia
                strError = priceSpecification.PriceSpecification_21(dsCalculation, dsDMuc, this.dsCustomerData);
                if (strError != "")
                {
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsPriceSpecification", "PriceSpecification_2", "Xác định đơn giá - " + strError);
                    return strError;
                }

                strError = priceSpecification.PriceSpecification_3(dsCalculation, dsCustomerData, dsInvoiceData, strTen_DNhap, strNgayGhi, ky, thang, nam);
                if (strError != "")
                {
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsPriceSpecification", "PriceSpecification_3", "Xác định đơn giá - " + strError);
                    return strError;
                }
                WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsPriceSpecification", "PriceSpecification_3", "Kết thúc xác định đơn giá");


                //Tinh hoa don
                //Kiem tra tham so G_INCHUNGRIENG = 'C' thi se ra hoa don TC
                if (dsStaticCatalog.Tables.Contains("S_PARAMETER") == true)
                {
                    if (dsStaticCatalog.Tables["S_PARAMETER"].Select("NAME = 'G_INCHUNGRIENG' AND (PRAVALUE = 'C' OR PRAVALUE = 'c')").Length > 0)
                    {

                        strError = invoiceCalculation.InvoiceCalculation_TC(ref dsInvoiceData, dsCustomerData, dsCalculation, strMa_DViQLy, strMa_SoGCS, ky, thang, nam, dsDMuc);
                    }
                    else
                    {

                        strError = invoiceCalculation.InvoiceCalculation_2022(ref dsInvoiceData, dsCustomerData, dsCalculation, strMa_DViQLy, strMa_SoGCS, ky, thang, nam, dsDMuc);
                    }
                }
                else
                {

                    strError = invoiceCalculation.InvoiceCalculation_2022(ref dsInvoiceData, dsCustomerData, dsCalculation, strMa_DViQLy, strMa_SoGCS, ky, thang, nam, dsDMuc);
                }
                if (strError != "")
                {
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsInvoiceCalculation", "InvoiceCalculation", "Tính hoá đơn - " + strError);
                    return strError;
                }
                else
                {
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsInvoiceCalculation", "InvoiceCalculation", "Tính xong hoá đơn");
                }



                return "";
            }
            catch (Exception ex)
            {
                //Ghi log vao dataset
                WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "BillingImplementation", "BillingImplementation", "BillingImplementation", "Lỗi khi tính hoá đơn: sổ " + strMa_SoGCS + " - hoá đơn kỳ " + ky + " tháng " + thang + "/" + nam);
                return ex.Message;
            }
            finally
            {
                //dsCustomerData.Reset();
                //dsInvoiceData.Reset();
                //dsCalculation.Reset();
            }
        }
        private async Task<string> CommonImplementationPlus_DCNAsync(string strMa_DViQLy, string strMa_SoGCS, short ky, short thang, short nam, string strTen_DNhap, long lngCurrentLibID, long lngWorkflowID, string strNgayGhi)
        {
            try
            {
                dsCustomerData = new DataSet();
                dsInvoiceData = new DataSet();
                DataSet dsInvoiceDataDCN = new DataSet();
                dsCalculation = new DS_CALCULATIONTABLES();

                string strError = "";

                //Bat dau tinh hoa don
                WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "BillingImplementation", "BillingImplementation", "BillingImplementation", "Tính hoá đơn sổ " + strMa_SoGCS + " - kỳ " + ky + " tháng " + thang + "/" + nam);
                //Kiem tra so phu ghep tong
                strError = await inputDataReading.checkPhuGhepTongPlusAsync(strMa_DViQLy, strMa_SoGCS, ky, thang, nam, strNgayGhi);
                if (strError != "" && strError != " ")
                {
                    if (strError == "1")
                    {
                        WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busInputDataReading", "busInputDataReading.cls_InputDataReading", "checkPhuGhepTong", "Kiểm tra sổ phụ ghép tổng.");
                        strError = outputDataWriting.InsertInvoiceData_PGT_Plus(strMa_DViQLy, strMa_SoGCS, strTen_DNhap, lngCurrentLibID, lngWorkflowID, ky, thang, nam);
                        if (strError != "")
                        {
                            WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busOutputDataWriting", "busOutputDataWriting.cls_OutputDataWriting", "InsertInvoiceData", "Cập nhật dữ liệu hoá đơn - " + strError);
                            return strError;
                        }
                        else
                        {
                            WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busOutputDataWriting", "busOutputDataWriting.cls_OutputDataWriting", "InsertInvoiceData", "Cập nhật dữ liệu hoá đơn");
                            WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "BillingImplementation", "BillingImplementation", "BillingImplementation", "Tính xong sổ " + strMa_SoGCS + " - hoá đơn kỳ " + ky + " tháng " + thang + "/" + nam);
                            return "";
                        }
                    }
                    else if (strError == "2")
                    {
                        WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busInputDataReading", "busInputDataReading.cls_InputDataReading", "checkPhuGhepTong", "Kiểm tra sổ phụ ghép tổng: Không tìm thấy dữ liệu trong HDG_DDO_SOGCS.");
                        return strError;
                    }
                    else
                    {
                        WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busInputDataReading", "busInputDataReading.cls_InputDataReading", "checkPhuGhepTong", "Kiểm tra sổ phụ ghép tổng - " + strError);
                        return strError;
                    }
                }
                else
                {
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busInputDataReading", "busInputDataReading.cls_InputDataReading", "checkPhuGhepTong", "Kiểm tra sổ phụ ghép tổng");
                }
                //Doc du lieu dau vao
                strError = inputDataReading.getCustomerDataReadingPlus(ref dsCustomerData, strMa_DViQLy, strMa_SoGCS, ky, thang, nam, strNgayGhi);
                //dsCustomerData.WriteXml("D:/CustomerDataAPI.xml", XmlWriteMode.WriteSchema);
                //dsCustomerData = new DataSet();
                //dsCustomerData.ReadXml("D:/CustomerDataAPI.xml", XmlReadMode.ReadSchema);
                if (strError != "")
                {
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busInputDataReading", "busInputDataReading.cls_InputDataReading", "getCustomerDataReading", "Đọc dữ liệu đầu vào - " + strError);
                    return strError;
                }
                else
                {
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busInputDataReading", "busInputDataReading.cls_InputDataReading", "getCustomerDataReading", "Đọc dữ liệu đầu vào");
                }
                //Loc bien ban ap gia, chi lay cac bien ban co lien quan den ky ghi chi so hien tai
                strError = this.FilterHDG_BBAN_APGIA(ref dsCustomerData);
                //decimal _tphu = dsCustomerData.Tables["HDG_QHE_DDO"].Select("MA_DDO_CHINH='PD05000093426001'").Length;
                if (strError != "")
                {
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "BillingImplementation", "BillingImplementation.clsBillingImplementation.cs", "FilterHDG_BBAN_APGIA", "Lọc biên bản áp giá - " + strError);
                    return strError;
                }
                else
                {
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "BillingImplementation", "BillingImplementation.clsBillingImplementation.cs", "FilterHDG_BBAN_APGIA", "Lọc biên bản áp giá");
                }
                strError = this.AddColumn(ref dsCustomerData, dsStaticCatalog);
                if (strError != "")
                {
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "AddColumn", "Thêm cột mới - " + strError);
                    return strError;
                }
                //Kiem tra du lieu dau vao
                strError = inputDataReading.CheckValidData(strMa_DViQLy, this.dsCustomerData, clsBillingImplementation.dsStaticCatalog);
                if (strError != "")
                {
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busInputDataReading", "busInputDataReading.cls_InputDataReading", "CheckValidData", "Kiểm tra dữ liệu đầu vào - " + strError);
                    return strError;
                }
                else
                {
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busInputDataReading", "busInputDataReading.cls_InputDataReading", "CheckValidData", "Kiểm tra dữ liệu đầu vào");
                }
                //var temp = dsCustomerData.Tables["HDG_BBAN_APGIA"].Select("MA_DDO = 'PC12LL0890486001'");
                //Tao cau truc du lieu dau ra
                dsInvoiceData = cls_OutputDataWriting.createDSInvoiceData();


                strError = this.CommonCalculationNoInsert_DCN(strMa_DViQLy, strMa_SoGCS, strNgayGhi, ky, thang, nam, strTen_DNhap, lngCurrentLibID, lngWorkflowID, dsCustomerData, clsBillingImplementation.dsStaticCatalog, ref dsInvoiceData);
                if (strError != "")
                {
                    //WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "BillingImplementation", "BillingImplementation.CommonCalculationNoInsert_DCN", "InsertInvoiceData", "Cập nhật dữ liệu hoá đơn - " + strError);
                    return strError;
                }

                if (thang == 12 && nam <= 2025 && nam >= 2022 && dsCustomerData.Tables.Contains("GCS_CSO_DCN") && dsCustomerData.Tables["GCS_CSO_DCN"].Rows.Count > 0)
                {
                    //tính thêm 1 lần nữa
                    dsInvoiceDataDCN = cls_OutputDataWriting.createDSInvoiceData();
                    #region Đổi tên bảng GCS_CHISO
                    dsCustomerData.Tables["GCS_CHISO"].TableName = "GCS_CHISO_DDK";
                    dsCustomerData.Tables["GCS_CHISO_GT"].TableName = "GCS_CHISO_DDK_GT";
                    dsCustomerData.Tables["GCS_CHISO_TP"].TableName = "GCS_CHISO_DDK_TP";
                    dsCustomerData.Tables["GCS_CHISO_BQ"].TableName = "GCS_CHISO_DDK_BQ";
                    dsCustomerData.Tables["GCS_CSO_DCN"].TableName = "GCS_CHISO";
                    dsCustomerData.Tables["GCS_CSO_DCN_GT"].TableName = "GCS_CHISO_GT";
                    dsCustomerData.Tables["GCS_CSO_DCN_TP"].TableName = "GCS_CHISO_TP";
                    dsCustomerData.Tables["GCS_CSO_DCN_BQ"].TableName = "GCS_CHISO_BQ";
                    #endregion
                    strError = this.AddColumn(ref dsCustomerData, dsStaticCatalog);
                    if (strError != "")
                    {
                        WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busBillingCalculation", "busBillingCalculation.clsRawDataCalculation", "AddColumn", "Thêm cột mới DCN - " + strError);
                        return strError;
                    }
                    strError = this.CommonCalculationNoInsert_DCN(strMa_DViQLy, strMa_SoGCS,strNgayGhi, ky, thang, nam, strTen_DNhap, lngCurrentLibID, lngWorkflowID, dsCustomerData, clsBillingImplementation.dsStaticCatalog, ref dsInvoiceDataDCN);
                    if (strError != "")
                    {
                        return strError;
                    }
                    strError = outputDataWriting.InsertInvoiceDataPlus_DCN(this.dsInvoiceData, dsInvoiceDataDCN, this.dsCustomerData, strMa_DViQLy, strMa_SoGCS, strTen_DNhap, lngCurrentLibID, lngWorkflowID, ky, thang, nam);
                }
                else
                    //Cap nhat du lieu
                    strError = outputDataWriting.InsertInvoiceDataPlus_2022(this.dsInvoiceData, this.dsCustomerData, strMa_DViQLy, strMa_SoGCS, strTen_DNhap, lngCurrentLibID, lngWorkflowID, ky, thang, nam);
                if (strError != "")
                {
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busOutputDataWriting", "busOutputDataWriting.cls_OutputDataWriting", "InsertInvoiceData", "Cập nhật dữ liệu hoá đơn - " + strError);
                    return strError;
                }
                else
                {
                    WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "busOutputDataWriting", "busOutputDataWriting.cls_OutputDataWriting", "InsertInvoiceData", "Cập nhật dữ liệu hoá đơn");

                }
                //Ket thuc tinh hoa
                WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "BillingImplementation", "BillingImplementation", "BillingImplementation", "Tính xong sổ " + strMa_SoGCS + " - hoá đơn kỳ " + ky + " tháng " + thang + "/" + nam);

                return "";
            }
            catch (Exception ex)
            {
                //Ghi log vao dat
                WriteLogPlus(strMa_DViQLy, strMa_SoGCS, "BillingImplementation", "BillingImplementation", "BillingImplementation", "Lỗi khi tính hoá đơn: sổ " + strMa_SoGCS + " - hoá đơn kỳ " + ky + " tháng " + thang + "/" + nam);
                return ex.Message;
            }
            finally
            {
                dsCustomerData.Reset();
                dsInvoiceData.Reset();
                dsCalculation.Reset();
            }

        }

        #endregion
        //
    }


}
