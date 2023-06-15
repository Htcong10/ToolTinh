using BillingLibrary;
using CMISLibrary;
using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;

namespace busBillingCalculation
{
    public class InvoiceCalculationItem
    {
        decimal dec100 = 100;
        private ManualResetEvent _doneEvent;
        DataRow[] arrKHang;
        DataSet  dsStaticCatalog, dsCustomerData, dsCalculation;
        public DataSet dsInvoiceData;
        string strMaDViQLy, strMaSoGCS;
        Int16 i16Ky, i16Thang, i16Nam;
        List<string> lstError;
        public InvoiceCalculationItem(DataRow[] _arrKHang,  DataSet _dsInvoiceData, DataSet _dsCustomerData, DataSet _dsCalculation, string _strMaDViQLy, string _strMaSoGCS, Int16 _i16Ky, Int16 _i16Thang, Int16 _i16Nam, DataSet _dsStaticCatalog, ManualResetEvent doneEvent, ref List<string> _lstError)
        {
            arrKHang = _arrKHang;
            dsInvoiceData = _dsInvoiceData;
            dsCustomerData = _dsCustomerData;
            dsCalculation = _dsCalculation;
            dsStaticCatalog = _dsStaticCatalog;
            strMaDViQLy = _strMaDViQLy;
            strMaSoGCS = _strMaSoGCS;
            i16Ky = _i16Ky;
            i16Thang = _i16Thang;
            i16Nam = _i16Nam;
            lstError = _lstError;
            _doneEvent = doneEvent;
        }
        
        public void ThreadPoolCallback(object threadContext)
        {
            //int threadIndex = (int)threadContext;
            //Console.WriteLine($"Thread {threadIndex} started...");
            string strResult = this.InvoiceCalculation();
            //Console.WriteLine($"Thread {threadIndex} result calculated...");
            if(strResult.Trim().Length>0) this.lstError.Add(strResult);
            this._doneEvent.Set();
        }
        public string InvoiceCalculation()//(DataRow[] arrKHang, ref DataSet dsInvoiceData, DataSet dsCustomerData, DataSet dsCalculation, string strMaDViQLy, string strMaSoGCS, Int16 i16Ky, Int16 i16Thang, Int16 i16Nam, DataSet dsStaticCatalog)
        {
            try
            {
                decimal decTyGia = 1;
                string strMaNhomNN = "";
                string strMaNGia = "";
                if (dsInvoiceData == null) return "NoDataFound! - Lỗi không tìm thấy dữ liệu!";
                if (dsInvoiceData.Tables["HDN_HDONCTIET"] == null) return "HDN_HDONCTIET = null!";

                DataView dwCS_GT = new DataView();
                DataView dwCS = new DataView();
                DataView dwCT = new DataView();
                DataView dwCF = new DataView();
                dwCS = new DataView(dsCustomerData.Tables["GCS_CHISO"]);
                dwCT = new DataView(dsInvoiceData.Tables["HDN_HDONCTIET"]);


                DataView dv = new DataView(dsStaticCatalog.Tables["D_GIA_NHOMNN"]);
                dv.Sort = "NGAY_ADUNG DESC";
                DataView dvTyGia = new DataView(dsStaticCatalog.Tables["D_TY_GIA"]);
                dvTyGia.Sort = "ID_TY_GIA DESC";
                DataView dwSL4 = new DataView(dsCalculation.Tables["SL_4"]);
                dwSL4.Sort = "NGAY_HLUCGIA DESC";
                if (dsCustomerData.Tables["GCS_CHISO_GT"] != null)
                    dwCS_GT = new DataView(dsCustomerData.Tables["GCS_CHISO_GT"]);
                if (dsInvoiceData.Tables["HDN_HDONCOSFI"] != null)
                {
                    dwCF = dsInvoiceData.Tables["HDN_HDONCOSFI"].DefaultView;
                    dwCF.Sort = "MA_DDO, ID_CHISO ASC";
                }


                //if (dsCustomerData.Tables["D_SOGCS"] == null || dsCustomerData.Tables["D_SOGCS"].Rows.Count == 0) return "D_SOGCS = null";
                //string strMaSoGCS = dsCustomerData.Tables["D_SOGCS"].Rows[0]["MA_SOGCS"].ToString();
                //Có dữ liệu trong bảng HDN_HDONCTIET            
                foreach (DataRow dr in arrKHang)
                {
                    ////Debug.WriteLine(DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff") + " InvoiceCalculation Forbegin");
                    strMaNhomNN = "";
                    strMaNGia = "";
                    decTyGia = 1;
                    //abc
                    //Duyệt từng khách hàng lấy ra danh sách điểm đo của khách hàng đó 
                    try
                    {
                        if(dr["MA_KHANG"].ToString().Trim()== "PA02LT0000339")
                        {

                        }    
                        DataRow[] arrPThucTT = dsCustomerData.Tables["HDG_PTHUC_TTOAN"].Select("MA_KHANG='" + dr["MA_KHANG"] + "'");
                        if (arrPThucTT == null || arrPThucTT.Length == 0) return "Không tìm thấy phương thức thanh toán của khách hàng: " + dr["MA_KHANG"].ToString();
                        if (arrPThucTT[0]["HTHUC_TTOAN"].ToString().Trim().Length == 0) return "Lỗi hình thức thanh toán rỗng";
                        string strMaHTTT = arrPThucTT[0]["HTHUC_TTOAN"].ToString().Split(';')[0];
                        string strMaPTTT = arrPThucTT[0]["PTHUC_TTOAN"].ToString().Split(';')[0];
                        //System.Windows.Forms.MessageBox.Show("B0: " + Convert.ToString(dr["MA_KHANG"]));
                        DataRow[] drQheGT = null;
                        if (dsCustomerData.Tables["HDG_QHE_DDO"] != null)
                            drQheGT = dsCustomerData.Tables["HDG_QHE_DDO"].Select("MA_KHANG_CHINH='" + dr["MA_KHANG"] + "' AND LOAI_QHE='40'");
                        ////Debug.WriteLine(DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff") + " InvoiceCalculation HTHUC_TTOAN+HDG_QHE_DDO");
                        //System.Windows.Forms.MessageBox.Show("B01: " + Convert.ToString(dr["MA_KHANG"]));
                        if (drQheGT == null || drQheGT.Length == 0)
                        {
                            //Không có quan hệ ghép tổng //"' AND MA_SOGCS='" + strMaSoGCS +
                            DataRow[] drDDo = dsInvoiceData.Tables["HDN_HDONCTIET"].Select("MA_KHANG='" + dr["MA_KHANG"] + "' AND BCS IN ('KT','BT','CD','TD')");
                            if (drDDo == null || drDDo.Length == 0) continue;
                            //DungNT: kiểm tra nhóm giá tiền <> VND để nhân với tỷ giá
                            string[] arrMaNhomNN = drDDo.Select(c => c.Field<string>("MA_NHOMNN")).ToArray();
                            string filter = "";
                            foreach (string str in arrMaNhomNN)
                            {
                                filter += filter.Trim().Length == 0 ? "'" + str + "'" : ",'" + str + "'";
                            }
                            //if (dsStaticCatalog != null && dsStaticCatalog.Tables["D_GIA_NHOMNN"] != null)
                            //{                                
                            dv.RowFilter = "LOAI_TIEN<>'VND' and MA_NHOMNN in (" + filter + ")";


                            if (dv != null && dv.Count > 0)
                            {
                                string strLoaiTien = dv[0]["LOAI_TIEN"].ToString();
                                //if (dsStaticCatalog != null && dsStaticCatalog.Tables["D_TY_GIA"] != null && dsStaticCatalog.Tables["D_TY_GIA"].Rows.Count > 0)
                                //{
                                //DataView dvTyGia = new DataView(dsStaticCatalog.Tables["D_TY_GIA"]);
                                dvTyGia.RowFilter = "LOAI_TIEN='" + strLoaiTien + "'";
                                //dvTyGia.Sort = "ID_TY_GIA DESC";
                                //dvTyGia.Sort = "NAM DESC, THANG DESC, ID_TY_GIA DESC";
                                if (dvTyGia != null && dvTyGia.Count > 0)
                                {
                                    decTyGia = Utility.DecimalDbnull(dvTyGia[0]["TYGIA_QDOI"]);
                                    if (decTyGia == 0) decTyGia = 1;
                                }
                                //}
                            }
                            //}
                            ////Debug.WriteLine(DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff") + " InvoiceCalculation decTyGia");

                            //Kết thúc
                            decimal decTienDienTD = drDDo.Sum(c => c.Field<decimal>("SO_TIEN"));
                            decimal decDienTThuTD = drDDo.Sum(c => c.Field<decimal>("DIEN_TTHU"));
                            //decimal decTienDienTD = 0m;
                            //decimal decDienTThuTD = 0m;
                            //foreach ( DataRow drTemp in drDDo)
                            //{
                            //    decTienDienTD += Convert.ToDecimal(drTemp["SO_TIEN"]);
                            //    decDienTThuTD += Convert.ToDecimal(drTemp["DIEN_TTHU"]);
                            //}    
                            decimal decTyLeThue = Convert.ToDecimal(dr["TLE_THUE"]);
                            ////Debug.WriteLine(DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff") + " InvoiceCalculation SumDienTT");
                            //Lấy danh sách chỉ số để lấy min ngày đầu kỳ và mã ngày cuối kỳ
                            //long[] arrIDChiSo = drDDo.Select(c => c.Field<long>("ID_CHISO")).ToArray();
                            //string strFilterID = "";
                            //foreach (long lngIDCHISO in arrIDChiSo)
                            //{
                            //    if (strFilterID.Length == 0)
                            //    {
                            //        strFilterID += "ID_CHISO=" + lngIDCHISO.ToString() + "";
                            //    }
                            //    else
                            //    {
                            //        strFilterID += " OR ID_CHISO=" + lngIDCHISO.ToString() + "";
                            //    }
                            //}
                            //System.Windows.Forms.MessageBox.Show("B1: " + Convert.ToString(dr["MA_KHANG"]));
                            //Lam nhu the nay la sai -> DataRow[] arrChiSo = dsCustomerData.Tables["GCS_CHISO"].Select(strFilterID);
                            DataRow[] arrChiSo = dsCustomerData.Tables["GCS_CHISO"].Select("MA_KHANG = '" + dr["MA_KHANG"] + "' AND BCS IN ('KT','BT','CD','TD') AND LOAI_CHISO IN ('DDK','DDN','CSC','CCS')");
                            DateTime dtNgayDKMin = arrChiSo.Min(c => c.Field<DateTime>("NGAY_DKY"));
                            DateTime dtNgayCKMax = arrChiSo.Max(c => c.Field<DateTime>("NGAY_CKY"));
                            //DateTime dtNgayDKMin = new DateTime(3000, 1, 1);
                            //DateTime dtNgayCKMax = new DateTime(1900, 1, 1);
                            //foreach ( DataRow drTemp in arrChiSo)
                            //{
                            //    DateTime dtTimeDK = Convert.ToDateTime(drTemp["NGAY_DKY"]);
                            //    DateTime dtTimeCK = Convert.ToDateTime(drTemp["NGAY_DKY"]);
                            //    if (dtNgayDKMin > dtTimeDK) dtNgayDKMin = dtTimeDK;
                            //    if (dtNgayCKMax < dtTimeCK) dtNgayCKMax = dtTimeCK;
                            //}    
                            ////Debug.WriteLine(DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff") + " InvoiceCalculation MinMax");
                            DataRow[] drKHTT = dsCustomerData.Tables["HDG_KHACH_HANG_TT"].Select("MA_KHANG='" + dr["MA_KHTT"] + "'");
                            string strTenKHTT = (drKHTT == null || drKHTT.Length == 0) ? dr["TEN_KHANG"].ToString() : drKHTT[0]["TEN_KHANG"].ToString();
                            string strDChiKHTT = (drKHTT == null || drKHTT.Length == 0) ? dr["SO_NHA"].ToString() + " " + dr["DUONG_PHO"].ToString() : drKHTT[0]["SO_NHA"].ToString() + " " + drKHTT[0]["DUONG_PHO"].ToString();
                            DataRow[] arrDDoChinh = null;
                            //arrDDoChinh = dsInvoiceData.Tables["HDN_HDONCTIET"].Select("MA_KHANG='" + dr["MA_KHANG"] + "' and BCS <>'VC'");
                            //DataRow drDDoChinhMax = arrDDoChinh.Where(c => c.Field<string>("MA_DDO") == arrDDoChinh.Min(b => b.Field<string>("MA_DDO"))).ToArray()[0];
                            //arrDDoChinh = dsInvoiceData.Tables["HDN_HDONCTIET"].Select("MA_KHANG='" + dr["MA_KHANG"] + "' and BCS <>'VC'").OrderBy();
                            DataRow drDDoChinhMax = drDDo.OrderBy(b => b.Field<string>("MA_DDO")).ToArray()[0];
                            //decimal decSoHo = Convert.ToDecimal(dsCustomerData.Tables["HDG_DIEM_DO"].Select("MA_DDO='" + drDDoChinhMax["MA_DDO"] + "'")[0]["SO_HO"]);
                            //decimal decSoHo = Convert.ToDecimal(dsCalculation.Tables["SL_4"].Select("MA_DDO='" + drDDoChinhMax["MA_DDO"] + "'")[0]["SO_HO"]);
                            //Viết lại phần lấy số hộ
                            ////Debug.WriteLine(DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff") + " InvoiceCalculation SumTien");

                            decimal decSoHo = 0;
                            dwSL4.RowFilter = "MA_DDO='" + drDDoChinhMax["MA_DDO"] + "' AND BTHANG_ID <> 0";
                            if (dwSL4.Count > 0)
                            {
                                //dwSL4.Sort = "NGAY_HLUCGIA DESC";
                                strMaNhomNN = Convert.ToString(dwSL4[0]["MA_NHOMNN"]);
                                strMaNGia = Convert.ToString(dwSL4[0]["MA_NGIA"]);
                                decSoHo = Convert.ToDecimal(dwSL4[0]["SO_HO"]);
                                //DateTime ngay_hluc = Convert.ToDateTime(dwSL4[0]["NGAY_HLUCGIA"]);
                                ////decimal IsGiaNhomA_N = 0;
                                //if (strMaNhomNN.Contains("SHB") && strMaNhomNN != "SHBN" && "A;N".Contains(strMaNGia))
                                //{
                                //    dwSL4.RowFilter = "MA_DDO='" + drDDoChinhMax["MA_DDO"] + "' AND BTHANG_ID <> 0 and MA_NHOMNN='" + strMaNhomNN + "'";
                                //    //dwSL4.Sort = "NGAY_HLUCGIA DESC";
                                //    foreach (DataRowView drvSL4 in dwSL4)
                                //    {
                                //        if (Convert.ToDateTime(drvSL4["NGAY_HLUCGIA"]) == ngay_hluc)
                                //        {
                                //            if ("A;N".Contains(drvSL4["MA_NGIA"].ToString().Trim()) && drvSL4["MA_NGIA"].ToString().Trim() != strMaNGia)
                                //            {
                                //                decSoHo += Convert.ToDecimal(drvSL4["SO_HO"]);
                                //                break;
                                //            }
                                //        }
                                //    }
                                //}
                            }
                            ////Debug.WriteLine(DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff") + " InvoiceCalculation decSoHo");

                            //DataRow[] rowsSoHo = dsCalculation.Tables["SL_4"].Select("MA_DDO='" + drDDoChinhMax["MA_DDO"] + "' AND BTHANG_ID <> 0");
                            //if (rowsSoHo.Length == 0)
                            //{
                            //    rowsSoHo = dsCalculation.Tables["SL_4"].Select("MA_DDO='" + drDDoChinhMax["MA_DDO"] + "'");
                            //}
                            //decimal decSoHo = Convert.ToDecimal(rowsSoHo[0]["SO_HO"]);



                            DataRow[] drCosfi = dsInvoiceData.Tables["HDN_HDONCOSFI"].Select("MA_DDO='" + drDDoChinhMax["MA_DDO"] + "' AND MA_KHANG='" + dr["MA_KHANG"] + "'");
                            if (drCosfi != null && drCosfi.Length > 0)
                            {
                                drCosfi = drCosfi.OrderByDescending(c => c.Field<long>("ID_CHISO")).ToArray();
                            }
                            //decimal decTienDienTD = SummaryMoneyHC(dsInvoiceData, dr["MA_KHANG"].ToString(), strMaSoGCS.ToString());                        
                            //if (decTienDienTD == -1) return "Không tìm thấy dữ liệu của khách hàng " + dr["MA_KHANG"] + " trong bảng HDN_HDONCTIET";
                            //System.Windows.Forms.MessageBox.Show("B2: " + Convert.ToString(dr["MA_KHANG"]));
                            try
                            {
                                long lngID_HDonTD = 0;//Sau sẽ chuyển sang lấy ID_HDON tự sinh
                                //Đẩy bản ghi TD vào HDN_HDON
                                DataRow drHDN_HDON = dsInvoiceData.Tables["HDN_HDON"].NewRow();
                                drHDN_HDON["COSFI"] = (drCosfi == null || drCosfi.Length == 0) ? System.DBNull.Value : drCosfi[0]["COSFI"];
                                drHDN_HDON["DCHI_KHANG"] = dr["SO_NHA"].ToString() + " " + dr["DUONG_PHO"].ToString();
                                drHDN_HDON["DCHI_KHANGTT"] = strDChiKHTT;
                                drHDN_HDON["DIEN_TTHU"] = decDienTThuTD;
                                drHDN_HDON["ID_HDON"] = lngID_HDonTD;
                                drHDN_HDON["KCOSFI"] = (drCosfi == null || drCosfi.Length == 0) ? System.DBNull.Value : drCosfi[0]["KCOSFI"];
                                drHDN_HDON["KY"] = i16Ky;
                                drHDN_HDON["MA_HTTT"] = strMaHTTT;
                                drHDN_HDON["LOAI_HDON"] = "TD";
                                drHDN_HDON["MA_CNANG"] = drDDo[0]["MA_CNANG"];
                                drHDN_HDON["MA_DVIQLY"] = drDDo[0]["MA_DVIQLY"];
                                drHDN_HDON["MA_KHANG"] = dr["MA_KHANG"];
                                drHDN_HDON["DTHOAI"] = dr["DTHOAI"];
                                drHDN_HDON["MA_KHANGTT"] = dr["MA_KHTT"].ToString().Trim().Length == 0 ? dr["MA_KHANG"] : dr["MA_KHTT"];
                                drHDN_HDON["MA_KVUC"] = drDDoChinhMax["MA_KVUC"];
                                drHDN_HDON["MA_NHANG"] = dr["MA_NHANG"];
                                drHDN_HDON["MA_SOGCS"] = drDDo[0]["MA_SOGCS"];
                                drHDN_HDON["MA_TO"] = drDDo[0]["MA_TO"];
                                drHDN_HDON["MASO_THUE"] = dr["MASO_THUE"];
                                drHDN_HDON["NAM"] = i16Nam;
                                drHDN_HDON["NGAY_CKY"] = dtNgayCKMax;
                                drHDN_HDON["NGAY_DKY"] = dtNgayDKMin;
                                drHDN_HDON["NGAY_SUA"] = drDDo[0]["NGAY_SUA"];
                                drHDN_HDON["NGAY_TAO"] = drDDo[0]["NGAY_TAO"];
                                drHDN_HDON["NGUOI_SUA"] = drDDo[0]["NGUOI_SUA"];
                                drHDN_HDON["NGUOI_TAO"] = drDDo[0]["NGUOI_TAO"];
                                drHDN_HDON["SO_CTO"] = drDDoChinhMax["SO_CTO"];
                                drHDN_HDON["SO_HO"] = decSoHo;
                                drHDN_HDON["SO_LANIN"] = "0";
                                drHDN_HDON["SO_TIEN"] = Math.Round(decTienDienTD * decTyGia, 0, MidpointRounding.AwayFromZero);
                                drHDN_HDON["STT"] = drDDoChinhMax["STT"];
                                drHDN_HDON["TEN_KHANG"] = dr["TEN_KHANG"];
                                drHDN_HDON["TEN_KHANGTT"] = strTenKHTT;
                                drHDN_HDON["THANG"] = i16Thang;
                                drHDN_HDON["TIEN_GTGT"] = Math.Round(decTyLeThue * decTienDienTD * decTyGia / dec100, 0, MidpointRounding.AwayFromZero);
                                drHDN_HDON["TKHOAN_KHANG"] = dr["TKHOAN_KHANG"];
                                //drHDN_HDON["TONG_TIEN"] = Math.Round(decTienDienTD * decTyLeThue*decTyGia / dec100, 0, MidpointRounding.AwayFromZero) + decTienDienTD;
                                drHDN_HDON["TONG_TIEN"] = Math.Round(decTienDienTD * decTyLeThue * decTyGia / dec100 + decTienDienTD * decTyGia, 0, MidpointRounding.AwayFromZero);
                                drHDN_HDON["TYLE_THUE"] = dr["TLE_THUE"];
                                //DũngNT: Bổ sung 5 trường thay đổi CSDL ngày 23/12/2009
                                drHDN_HDON["LOAI_KHANG"] = dr["LOAI_KHANG"];
                                drHDN_HDON["MANHOM_KHANG"] = dr["MANHOM_KHANG"];
                                drHDN_HDON["MA_LOAIDN"] = dr["MA_LOAIDN"];
                                drHDN_HDON["MA_PTTT"] = strMaPTTT;
                                drHDN_HDON["DCHI_TTOAN"] = dr["DCHI_TTOAN"];
                                drHDN_HDON["TIEN_TD"] = drHDN_HDON["SO_TIEN"];
                                drHDN_HDON["THUE_TD"] = drHDN_HDON["TIEN_GTGT"];
                                drHDN_HDON["TIEN_VC"] = 0;
                                drHDN_HDON["THUE_VC"] = 0;
                                //Tạm thời để trống DCHI_TTOAN, trước khi đẩy vào CSDL sẽ fill trường này từ bảng HDG_HOP_DONG
                                dsInvoiceData.Tables["HDN_HDON"].Rows.Add(drHDN_HDON);
                                //dsInvoiceData.AcceptChanges();
                            }
                            catch (Exception e)
                            {
                                return "Lỗi khi tạo bản ghi TD cho khách hàng: " + dr["MA_KHANG"] + " " + e.Message;
                            }
                            ////Debug.WriteLine(DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff") + " InvoiceCalculation HDN_HDON_TD");
                            //finally
                            //{
                            //    dsCustomerData.AcceptChanges();
                            //    dsInvoiceData.AcceptChanges();
                            //}
                            //Lấy thông tin vô công trong bảng HDN_HDONCOSFI
                            //DũngNT bổ sung điều kiện KCOSFI<>0
                            //Bổ sung tính TIEN_VOCONG, TIEN_HUUCONG
                            //DataRow[] drVC = dsInvoiceData.Tables["HDN_HDONCOSFI"].Select("MA_KHANG='" + dr["MA_KHANG"] + "' AND MA_SOGCS='" + strMaSoGCS + "' AND KIMUA_CSPK = 1 AND KCOSFI <> 0");
                            //dwCF = dsInvoiceData.Tables["HDN_HDONCOSFI"].DefaultView;
                            dwCF.RowFilter = "MA_KHANG='" + dr["MA_KHANG"] + "' AND MA_SOGCS='" + strMaSoGCS + "' AND KIMUA_CSPK = 1";
                            //dwCF.Sort = "MA_DDO, ID_CHISO ASC";
                            if (dwCF != null && dwCF.Count > 0)
                            {
                                long id_HC_Max = 0;
                                string strMaDDo = "";
                                foreach (DataRowView drVoCong in dwCF)
                                {
                                    //Dũng NT hiệu chỉnh lại đoạn tính tiền hữu công / vô công đợt đổi CSPK 10/12/2014
                                    //Dựa theo ID_CHISO của dòng vô công
                                    if (Convert.ToString(drVoCong["MA_DDO"]) != strMaDDo)
                                        id_HC_Max = 0;
                                    long id_CS_VC = Convert.ToInt64(drVoCong["ID_CHISO"]);
                                    strMaDDo = Convert.ToString(drVoCong["MA_DDO"]);
                                    DateTime dtNgayCKy = new DateTime(1900, 1, 1);
                                    dwCS.RowFilter = "MA_DDO='" + strMaDDo + "'";

                                    foreach (DataRowView drv in dwCS)
                                    {
                                        if (Convert.ToInt64(drv["ID_CHISO"]) == id_CS_VC)
                                        {
                                            dtNgayCKy = Convert.ToDateTime(drv["NGAY_CKY"]);
                                            break;
                                        }
                                    }
                                    //Lấy danh sách ID_CHISO hữu công
                                    List<long> lstID_CHISO_HC = new List<long>();
                                    foreach (DataRowView drv in dwCS)
                                    {
                                        if (Convert.ToDateTime(drv["NGAY_CKY"]) <= dtNgayCKy && Convert.ToString(drv["BCS"]) != "VC")
                                        {
                                            lstID_CHISO_HC.Add(Convert.ToInt64(drv["ID_CHISO"]));
                                        }
                                    }

                                    decimal decTienHuuCong = 0;
                                    if (Convert.ToDecimal(drVoCong["MA_CNANG"]) != 0)
                                    {
                                        //Treo thao doi loai cong to tu 1 -> 3,5,8 va nguoc lai
                                        dwCT.RowFilter = "MA_DDO = '" + strMaDDo + "' and BCS in ('BT','CD','TD')";
                                        foreach (DataRowView drv in dwCT)
                                        {
                                            if (lstID_CHISO_HC.Contains(Convert.ToInt64(drv["ID_CHISO"])) && Convert.ToInt64(drv["ID_CHISO"]) > id_HC_Max)
                                            {
                                                decTienHuuCong += CMISLibrary.Utility.DecimalDbnull(drv["SO_TIEN"]);
                                            }
                                        }
                                        //decTienHuuCong = CMISLibrary.Utility.DecimalDbnull(dsInvoiceData.Tables["HDN_HDONCTIET"].Compute("SUM(SO_TIEN)", "MA_DDO = '" + drVoCong["MA_DDO"].ToString().Trim() + "' AND BCS IN ('BT','CD','TD')"));
                                    }
                                    else
                                    {
                                        dwCT.RowFilter = "MA_DDO = '" + strMaDDo + "' and BCS in ('BT','CD','TD','KT')";
                                        foreach (DataRowView drv in dwCT)
                                        {
                                            if (lstID_CHISO_HC.Contains(Convert.ToInt64(drv["ID_CHISO"])) && Convert.ToInt64(drv["ID_CHISO"]) > id_HC_Max)
                                            {
                                                decTienHuuCong += CMISLibrary.Utility.DecimalDbnull(drv["SO_TIEN"]);
                                            }
                                        }
                                        //decTienHuuCong = CMISLibrary.Utility.DecimalDbnull(dsInvoiceData.Tables["HDN_HDONCTIET"].Compute("SUM(SO_TIEN)", "MA_DDO = '" + drVoCong["MA_DDO"].ToString().Trim() + "' AND BCS IN ('KT','BT','CD','TD')"));
                                    }
                                    id_HC_Max = lstID_CHISO_HC.Max();
                                    lstID_CHISO_HC.Clear();
                                    if (drVoCong["KCOSFI"].ToString().Trim() == "0") continue;
                                    //Set lai ve gia tri mac dinh
                                    drVoCong["MA_CNANG"] = 0;
                                    decimal decTienVoCong = decTienHuuCong * Convert.ToDecimal(drVoCong["KCOSFI"]) / 100;

                                    drVoCong["TIEN_VOCONG"] = Math.Round(decTienVoCong, 0, MidpointRounding.AwayFromZero);
                                    drVoCong["TIEN_HUUCONG"] = Math.Round(decTienHuuCong, 0, MidpointRounding.AwayFromZero);
                                    var Temp = dsInvoiceData.Tables["HDN_HDONCTIET"].Select("MA_DDO = '" + drVoCong["MA_DDO"].ToString().Trim() + "' AND BCS = 'VC'");
                                    if (Temp != null && Temp.Count() > 0)
                                    {
                                        DataRow drChiTiet = Temp.Where(c => c.Field<long>("ID_CHISO") == Convert.ToInt64(drVoCong["ID_CHISO"])).FirstOrDefault();
                                        if (drChiTiet != null)
                                            drChiTiet["SO_TIEN"] = Math.Round(decTienVoCong, 0, MidpointRounding.AwayFromZero);
                                        //dsInvoiceData.Tables["HDN_HDONCTIET"].AcceptChanges();
                                    }
                                }
                                //dwCF.Table.AcceptChanges();
                                dwCF.RowFilter = "";

                                DataRow[] drVC = dsInvoiceData.Tables["HDN_HDONCOSFI"].Select("MA_KHANG='" + dr["MA_KHANG"] + "' AND MA_SOGCS='" + strMaSoGCS + "' AND KIMUA_CSPK = 1 AND KCOSFI <> 0");
                                if (drVC != null && drVC.Length > 0)
                                {
                                    //System.Windows.Forms.MessageBox.Show("B3: " + Convert.ToString(dr["MA_KHANG"]));
                                    DataRow[] arrChiTietVC = dsInvoiceData.Tables["HDN_HDONCTIET"].Select("MA_KHANG='" + dr["MA_KHANG"] + "' AND MA_SOGCS='" + strMaSoGCS + "' AND BCS='VC'");
                                    decimal decTienDienVC = drVC.Sum(c => c.Field<decimal>("TIEN_VOCONG"));
                                    //decimal decDienTThuVC = drVC.Sum(c => c.Field<decimal>("VO_CONG"));
                                    decimal decDienTThuVC = arrChiTietVC.Sum(c => c.Field<decimal>("DIEN_TTHU"));
                                    //Lấy danh sách chỉ số để lấy min ngày đầu kỳ và mã ngày cuối kỳ
                                    long[] arrIDChiSoVC = arrChiTietVC.Select(c => c.Field<long>("ID_CHISO")).ToArray();
                                    //strFilterID = "";
                                    //foreach (long lngIDCHISO in arrIDChiSoVC)
                                    //{
                                    //    if (strFilterID.Length == 0)
                                    //    {
                                    //        strFilterID += "ID_CHISO=" + lngIDCHISO.ToString() + "";
                                    //    }
                                    //    else
                                    //    {
                                    //        strFilterID += "OR ID_CHISO=" + lngIDCHISO.ToString() + "";
                                    //    }
                                    //}
                                    //Lam nhu the nay la sai -> DataRow[] arrChiSoVC = dsCustomerData.Tables["GCS_CHISO"].Select(strFilterID);
                                    DataRow[] arrChiSoVC = dsCustomerData.Tables["GCS_CHISO"].Select("MA_KHANG = '" + dr["MA_KHANG"] + "' AND BCS IN ('KT','BT','CD','TD') AND LOAI_CHISO IN ('DDK','DDN','CSC','CCS')");
                                    DateTime dtNgayDKMinVC = arrChiSoVC.Min(c => c.Field<DateTime>("NGAY_DKY"));
                                    DateTime dtNgayCKMaxVC = arrChiSoVC.Max(c => c.Field<DateTime>("NGAY_CKY"));
                                    //Đẩy thêm bản ghi VC vào HDN_HDON
                                    //System.Windows.Forms.MessageBox.Show("B4: " + Convert.ToString(dr["MA_KHANG"]));
                                    try
                                    {
                                        long lngID_HDonVC = 0;//Sau sẽ chuyển sang lấy ID_HDON tự sinh
                                        //Đẩy thêm bản ghi VC vào HDN_HDON
                                        DataRow drHDN_HDON = dsInvoiceData.Tables["HDN_HDON"].NewRow();
                                        drHDN_HDON["COSFI"] = (drCosfi == null || drCosfi.Length == 0) ? System.DBNull.Value : drCosfi[0]["COSFI"];
                                        drHDN_HDON["DCHI_KHANG"] = dr["SO_NHA"].ToString() + " " + dr["DUONG_PHO"].ToString();
                                        drHDN_HDON["DCHI_KHANGTT"] = strDChiKHTT;
                                        drHDN_HDON["DIEN_TTHU"] = decDienTThuVC;
                                        drHDN_HDON["ID_HDON"] = lngID_HDonVC;
                                        drHDN_HDON["KCOSFI"] = (drCosfi == null || drCosfi.Length == 0) ? System.DBNull.Value : drCosfi[0]["KCOSFI"];
                                        drHDN_HDON["KY"] = i16Ky; drHDN_HDON["MA_HTTT"] = strMaHTTT;
                                        drHDN_HDON["LOAI_HDON"] = "VC";
                                        drHDN_HDON["MA_CNANG"] = drDDo[0]["MA_CNANG"];
                                        drHDN_HDON["MA_DVIQLY"] = drDDo[0]["MA_DVIQLY"];
                                        drHDN_HDON["MA_KHANG"] = dr["MA_KHANG"];
                                        drHDN_HDON["DTHOAI"] = dr["DTHOAI"];
                                        drHDN_HDON["MA_KHANGTT"] = dr["MA_KHTT"].ToString().Trim().Length == 0 ? dr["MA_KHANG"] : dr["MA_KHTT"];
                                        //drHDN_HDON["MA_KHANGTT"] = dr["MA_KHTT"];
                                        drHDN_HDON["MA_KVUC"] = drDDoChinhMax["MA_KVUC"];
                                        drHDN_HDON["MA_NHANG"] = dr["MA_NHANG"];
                                        drHDN_HDON["MA_SOGCS"] = drDDo[0]["MA_SOGCS"];
                                        drHDN_HDON["MA_TO"] = drDDo[0]["MA_TO"];
                                        drHDN_HDON["MASO_THUE"] = dr["MASO_THUE"];
                                        drHDN_HDON["NAM"] = i16Nam;
                                        drHDN_HDON["NGAY_CKY"] = dtNgayCKMaxVC;
                                        drHDN_HDON["NGAY_DKY"] = dtNgayDKMinVC;
                                        drHDN_HDON["NGAY_SUA"] = drDDo[0]["NGAY_SUA"];
                                        drHDN_HDON["NGAY_TAO"] = drDDo[0]["NGAY_TAO"];
                                        drHDN_HDON["NGUOI_SUA"] = drDDo[0]["NGUOI_SUA"];
                                        drHDN_HDON["NGUOI_TAO"] = drDDo[0]["NGUOI_TAO"];
                                        drHDN_HDON["SO_CTO"] = drDDoChinhMax["SO_CTO"];
                                        drHDN_HDON["SO_HO"] = decSoHo;
                                        drHDN_HDON["SO_LANIN"] = "0";
                                        drHDN_HDON["SO_TIEN"] = Math.Round(decTienDienVC * decTyGia, 0, MidpointRounding.AwayFromZero);
                                        drHDN_HDON["STT"] = drDDoChinhMax["STT"];
                                        drHDN_HDON["TEN_KHANG"] = dr["TEN_KHANG"];
                                        drHDN_HDON["TEN_KHANGTT"] = strTenKHTT;
                                        drHDN_HDON["THANG"] = i16Thang;
                                        drHDN_HDON["TIEN_GTGT"] = Math.Round(decTyLeThue * decTienDienVC * decTyGia / dec100, 0, MidpointRounding.AwayFromZero);
                                        drHDN_HDON["TKHOAN_KHANG"] = dr["TKHOAN_KHANG"];
                                        //drHDN_HDON["TONG_TIEN"] = Math.Round(decTienDienVC * decTyLeThue / dec100, 0, MidpointRounding.AwayFromZero) + decTienDienVC;
                                        drHDN_HDON["TONG_TIEN"] = Math.Round(decTienDienVC * decTyLeThue * decTyGia / dec100 + decTienDienVC * decTyGia, 0, MidpointRounding.AwayFromZero);
                                        drHDN_HDON["TYLE_THUE"] = dr["TLE_THUE"];
                                        //DũngNT: Bổ sung 5 trường thay đổi CSDL ngày 23/12/2009
                                        drHDN_HDON["LOAI_KHANG"] = dr["LOAI_KHANG"];
                                        drHDN_HDON["MANHOM_KHANG"] = dr["MANHOM_KHANG"];
                                        drHDN_HDON["MA_LOAIDN"] = dr["MA_LOAIDN"];
                                        drHDN_HDON["MA_PTTT"] = strMaPTTT;
                                        drHDN_HDON["DCHI_TTOAN"] = dr["DCHI_TTOAN"];
                                        drHDN_HDON["TIEN_TD"] = 0;
                                        drHDN_HDON["THUE_TD"] = 0;
                                        drHDN_HDON["TIEN_VC"] = drHDN_HDON["SO_TIEN"];
                                        drHDN_HDON["THUE_VC"] = drHDN_HDON["TIEN_GTGT"];
                                        //Tạm thời để trống DCHI_TTOAN, trước khi đẩy vào CSDL sẽ fill trường này từ bảng HDG_HOP_DONG
                                        if (drHDN_HDON["TONG_TIEN"].ToString().Trim() != "" && drHDN_HDON["TONG_TIEN"].ToString().Trim() != "0")
                                            dsInvoiceData.Tables["HDN_HDON"].Rows.Add(drHDN_HDON);
                                        //dsInvoiceData.AcceptChanges();

                                    }
                                    catch (Exception e)
                                    {
                                        return "Lỗi khi tạo bản ghi VC cho khách hàng: " + dr["MA_KHANG"] + " " + e.Message;
                                    }
                                }
                                //finally
                                //{
                                //    dsCustomerData.AcceptChanges();
                                //    dsInvoiceData.AcceptChanges();
                                //}
                                //System.Windows.Forms.MessageBox.Show("B5: " + Convert.ToString(dr["MA_KHANG"]));
                            }
                            ////Debug.WriteLine(DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff") + " InvoiceCalculation HDN_HDON_VC");
                        }
                        else
                        {
                            //Có quan hệ ghép tổng
                            //drQheGT: danh sách các quan hệ điểm đo chính của khách hàng
                            string[] arrMaDDoChinh = drQheGT.Select(c => c.Field<string>("MA_DDO_CHINH")).Distinct().ToArray();
                            foreach (string strMaDDoChinh in arrMaDDoChinh)
                            {
                                //Với mỗi quan hệ ghép tổng tạo một hóa đơn, nếu ký mua CSPK thì tạo 2 hóa đơn
                                string[] arrMaDDo = drQheGT.Where(c => c.Field<string>("MA_DDO_CHINH") == strMaDDoChinh).Select(b => b.Field<string>("MA_DDO_PHU")).Distinct().ToArray();
                                string[] arrMaDDoCungSo = dsInvoiceData.Tables["HDN_HDONCTIET"].AsEnumerable().Where(c => c.Field<string>("MA_KHANG") == dr["MA_KHANG"].ToString() && c.Field<string>("MA_SOGCS") == drQheGT[0]["MA_SOGCS_CHINH"].ToString()).Select(b => b.Field<string>("MA_DDO")).Distinct().ToArray();
                                arrMaDDo = arrMaDDo.Union(arrMaDDoCungSo).ToArray();
                                DataRow[] drDDo = (from a in dsInvoiceData.Tables["HDN_HDONCTIET"].AsEnumerable()
                                                   where a.Field<string>("MA_KHANG") == dr["MA_KHANG"].ToString()
                                                   && (a.Field<string>("MA_DDO") == strMaDDoChinh || arrMaDDo.Contains(a.Field<string>("MA_DDO")))
                                                   && "KT;BT;CD;TD".Contains(a.Field<string>("BCS"))
                                                   select a).ToArray();
                                if (drDDo == null || drDDo.Length == 0) continue;
                                //DungNT: kiểm tra nhóm giá tiền <> VND để nhân với tỷ giá
                                string[] arrMaNhomNN = drDDo.Select(c => c.Field<string>("MA_NHOMNN")).ToArray();
                                string filter = "";
                                foreach (string str in arrMaNhomNN)
                                {
                                    filter += filter.Trim().Length == 0 ? "'" + str + "'" : ",'" + str + "'";
                                }
                                if (dsStaticCatalog != null && dsStaticCatalog.Tables["D_GIA_NHOMNN"] != null)
                                {
                                    //DataView dv = new DataView(dsStaticCatalog.Tables["D_GIA_NHOMNN"]);
                                    dv.RowFilter = "LOAI_TIEN<>'VND' and MA_NHOMNN in (" + filter + ")";
                                    //dv.Sort = "NGAY_ADUNG DESC";

                                    if (dv != null && dv.Count > 0)
                                    {
                                        string strLoaiTien = dv[0]["LOAI_TIEN"].ToString();
                                        if (dsStaticCatalog != null && dsStaticCatalog.Tables["D_TY_GIA"] != null && dsStaticCatalog.Tables["D_TY_GIA"].Rows.Count > 0)
                                        {
                                            //DataView dvTyGia = new DataView(dsStaticCatalog.Tables["D_TY_GIA"]);
                                            dvTyGia.RowFilter = "LOAI_TIEN='" + strLoaiTien + "'";
                                            //dvTyGia.Sort = "ID_TY_GIA DESC";
                                            //dvTyGia.Sort = "NAM DESC, THANG DESC, ID_TY_GIA DESC";
                                            if (dvTyGia != null && dvTyGia.Count > 0)
                                            {
                                                decTyGia = Utility.DecimalDbnull(dvTyGia[0]["TYGIA_QDOI"]);
                                                if (decTyGia == 0) decTyGia = 1;
                                            }
                                        }
                                    }
                                }
                                //Kết thúc
                                decimal decTienDienTD = drDDo.Sum(c => c.Field<decimal>("SO_TIEN"));
                                decimal decDienTThuTD = drDDo.Sum(c => c.Field<decimal>("DIEN_TTHU"));
                                decimal decTyLeThue = Convert.ToDecimal(dr["TLE_THUE"]);
                                //Lấy danh sách chỉ số để lấy min ngày đầu kỳ và mã ngày cuối kỳ
                                //long[] arrIDChiSo = drDDo.Select(c => c.Field<long>("ID_CHISO")).ToArray();
                                //string strFilterID = "";
                                //foreach (long lngIDCHISO in arrIDChiSo)
                                //{
                                //    if (strFilterID.Length == 0)
                                //    {
                                //        strFilterID += "ID_CHISO=" + lngIDCHISO.ToString() + "";
                                //    }
                                //    else
                                //    {
                                //        strFilterID += " OR ID_CHISO=" + lngIDCHISO.ToString() + "";
                                //    }
                                //}

                                //Lấy các chỉ số từ bảng GCS_CHISO va GCS_CHISO_GT
                                //Lam nhu the nay la sai -> DataRow[] arrChiSo = dsCustomerData.Tables["GCS_CHISO"].Select(strFilterID);
                                DataRow[] arrChiSo = dsCustomerData.Tables["GCS_CHISO"].Select("MA_KHANG = '" + dr["MA_KHANG"] + "' AND BCS IN ('KT','BT','CD','TD') AND LOAI_CHISO IN ('DDK','DDN','CSC','CCS')");
                                //DateTime dtNgayDKMin = DateTime.Parse("01/01/1900", new System.Globalization.CultureInfo("vi-VN"));
                                //DateTime dtNgayCKMax = DateTime.Parse("01/01/1900", new System.Globalization.CultureInfo("vi-VN"));
                                DateTime dtNgayDKMin = new DateTime(1900, 1, 1);
                                DateTime dtNgayCKMax = new DateTime(1900, 1, 1);
                                if (arrChiSo.Count() != 0)
                                {
                                    dtNgayDKMin = arrChiSo.Min(c => c.Field<DateTime>("NGAY_DKY"));
                                    dtNgayCKMax = arrChiSo.Max(c => c.Field<DateTime>("NGAY_CKY"));
                                }
                                else
                                {
                                    if (dsCustomerData.Tables.Contains("GCS_CHISO_GT") == true)
                                    {
                                        //arrChiSo = dsCustomerData.Tables["GCS_CHISO_GT"].Select(strFilterID);
                                        arrChiSo = dsCustomerData.Tables["GCS_CHISO_GT"].Select("MA_KHANG = '" + dr["MA_KHANG"] + "' AND BCS IN ('KT','BT','CD','TD') AND LOAI_CHISO IN ('DDK','DDN','CSC','CCS')");
                                        if (arrChiSo.Count() != 0)
                                        {
                                            dtNgayDKMin = arrChiSo.Min(c => c.Field<DateTime>("NGAY_DKY"));
                                            dtNgayCKMax = arrChiSo.Max(c => c.Field<DateTime>("NGAY_CKY"));
                                        }
                                    }
                                }

                                DataRow[] drKHTT = dsCustomerData.Tables["HDG_KHACH_HANG_TT"].Select("MA_KHANG='" + dr["MA_KHTT"] + "'");
                                string strTenKHTT = (drKHTT == null || drKHTT.Length == 0) ? dr["TEN_KHANG"].ToString() : drKHTT[0]["TEN_KHANG"].ToString();
                                string strDChiKHTT = (drKHTT == null || drKHTT.Length == 0) ? dr["SO_NHA"].ToString() + " " + dr["DUONG_PHO"].ToString() : drKHTT[0]["SO_NHA"].ToString() + " " + drKHTT[0]["DUONG_PHO"].ToString();
                                DataRow[] arrDDoChinh = (from a in dsInvoiceData.Tables["HDN_HDONCTIET"].AsEnumerable()
                                                         where a.Field<string>("MA_KHANG") == dr["MA_KHANG"].ToString()
                                                         && a.Field<string>("MA_DDO") == strMaDDoChinh
                                                         select a).ToArray();
                                //Hieu chinh truong hop diem do chinh ghep tong khong co san luong va chi tiet hoa don)
                                DataRow drDDoChinhMax;
                                if (arrDDoChinh == null || arrDDoChinh.Length == 0)
                                {
                                    //arrDDoChinh = drDDo;
                                    drDDoChinhMax = drDDo[0];
                                }
                                else
                                {
                                    drDDoChinhMax = arrDDoChinh[0];
                                }

                                //decimal decSoHo = Convert.ToDecimal(dsCustomerData.Tables["HDG_DIEM_DO"].Select("MA_DDO='" + strMaDDoChinh + "'")[0]["SO_HO"]);
                                //decimal decSoHo = Convert.ToDecimal(dsCalculation.Tables["SL_4"].Select("MA_DDO='" + strMaDDoChinh + "'")[0]["SO_HO"]);
                                //DataRow[] rowsSoHo = dsCalculation.Tables["SL_4"].Select("MA_DDO='" + drDDoChinhMax["MA_DDO"] + "' AND BTHANG_ID <> 0");
                                //if (rowsSoHo.Length == 0)
                                //{
                                //    rowsSoHo = dsCalculation.Tables["SL_4"].Select("MA_DDO='" + drDDoChinhMax["MA_DDO"] + "'");
                                //}
                                //decimal decSoHo = Convert.ToDecimal(rowsSoHo[0]["SO_HO"]);
                                //Viết lại phần lấy số hộ
                                //DataView dwSL4 = new DataView(dsCalculation.Tables["SL_4"]);
                                decimal decSoHo = 0;
                                dwSL4.RowFilter = "MA_DDO='" + drDDoChinhMax["MA_DDO"] + "' AND BTHANG_ID <> 0";
                                if (dwSL4.Count > 0)
                                {
                                    //dwSL4.Sort = "NGAY_HLUCGIA DESC";
                                    strMaNhomNN = Convert.ToString(dwSL4[0]["MA_NHOMNN"]);
                                    strMaNGia = Convert.ToString(dwSL4[0]["MA_NGIA"]);
                                    decSoHo = Convert.ToDecimal(dwSL4[0]["SO_HO"]);
                                    DateTime ngay_hluc = Convert.ToDateTime(dwSL4[0]["NGAY_HLUCGIA"]);
                                    //decimal IsGiaNhomA_N = 0;
                                    if (strMaNhomNN.Contains("SHB") && strMaNhomNN != "SHBN" && "A;N".Contains(strMaNGia))
                                    {
                                        dwSL4.RowFilter = "MA_DDO='" + drDDoChinhMax["MA_DDO"] + "' AND BTHANG_ID <> 0 and MA_NHOMNN='" + strMaNhomNN + "'";
                                        //dwSL4.Sort = "NGAY_HLUCGIA DESC";
                                        foreach (DataRowView drvSL4 in dwSL4)
                                        {
                                            if (Convert.ToDateTime(drvSL4["NGAY_HLUCGIA"]) == ngay_hluc)
                                            {
                                                if ("A;N".Contains(drvSL4["MA_NGIA"].ToString().Trim()) && drvSL4["MA_NGIA"].ToString().Trim() != strMaNGia)
                                                {
                                                    decSoHo += Convert.ToDecimal(drvSL4["SO_HO"]);
                                                    break;
                                                }
                                            }
                                        }
                                    }
                                }
                                DataRow[] drCosfi = dsInvoiceData.Tables["HDN_HDONCOSFI"].Select("MA_DDO='" + drDDoChinhMax["MA_DDO"] + "' AND MA_KHANG='" + dr["MA_KHANG"] + "'");
                                if (drCosfi != null && drCosfi.Length > 0)
                                {
                                    drCosfi = drCosfi.OrderByDescending(c => c.Field<long>("ID_CHISO")).ToArray();
                                }
                                try
                                {
                                    long lngID_HDonTD = 0;//Sau sẽ chuyển sang lấy ID_HDON tự sinh
                                    //Đẩy bản ghi TD vào HDN_HDON
                                    DataRow drHDN_HDON = dsInvoiceData.Tables["HDN_HDON"].NewRow();
                                    drHDN_HDON["COSFI"] = (drCosfi == null || drCosfi.Length == 0) ? System.DBNull.Value : drCosfi[0]["COSFI"];
                                    drHDN_HDON["DCHI_KHANG"] = dr["SO_NHA"].ToString() + " " + dr["DUONG_PHO"].ToString();
                                    drHDN_HDON["DCHI_KHANGTT"] = strDChiKHTT;
                                    drHDN_HDON["DIEN_TTHU"] = decDienTThuTD;
                                    drHDN_HDON["ID_HDON"] = lngID_HDonTD;
                                    drHDN_HDON["KCOSFI"] = (drCosfi == null || drCosfi.Length == 0) ? System.DBNull.Value : drCosfi[0]["KCOSFI"];
                                    drHDN_HDON["KY"] = i16Ky;
                                    drHDN_HDON["MA_HTTT"] = strMaHTTT;
                                    drHDN_HDON["LOAI_HDON"] = "TD";
                                    //Hieu chinh truong hop diem do chinh ghep tong khong co chi tiet hoa don
                                    if (arrDDoChinh == null || arrDDoChinh.Length == 0)
                                    {
                                        DataRow[] drDDoChinhMax_Tmp = dsCustomerData.Tables["HDG_DDO_SOGCS"].Select("MA_DDO = '" + strMaDDoChinh + "' AND MA_KHANG = '" + dr["MA_KHANG"].ToString() + "'");
                                        if (drDDoChinhMax_Tmp.Length != 0)
                                        {
                                            drHDN_HDON["MA_KVUC"] = Convert.ToString(drDDoChinhMax_Tmp[0]["MA_KVUC"]);
                                            drHDN_HDON["MA_SOGCS"] = Convert.ToString(drDDoChinhMax_Tmp[0]["MA_SOGCS"]);
                                            drHDN_HDON["STT"] = Convert.ToString(drDDoChinhMax_Tmp[0]["STT"]);
                                        }
                                        else
                                        {
                                            drHDN_HDON["MA_KVUC"] = "";
                                            drHDN_HDON["MA_SOGCS"] = "";
                                            drHDN_HDON["STT"] = "";
                                        }

                                        drDDoChinhMax_Tmp = dsCustomerData.Tables["HDG_VITRI_DDO"].Select("MA_DDO = '" + strMaDDoChinh + "'");
                                        if (drDDoChinhMax_Tmp.Length != 0)
                                        {
                                            drHDN_HDON["MA_TO"] = Convert.ToString(drDDoChinhMax_Tmp[0]["MA_TO"]);
                                        }
                                        else
                                        {
                                            drHDN_HDON["MA_TO"] = "";
                                        }

                                        drDDoChinhMax_Tmp = dsCustomerData.Tables["GCS_CHISO"].Select("MA_DDO = '" + strMaDDoChinh + "' AND MA_KHANG = '" + dr["MA_KHANG"].ToString() + "' AND BCS IN ('BT','CD','TD','KT')");
                                        if (drDDoChinhMax_Tmp.Length != 0)
                                        {
                                            drHDN_HDON["SO_CTO"] = Convert.ToString(drDDoChinhMax_Tmp[0]["SO_CTO"]);
                                        }
                                        else
                                        {
                                            drHDN_HDON["SO_CTO"] = "";
                                        }
                                    }
                                    else
                                    {
                                        drHDN_HDON["MA_KVUC"] = drDDoChinhMax["MA_KVUC"];
                                        drHDN_HDON["MA_SOGCS"] = drDDoChinhMax["MA_SOGCS"];
                                        drHDN_HDON["MA_TO"] = drDDoChinhMax["MA_TO"];
                                        drHDN_HDON["SO_CTO"] = drDDoChinhMax["SO_CTO"];
                                        drHDN_HDON["STT"] = drDDoChinhMax["STT"];
                                    }

                                    drHDN_HDON["MA_CNANG"] = drDDoChinhMax["MA_CNANG"];
                                    drHDN_HDON["MA_DVIQLY"] = drDDoChinhMax["MA_DVIQLY"];
                                    drHDN_HDON["MA_KHANG"] = dr["MA_KHANG"];
                                    drHDN_HDON["DTHOAI"] = dr["DTHOAI"];
                                    drHDN_HDON["MA_KHANGTT"] = dr["MA_KHTT"].ToString().Trim().Length == 0 ? dr["MA_KHANG"] : dr["MA_KHTT"];
                                    drHDN_HDON["MA_NHANG"] = dr["MA_NHANG"];
                                    drHDN_HDON["MASO_THUE"] = dr["MASO_THUE"];
                                    drHDN_HDON["NAM"] = i16Nam;
                                    drHDN_HDON["NGAY_CKY"] = dtNgayCKMax;
                                    drHDN_HDON["NGAY_DKY"] = dtNgayDKMin;
                                    drHDN_HDON["NGAY_SUA"] = drDDoChinhMax["NGAY_SUA"];
                                    drHDN_HDON["NGAY_TAO"] = drDDoChinhMax["NGAY_TAO"];
                                    drHDN_HDON["NGUOI_SUA"] = drDDoChinhMax["NGUOI_SUA"];
                                    drHDN_HDON["NGUOI_TAO"] = drDDoChinhMax["NGUOI_TAO"];

                                    drHDN_HDON["SO_HO"] = decSoHo;
                                    drHDN_HDON["SO_LANIN"] = "0";
                                    drHDN_HDON["SO_TIEN"] = Math.Round(decTienDienTD * decTyGia, 0, MidpointRounding.AwayFromZero);
                                    drHDN_HDON["TEN_KHANG"] = dr["TEN_KHANG"];
                                    drHDN_HDON["TEN_KHANGTT"] = strTenKHTT;
                                    drHDN_HDON["THANG"] = i16Thang;
                                    drHDN_HDON["TIEN_GTGT"] = Math.Round(decTyLeThue * decTienDienTD * decTyGia / dec100, 0, MidpointRounding.AwayFromZero);
                                    drHDN_HDON["TKHOAN_KHANG"] = dr["TKHOAN_KHANG"];
                                    //drHDN_HDON["TONG_TIEN"] = Math.Round(decTienDienTD * decTyLeThue*decTyGia / dec100, 0, MidpointRounding.AwayFromZero) + decTienDienTD;
                                    drHDN_HDON["TONG_TIEN"] = Math.Round(decTienDienTD * decTyLeThue * decTyGia / dec100 + decTienDienTD * decTyGia, 0, MidpointRounding.AwayFromZero);
                                    drHDN_HDON["TYLE_THUE"] = dr["TLE_THUE"];
                                    //DũngNT: Bổ sung 5 trường thay đổi CSDL ngày 23/12/2009
                                    drHDN_HDON["LOAI_KHANG"] = dr["LOAI_KHANG"];
                                    drHDN_HDON["MANHOM_KHANG"] = dr["MANHOM_KHANG"];
                                    drHDN_HDON["MA_LOAIDN"] = dr["MA_LOAIDN"];
                                    drHDN_HDON["MA_PTTT"] = strMaPTTT;
                                    drHDN_HDON["DCHI_TTOAN"] = dr["DCHI_TTOAN"];
                                    drHDN_HDON["TIEN_TD"] = drHDN_HDON["SO_TIEN"];
                                    drHDN_HDON["THUE_TD"] = drHDN_HDON["TIEN_GTGT"];
                                    drHDN_HDON["TIEN_VC"] = 0;
                                    drHDN_HDON["THUE_VC"] = 0;
                                    //Tạm thời để trống DCHI_TTOAN, trước khi đẩy vào CSDL sẽ fill trường này từ bảng HDG_HOP_DONG
                                    dsInvoiceData.Tables["HDN_HDON"].Rows.Add(drHDN_HDON);
                                    //dsInvoiceData.AcceptChanges();
                                }
                                catch (Exception e)
                                {
                                    return "Lỗi khi tạo bản ghi TD cho khách hàng ghép tổng: " + dr["MA_KHANG"] + " " + e.Message;
                                }
                                //finally
                                //{
                                //    dsCustomerData.AcceptChanges();
                                //    dsInvoiceData.AcceptChanges();
                                //}
                                //Lấy thông tin vô công trong bảng HDN_HDONCOSFI

                                //dwCF = dsInvoiceData.Tables["HDN_HDONCOSFI"].DefaultView;
                                dwCF.RowFilter = "MA_KHANG='" + dr["MA_KHANG"] + "' AND KIMUA_CSPK=1";
                                //dwCF.Sort = "MA_DDO, ID_CHISO ASC";
                                if (dwCF != null && dwCF.Count > 0)
                                {
                                    long id_HC_Max = 0;
                                    string strMaDDo = "";
                                    foreach (DataRowView drVoCong in dwCF)
                                    {
                                        //Dũng NT hiệu chỉnh lại đoạn tính tiền hữu công / vô công đợt đổi CSPK 10/12/2014
                                        //Dựa theo ID_CHISO của dòng vô công
                                        if (Convert.ToString(drVoCong["MA_DDO"]) != strMaDDo)
                                            id_HC_Max = 0;
                                        long id_CS_VC = Convert.ToInt64(drVoCong["ID_CHISO"]);
                                        strMaDDo = Convert.ToString(drVoCong["MA_DDO"]);
                                        DateTime dtNgayCKy = new DateTime(1900, 1, 1);
                                        //Lấy danh sách ID_CHISO hữu công
                                        List<long> lstID_CHISO_HC = new List<long>();
                                        dwCS.RowFilter = "";
                                        if (dwCS_GT != null)
                                            dwCS_GT.RowFilter = "";
                                        dwCS.RowFilter = "MA_DDO='" + strMaDDo + "'";
                                        if (dwCS.Count > 0)
                                        {
                                            foreach (DataRowView drv in dwCS)
                                            {
                                                if (Convert.ToInt64(drv["ID_CHISO"]) == id_CS_VC)
                                                {
                                                    dtNgayCKy = Convert.ToDateTime(drv["NGAY_CKY"]);
                                                    break;
                                                }
                                            }
                                            foreach (DataRowView drv in dwCS)
                                            {
                                                if (Convert.ToDateTime(drv["NGAY_CKY"]) <= dtNgayCKy && Convert.ToString(drv["BCS"]) != "VC")
                                                {
                                                    lstID_CHISO_HC.Add(Convert.ToInt64(drv["ID_CHISO"]));
                                                }
                                            }
                                        }
                                        else
                                        {
                                            dwCS_GT.RowFilter = "MA_DDO='" + strMaDDo + "'";
                                            foreach (DataRowView drv in dwCS_GT)
                                            {
                                                if (Convert.ToInt64(drv["ID_CHISO"]) == id_CS_VC)
                                                {
                                                    dtNgayCKy = Convert.ToDateTime(drv["NGAY_CKY"]);
                                                    break;
                                                }
                                            }
                                            foreach (DataRowView drv in dwCS_GT)
                                            {
                                                if (Convert.ToDateTime(drv["NGAY_CKY"]) <= dtNgayCKy && Convert.ToString(drv["BCS"]) != "VC")
                                                {
                                                    lstID_CHISO_HC.Add(Convert.ToInt64(drv["ID_CHISO"]));
                                                }
                                            }
                                        }



                                        decimal decTienHuuCong = 0;
                                        if (Convert.ToDecimal(drVoCong["MA_CNANG"]) != 0)
                                        {
                                            //Treo thao doi loai cong to tu 1 -> 3,5,8 va nguoc lai
                                            dwCT.RowFilter = "MA_DDO = '" + strMaDDo + "' and BCS in ('BT','CD','TD')";
                                            foreach (DataRowView drv in dwCT)
                                            {
                                                if (lstID_CHISO_HC.Contains(Convert.ToInt64(drv["ID_CHISO"])) && Convert.ToInt64(drv["ID_CHISO"]) > id_HC_Max)
                                                {
                                                    decTienHuuCong += CMISLibrary.Utility.DecimalDbnull(drv["SO_TIEN"]);
                                                }
                                            }
                                            //decTienHuuCong = CMISLibrary.Utility.DecimalDbnull(dsInvoiceData.Tables["HDN_HDONCTIET"].Compute("SUM(SO_TIEN)", "MA_DDO = '" + drVoCong["MA_DDO"].ToString().Trim() + "' AND BCS IN ('BT','CD','TD')"));
                                        }
                                        else
                                        {
                                            dwCT.RowFilter = "MA_DDO = '" + strMaDDo + "' and BCS in ('BT','CD','TD','KT')";
                                            foreach (DataRowView drv in dwCT)
                                            {
                                                if (lstID_CHISO_HC.Contains(Convert.ToInt64(drv["ID_CHISO"])) && Convert.ToInt64(drv["ID_CHISO"]) > id_HC_Max)
                                                {
                                                    decTienHuuCong += CMISLibrary.Utility.DecimalDbnull(drv["SO_TIEN"]);
                                                }
                                            }
                                            //decTienHuuCong = CMISLibrary.Utility.DecimalDbnull(dsInvoiceData.Tables["HDN_HDONCTIET"].Compute("SUM(SO_TIEN)", "MA_DDO = '" + drVoCong["MA_DDO"].ToString().Trim() + "' AND BCS IN ('KT','BT','CD','TD')"));
                                        }
                                        id_HC_Max = lstID_CHISO_HC.Max();
                                        lstID_CHISO_HC.Clear();
                                        if (drVoCong["KCOSFI"].ToString().Trim() == "0") continue;
                                        //Set lai ve gia tri mac dinh
                                        drVoCong["MA_CNANG"] = 0;
                                        decimal decTienVoCong = decTienHuuCong * Convert.ToDecimal(drVoCong["KCOSFI"]) / 100;

                                        drVoCong["TIEN_VOCONG"] = Math.Round(decTienVoCong, 0, MidpointRounding.AwayFromZero);
                                        drVoCong["TIEN_HUUCONG"] = Math.Round(decTienHuuCong, 0, MidpointRounding.AwayFromZero);
                                        var Temp = dsInvoiceData.Tables["HDN_HDONCTIET"].Select("MA_DDO = '" + drVoCong["MA_DDO"].ToString().Trim() + "' AND BCS = 'VC'");
                                        if (Temp != null && Temp.Count() > 0)
                                        {
                                            DataRow drChiTiet = Temp.Where(c => c.Field<long>("ID_CHISO") == Convert.ToInt64(drVoCong["ID_CHISO"])).FirstOrDefault();
                                            if (drChiTiet != null)
                                                drChiTiet["SO_TIEN"] = Math.Round(decTienVoCong, 0, MidpointRounding.AwayFromZero);
                                            //dsInvoiceData.Tables["HDN_HDONCTIET"].AcceptChanges();
                                        }
                                    }
                                    //dwCF.Table.AcceptChanges();
                                    dwCF.RowFilter = "";

                                    DataRow[] drVC = dsInvoiceData.Tables["HDN_HDONCOSFI"].Select("MA_KHANG='" + dr["MA_KHANG"] + "' AND KIMUA_CSPK=1 AND KCOSFI<>0");
                                    if (drVC != null && drVC.Length > 0)
                                    {
                                        DataRow[] arrChiTietVC = dsInvoiceData.Tables["HDN_HDONCTIET"].Select("MA_KHANG='" + dr["MA_KHANG"] + "' AND BCS='VC'");
                                        decimal decTienDienVC = drVC.Sum(c => c.Field<decimal>("TIEN_VOCONG"));
                                        //decimal decDienTThuVC = drVC.Sum(c => c.Field<decimal>("VO_CONG"));
                                        decimal decDienTThuVC = arrChiTietVC.Sum(c => c.Field<decimal>("DIEN_TTHU"));
                                        //Lấy danh sách chỉ số để lấy min ngày đầu kỳ và mã ngày cuối kỳ
                                        long[] arrIDChiSoVC = arrChiTietVC.Select(c => c.Field<long>("ID_CHISO")).ToArray();
                                        //strFilterID = "";
                                        //foreach (long lngIDCHISO in arrIDChiSoVC)
                                        //{
                                        //    if (strFilterID.Length == 0)
                                        //    {
                                        //        strFilterID += "ID_CHISO=" + lngIDCHISO.ToString() + "";
                                        //    }
                                        //    else
                                        //    {
                                        //        strFilterID += "OR ID_CHISO=" + lngIDCHISO.ToString() + "";
                                        //    }
                                        //}

                                        //Lam the nay la sai -> DataRow[] arrChiSoVC = dsCustomerData.Tables["GCS_CHISO"].Select(strFilterID);
                                        DataRow[] arrChiSoVC = dsCustomerData.Tables["GCS_CHISO"].Select("MA_KHANG = '" + dr["MA_KHANG"] + "' AND BCS IN ('KT','BT','CD','TD') AND LOAI_CHISO IN ('DDK','DDN','CSC','CCS')");
                                        DateTime dtNgayDKMinVC = DateTime.Parse("01/01/1900", new System.Globalization.CultureInfo("vi-VN"));
                                        DateTime dtNgayCKMaxVC = DateTime.Parse("01/01/1900", new System.Globalization.CultureInfo("vi-VN"));

                                        if (arrChiSoVC.Count() != 0)
                                        {
                                            dtNgayDKMinVC = arrChiSoVC.Min(c => c.Field<DateTime>("NGAY_DKY"));
                                            dtNgayCKMaxVC = arrChiSoVC.Max(c => c.Field<DateTime>("NGAY_CKY"));
                                        }
                                        else
                                        {
                                            if (dsCustomerData.Tables.Contains("GCS_CHISO_GT") == true)
                                            {
                                                arrChiSoVC = dsCustomerData.Tables["GCS_CHISO_GT"].Select("MA_KHANG = '" + dr["MA_KHANG"] + "' AND BCS IN ('KT','BT','CD','TD') AND LOAI_CHISO IN ('DDK','DDN','CSC','CCS')");
                                                if (arrChiSoVC.Count() != 0)
                                                {
                                                    dtNgayDKMinVC = arrChiSoVC.Min(c => c.Field<DateTime>("NGAY_DKY"));
                                                    dtNgayCKMaxVC = arrChiSoVC.Max(c => c.Field<DateTime>("NGAY_CKY"));
                                                }
                                            }
                                        }

                                        //Đẩy thêm bản ghi VC vào HDN_HDON
                                        try
                                        {
                                            long lngID_HDonVC = 0;//Sau sẽ chuyển sang lấy ID_HDON tự sinh
                                            //Đẩy thêm bản ghi VC vào HDN_HDON
                                            DataRow drHDN_HDON = dsInvoiceData.Tables["HDN_HDON"].NewRow();
                                            drHDN_HDON["COSFI"] = (drCosfi == null || drCosfi.Length == 0) ? System.DBNull.Value : drCosfi[0]["COSFI"];
                                            drHDN_HDON["DCHI_KHANG"] = dr["SO_NHA"].ToString() + " " + dr["DUONG_PHO"].ToString();
                                            drHDN_HDON["DCHI_KHANGTT"] = strDChiKHTT;
                                            drHDN_HDON["DIEN_TTHU"] = decDienTThuVC;
                                            drHDN_HDON["ID_HDON"] = lngID_HDonVC;
                                            drHDN_HDON["KCOSFI"] = (drCosfi == null || drCosfi.Length == 0) ? System.DBNull.Value : drCosfi[0]["KCOSFI"];
                                            drHDN_HDON["KY"] = i16Ky;
                                            drHDN_HDON["MA_HTTT"] = strMaHTTT;
                                            drHDN_HDON["LOAI_HDON"] = "VC";
                                            drHDN_HDON["MA_CNANG"] = drDDoChinhMax["MA_CNANG"];
                                            drHDN_HDON["MA_DVIQLY"] = drDDoChinhMax["MA_DVIQLY"];
                                            drHDN_HDON["MA_KHANG"] = dr["MA_KHANG"];
                                            drHDN_HDON["DTHOAI"] = dr["DTHOAI"];
                                            drHDN_HDON["MA_KHANGTT"] = dr["MA_KHTT"].ToString().Trim().Length == 0 ? dr["MA_KHANG"] : dr["MA_KHTT"];

                                            //Hieu chinh truong hop diem do chinh ghep tong khong co chi tiet hoa don
                                            if (arrDDoChinh == null || arrDDoChinh.Length == 0)
                                            {
                                                DataRow[] drDDoChinhMax_Tmp = dsCustomerData.Tables["HDG_DDO_SOGCS"].Select("MA_DDO = '" + strMaDDoChinh + "' AND MA_KHANG = '" + dr["MA_KHANG"].ToString() + "'");
                                                if (drDDoChinhMax_Tmp.Length != 0)
                                                {
                                                    drHDN_HDON["MA_KVUC"] = Convert.ToString(drDDoChinhMax_Tmp[0]["MA_KVUC"]);
                                                    drHDN_HDON["MA_SOGCS"] = Convert.ToString(drDDoChinhMax_Tmp[0]["MA_SOGCS"]);
                                                    drHDN_HDON["STT"] = Convert.ToString(drDDoChinhMax_Tmp[0]["STT"]);
                                                }
                                                else
                                                {
                                                    drHDN_HDON["MA_KVUC"] = "";
                                                    drHDN_HDON["MA_SOGCS"] = "";
                                                    drHDN_HDON["STT"] = "";
                                                }

                                                drDDoChinhMax_Tmp = dsCustomerData.Tables["HDG_VITRI_DDO"].Select("MA_DDO = '" + strMaDDoChinh + "'");
                                                if (drDDoChinhMax_Tmp.Length != 0)
                                                {
                                                    drHDN_HDON["MA_TO"] = Convert.ToString(drDDoChinhMax_Tmp[0]["MA_TO"]);
                                                }
                                                else
                                                {
                                                    drHDN_HDON["MA_TO"] = "";
                                                }

                                                drDDoChinhMax_Tmp = dsCustomerData.Tables["GCS_CHISO"].Select("MA_DDO = '" + strMaDDoChinh + "' AND MA_KHANG = '" + dr["MA_KHANG"].ToString() + "' AND BCS IN ('VC')");
                                                if (drDDoChinhMax_Tmp.Length != 0)
                                                {
                                                    drHDN_HDON["SO_CTO"] = Convert.ToString(drDDoChinhMax_Tmp[0]["SO_CTO"]);
                                                }
                                                else
                                                {
                                                    drDDoChinhMax_Tmp = dsCustomerData.Tables["GCS_CHISO"].Select("MA_DDO = '" + strMaDDoChinh + "' AND MA_KHANG = '" + dr["MA_KHANG"].ToString() + "' AND BCS IN ('BT','CD','TD','KT')");
                                                    if (drDDoChinhMax_Tmp.Length != 0)
                                                    {
                                                        drHDN_HDON["SO_CTO"] = Convert.ToString(drDDoChinhMax_Tmp[0]["SO_CTO"]);
                                                    }
                                                    else
                                                    {
                                                        drHDN_HDON["SO_CTO"] = "";
                                                    }
                                                }
                                            }
                                            else
                                            {
                                                drHDN_HDON["MA_KVUC"] = drDDoChinhMax["MA_KVUC"];
                                                drHDN_HDON["MA_SOGCS"] = drDDoChinhMax["MA_SOGCS"];
                                                drHDN_HDON["MA_TO"] = drDDoChinhMax["MA_TO"];
                                                drHDN_HDON["SO_CTO"] = drDDoChinhMax["SO_CTO"];
                                                drHDN_HDON["STT"] = drDDoChinhMax["STT"];
                                            }


                                            drHDN_HDON["MA_NHANG"] = dr["MA_NHANG"];

                                            drHDN_HDON["MASO_THUE"] = dr["MASO_THUE"];
                                            drHDN_HDON["NAM"] = i16Nam;
                                            drHDN_HDON["NGAY_CKY"] = dtNgayCKMaxVC;
                                            drHDN_HDON["NGAY_DKY"] = dtNgayDKMinVC;
                                            drHDN_HDON["NGAY_SUA"] = drDDoChinhMax["NGAY_SUA"];
                                            drHDN_HDON["NGAY_TAO"] = drDDoChinhMax["NGAY_TAO"];
                                            drHDN_HDON["NGUOI_SUA"] = drDDoChinhMax["NGUOI_SUA"];
                                            drHDN_HDON["NGUOI_TAO"] = drDDoChinhMax["NGUOI_TAO"];
                                            drHDN_HDON["SO_HO"] = decSoHo;
                                            drHDN_HDON["SO_LANIN"] = "0";
                                            drHDN_HDON["SO_TIEN"] = Math.Round(decTienDienVC * decTyGia, 0, MidpointRounding.AwayFromZero);
                                            drHDN_HDON["TEN_KHANG"] = dr["TEN_KHANG"];
                                            drHDN_HDON["TEN_KHANGTT"] = strTenKHTT;
                                            drHDN_HDON["THANG"] = i16Thang;
                                            drHDN_HDON["TIEN_GTGT"] = Math.Round(decTyLeThue * decTienDienVC * decTyGia / dec100, 0, MidpointRounding.AwayFromZero);
                                            drHDN_HDON["TKHOAN_KHANG"] = dr["TKHOAN_KHANG"];
                                            //drHDN_HDON["TONG_TIEN"] = Math.Round(decTienDienVC * decTyLeThue / dec100, 0, MidpointRounding.AwayFromZero) + decTienDienVC;
                                            drHDN_HDON["TONG_TIEN"] = Math.Round(decTienDienVC * decTyLeThue * decTyGia / dec100 + decTienDienVC * decTyGia, 0, MidpointRounding.AwayFromZero);
                                            drHDN_HDON["TYLE_THUE"] = dr["TLE_THUE"];
                                            //DũngNT: Bổ sung 5 trường thay đổi CSDL ngày 23/12/2009
                                            drHDN_HDON["LOAI_KHANG"] = dr["LOAI_KHANG"];
                                            drHDN_HDON["MANHOM_KHANG"] = dr["MANHOM_KHANG"];
                                            drHDN_HDON["MA_LOAIDN"] = dr["MA_LOAIDN"];
                                            drHDN_HDON["MA_PTTT"] = strMaPTTT;
                                            drHDN_HDON["DCHI_TTOAN"] = dr["DCHI_TTOAN"];
                                            drHDN_HDON["TIEN_TD"] = 0;
                                            drHDN_HDON["THUE_TD"] = 0;
                                            drHDN_HDON["TIEN_VC"] = drHDN_HDON["SO_TIEN"];
                                            drHDN_HDON["THUE_VC"] = drHDN_HDON["TIEN_GTGT"];
                                            //Tạm thời để trống DCHI_TTOAN, trước khi đẩy vào CSDL sẽ fill trường này từ bảng HDG_HOP_DONG
                                            if (drHDN_HDON["TONG_TIEN"].ToString().Trim() != "" && drHDN_HDON["TONG_TIEN"].ToString().Trim() != "0")
                                                dsInvoiceData.Tables["HDN_HDON"].Rows.Add(drHDN_HDON);
                                            //dsInvoiceData.AcceptChanges();
                                        }
                                        catch (Exception e)
                                        {
                                            return "Lỗi khi tạo bản ghi VC cho khách hàng ghép tổng: " + dr["MA_KHANG"] + " " + e.Message;
                                        }
                                    }
                                    //finally
                                    //{
                                    //    dsCustomerData.AcceptChanges();
                                    //    dsInvoiceData.AcceptChanges();
                                    //}
                                }
                            }

                        }
                    }
                    catch (Exception e)
                    {
                        return "Lỗi khi tính tổng tiền cho khách hàng: " + dr["MA_KHANG"] + " " + e.Message;
                    }
                    //finally
                    //{
                    //    dsCustomerData.AcceptChanges();
                    //    dsInvoiceData.AcceptChanges();
                    //}
                }
                if (dsInvoiceData.Tables["HDN_HDON"] == null || dsInvoiceData.Tables["HDN_HDON"].Rows.Count == 0)
                {
                    if (dsCustomerData.Tables["GCS_CHISO"].Select("BCS IN ('KT','BT','CD','TD') AND SAN_LUONG + SLUONG_TTIEP - SLUONG_TRPHU > 0").Length != 0)
                    {
                        return "Không có dữ liệu hóa đơn";
                    }
                    else
                    {
                        if (dsCustomerData.Tables.Contains("GCS_CHISO_GT") == true)
                        {
                            if (dsCustomerData.Tables["GCS_CHISO_GT"].Select("BCS IN ('KT','BT','CD','TD') AND SAN_LUONG + SLUONG_TTIEP - SLUONG_TRPHU > 0").Length != 0)
                            {
                                return "Không có dữ liệu hóa đơn";
                            }
                        }
                    }
                }
                return "";
            }
            catch (Exception ex)
            {
                return "Lỗi khi tạo dữ liệu hoá đơn: " + ex.Message;
            }
            //finally
            //{
            //    dsCustomerData.AcceptChanges();
            //    dsInvoiceData.AcceptChanges();
            //}
        }
        
    }
}
