using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using BillingLibrary;

namespace busBillingCalculation
{
    public class clsPriceSpecification
    {

        //Tinh san luong chi tiet va don gia theo cac doi tuong la bac thang 
        public string PriceSpecification_1(DS_CALCULATIONTABLES dsCalculation, DataSet dsStaticCatalog, DataSet dsCustomerData)
        {
            try
            {
                DataView dwBThang, dwBThangID, dwSL3;
                DataRow[] rowsSL3;
                DateTime ngay_dau, ngay_cuoi, ngay_hlucBT, dtDG2019;
                string strMa_NhomNN = "", strMa_NGia = "", strMa_DDo = "";
                Decimal IsExists = 0, IsExistsBB = 0;
                Decimal sl = 0, sl_ct = 0, sl_dm = 0;
                Decimal so_ho = 0, so_ngaydmuc = 0;
                //Decimal delta_dmuc = 0;
                DataRow drSan_Luong;
                string strDDo_BThang = "";
                Decimal slBTCDTD = 0;
                dtDG2019 = new DateTime(2099, 3, 20);
                //Tach san luong chi tiet theo cac doi tuong la bac thang hay khong
                //Day du lieu vao bang SL_4
                if (dsCalculation == null)
                    return "dsCalculationIsNull";
                else
                    if (dsCalculation.Tables.Contains("SL_3") == false)
                    return "NotExistsSL_3";
                else
                        if (dsCalculation.Tables.Contains("SL_4") == false)
                    return "NotExistsSL_4";

                if (dsStaticCatalog == null)
                    return "dsStaticCatalogIsNull";
                else
                    if (dsStaticCatalog.Tables.Contains("D_BAC_THANG") == false)
                    return "NotExistsD_BAC_THANG";

                dwSL3 = new DataView(dsCalculation.Tables["SL_3"]);
                dwSL3.Sort = "MA_DVIQLY, MA_DDO, NGAY_DAU, NGAY_CUOI, MA_NHOMNN, BCS";
                strDDo_BThang = "|";

                dwBThang = new DataView(dsStaticCatalog.Tables["D_BAC_THANG"]);
                dwBThang.RowFilter = "MA_NHOMNN = 'SHBT' AND MA_NGIA = 'A'";
                dwBThang.Sort = "NGAY_HLUC DESC";

                dwBThangID = new DataView(dsStaticCatalog.Tables["D_BAC_THANG"]);
                dwBThangID.RowFilter = "MA_NHOMNN = 'SHBT' AND MA_NGIA = 'A'";
                dwBThangID.Sort = "BTHANG_ID";

                foreach (DataRowView drSL3 in dwSL3)
                {
                    //Khoi tao du lieu
                    strMa_DDo = Convert.ToString(drSL3["MA_DDO"]);
                    if (strMa_DDo == "PD03000017619006")
                    {
                    }
                    strMa_NhomNN = Convert.ToString(drSL3["MA_NHOMNN"]);
                    strMa_NGia = Convert.ToString(drSL3["MA_NGIA"]);
                    ngay_dau = Convert.ToDateTime(drSL3["NGAY_DAU"]);
                    ngay_cuoi = Convert.ToDateTime(drSL3["NGAY_CUOI"]);
                    sl = Convert.ToDecimal(drSL3["SAN_LUONG"]);
                    ngay_hlucBT = Convert.ToDateTime(drSL3["NGAY_HLUCGIA"]);
                    IsExists = 0;

                    //dw = new DataView(dsStaticCatalog.Tables["D_BAC_THANG"]);
                    dwBThang.RowFilter = "MA_NHOMNN = '" + strMa_NhomNN + "' AND MA_NGIA = '" + strMa_NGia + "'";
                    //dw.Sort = "NGAY_HLUC DESC";

                    foreach (DataRowView drBT in dwBThang)
                    {
                        ngay_hlucBT = DateTime.Parse(Convert.ToDateTime(drBT["NGAY_HLUC"]).ToString("dd/MM/yyyy", new System.Globalization.CultureInfo("vi-VN")), new System.Globalization.CultureInfo("vi-VN"));
                        if (ngay_dau >= ngay_hlucBT)
                        {
                            IsExists = 1;
                            break;
                        }
                    }

                    if (IsExists == 1)
                    {
                        //Dang tinh toan bac thang       
                        //Xu ly cong to dien tu 
                        if ("|BT|CD|TD|".Contains("|" + Convert.ToString(drSL3["BCS"]) + "|") == true)
                        {
                            if (strDDo_BThang.Contains("|" + strMa_DDo + "-" + ngay_dau.ToString("dd/MM/yyyy", new System.Globalization.CultureInfo("vi-VN")) + "-" + ngay_cuoi.ToString("dd/MM/yyyy", new System.Globalization.CultureInfo("vi-VN")) + "|") == false)
                            {
                                //Xu ly cong san luong
                                rowsSL3 = dsCalculation.Tables["SL_3"].Select("MA_DDO = '" + strMa_DDo + "' AND MA_NHOMNN = '" + strMa_NhomNN + "' AND MA_NGIA = '" + strMa_NGia + "' AND BCS IN ('BT','CD','TD')");
                                slBTCDTD = 0;
                                foreach (DataRow drBTCDTD in rowsSL3)
                                {
                                    if (ngay_dau == Convert.ToDateTime(drBTCDTD["NGAY_DAU"]) && ngay_cuoi == Convert.ToDateTime(drBTCDTD["NGAY_CUOI"]) && Convert.ToDateTime(drSL3["NGAY_HLUCGIA"]) == Convert.ToDateTime(drBTCDTD["NGAY_HLUCGIA"]))
                                    {
                                        slBTCDTD = slBTCDTD + Convert.ToDecimal(drBTCDTD["SAN_LUONG"]);
                                    }
                                }
                                drSL3.Row["SAN_LUONG"] = slBTCDTD;
                                drSL3.Row["BCS"] = "BT";
                                sl = Convert.ToDecimal(drSL3.Row["SAN_LUONG"]);

                                strDDo_BThang = strDDo_BThang + strMa_DDo + "-" + ngay_dau.ToString("dd/MM/yyyy", new System.Globalization.CultureInfo("vi-VN")) + "-" + ngay_cuoi.ToString("dd/MM/yyyy", new System.Globalization.CultureInfo("vi-VN")) + "|";
                            }
                            else
                            {
                                dsCalculation.Tables["SL_3"].Rows.Remove(drSL3.Row);
                                continue;
                            }
                        }

                        //Khoi tao du lieu
                        so_ho = Convert.ToDecimal(drSL3["SO_HO"]);
                        so_ngaydmuc = Convert.ToDecimal(drSL3["SO_NGAYDMUC"]);
                        //DungNT sửa ngày 23-10-2009: Sửa công thức tính delta_dmuc
                        //if (strMa_DDo == "PP01000152588001")
                        //{
                        //    strMa_DDo = "PP01000152588001";
                        //}
                        //delta_dmuc = this.DeltaDinhMuc(ngay_dau, ngay_cuoi, so_ngaydmuc);
                        if ("|SHBB|SHBC|SHBD|SHBH|SHBL|SHBM|".Contains("|" + strMa_NhomNN + "|") == true)
                            IsExistsBB = 1;
                        else
                            IsExistsBB = 0;

                        //Tach san luong bac thang
                        dwBThangID.RowFilter = "MA_NHOMNN = '" + strMa_NhomNN + "' AND MA_NGIA = '" + strMa_NGia + "'";
                        //dw.Sort = "BTHANG_ID";
                        foreach (DataRowView drBT in dwBThangID)
                        {
                            if (ngay_hlucBT == Convert.ToDateTime(drBT["NGAY_HLUC"]))
                            {
                                if (sl > 0)
                                {
                                    sl_dm = Convert.ToDecimal(drBT["DINH_MUC"]);
                                    if (sl_dm == 0)
                                        sl_ct = sl;
                                    else
                                    {
                                        if (IsExistsBB == 0)
                                        {
                                            //sl_ct = Math.Round(Convert.ToDecimal(sl_dm * delta_dmuc * so_ho), 0, MidpointRounding.AwayFromZero);                                            
                                            sl_ct = this.DeltaDinhMuc(ngay_dau, ngay_cuoi, so_ngaydmuc, sl_dm, so_ho, dsCustomerData, strMa_DDo);
                                            if (sl_ct > sl)
                                                sl_ct = sl;
                                        }
                                        else
                                        {
                                            //Nếu sau ngày đổi giá 2019, tính theo định mức ngày, trước đó tính bình thường
                                            if (ngay_cuoi > dtDG2019)
                                                sl_ct = this.DeltaDinhMuc(ngay_dau, ngay_cuoi, so_ngaydmuc, sl_dm, so_ho, dsCustomerData, strMa_DDo);
                                            else
                                                sl_ct = Math.Round(Convert.ToDecimal(sl_dm * so_ho), 0, MidpointRounding.AwayFromZero);
                                            if (sl_ct > sl)
                                                sl_ct = sl;
                                        }
                                    }
                                    sl = sl - sl_ct;

                                    //Day du lieu vao SL_4
                                    drSan_Luong = dsCalculation.Tables["SL_4"].NewRow();
                                    drSan_Luong["MA_DVIQLY"] = drSL3["MA_DVIQLY"];
                                    drSan_Luong["MA_DDO"] = drSL3["MA_DDO"];
                                    drSan_Luong["MA_KHANG"] = drSL3["MA_KHANG"];
                                    drSan_Luong["ID_CHISO"] = drSL3["ID_CHISO"];
                                    drSan_Luong["ID_BCS"] = drSL3["ID_BCS"];
                                    drSan_Luong["BCS"] = drSL3["BCS"];
                                    drSan_Luong["SO_CTO"] = drSL3["SO_CTO"];
                                    drSan_Luong["NGAY_DAU"] = drSL3["NGAY_DAU"];
                                    drSan_Luong["NGAY_CUOI"] = drSL3["NGAY_CUOI"];
                                    drSan_Luong["SAN_LUONG"] = drSL3["SAN_LUONG"];
                                    drSan_Luong["DTUONG_GIA"] = drSL3["DTUONG_GIA"];
                                    drSan_Luong["NGAY_HLUCGIA"] = drSL3["NGAY_HLUCGIA"];
                                    drSan_Luong["SO_NGAYDMUC"] = drSL3["SO_NGAYDMUC"];

                                    drSan_Luong["SO_THUTU"] = drSL3["SO_THUTU"];
                                    drSan_Luong["DINH_MUC"] = drSL3["DINH_MUC"];
                                    drSan_Luong["LOAI_DMUC"] = drSL3["LOAI_DMUC"];
                                    drSan_Luong["TGIAN_BDIEN"] = drSL3["TGIAN_BDIEN"];
                                    drSan_Luong["MA_NHOMNN"] = drSL3["MA_NHOMNN"];
                                    drSan_Luong["MA_NGIA"] = drSL3["MA_NGIA"];
                                    drSan_Luong["MA_NN"] = drSL3["MA_NN"];
                                    drSan_Luong["SO_HO"] = drSL3["SO_HO"];
                                    drSan_Luong["MA_CAPDAP"] = drSL3["MA_CAPDAP"];
                                    drSan_Luong["ID_BBANAGIA"] = drSL3["ID_BBANAGIA"];
                                    drSan_Luong["KY"] = drSL3["KY"];
                                    drSan_Luong["THANG"] = drSL3["THANG"];
                                    drSan_Luong["NAM"] = drSL3["NAM"];

                                    drSan_Luong["SLCT"] = sl_ct;
                                    drSan_Luong["MA_SLCT_TC"] = Convert.ToString(strMa_NhomNN + drBT["DON_GIA"]);
                                    drSan_Luong["BTHANG_ID"] = drBT["BTHANG_ID"];
                                    drSan_Luong["DON_GIA"] = drBT["DON_GIA"];
                                    drSan_Luong["LOAI_TIEN"] = "VND";//KH USD không có bậc thang
                                    drSan_Luong["NGAY_APDUNG"] = Convert.ToDateTime(drBT["NGAY_HLUC"]);
                                    dsCalculation.Tables["SL_4"].Rows.Add(drSan_Luong);
                                }
                                else
                                    break; //Ket thuc tinh bac thang
                            }
                        }
                    }
                    else
                    {
                        //Giu nguyen, khong can tach bac thang
                        //Day du lieu vao SL_4
                        drSan_Luong = dsCalculation.Tables["SL_4"].NewRow();
                        drSan_Luong["MA_DVIQLY"] = drSL3["MA_DVIQLY"];
                        drSan_Luong["MA_DDO"] = drSL3["MA_DDO"];
                        drSan_Luong["MA_KHANG"] = drSL3["MA_KHANG"];
                        drSan_Luong["ID_CHISO"] = drSL3["ID_CHISO"];
                        drSan_Luong["ID_BCS"] = drSL3["ID_BCS"];
                        drSan_Luong["BCS"] = drSL3["BCS"];
                        drSan_Luong["SO_CTO"] = drSL3["SO_CTO"];
                        drSan_Luong["NGAY_DAU"] = drSL3["NGAY_DAU"];
                        drSan_Luong["NGAY_CUOI"] = drSL3["NGAY_CUOI"];
                        drSan_Luong["SAN_LUONG"] = drSL3["SAN_LUONG"];
                        drSan_Luong["DTUONG_GIA"] = drSL3["DTUONG_GIA"];
                        drSan_Luong["NGAY_HLUCGIA"] = drSL3["NGAY_HLUCGIA"];
                        drSan_Luong["SO_NGAYDMUC"] = drSL3["SO_NGAYDMUC"];

                        drSan_Luong["SO_THUTU"] = drSL3["SO_THUTU"];
                        drSan_Luong["DINH_MUC"] = drSL3["DINH_MUC"];
                        drSan_Luong["LOAI_DMUC"] = drSL3["LOAI_DMUC"];
                        drSan_Luong["TGIAN_BDIEN"] = drSL3["TGIAN_BDIEN"];
                        drSan_Luong["MA_NHOMNN"] = drSL3["MA_NHOMNN"];
                        drSan_Luong["MA_NGIA"] = drSL3["MA_NGIA"];
                        drSan_Luong["MA_NN"] = drSL3["MA_NN"];
                        drSan_Luong["SO_HO"] = drSL3["SO_HO"];
                        drSan_Luong["MA_CAPDAP"] = drSL3["MA_CAPDAP"];
                        drSan_Luong["ID_BBANAGIA"] = drSL3["ID_BBANAGIA"];
                        drSan_Luong["KY"] = drSL3["KY"];
                        drSan_Luong["THANG"] = drSL3["THANG"];
                        drSan_Luong["NAM"] = drSL3["NAM"];

                        drSan_Luong["SLCT"] = drSL3["SAN_LUONG"];
                        drSan_Luong["MA_SLCT_TC"] = strMa_NhomNN;
                        drSan_Luong["BTHANG_ID"] = 0;
                        drSan_Luong["DON_GIA"] = 0;
                        drSan_Luong["NGAY_APDUNG"] = DateTime.Parse("01/01/1900", new System.Globalization.CultureInfo("vi-VN"));
                        dsCalculation.Tables["SL_4"].Rows.Add(drSan_Luong);
                    }
                }
                //dsCalculation.Tables["SL_4"].AcceptChanges();

                return "";
            }
            catch (Exception ex)
            {
                return "PriceSpecification_1: " + ex.Message;
            }
            //finally
            //{
            //    dsCalculation.AcceptChanges();
            //}
        }

        //Tinh san luong chi tiet va don gia theo cac doi tuong la bac thang 
        //Su dung trong ky doi gia, khong lam tron theo chieu ngang cua tung bac thang
        //Ban buon se bi tinh noi suy theo so ngay gia cu va gia moi
        public string PriceSpecification_11(DS_CALCULATIONTABLES dsCalculation, DataSet dsStaticCatalog, DataSet dsCustomerData)
        {
            try
            {
                DataView dwBThang, dwBThangID, dwSL3, dwSL1, dwBBApGia;
                DataRow[] rowsSL3;
                DateTime ngay_dau, ngay_cuoi, ngay_hlucBT, ngay_ckymax, dtDG2019;
                string strMa_NhomNN = "", strMa_NGia = "", strMa_DDo = "";
                //Bổ sung thêm biến IsGiaNhomA_N kiểm tra có phải trường hợp 2 thành phần giá nhóm A và nhóm N không
                Decimal IsExists = 0, IsExistsBB = 0, IsGiaNhomA_N = 0;
                Decimal sl = 0, sl_ct = 0, sl_dm = 0;
                Decimal so_ho = 0, so_ngaydmuc = 0;
                //Decimal delta_dmuc = 0;
                DataRow drSan_Luong;
                string strDDo_BThang = "";
                Decimal slBTCDTD = 0;
                dtDG2019 = new DateTime(2099, 3, 20);
                //Tach san luong chi tiet theo cac doi tuong la bac thang hay khong
                //Day du lieu vao bang SL_4
                if (dsCalculation == null)
                    return "dsCalculationIsNull";
                else
                    if (dsCalculation.Tables.Contains("SL_3") == false)
                    return "NotExistsSL_3";
                else
                        if (dsCalculation.Tables.Contains("SL_4") == false)
                    return "NotExistsSL_4";

                if (dsStaticCatalog == null)
                    return "dsStaticCatalogIsNull";
                else
                    if (dsStaticCatalog.Tables.Contains("D_BAC_THANG") == false)
                    return "NotExistsD_BAC_THANG";
                dwSL1 = new DataView(dsCalculation.Tables["SL_1"]);


                dwSL3 = new DataView(dsCalculation.Tables["SL_3"]);
                dwSL3.Sort = "MA_DVIQLY, MA_DDO, NGAY_DAU, NGAY_CUOI, MA_NHOMNN, MA_NGIA, BCS";
                strDDo_BThang = "|";

                dwBThang = new DataView(dsStaticCatalog.Tables["D_BAC_THANG"]);
                dwBThang.RowFilter = "MA_NHOMNN = 'SHBT' AND MA_NGIA = 'A'";
                dwBThang.Sort = "NGAY_HLUC DESC";

                dwBThangID = new DataView(dsStaticCatalog.Tables["D_BAC_THANG"]);
                dwBThangID.RowFilter = "MA_NHOMNN = 'SHBT' AND MA_NGIA = 'A'";
                dwBThangID.Sort = "BTHANG_ID";

                foreach (DataRowView drSL3 in dwSL3)
                {
                    //Khoi tao du lieu
                    int i32IsChotDoiGia = 0;
                    strMa_DDo = Convert.ToString(drSL3["MA_DDO"]);
                    IsGiaNhomA_N = 0;
                    if (strMa_DDo == "PA22060716348073")
                    {
                        strMa_DDo = "PA22060716348073";
                    }
                    so_ho = Convert.ToDecimal(drSL3["SO_HO"]);
                    //Kiem tra va xu ly rieng cho cac so ghi chi so ngay 01, trung ngay doi gia
                    ngay_ckymax = DateTime.Parse("01/01/1900", new System.Globalization.CultureInfo("vi-VN"));
                    if (dsCustomerData.Tables["GCS_CHISO"].Select("MA_DDO = '" + strMa_DDo + "'").Length != 0)
                    {
                        ngay_ckymax = Convert.ToDateTime(dsCustomerData.Tables["GCS_CHISO"].Compute("MAX(NGAY_CKY)", "MA_DDO = '" + strMa_DDo + "'"));
                        //Kiểm tra có chốt đổi giá không
                        if (dsCustomerData.Tables["GCS_CHISO"].Select("MA_DDO = '" + strMa_DDo + "' and LOAI_CHISO='CCS'").Length != 0)
                            i32IsChotDoiGia = 1;
                    }
                    else
                    {
                        if (dsCustomerData.Tables.Contains("GCS_CHISO_GT") == true)
                        {
                            if (dsCustomerData.Tables["GCS_CHISO_GT"].Select("MA_DDO = '" + strMa_DDo + "'").Length != 0)
                            {
                                ngay_ckymax = Convert.ToDateTime(dsCustomerData.Tables["GCS_CHISO_GT"].Compute("MAX(NGAY_CKY)", "MA_DDO = '" + strMa_DDo + "'"));
                                //Kiểm tra có chốt đổi giá không
                                if (dsCustomerData.Tables["GCS_CHISO_GT"].Select("MA_DDO = '" + strMa_DDo + "' and LOAI_CHISO='CCS'").Length != 0)
                                    i32IsChotDoiGia = 1;
                            }
                        }
                    }

                    strMa_NhomNN = Convert.ToString(drSL3["MA_NHOMNN"]);
                    strMa_NGia = Convert.ToString(drSL3["MA_NGIA"]);
                    ngay_dau = Convert.ToDateTime(drSL3["NGAY_DAU"]);
                    ngay_cuoi = Convert.ToDateTime(drSL3["NGAY_CUOI"]);
                    sl = Convert.ToDecimal(drSL3["SAN_LUONG"]);
                    ngay_hlucBT = Convert.ToDateTime(drSL3["NGAY_HLUCGIA"]);
                    IsExists = 0;
                    if (strMa_DDo == "PA04DA0014865001")
                    {
                        strMa_DDo = "PA04DA0014865001";
                    }

                    //dw = new DataView(dsStaticCatalog.Tables["D_BAC_THANG"]);
                    dwBThang.RowFilter = "MA_NHOMNN = '" + strMa_NhomNN + "' AND MA_NGIA = '" + strMa_NGia + "'";
                    //dw.Sort = "NGAY_HLUC DESC";
                    //dwBThang.Sort = "NGAY_HLUC, BTHANG_ID";

                    foreach (DataRowView drBT in dwBThang)
                    {
                        ngay_hlucBT = DateTime.Parse(Convert.ToDateTime(drBT["NGAY_HLUC"]).ToString("dd/MM/yyyy", new System.Globalization.CultureInfo("vi-VN")), new System.Globalization.CultureInfo("vi-VN"));
                        if (ngay_ckymax != DateTime.Parse("01/01/1900", new System.Globalization.CultureInfo("vi-VN")))
                        {
                            if (ngay_ckymax == ngay_hlucBT && i32IsChotDoiGia == 0)
                            {
                                //Xu ly rieng cho cac diem do co so ghi ngay trung voi ngay doi gia, và KHÔNG CHỐT
                                //se tinh toan bo vao bac thang cua bang gia cu
                                continue;
                            }
                        }

                        if (ngay_dau >= ngay_hlucBT)
                        {
                            IsExists = 1;
                            break;
                        }
                    }

                    if (IsExists == 1)
                    {
                        //Dang tinh toan bac thang       
                        //Xu ly cong to dien tu 
                        if ("|BT|CD|TD|".Contains("|" + Convert.ToString(drSL3["BCS"]) + "|") == true)
                        {
                            if (strDDo_BThang.Contains("|" + strMa_DDo + "-" + ngay_dau.ToString("dd/MM/yyyy", new System.Globalization.CultureInfo("vi-VN")) + "-" + ngay_cuoi.ToString("dd/MM/yyyy", new System.Globalization.CultureInfo("vi-VN")) + "-" + strMa_NGia + "|") == false)
                            {
                                //Xu ly cong san luong
                                rowsSL3 = dsCalculation.Tables["SL_3"].Select("MA_DDO = '" + strMa_DDo + "' AND MA_NHOMNN = '" + strMa_NhomNN + "' AND MA_NGIA = '" + strMa_NGia + "' AND BCS IN ('BT','CD','TD')");
                                slBTCDTD = 0;
                                foreach (DataRow drBTCDTD in rowsSL3)
                                {
                                    if (ngay_dau == Convert.ToDateTime(drBTCDTD["NGAY_DAU"]) && ngay_cuoi == Convert.ToDateTime(drBTCDTD["NGAY_CUOI"]) && Convert.ToDateTime(drSL3["NGAY_HLUCGIA"]) == Convert.ToDateTime(drBTCDTD["NGAY_HLUCGIA"]))
                                    {
                                        slBTCDTD = slBTCDTD + Convert.ToDecimal(drBTCDTD["SAN_LUONG"]);
                                    }
                                }
                                drSL3.Row["SAN_LUONG"] = slBTCDTD;
                                drSL3.Row["BCS"] = "BT";
                                sl = Convert.ToDecimal(drSL3.Row["SAN_LUONG"]);

                                strDDo_BThang = strDDo_BThang + strMa_DDo + "-" + ngay_dau.ToString("dd/MM/yyyy", new System.Globalization.CultureInfo("vi-VN")) + "-" + ngay_cuoi.ToString("dd/MM/yyyy", new System.Globalization.CultureInfo("vi-VN")) + "-" + strMa_NGia + "|";
                            }
                            else
                            {
                                dsCalculation.Tables["SL_3"].Rows.Remove(drSL3.Row);
                                continue;
                            }
                        }

                        //Khoi tao du lieu

                        so_ngaydmuc = Convert.ToDecimal(drSL3["SO_NGAYDMUC"]);
                        //DungNT sửa ngày 23-10-2009: Sửa công thức tính delta_dmuc
                        //if (strMa_DDo == "PP01000152588001")
                        //{
                        //    strMa_DDo = "PP01000152588001";
                        //}
                        //delta_dmuc = this.DeltaDinhMuc(ngay_dau, ngay_cuoi, so_ngaydmuc);

                        if ("|SHBB|SHBC|SHBD|SHBH|SHBL|SHBM|".Contains("|" + strMa_NhomNN + "|") == true)
                            IsExistsBB = 1;
                        else
                            IsExistsBB = 0;

                        //Tach san luong bac thang
                        dwBThangID.RowFilter = "MA_NHOMNN = '" + strMa_NhomNN + "' AND MA_NGIA = '" + strMa_NGia + "'";
                        dwBThangID.Sort = "NGAY_HLUC, BTHANG_ID";

                        //Kiem tra va so sanh san luong dinh muc so voi san luong thuc te
                        dwSL1.RowFilter = "MA_DDO = '" + strMa_DDo + "'";
                        Decimal IsChangePrice_tmp = 0, IsSumSL = 0, sl_tt = 0, sl_dmuc = 0;
                        DateTime ngay_dkymin_tmp = DateTime.Parse("01/01/1900", new System.Globalization.CultureInfo("vi-VN"));
                        DateTime ngay_ckymax_tmp = DateTime.Parse("01/01/1900", new System.Globalization.CultureInfo("vi-VN"));

                        if (dwSL1.Count > 0)
                        {
                            if (dwSL1[0]["CHANGE_PRICE"].ToString().Trim().Length > 0)
                            {
                                IsChangePrice_tmp = Convert.ToDecimal(dwSL1[0]["CHANGE_PRICE"]);
                                if (IsChangePrice_tmp > 0)
                                {
                                    //Co doi gia nha nuoc
                                    //Kiem tra va so sanh tong san luong tieu thu so voi san luong dinh muc theo bac thang
                                    //Tinh san luong thuc te
                                    sl_tt = 0;
                                    foreach (DataRowView drwSL3_1 in dwSL3)
                                    {
                                        if (strMa_DDo == Convert.ToString(drwSL3_1["MA_DDO"]) && strMa_NhomNN == Convert.ToString(drwSL3_1["MA_NHOMNN"]) && strMa_NGia == Convert.ToString(drwSL3_1["MA_NGIA"]))
                                        {
                                            sl_tt += CMISLibrary.Utility.DecimalDbnull(drwSL3_1["SAN_LUONG"]);
                                        }
                                    }

                                    //Tinh san luong dinh muc
                                    //Dũng NT sửa bỏ đoạn này, thay thế bằng đoạn lấy ngày trong SL_3
                                    //if (dsCustomerData.Tables["GCS_CHISO"].Select("MA_DDO = '" + strMa_DDo + "' AND LOAI_CHISO IN ('DDK','DDN','CSC','CCS')").Length != 0)
                                    //{
                                    //    ngay_dkymin_tmp = Convert.ToDateTime(dsCustomerData.Tables["GCS_CHISO"].Compute("MIN(NGAY_DKY)", "MA_DDO = '" + strMa_DDo + "' AND LOAI_CHISO IN ('DDK','DDN','CSC','CCS')"));
                                    //    ngay_ckymax_tmp = Convert.ToDateTime(dsCustomerData.Tables["GCS_CHISO"].Compute("MAX(NGAY_CKY)", "MA_DDO = '" + strMa_DDo + "' AND LOAI_CHISO IN ('DDK','DDN','CSC','CCS')"));
                                    //}
                                    //else
                                    //{
                                    //    if (dsCustomerData.Tables.Contains("GCS_CHISO_GT") == true)
                                    //    {
                                    //        if (dsCustomerData.Tables["GCS_CHISO_GT"].Select("MA_DDO = '" + strMa_DDo + "' AND LOAI_CHISO IN ('DDK','DDN','CSC','CCS')").Length != 0)
                                    //        {
                                    //            ngay_dkymin_tmp = Convert.ToDateTime(dsCustomerData.Tables["GCS_CHISO_GT"].Compute("MIN(NGAY_DKY)", "MA_DDO = '" + strMa_DDo + "' AND LOAI_CHISO IN ('DDK','DDN','CSC','CCS')"));
                                    //            ngay_ckymax_tmp = Convert.ToDateTime(dsCustomerData.Tables["GCS_CHISO_GT"].Compute("MAX(NGAY_CKY)", "MA_DDO = '" + strMa_DDo + "' AND LOAI_CHISO IN ('DDK','DDN','CSC','CCS')"));
                                    //        }
                                    //    }
                                    //}
                                    //Tính min max khoảng tính bậc thang theo SL_3
                                    //Bị hạn chế trong trường hợp phân khoảng: |---SHBT---|---Ko bac thang---|---SHBT---|
                                    ngay_dkymin_tmp = Convert.ToDateTime(dsCalculation.Tables["SL_3"].Compute("MIN(NGAY_DAU)", "MA_DDO = '" + strMa_DDo + "' AND MA_NHOMNN='" + strMa_NhomNN + "' and MA_NGIA='" + strMa_NGia + "'"));
                                    ngay_ckymax_tmp = Convert.ToDateTime(dsCalculation.Tables["SL_3"].Compute("MAX(NGAY_CUOI)", "MA_DDO = '" + strMa_DDo + "' AND MA_NHOMNN='" + strMa_NhomNN + "' and MA_NGIA='" + strMa_NGia + "'"));
                                    foreach (DataRowView drwBThang in dwBThangID)
                                    {
                                        if (ngay_hlucBT == Convert.ToDateTime(drwBThang["NGAY_HLUC"]))
                                        {
                                            sl_dmuc = CMISLibrary.Utility.DecimalDbnull(drwBThang["DINH_MUC"]);
                                            break;
                                        }
                                    }

                                    sl_dmuc = this.DeltaDinhMuc(ngay_dkymin_tmp, ngay_ckymax_tmp, so_ngaydmuc, sl_dmuc, so_ho, dsCustomerData, strMa_DDo);
                                    if (sl_tt <= sl_dmuc)
                                    {
                                        //Tong san luong lai, khong tinh ra bac thang nua, chi ap dung cho doi gia
                                        IsSumSL = 1;
                                    }
                                    else
                                    {
                                        IsSumSL = 0;
                                    }
                                }
                                else
                                {
                                    IsSumSL = 0;
                                }
                            }
                            else
                            {
                                IsSumSL = 0;
                            }
                        }
                        else
                        {
                            IsSumSL = 0;
                        }
                        //Dũng NT bổ sung: nếu là nhóm bậc thang của Lý Sơn thì tính tách bậc thang như bình thường
                        if ("SHVC".Contains(strMa_NhomNN)) IsSumSL = 0;
                        //dw.Sort = "BTHANG_ID";
                        foreach (DataRowView drBT in dwBThangID)
                        {
                            if (ngay_hlucBT == Convert.ToDateTime(drBT["NGAY_HLUC"]))
                            {
                                if (sl > 0)
                                {
                                    sl_dm = Convert.ToDecimal(drBT["DINH_MUC"]);
                                    if (sl_dm == 0)
                                        sl_ct = sl;
                                    else
                                    {
                                        dwSL1.RowFilter = "MA_DDO = '" + strMa_DDo + "'";
                                        Decimal IsChangePrice = 0;
                                        if (dwSL1.Count > 0)
                                        {
                                            if (dwSL1[0]["CHANGE_PRICE"].ToString().Trim().Length > 0)
                                                IsChangePrice = Convert.ToDecimal(dwSL1[0]["CHANGE_PRICE"]);
                                        }
                                        if (IsChangePrice > 0)
                                        {
                                            if (IsSumSL == 0)
                                            {
                                                //Doi gia, tinh theo dinh muc ngay                                            
                                                sl_ct = this.DeltaDinhMuc(ngay_dau, ngay_cuoi, so_ngaydmuc, sl_dm, so_ho, dsCustomerData, strMa_DDo);
                                                if (sl_ct > sl)
                                                    sl_ct = sl;
                                            }
                                            else
                                            {
                                                //Tinh toan bo cho bac thang dau tien
                                                sl_ct = sl;
                                            }
                                        }
                                        else
                                        {
                                            //Khong doi gia, tinh theo tron thang
                                            if (IsExistsBB == 0)
                                            {
                                                //sl_ct = Math.Round(Convert.ToDecimal(sl_dm * delta_dmuc * so_ho), 0, MidpointRounding.AwayFromZero);                                            
                                                sl_ct = this.DeltaDinhMuc(ngay_dau, ngay_cuoi, so_ngaydmuc, sl_dm, so_ho, dsCustomerData, strMa_DDo);
                                                if (sl_ct > sl)
                                                    sl_ct = sl;
                                            }
                                            else
                                            {
                                                if (ngay_cuoi > dtDG2019)
                                                    sl_ct = this.DeltaDinhMuc(ngay_dau, ngay_cuoi, so_ngaydmuc, sl_dm, so_ho, dsCustomerData, strMa_DDo);
                                                else
                                                    sl_ct = Math.Round(Convert.ToDecimal(sl_dm * so_ho), 0, MidpointRounding.AwayFromZero);
                                                if (sl_ct > sl)
                                                    sl_ct = sl;
                                            }
                                        }
                                    }
                                    sl = sl - sl_ct;

                                    //Day du lieu vao SL_4
                                    drSan_Luong = dsCalculation.Tables["SL_4"].NewRow();
                                    drSan_Luong["MA_DVIQLY"] = drSL3["MA_DVIQLY"];
                                    drSan_Luong["MA_DDO"] = drSL3["MA_DDO"];
                                    drSan_Luong["MA_KHANG"] = drSL3["MA_KHANG"];
                                    drSan_Luong["ID_CHISO"] = drSL3["ID_CHISO"];
                                    drSan_Luong["ID_BCS"] = drSL3["ID_BCS"];
                                    drSan_Luong["BCS"] = drSL3["BCS"];
                                    drSan_Luong["SO_CTO"] = drSL3["SO_CTO"];
                                    drSan_Luong["NGAY_DAU"] = drSL3["NGAY_DAU"];
                                    drSan_Luong["NGAY_CUOI"] = drSL3["NGAY_CUOI"];
                                    drSan_Luong["SAN_LUONG"] = drSL3["SAN_LUONG"];
                                    drSan_Luong["DTUONG_GIA"] = drSL3["DTUONG_GIA"];
                                    drSan_Luong["NGAY_HLUCGIA"] = drSL3["NGAY_HLUCGIA"];
                                    drSan_Luong["SO_NGAYDMUC"] = drSL3["SO_NGAYDMUC"];

                                    drSan_Luong["SO_THUTU"] = drSL3["SO_THUTU"];
                                    drSan_Luong["DINH_MUC"] = drSL3["DINH_MUC"];
                                    drSan_Luong["LOAI_DMUC"] = drSL3["LOAI_DMUC"];
                                    drSan_Luong["TGIAN_BDIEN"] = drSL3["TGIAN_BDIEN"];
                                    drSan_Luong["MA_NHOMNN"] = drSL3["MA_NHOMNN"];
                                    drSan_Luong["MA_NGIA"] = drSL3["MA_NGIA"];
                                    drSan_Luong["MA_NN"] = drSL3["MA_NN"];
                                    drSan_Luong["SO_HO"] = so_ho;
                                    drSan_Luong["MA_CAPDAP"] = drSL3["MA_CAPDAP"];
                                    drSan_Luong["ID_BBANAGIA"] = drSL3["ID_BBANAGIA"];
                                    drSan_Luong["KY"] = drSL3["KY"];
                                    drSan_Luong["THANG"] = drSL3["THANG"];
                                    drSan_Luong["NAM"] = drSL3["NAM"];

                                    drSan_Luong["SLCT"] = sl_ct;
                                    drSan_Luong["MA_SLCT_TC"] = Convert.ToString(strMa_NhomNN + drBT["DON_GIA"]);
                                    drSan_Luong["BTHANG_ID"] = drBT["BTHANG_ID"];
                                    drSan_Luong["DON_GIA"] = drBT["DON_GIA"];
                                    drSan_Luong["LOAI_TIEN"] = "VND";//KH USD không có bậc thang
                                    drSan_Luong["NGAY_APDUNG"] = Convert.ToDateTime(drBT["NGAY_HLUC"]);
                                    dsCalculation.Tables["SL_4"].Rows.Add(drSan_Luong);
                                }
                                else
                                    break; //Ket thuc tinh bac thang
                            }
                        }
                    }
                    else
                    {
                        //Giu nguyen, khong can tach bac thang
                        //Day du lieu vao SL_4
                        drSan_Luong = dsCalculation.Tables["SL_4"].NewRow();
                        drSan_Luong["MA_DVIQLY"] = drSL3["MA_DVIQLY"];
                        drSan_Luong["MA_DDO"] = drSL3["MA_DDO"];
                        drSan_Luong["MA_KHANG"] = drSL3["MA_KHANG"];
                        drSan_Luong["ID_CHISO"] = drSL3["ID_CHISO"];
                        drSan_Luong["ID_BCS"] = drSL3["ID_BCS"];
                        drSan_Luong["BCS"] = drSL3["BCS"];
                        drSan_Luong["SO_CTO"] = drSL3["SO_CTO"];
                        drSan_Luong["NGAY_DAU"] = drSL3["NGAY_DAU"];
                        drSan_Luong["NGAY_CUOI"] = drSL3["NGAY_CUOI"];
                        drSan_Luong["SAN_LUONG"] = drSL3["SAN_LUONG"];
                        drSan_Luong["DTUONG_GIA"] = drSL3["DTUONG_GIA"];
                        drSan_Luong["NGAY_HLUCGIA"] = drSL3["NGAY_HLUCGIA"];
                        drSan_Luong["SO_NGAYDMUC"] = drSL3["SO_NGAYDMUC"];

                        drSan_Luong["SO_THUTU"] = drSL3["SO_THUTU"];
                        drSan_Luong["DINH_MUC"] = drSL3["DINH_MUC"];
                        drSan_Luong["LOAI_DMUC"] = drSL3["LOAI_DMUC"];
                        drSan_Luong["TGIAN_BDIEN"] = drSL3["TGIAN_BDIEN"];
                        drSan_Luong["MA_NHOMNN"] = drSL3["MA_NHOMNN"];
                        drSan_Luong["MA_NGIA"] = drSL3["MA_NGIA"];
                        drSan_Luong["MA_NN"] = drSL3["MA_NN"];
                        drSan_Luong["SO_HO"] = drSL3["SO_HO"];
                        drSan_Luong["MA_CAPDAP"] = drSL3["MA_CAPDAP"];
                        drSan_Luong["ID_BBANAGIA"] = drSL3["ID_BBANAGIA"];
                        drSan_Luong["KY"] = drSL3["KY"];
                        drSan_Luong["THANG"] = drSL3["THANG"];
                        drSan_Luong["NAM"] = drSL3["NAM"];

                        drSan_Luong["SLCT"] = drSL3["SAN_LUONG"];
                        drSan_Luong["MA_SLCT_TC"] = strMa_NhomNN;
                        drSan_Luong["BTHANG_ID"] = 0;
                        drSan_Luong["DON_GIA"] = 0;
                        drSan_Luong["NGAY_APDUNG"] = DateTime.Parse("01/01/1900", new System.Globalization.CultureInfo("vi-VN"));
                        dsCalculation.Tables["SL_4"].Rows.Add(drSan_Luong);
                    }
                }
                //dsCalculation.Tables["SL_4"].AcceptChanges();

                return "";
            }
            catch (Exception ex)
            {
                return "PriceSpecification_1: " + ex.Message;
            }
            //finally
            //{
            //    dsCalculation.AcceptChanges();
            //}
        }

        private decimal SoNgayKSD(DateTime ngaydau, DateTime ngaycuoi, DataSet dsCustomerData, string strMa_DDo)
        {
            try
            {
                decimal songaytru = 0;
                DateTime mindky = DateTime.Parse("01/01/3000", new System.Globalization.CultureInfo("vi-VN"));
                DateTime maxcky = DateTime.Parse("01/01/1900", new System.Globalization.CultureInfo("vi-VN"));
                //Tinh lai so ngay dung dien trong khoang (ngaydau, ngaycuoi)
                //Tim max ngay cuoi ky <= ngaydau; tim min ngay dau ky >= ngaycuoi
                DataRow[] rows;
                DataTable dt = dsCustomerData.Tables["GCS_CHISO"].Clone();
                rows = dsCustomerData.Tables["GCS_CHISO"].Select("MA_DDO = '" + strMa_DDo + "'");
                if (rows.Length == 0)
                {
                    rows = dsCustomerData.Tables["GCS_CHISO_GT"].Select("MA_DDO = '" + strMa_DDo + "'");
                }
                if (rows.Length == 0)
                {
                    //Khong co trong chi so thi thoi, khong kiem tra
                    songaytru = 0;
                }
                else
                {
                    //Tinh so ngay khong su dung dien
                    foreach (DataRow dr in rows)
                    {
                        if (ngaydau >= Convert.ToDateTime(dr["NGAY_CKY"]))
                        {
                            if (Convert.ToDateTime(dr["NGAY_CKY"]) > maxcky)
                            {
                                maxcky = Convert.ToDateTime(dr["NGAY_CKY"]);
                            }
                        }
                        if (ngaycuoi <= Convert.ToDateTime(dr["NGAY_DKY"]))
                        {
                            if (Convert.ToDateTime(dr["NGAY_DKY"]) < mindky)
                            {
                                mindky = Convert.ToDateTime(dr["NGAY_DKY"]);
                            }
                        }
                    }
                    if (maxcky == DateTime.Parse("01/01/1900", new System.Globalization.CultureInfo("vi-VN")))
                    {
                        //Khong tim thay max ngay cuoi ky
                        maxcky = ngaydau;
                    }
                    if (mindky == DateTime.Parse("01/01/3000", new System.Globalization.CultureInfo("vi-VN")))
                    {
                        //Khong tim thay min ngay dau ky
                        mindky = ngaycuoi;
                    }
                    foreach (DataRow dr in rows)
                    {
                        if (Convert.ToDateTime(dr["NGAY_DKY"]) >= maxcky && Convert.ToDateTime(dr["NGAY_CKY"]) >= maxcky && Convert.ToDateTime(dr["NGAY_DKY"]) <= mindky && Convert.ToDateTime(dr["NGAY_CKY"]) <= mindky && (Convert.ToString(dr["LOAI_CHISO"]) == "DDN" || Convert.ToString(dr["LOAI_CHISO"]) == "DUP") && (Convert.ToString(dr["BCS"]) == "BT" || Convert.ToString(dr["BCS"]) == "KT"))
                        {
                            DataRow dr1 = dt.NewRow();
                            dr1.ItemArray = dr.ItemArray;
                            dt.Rows.Add(dr1);
                        }
                    }

                    //Tinh so ngay khong su dung dien
                    if (dt.Rows.Count == 0 || dt.Rows.Count == 1)
                    {
                        //Chi co 1 ban ghi
                        songaytru = 0;
                    }
                    else
                    {
                        DataView dw = new DataView(dt);
                        DateTime ngayDDN = DateTime.Parse("01/01/1900", new System.Globalization.CultureInfo("vi-VN"));
                        dw.Sort = "MA_DVIQLY, ID_CHISO";
                        string strLoai_ChiSo = "";
                        foreach (DataRowView drw in dw)
                        {
                            //Kiem tra so lieu
                            if (strLoai_ChiSo == "")
                            {
                                strLoai_ChiSo = Convert.ToString(drw["LOAI_CHISO"]);
                            }
                            else
                            {
                                if (strLoai_ChiSo == "DUP")
                                {
                                    //La DUP
                                    if (Convert.ToString(drw["LOAI_CHISO"]) == "DUP")
                                    {
                                        //Loi so lieu, thoat luon
                                        return 0;
                                    }
                                    else
                                    {
                                        strLoai_ChiSo = Convert.ToString(drw["LOAI_CHISO"]);
                                    }
                                }
                                else
                                {
                                    //La DDN
                                    if (Convert.ToString(drw["LOAI_CHISO"]) == "DDN")
                                    {
                                        //Loi so lieu, thoat luon
                                        return 0;
                                    }
                                    else
                                    {
                                        strLoai_ChiSo = Convert.ToString(drw["LOAI_CHISO"]);
                                    }
                                }
                            }

                            if (strLoai_ChiSo == "DDN")
                            {
                                //DDN
                                ngayDDN = Convert.ToDateTime(drw["NGAY_CKY"]);
                            }
                            else
                            {
                                //DUP
                                if (ngayDDN == DateTime.Parse("01/01/1900", new System.Globalization.CultureInfo("vi-VN")))
                                {
                                    continue;
                                }
                                else
                                {
                                    if (ngayDDN >= Convert.ToDateTime(drw["NGAY_CKY"]))
                                    {
                                        continue;
                                    }
                                    else
                                    {
                                        songaytru = songaytru + Convert.ToDecimal(Convert.ToDateTime(drw["NGAY_CKY"]).Subtract(ngayDDN).TotalDays - 1);
                                    }
                                }
                            }
                        }
                    }
                }

                if (ngaycuoi.AddDays(-Convert.ToDouble(songaytru)) < ngaydau)
                {
                    return 0;
                }
                else
                {
                    return songaytru;
                }
            }
            catch
            {
                return 0;
            }
        }

        //Tinh delta dinh muc bac thang tong quat
        private decimal DeltaDinhMuc(DateTime ngaydau, DateTime ngaycuoi, decimal songaydmuc, decimal sldm, decimal soho, DataSet dsCustomerData, string strMa_DDo)
        {
            try
            {
                if (songaydmuc <= 0)
                {
                    return 0;
                }
                else
                {
                    if (ngaydau > ngaycuoi)
                    {
                        return 0;
                    }
                    else
                    {
                        if (ngaydau == ngaycuoi)
                        {
                            //return Convert.ToDecimal(1 / songaydmuc);
                            return Math.Round(1 * sldm * soho / songaydmuc, 0, MidpointRounding.AwayFromZero);
                        }
                        else
                        {
                            decimal songaytru = this.SoNgayKSD(ngaydau, ngaycuoi, dsCustomerData, strMa_DDo);
                            ngaycuoi = ngaycuoi.AddDays(-Convert.ToDouble(songaytru));
                            if (ngaydau == ngaycuoi)
                            {
                                return Math.Round(1 * sldm * soho / songaydmuc, 0, MidpointRounding.AwayFromZero);
                            }

                            //Xu ly cho cac truong hop thong thuong
                            DateTime ngay1 = ngaydau, ngay2, ngay3 = ngaydau, ngay4;
                            decimal i = 0, delta = 0;
                            Int32 IsExists = 0, songay = 0;
                            while (ngay1 < ngaycuoi)
                            {
                                ngay2 = ngay1.AddMonths(1);
                                if (ngay2 <= ngaycuoi)
                                {
                                    i += 1;
                                    ngay3 = ngay2;
                                    songay = System.DateTime.DaysInMonth(ngay1.Year, ngay1.Month);
                                    ngay4 = ngay2.AddDays(Convert.ToDouble(-songay));
                                    //15/02/2019
                                    //if (ngay4 < ngay1) -- Bổ sung thêm điều kiện số vòng lặp > 1
                                    //Để tránh tình huống ngay1.AddMonths(1) < số ngày định mức (ví dụ từ 30/01 đến 28/02)
                                    if (ngay4 < ngay1 && i > 1)
                                    {
                                        IsExists = 1;
                                    }
                                }
                                else
                                {
                                    ngay3 = ngay1;
                                }
                                ngay1 = ngay2;
                            }
                            if (ngay3 == ngaycuoi)
                            {
                                if (IsExists == 0)
                                {
                                    //delta = i + Convert.ToDecimal(ngaycuoi.Subtract(ngay3).TotalDays + 1) / songaydmuc;
                                    delta = Math.Round(i * sldm * soho + Convert.ToDecimal(ngaycuoi.Subtract(ngay3).TotalDays + 1) * sldm * soho / songaydmuc, 0, MidpointRounding.AwayFromZero);
                                }
                                else
                                {
                                    delta = Math.Round(i * sldm * soho, 0, MidpointRounding.AwayFromZero);
                                }
                            }
                            else
                            {
                                if (ngay3 < ngaycuoi)
                                {
                                    if (IsExists == 0)
                                    {
                                        delta = Math.Round(i * sldm * soho + Convert.ToDecimal(ngaycuoi.Subtract(ngay3).TotalDays + 1) * sldm * soho / songaydmuc, 0, MidpointRounding.AwayFromZero);
                                    }
                                    else
                                    {
                                        delta = Math.Round(i * sldm * soho + Convert.ToDecimal(ngaycuoi.Subtract(ngay3).TotalDays) * sldm * soho / songaydmuc, 0, MidpointRounding.AwayFromZero);
                                    }
                                }
                                else
                                {
                                    delta = 0;
                                }
                            }
                            return delta;
                        }
                    }
                }
            }
            catch
            {
                return 0;
            }
        }

        //Xac dinh don gia cua cac doi tuong khong phai bac thang (co BTHANG_ID = 0)
        public string PriceSpecification_2(DS_CALCULATIONTABLES dsCalculation, DataSet dsStaticCatalog)
        {
            try
            {
                DataView dwSL4, dwTChieuDA, dwGiaNhomNN;
                string strMa_NhomNN = "", strMa_CapDAp = "", strTGian_BDien = "", strMa_NGia = "", strKhoang_DAp = "";
                Decimal Don_Gia = 0;
                string strLoaiTien = "VND";
                DateTime ngay_dau, ngay_cuoi, ngay_hluc;

                //Xac dinh don gia cua cac doi tuong khong phai bac thang (co BTHANG_ID = 0)
                //Update du lieu vao bang SL_4
                if (dsCalculation == null)
                    return "dsCalculationIsNull";
                else
                    if (dsCalculation.Tables.Contains("SL_4") == false)
                    return "NotExistsSL_4";

                if (dsStaticCatalog == null)
                    return "dsStaticCatalogIsNull";
                else
                    if (dsStaticCatalog.Tables.Contains("D_GIA_NHOMNN") == false)
                    return "NotExistsD_GIA_NHOMNN";
                else
                        if (dsStaticCatalog.Tables.Contains("D_THAMCHIEU_CAPDA") == false)
                    return "NotExistsD_THAMCHIEU_CAPDA";

                dwSL4 = new DataView(dsCalculation.Tables["SL_4"]);
                dwSL4.RowFilter = "BTHANG_ID = 0";
                dwSL4.Sort = "MA_NHOMNN, MA_CAPDAP, TGIAN_BDIEN, MA_NGIA";

                dwTChieuDA = new DataView(dsStaticCatalog.Tables["D_THAMCHIEU_CAPDA"]);
                dwTChieuDA.RowFilter = "MA_NHOMNN = 'SHBT'";
                dwTChieuDA.Sort = "NGAY_ADUNG DESC";

                dwGiaNhomNN = new DataView(dsStaticCatalog.Tables["D_GIA_NHOMNN"]);
                dwGiaNhomNN.RowFilter = "MA_NHOMNN = 'SHBT'";
                dwGiaNhomNN.Sort = "NGAY_ADUNG DESC";

                strMa_NhomNN = "";
                strMa_CapDAp = "";
                strTGian_BDien = "";
                strMa_NGia = "";
                strKhoang_DAp = "";
                Don_Gia = 0;
                ngay_hluc = DateTime.Parse("01/01/1900", new System.Globalization.CultureInfo("vi-VN"));

                foreach (DataRowView drSL4 in dwSL4)
                {
                    if (Convert.ToString(drSL4["MA_DDO"]) == "PD03000017619006")
                    {
                        drSL4["MA_DDO"] = "PD03000017619006";
                    }
                    if (strMa_NhomNN != Convert.ToString(drSL4["MA_NHOMNN"]) || strMa_CapDAp != Convert.ToString(drSL4["MA_CAPDAP"]) || strTGian_BDien != Convert.ToString(drSL4["TGIAN_BDIEN"]) || strMa_NGia != Convert.ToString(drSL4["MA_NGIA"]))
                    {
                        strMa_NhomNN = Convert.ToString(drSL4["MA_NHOMNN"]);
                        strMa_CapDAp = Convert.ToString(drSL4["MA_CAPDAP"]);
                        strTGian_BDien = Convert.ToString(drSL4["TGIAN_BDIEN"]);
                        strMa_NGia = Convert.ToString(drSL4["MA_NGIA"]);
                        ngay_dau = Convert.ToDateTime(drSL4["NGAY_DAU"]);
                        ngay_cuoi = Convert.ToDateTime(drSL4["NGAY_CUOI"]);
                        strKhoang_DAp = "";
                        Don_Gia = 0;
                        ngay_hluc = DateTime.Parse("01/01/1900", new System.Globalization.CultureInfo("vi-VN"));

                        //Xac dinh khoang dien ap
                        //dwGia = new DataView(dsStaticCatalog.Tables["D_THAMCHIEU_CAPDA"]);
                        dwTChieuDA.RowFilter = "MA_NHOMNN = '" + strMa_NhomNN + "' AND MA_CAPDA = '" + strMa_CapDAp + "'";
                        //dwGia.Sort = "NGAY_ADUNG DESC";

                        foreach (DataRowView drCapDA in dwTChieuDA)
                        {
                            ngay_hluc = DateTime.Parse(Convert.ToDateTime(drCapDA["NGAY_ADUNG"]).ToString("dd/MM/yyyy", new System.Globalization.CultureInfo("vi-VN")), new System.Globalization.CultureInfo("vi-VN"));
                            if (ngay_cuoi >= ngay_hluc)
                            {
                                strKhoang_DAp = Convert.ToString(drCapDA["KHOANG_DA"]);
                                break;
                            }
                        }

                        //Xac dinh don gia
                        if (strKhoang_DAp != "")
                        {
                            //dwGia = new DataView(dsStaticCatalog.Tables["D_GIA_NHOMNN"]);
                            dwGiaNhomNN.RowFilter = "MA_NHOMNN = '" + strMa_NhomNN + "' AND KHOANG_DA = '" + strKhoang_DAp + "' AND THOIGIAN_BDIEN = '" + strTGian_BDien + "' AND MA_NGIA = '" + strMa_NGia + "'";
                            //dwGia.Sort = "NGAY_ADUNG DESC";

                            foreach (DataRowView drGia in dwGiaNhomNN)
                            {
                                ngay_hluc = DateTime.Parse(Convert.ToDateTime(drGia["NGAY_ADUNG"]).ToString("dd/MM/yyyy", new System.Globalization.CultureInfo("vi-VN")), new System.Globalization.CultureInfo("vi-VN"));
                                if (ngay_cuoi >= ngay_hluc)
                                {
                                    Don_Gia = Convert.ToDecimal(drGia["DON_GIA"]);
                                    strLoaiTien = drGia["LOAI_TIEN"].ToString();
                                    break;
                                }
                            }
                        }
                    }

                    //Cap nhat don gia
                    drSL4.Row["DON_GIA"] = Don_Gia;
                    drSL4.Row["LOAI_TIEN"] = strLoaiTien;
                    if (Don_Gia == 0)
                        drSL4.Row["NGAY_APDUNG"] = DateTime.Parse("01/01/1900", new System.Globalization.CultureInfo("vi-VN"));
                    else
                        drSL4.Row["NGAY_APDUNG"] = ngay_hluc;
                }
                //dsCalculation.Tables["SL_4"].AcceptChanges();
                return "";
            }
            catch (Exception ex)
            {
                return "PriceSpecification_2: " + ex.Message;
            }
            //finally
            //{
            //    dsCalculation.AcceptChanges();
            //}
        }

        //Xac dinh don gia cua cac doi tuong khong phai bac thang (co BTHANG_ID = 0)
        //Su dung trong ky doi gia
        public string PriceSpecification_21(DS_CALCULATIONTABLES dsCalculation, DataSet dsStaticCatalog, DataSet dsCustomerData)
        {
            try
            {
                DataView dwSL4, dwTChieuDA, dwGiaNhomNN;
                string strMa_NhomNN = "", strMa_CapDAp = "", strTGian_BDien = "", strMa_NGia = "", strKhoang_DAp = "";
                Decimal Don_Gia = 0;
                string strLoaiTien = "VND", strMa_DDo = "";
                DateTime ngay_dau, ngay_cuoi, ngay_hluc, ngay_ckymax;
                int i32IsChotDoiGia = 0;
                //Xac dinh don gia cua cac doi tuong khong phai bac thang (co BTHANG_ID = 0)
                //Update du lieu vao bang SL_4
                if (dsCalculation == null)
                    return "dsCalculationIsNull";
                else
                    if (dsCalculation.Tables.Contains("SL_4") == false)
                    return "NotExistsSL_4";

                if (dsStaticCatalog == null)
                    return "dsStaticCatalogIsNull";
                else
                    if (dsStaticCatalog.Tables.Contains("D_GIA_NHOMNN") == false)
                    return "NotExistsD_GIA_NHOMNN";
                else
                        if (dsStaticCatalog.Tables.Contains("D_THAMCHIEU_CAPDA") == false)
                    return "NotExistsD_THAMCHIEU_CAPDA";

                dwSL4 = new DataView(dsCalculation.Tables["SL_4"]);
                dwSL4.RowFilter = "BTHANG_ID = 0";
                dwSL4.Sort = "MA_NHOMNN, MA_CAPDAP, TGIAN_BDIEN, MA_NGIA";

                dwTChieuDA = new DataView(dsStaticCatalog.Tables["D_THAMCHIEU_CAPDA"]);
                dwTChieuDA.RowFilter = "MA_NHOMNN = 'SHBT'";
                dwTChieuDA.Sort = "NGAY_ADUNG DESC";

                dwGiaNhomNN = new DataView(dsStaticCatalog.Tables["D_GIA_NHOMNN"]);
                dwGiaNhomNN.RowFilter = "MA_NHOMNN = 'SHBT'";
                dwGiaNhomNN.Sort = "NGAY_ADUNG DESC";

                strMa_NhomNN = "";
                strMa_CapDAp = "";
                strTGian_BDien = "";
                strMa_NGia = "";
                strKhoang_DAp = "";
                Don_Gia = 0;
                ngay_hluc = DateTime.Parse("01/01/1900", new System.Globalization.CultureInfo("vi-VN"));

                foreach (DataRowView drSL4 in dwSL4)
                {
                    i32IsChotDoiGia = 0;
                    strMa_DDo = Convert.ToString(drSL4["MA_DDO"]);
                    //Kiem tra va xu ly rieng cho cac so ghi chi so ngay 01, trung ngay doi gia
                    ngay_ckymax = DateTime.Parse("01/01/1900", new System.Globalization.CultureInfo("vi-VN"));
                    if (dsCustomerData.Tables["GCS_CHISO"].Select("MA_DDO = '" + strMa_DDo + "'").Length != 0)
                    {
                        ngay_ckymax = Convert.ToDateTime(dsCustomerData.Tables["GCS_CHISO"].Compute("MAX(NGAY_CKY)", "MA_DDO = '" + strMa_DDo + "'"));
                        //Kiểm tra có chốt đổi giá không
                        if (dsCustomerData.Tables["GCS_CHISO"].Select("MA_DDO = '" + strMa_DDo + "' and LOAI_CHISO='CCS'").Length != 0)
                            i32IsChotDoiGia = 1;
                    }
                    else
                    {
                        if (dsCustomerData.Tables.Contains("GCS_CHISO_GT") == true)
                        {
                            if (dsCustomerData.Tables["GCS_CHISO_GT"].Select("MA_DDO = '" + strMa_DDo + "'").Length != 0)
                            {
                                ngay_ckymax = Convert.ToDateTime(dsCustomerData.Tables["GCS_CHISO_GT"].Compute("MAX(NGAY_CKY)", "MA_DDO = '" + strMa_DDo + "'"));
                                //Kiểm tra có chốt đổi giá không
                                if (dsCustomerData.Tables["GCS_CHISO_GT"].Select("MA_DDO = '" + strMa_DDo + "' and LOAI_CHISO='CCS'").Length != 0)
                                    i32IsChotDoiGia = 1;
                            }
                        }
                    }

                    if (Convert.ToString(drSL4["MA_DDO"]) == "PC11AA0036241001")
                    {
                        drSL4["MA_DDO"] = "PC11AA0036241001";
                    }

                    //if (strMa_NhomNN != Convert.ToString(drSL4["MA_NHOMNN"]) || strMa_CapDAp != Convert.ToString(drSL4["MA_CAPDAP"]) || strTGian_BDien != Convert.ToString(drSL4["TGIAN_BDIEN"]) || strMa_NGia != Convert.ToString(drSL4["MA_NGIA"]))
                    //{
                    strMa_NhomNN = Convert.ToString(drSL4["MA_NHOMNN"]);
                    strMa_CapDAp = Convert.ToString(drSL4["MA_CAPDAP"]);
                    strTGian_BDien = Convert.ToString(drSL4["TGIAN_BDIEN"]);
                    strMa_NGia = Convert.ToString(drSL4["MA_NGIA"]);
                    ngay_dau = Convert.ToDateTime(drSL4["NGAY_DAU"]);
                    ngay_cuoi = Convert.ToDateTime(drSL4["NGAY_CUOI"]);
                    strKhoang_DAp = "";
                    Don_Gia = 0;
                    ngay_hluc = DateTime.Parse("01/01/1900", new System.Globalization.CultureInfo("vi-VN"));

                    //Xac dinh khoang dien ap
                    dwTChieuDA.RowFilter = "MA_NHOMNN = '" + strMa_NhomNN + "' AND MA_CAPDA = '" + strMa_CapDAp + "'";

                    foreach (DataRowView drCapDA in dwTChieuDA)
                    {
                        ngay_hluc = DateTime.Parse(Convert.ToDateTime(drCapDA["NGAY_ADUNG"]).ToString("dd/MM/yyyy", new System.Globalization.CultureInfo("vi-VN")), new System.Globalization.CultureInfo("vi-VN"));
                        if (ngay_ckymax != DateTime.Parse("01/01/1900", new System.Globalization.CultureInfo("vi-VN")))
                        {
                            if (ngay_ckymax == ngay_hluc && i32IsChotDoiGia == 0)
                            {
                                //Xu ly rieng cho cac diem do co so ghi ngay trung voi ngay doi gia, và KHÔNG CHỐT
                                //se tinh toan bo vao bac thang cua bang gia cu
                                continue;
                            }
                        }
                        if (ngay_cuoi >= ngay_hluc)
                        {
                            strKhoang_DAp = Convert.ToString(drCapDA["KHOANG_DA"]);
                            break;
                        }
                    }

                    //Xac dinh don gia
                    if (strKhoang_DAp != "")
                    {
                        dwGiaNhomNN.RowFilter = "MA_NHOMNN = '" + strMa_NhomNN + "' AND KHOANG_DA = '" + strKhoang_DAp + "' AND THOIGIAN_BDIEN = '" + strTGian_BDien + "' AND MA_NGIA = '" + strMa_NGia + "'";

                        foreach (DataRowView drGia in dwGiaNhomNN)
                        {
                            ngay_hluc = DateTime.Parse(Convert.ToDateTime(drGia["NGAY_ADUNG"]).ToString("dd/MM/yyyy", new System.Globalization.CultureInfo("vi-VN")), new System.Globalization.CultureInfo("vi-VN"));
                            if (ngay_ckymax != DateTime.Parse("01/01/1900", new System.Globalization.CultureInfo("vi-VN")))
                            {
                                if (ngay_ckymax == ngay_hluc && i32IsChotDoiGia == 0)
                                {
                                    //Xu ly rieng cho cac diem do co so ghi ngay trung voi ngay doi gia,
                                    //se tinh toan bo vao bac thang cua bang gia cu
                                    continue;
                                }
                            }
                            if (ngay_cuoi >= ngay_hluc)
                            {
                                Don_Gia = Convert.ToDecimal(drGia["DON_GIA"]);
                                strLoaiTien = drGia["LOAI_TIEN"].ToString();
                                break;
                            }
                        }
                    }
                    //}

                    //Cap nhat don gia
                    drSL4.Row["DON_GIA"] = Don_Gia;
                    drSL4.Row["LOAI_TIEN"] = strLoaiTien;
                    if (Don_Gia == 0)
                        drSL4.Row["NGAY_APDUNG"] = DateTime.Parse("01/01/1900", new System.Globalization.CultureInfo("vi-VN"));
                    else
                        drSL4.Row["NGAY_APDUNG"] = ngay_hluc;
                }
                return "";
            }
            catch (Exception ex)
            {
                return "PriceSpecification_2: " + ex.Message;
            }
        }

        public string PriceSpecification_3(DS_CALCULATIONTABLES dsCalculation, DataSet dsCustomerData, DataSet dsInvoiceData, string strTen_DNhap, string strNgayGhi, short ky, short thang, short nam)
        {
            try
            {
                DataRow drCT;
                DataView dwDDoSoGCS, dwDDoSoGCS_GT, dwVTriDDo, dwVTriDDo_GT, dwKHang, dwDDo, dwDDo_GT, dwVC;

                //Tinh chi tiet hoa don
                //Update du lieu vao bang HDN_HDONCTIET
                if (dsCalculation == null)
                    return "dsCalculationIsNull";
                else
                    if (dsCalculation.Tables.Contains("SL_1") == false)
                    return "NotExistsSL_1";
                else
                        if (dsCalculation.Tables.Contains("SL_4") == false)
                    return "NotExistsSL_4";

                if (dsCustomerData == null)
                    return "dsCustomerDataIsNull";
                else
                    if (dsCustomerData.Tables.Contains("HDG_DDO_SOGCS") == false)
                    return "NotExistsHDG_DDO_SOGCS";
                else
                        if (dsCustomerData.Tables.Contains("HDG_VITRI_DDO") == false)
                    return "NotExistsHDG_VITRI_DDO";

                if (dsInvoiceData == null)
                    return "dsInvoiceDataIsNull";
                else
                    if (dsInvoiceData.Tables.Contains("HDN_HDONCTIET") == false)
                    return "NotExistsHDN_HDONCTIET";
                //Bổ sung đoạn kiểm tra GCS_LICH_DCN
                int i32DoiLichGCS = 0;
                DateTime dtNgayGhiCu = new DateTime(1900, 1, 1);
                DateTime dt1900 = new DateTime(1900, 1, 1);
                if (dsCustomerData.Tables.Contains("GCS_LICH_DCN") && dsCustomerData.Tables["GCS_LICH_DCN"].Rows.Count > 0)
                {
                    short i16Ky = Convert.ToInt16(dsCustomerData.Tables["GCS_CHISO"].Rows[0]["KY"]);
                    short i16Thang = Convert.ToInt16(dsCustomerData.Tables["GCS_CHISO"].Rows[0]["THANG"]);
                    short i16Nam = Convert.ToInt16(dsCustomerData.Tables["GCS_CHISO"].Rows[0]["NAM"]);
                    i32DoiLichGCS = dsCustomerData.Tables["GCS_LICH_DCN"].Rows.Count;
                    DataRow[] arrRowDCN = dsCustomerData.Tables["GCS_LICH_DCN"].Select("THANG='" + i16Thang.ToString().Trim() + "' and KY='" + i16Ky.ToString().Trim() + "'");
                    if ((i32DoiLichGCS==2 && arrRowDCN.Length > 0 && i16Thang < (short)12) || (i32DoiLichGCS==1 && arrRowDCN.Length > 0 && i16Thang == (short)12))
                    {
                        //Tháng thay đổi
                        dtNgayGhiCu = new DateTime(Convert.ToInt32(i16Nam), Convert.ToInt32(i16Thang), Convert.ToInt32(arrRowDCN[0]["NGAY_GHI"]));
                    }
                }
                //Khoi tao truoc cac dataview
                dwDDoSoGCS = new DataView(dsCustomerData.Tables["HDG_DDO_SOGCS"]);
                if (dwDDoSoGCS.Count != 0)
                {
                    dwDDoSoGCS.RowFilter = "MA_DVIQLY = '" + Convert.ToString(dwDDoSoGCS[0]["MA_DVIQLY"]) + "'";
                }
                dwDDoSoGCS.Sort = "NGAY_HLUC DESC";
                if (dsCustomerData.Tables.Contains("HDG_DDO_SOGCS_GT") == true)
                {
                    dwDDoSoGCS_GT = new DataView(dsCustomerData.Tables["HDG_DDO_SOGCS_GT"]);
                    if (dwDDoSoGCS_GT.Count != 0)
                    {
                        dwDDoSoGCS_GT.RowFilter = "MA_DVIQLY = '" + Convert.ToString(dwDDoSoGCS_GT[0]["MA_DVIQLY"]) + "'";
                    }
                    dwDDoSoGCS_GT.Sort = "NGAY_HLUC DESC";
                }
                else
                {
                    dwDDoSoGCS_GT = null;
                }

                dwVTriDDo = new DataView(dsCustomerData.Tables["HDG_VITRI_DDO"]);
                if (dwVTriDDo.Count != 0)
                {
                    dwVTriDDo.RowFilter = "MA_DVIQLY = '" + Convert.ToString(dwVTriDDo[0]["MA_DVIQLY"]) + "'";
                }
                dwVTriDDo.Sort = "NGAY_HLUC DESC";

                if (dsCustomerData.Tables.Contains("HDG_VITRI_DDO_GT") == true)
                {
                    dwVTriDDo_GT = new DataView(dsCustomerData.Tables["HDG_VITRI_DDO_GT"]);
                    if (dwVTriDDo_GT.Count != 0)
                    {
                        dwVTriDDo_GT.RowFilter = "MA_DVIQLY = '" + Convert.ToString(dwVTriDDo_GT[0]["MA_DVIQLY"]) + "'";
                    }
                    dwVTriDDo_GT.Sort = "NGAY_HLUC DESC";
                }
                else
                {
                    dwVTriDDo_GT = null;
                }

                dwKHang = new DataView(dsCustomerData.Tables["HDG_KHACH_HANG"]);
                if (dwKHang.Count != 0)
                {
                    dwKHang.RowFilter = "MA_DVIQLY = '" + Convert.ToString(dwKHang[0]["MA_DVIQLY"]) + "'";
                }
                dwKHang.Sort = "NGAY_HLUC DESC";

                dwDDo = new DataView(dsCustomerData.Tables["HDG_DIEM_DO"]);
                if (dwDDo.Count != 0)
                {
                    dwDDo.RowFilter = "MA_DVIQLY = '" + Convert.ToString(dwDDo[0]["MA_DVIQLY"]) + "'";
                }
                dwDDo.Sort = "NGAY_HLUC DESC";
                if (dsCustomerData.Tables.Contains("HDG_DIEM_DO_GT") == true)
                {
                    dwDDo_GT = new DataView(dsCustomerData.Tables["HDG_DIEM_DO_GT"]);
                    if (dwDDo_GT.Count != 0)
                    {
                        dwDDo_GT.RowFilter = "MA_DVIQLY = '" + Convert.ToString(dwDDo_GT[0]["MA_DVIQLY"]) + "'";
                    }
                    dwDDo_GT.Sort = "NGAY_HLUC DESC";
                }
                else
                {
                    dwDDo_GT = null;
                }

                //Day du lieu cua cac dong BT, CD, TD, KT
                foreach (DataRow drSL4 in dsCalculation.Tables["SL_4"].Rows)
                {
                    drCT = dsInvoiceData.Tables["HDN_HDONCTIET"].NewRow();
                    drCT["MA_DVIQLY"] = drSL4["MA_DVIQLY"];
                    drCT["ID_HDONCTIET"] = 1;
                    drCT["ID_HDON"] = 1;
                    drCT["MA_KHANG"] = drSL4["MA_KHANG"];
                    drCT["MA_DDO"] = drSL4["MA_DDO"];

                    //dw = new DataView(dsCustomerData.Tables["HDG_DDO_SOGCS"]);
                    dwDDoSoGCS.RowFilter = "MA_DDO = '" + Convert.ToString(drSL4["MA_DDO"]) + "'";
                    //dw.Sort = "NGAY_HLUC DESC";
                    if (dwDDoSoGCS.Count == 0)
                    {
                        if (dsCustomerData.Tables.Contains("HDG_DDO_SOGCS_GT") == true)
                        {
                            //dw = new DataView(dsCustomerData.Tables["HDG_DDO_SOGCS_GT"]);
                            dwDDoSoGCS_GT.RowFilter = "MA_DDO = '" + Convert.ToString(drSL4["MA_DDO"]) + "'";
                            //dw.Sort = "NGAY_HLUC DESC";
                            if (dwDDoSoGCS_GT.Count == 0)
                            {
                                drCT["MA_SOGCS"] = "";
                                drCT["MA_KVUC"] = "";
                                drCT["STT"] = "";
                            }
                            else
                            {
                                drCT["MA_SOGCS"] = Convert.ToString(dwDDoSoGCS_GT[0]["MA_SOGCS"]);
                                drCT["MA_KVUC"] = Convert.ToString(dwDDoSoGCS_GT[0]["MA_KVUC"]);
                                drCT["STT"] = Convert.ToString(dwDDoSoGCS_GT[0]["STT"]);
                            }
                        }
                        else
                        {
                            drCT["MA_SOGCS"] = "";
                            drCT["MA_KVUC"] = "";
                            drCT["STT"] = "";
                        }
                    }
                    else
                    {
                        drCT["MA_SOGCS"] = Convert.ToString(dwDDoSoGCS[0]["MA_SOGCS"]);
                        drCT["MA_KVUC"] = Convert.ToString(dwDDoSoGCS[0]["MA_KVUC"]);
                        drCT["STT"] = Convert.ToString(dwDDoSoGCS[0]["STT"]);
                    }

                    drCT["KY"] = drSL4["KY"];
                    drCT["THANG"] = drSL4["THANG"];
                    drCT["NAM"] = drSL4["NAM"];
                    drCT["DIEN_TTHU"] = drSL4["SLCT"];
                    drCT["DON_GIA"] = drSL4["DON_GIA"];
                    if (drSL4["LOAI_TIEN"].ToString().Trim() == "VND")
                        drCT["SO_TIEN"] = Math.Round(Convert.ToDecimal(drSL4["SLCT"]) * Convert.ToDecimal(drSL4["DON_GIA"]), 0, MidpointRounding.AwayFromZero);
                    else
                        drCT["SO_TIEN"] = Math.Round(Convert.ToDecimal(drSL4["SLCT"]) * Convert.ToDecimal(drSL4["DON_GIA"]), 2, MidpointRounding.AwayFromZero);
                    drCT["MA_NHOMNN"] = drSL4["MA_NHOMNN"];
                    drCT["MA_NN"] = drSL4["MA_NN"];
                    drCT["MA_NGIA"] = drSL4["MA_NGIA"];
                    drCT["DINH_MUC"] = drSL4["DINH_MUC"];

                    drCT["LOAI_DMUC"] = drSL4["LOAI_DMUC"];
                    drCT["BCS"] = drSL4["BCS"];
                    drCT["TGIAN_BDIEN"] = drSL4["TGIAN_BDIEN"];

                    //dw = new DataView(dsCustomerData.Tables["HDG_VITRI_DDO"]);
                    dwVTriDDo.RowFilter = "MA_DDO = '" + Convert.ToString(drSL4["MA_DDO"]) + "'";
                    //dw.Sort = "NGAY_HLUC DESC";
                    if (dwVTriDDo.Count == 0)
                    {
                        if (dsCustomerData.Tables.Contains("HDG_VITRI_DDO_GT") == true)
                        {
                            //dw = new DataView(dsCustomerData.Tables["HDG_VITRI_DDO_GT"]);
                            dwVTriDDo_GT.RowFilter = "MA_DDO = '" + Convert.ToString(drSL4["MA_DDO"]) + "'";
                            //dw.Sort = "NGAY_HLUC DESC";
                            if (dwVTriDDo_GT.Count == 0)
                            {
                                drCT["MA_LO"] = "";
                                drCT["MA_TRAM"] = "";
                                drCT["MA_TO"] = "";
                            }
                            else
                            {
                                drCT["MA_LO"] = Convert.ToString(dwVTriDDo_GT[0]["MA_LO"]);
                                drCT["MA_TRAM"] = Convert.ToString(dwVTriDDo_GT[0]["MA_TRAM"]);
                                drCT["MA_TO"] = Convert.ToString(dwVTriDDo_GT[0]["MA_TO"]);
                            }
                        }
                        else
                        {
                            drCT["MA_LO"] = "";
                            drCT["MA_TRAM"] = "";
                            drCT["MA_TO"] = "";
                        }
                    }
                    else
                    {
                        drCT["MA_LO"] = Convert.ToString(dwVTriDDo[0]["MA_LO"]);
                        drCT["MA_TRAM"] = Convert.ToString(dwVTriDDo[0]["MA_TRAM"]);
                        drCT["MA_TO"] = Convert.ToString(dwVTriDDo[0]["MA_TO"]);
                    }

                    drCT["MA_CAPDA"] = drSL4["MA_CAPDAP"];
                    drCT["NGAY_APDUNG"] = drSL4["NGAY_APDUNG"];
                    drCT["ID_CHISO"] = drSL4["ID_CHISO"];
                    drCT["SO_CTO"] = drSL4["SO_CTO"];

                    drCT["NGAY_TAO"] = System.DateTime.Now;
                    drCT["NGUOI_TAO"] = strTen_DNhap;
                    drCT["NGAY_SUA"] = System.DateTime.Now;
                    drCT["NGUOI_SUA"] = strTen_DNhap;
                    drCT["MA_CNANG"] = 0;
                    //DũngNT: Bổ sung 3 trường thay đổi CSDL ngày 23/12/2009
                    //dw = new DataView(dsCustomerData.Tables["HDG_KHACH_HANG"]);
                    dwKHang.RowFilter = "MA_KHANG = '" + Convert.ToString(drSL4["MA_KHANG"]) + "'";
                    //dw.Sort = "NGAY_HLUC DESC";
                    if (dwKHang.Count == 0)
                    {
                        drCT["LOAI_KHANG"] = 2; //La gia tri se duoc insert vao trong truong hop bi loi lay so lieu trong HDG_KKHACH_HANG                        
                    }
                    else
                    {
                        drCT["LOAI_KHANG"] = Convert.ToInt16(dwKHang[0]["LOAI_KHANG"]);
                    }

                    //dw = new DataView(dsCustomerData.Tables["HDG_DIEM_DO"]);
                    dwDDo.RowFilter = "MA_DDO = '" + Convert.ToString(drSL4["MA_DDO"]) + "'";
                    //dw.Sort = "NGAY_HLUC DESC";
                    if (dwDDo.Count == 0)
                    {
                        if (dsCustomerData.Tables.Contains("HDG_DIEM_DO_GT") == true)
                        {
                            //dw = new DataView(dsCustomerData.Tables["HDG_DIEM_DO_GT"]);
                            dwDDo_GT.RowFilter = "MA_DDO = '" + Convert.ToString(drSL4["MA_DDO"]) + "'";
                            //dw.Sort = "NGAY_HLUC DESC";
                            if (dwDDo_GT.Count == 0)
                            {
                                drCT["LOAI_DDO"] = "";
                                drCT["SO_PHA"] = "";
                            }
                            else
                            {
                                drCT["LOAI_DDO"] = Convert.ToInt16(dwDDo_GT[0]["LOAI_DDO"]);
                                drCT["SO_PHA"] = Convert.ToInt16(dwDDo_GT[0]["SO_PHA"]);
                            }
                        }
                        else
                        {
                            drCT["LOAI_DDO"] = "";
                            drCT["SO_PHA"] = "";
                        }
                    }
                    else
                    {
                        drCT["LOAI_DDO"] = Convert.ToInt16(dwDDo[0]["LOAI_DDO"]);
                        drCT["SO_PHA"] = Convert.ToInt16(dwDDo[0]["SO_PHA"]);
                    }
                    if (i32DoiLichGCS > 0 && dtNgayGhiCu != dt1900) updateNoi_Dung(drSL4, drCT, i32DoiLichGCS, dtNgayGhiCu, strNgayGhi);
                    dsInvoiceData.Tables["HDN_HDONCTIET"].Rows.Add(drCT);
                }
                //dsInvoiceData.Tables["HDN_HDONCTIET"].AcceptChanges();

                //Day du lieu cua cac dong VC trong SL_1
                dwVC = new DataView(dsCalculation.Tables["SL_1"]);
                dwVC.RowFilter = "BCS = 'VC'";
                if (dwVC.Count != 0)
                {
                    foreach (DataRowView drSL1 in dwVC)
                    {
                        drCT = dsInvoiceData.Tables["HDN_HDONCTIET"].NewRow();
                        drCT["MA_DVIQLY"] = drSL1["MA_DVIQLY"];
                        drCT["ID_HDONCTIET"] = 1;
                        drCT["ID_HDON"] = 1;
                        drCT["MA_KHANG"] = drSL1["MA_KHANG"];
                        drCT["MA_DDO"] = drSL1["MA_DDO"];

                        //dw = new DataView(dsCustomerData.Tables["HDG_DDO_SOGCS"]);
                        dwDDoSoGCS.RowFilter = "MA_DDO = '" + Convert.ToString(drSL1["MA_DDO"]) + "'";
                        //dw.Sort = "NGAY_HLUC DESC";
                        if (dwDDoSoGCS.Count == 0)
                        {
                            if (dsCustomerData.Tables.Contains("HDG_DDO_SOGCS_GT") == true)
                            {
                                //dw = new DataView(dsCustomerData.Tables["HDG_DDO_SOGCS_GT"]);
                                dwDDoSoGCS_GT.RowFilter = "MA_DDO = '" + Convert.ToString(drSL1["MA_DDO"]) + "'";
                                //dw.Sort = "NGAY_HLUC DESC";
                                if (dwDDoSoGCS_GT.Count == 0)
                                {
                                    drCT["MA_SOGCS"] = "";
                                    drCT["MA_KVUC"] = "";
                                    drCT["STT"] = "";
                                }
                                else
                                {
                                    drCT["MA_SOGCS"] = Convert.ToString(dwDDoSoGCS_GT[0]["MA_SOGCS"]);
                                    drCT["MA_KVUC"] = Convert.ToString(dwDDoSoGCS_GT[0]["MA_KVUC"]);
                                    drCT["STT"] = Convert.ToString(dwDDoSoGCS_GT[0]["STT"]);
                                }
                            }
                            else
                            {
                                drCT["MA_SOGCS"] = "";
                                drCT["MA_KVUC"] = "";
                                drCT["STT"] = "";
                            }
                        }
                        else
                        {
                            drCT["MA_SOGCS"] = Convert.ToString(dwDDoSoGCS[0]["MA_SOGCS"]);
                            drCT["MA_KVUC"] = Convert.ToString(dwDDoSoGCS[0]["MA_KVUC"]);
                            drCT["STT"] = Convert.ToString(dwDDoSoGCS[0]["STT"]);
                        }

                        drCT["KY"] = drSL1["KY"];
                        drCT["THANG"] = drSL1["THANG"];
                        drCT["NAM"] = drSL1["NAM"];
                        drCT["DIEN_TTHU"] = drSL1["SAN_LUONG"];
                        drCT["DON_GIA"] = 0;

                        drCT["SO_TIEN"] = 0;
                        drCT["MA_NHOMNN"] = "";
                        drCT["MA_NN"] = "";
                        drCT["MA_NGIA"] = "";
                        drCT["DINH_MUC"] = "";

                        drCT["LOAI_DMUC"] = "";
                        drCT["BCS"] = drSL1["BCS"];
                        drCT["TGIAN_BDIEN"] = "";

                        //dw = new DataView(dsCustomerData.Tables["HDG_VITRI_DDO"]);
                        dwVTriDDo.RowFilter = "MA_DDO = '" + Convert.ToString(drSL1["MA_DDO"]) + "'";
                        //dw.Sort = "NGAY_HLUC DESC";
                        if (dwVTriDDo.Count == 0)
                        {
                            if (dsCustomerData.Tables.Contains("HDG_VITRI_DDO_GT") == true)
                            {
                                //dw = new DataView(dsCustomerData.Tables["HDG_VITRI_DDO_GT"]);
                                dwVTriDDo_GT.RowFilter = "MA_DDO = '" + Convert.ToString(drSL1["MA_DDO"]) + "'";
                                //dw.Sort = "NGAY_HLUC DESC";
                                if (dwVTriDDo_GT.Count == 0)
                                {
                                    drCT["MA_LO"] = "";
                                    drCT["MA_TRAM"] = "";
                                    drCT["MA_TO"] = "";
                                }
                                else
                                {
                                    drCT["MA_LO"] = Convert.ToString(dwVTriDDo_GT[0]["MA_LO"]);
                                    drCT["MA_TRAM"] = Convert.ToString(dwVTriDDo_GT[0]["MA_TRAM"]);
                                    drCT["MA_TO"] = Convert.ToString(dwVTriDDo_GT[0]["MA_TO"]);
                                }
                            }
                            else
                            {
                                drCT["MA_LO"] = "";
                                drCT["MA_TRAM"] = "";
                                drCT["MA_TO"] = "";
                            }
                        }
                        else
                        {
                            drCT["MA_LO"] = Convert.ToString(dwVTriDDo[0]["MA_LO"]);
                            drCT["MA_TRAM"] = Convert.ToString(dwVTriDDo[0]["MA_TRAM"]);
                            drCT["MA_TO"] = Convert.ToString(dwVTriDDo[0]["MA_TO"]);
                        }

                        drCT["MA_CAPDA"] = "";
                        drCT["NGAY_APDUNG"] = System.DBNull.Value;
                        drCT["ID_CHISO"] = drSL1["ID_CHISO"];
                        drCT["SO_CTO"] = drSL1["SO_CTO"];

                        drCT["NGAY_TAO"] = System.DateTime.Now;
                        drCT["NGUOI_TAO"] = strTen_DNhap;
                        drCT["NGAY_SUA"] = System.DateTime.Now;
                        drCT["NGUOI_SUA"] = strTen_DNhap;
                        drCT["MA_CNANG"] = 0;

                        //DũngNT: Bổ sung 3 trường thay đổi CSDL ngày 23/12/2009
                        //dw = new DataView(dsCustomerData.Tables["HDG_KHACH_HANG"]);
                        dwKHang.RowFilter = "MA_KHANG = '" + Convert.ToString(drSL1["MA_KHANG"]) + "'";
                        dwKHang.Sort = "NGAY_HLUC DESC";
                        if (dwKHang.Count == 0)
                        {
                            drCT["LOAI_KHANG"] = 2; //La gia tri se duoc insert vao trong truong hop bi loi lay so lieu trong HDG_KKHACH_HANG                        
                        }
                        else
                        {
                            drCT["LOAI_KHANG"] = Convert.ToInt16(dwKHang[0]["LOAI_KHANG"]);
                        }

                        //dw = new DataView(dsCustomerData.Tables["HDG_DIEM_DO"]);
                        dwDDo.RowFilter = "MA_DDO = '" + Convert.ToString(drSL1["MA_DDO"]) + "'";
                        //dw.Sort = "NGAY_HLUC DESC";
                        if (dwDDo.Count == 0)
                        {
                            if (dsCustomerData.Tables.Contains("HDG_DIEM_DO_GT") == true)
                            {
                                //dw = new DataView(dsCustomerData.Tables["HDG_DIEM_DO_GT"]);
                                dwDDo_GT.RowFilter = "MA_DDO = '" + Convert.ToString(drSL1["MA_DDO"]) + "'";
                                //dw.Sort = "NGAY_HLUC DESC";
                                if (dwDDo_GT.Count == 0)
                                {
                                    drCT["LOAI_DDO"] = "";
                                    drCT["SO_PHA"] = "";
                                }
                                else
                                {
                                    drCT["LOAI_DDO"] = Convert.ToInt16(dwDDo_GT[0]["LOAI_DDO"]);
                                    drCT["SO_PHA"] = Convert.ToInt16(dwDDo_GT[0]["SO_PHA"]);
                                }
                            }
                            else
                            {
                                drCT["LOAI_DDO"] = "";
                                drCT["SO_PHA"] = "";
                            }
                        }
                        else
                        {
                            drCT["LOAI_DDO"] = Convert.ToInt16(dwDDo[0]["LOAI_DDO"]);
                            drCT["SO_PHA"] = Convert.ToInt16(dwDDo[0]["SO_PHA"]);
                        }

                        dsInvoiceData.Tables["HDN_HDONCTIET"].Rows.Add(drCT);
                    }
                    //dsInvoiceData.Tables["HDN_HDONCTIET"].AcceptChanges();
                }

                return "";
            }
            catch (Exception ex)
            {
                return "PriceSpecification_3: " + ex.Message;
            }
            //finally
            //{                
            //    dsCalculation.AcceptChanges();
            //    dsCustomerData.AcceptChanges();
            //}
        }
        public string PriceSpecification_4(DataSet dsCustomerData, DataSet dsInvoiceData, string strMaSoGCS, string strNgayGhi, cls_Config config, bool isGiaCu)
        {
            string strDebug = "";
            try
            {
                DateTime dtHLucNSH = config.dtHLucHTroNSH;
                DateTime dtHLucSH = config.dtHLucHTroSH;
                DateTime dtHetHLucNSH = config.dtHetHLucHTroNSH;
                DateTime dtHetHLucSH = config.dtHetHLucHTroSH;

                decimal decTyLe = 0, decTienGoc = 0, decTienGTru = 0, decTongTien = 0, decTongGTru = 0, dec10 = 10, dec15 = 15, decTyLeSHBT = 0, decTongGTru_SHBT;
                string strLoaiDMuc = "%", strMaDDo = "";
                List<string> lstDDoDaTinh = new List<string>();
                DataView dvCTiet = null;
                if (dsCustomerData == null)
                    return "dsCustomerDataIsNull";
                else
                    if (dsCustomerData.Tables.Contains("HDG_DDO_SOGCS") == false)
                    return "NotExistsHDG_DDO_SOGCS";
                else
                        if (dsCustomerData.Tables.Contains("HDG_VITRI_DDO") == false)
                    return "NotExistsHDG_VITRI_DDO";

                if (dsInvoiceData == null)
                    return "dsInvoiceDataIsNull";
                else
                    if (dsInvoiceData.Tables.Contains("HDN_HDONCTIET") == false)
                    return "NotExistsHDN_HDONCTIET";
                dvCTiet = new DataView(dsInvoiceData.Tables["HDN_HDONCTIET"]);

                if (!isGiaCu)
                {
                    //Thực hiện tính toán lại các cột trong HDN_HDONCTIET
                    foreach (DataRow drCT in dsInvoiceData.Tables["HDN_HDONCTIET"].Rows)
                    {
                        decTyLe = 0;
                        decTienGoc = 0;
                        decTienGTru = 0;
                        decTongTien = 0;
                        decTongGTru = 0;
                        strLoaiDMuc = "%";
                        decTyLeSHBT = 0;
                        strMaDDo = drCT["MA_DDO"].ToString().Trim();
                        if (strMaDDo == "PD14000158477121")
                        {
                            strDebug = strMaDDo;

                        }
                        strDebug = strMaDDo;
                        //if (drCT["MA_KHANG"].ToString().Trim() == "PD06000035790" && drCT["MA_NHOMNN"].ToString().Trim() == "SHBT")
                        //{
                        //    decTyLe = 0;

                        //}
                        if (!lstDDoDaTinh.Contains(strMaDDo))
                        {
                            bool isGTruSHoat = false;
                            //Bổ sung thêm kiểm tra trạng thái giảm trừ nhóm SHBT

                            drCT["TIEN_GTRU"] = 0;
                            drCT["TIEN_GOC"] = drCT["SO_TIEN"];
                            drCT["TY_LE"] = 0;
                            int iLoaiKHang = Convert.ToInt32(drCT["LOAI_KHANG"]);
                            DataRow[] arrChiSo = dsCustomerData.Tables["GCS_CHISO"].Select("MA_KHANG = '" + drCT["MA_KHANG"] + "' AND BCS IN ('KT','BT','CD','TD') AND LOAI_CHISO IN ('DDK','DDN','CSC','CCS')");
                            //DateTime dtNgayCKy = arrChiSo.Max(c => c.Field<DateTime>("NGAY_CKY"));
                            DateTime dtNgayCKy = DateTime.ParseExact(strNgayGhi, "dd/MM/yyyy", System.Globalization.CultureInfo.InvariantCulture);
                            int thang = Convert.ToInt32(arrChiSo[0]["THANG"]);
                            int nam = Convert.ToInt32(arrChiSo[0]["NAM"]);
                            if (config.isHTroThang == false)
                            {
                                if (iLoaiKHang == 0)
                                {
                                    //SH
                                    if (dtNgayCKy.CompareTo(dtHLucSH) < 0 || dtNgayCKy.CompareTo(dtHetHLucSH) >= 0)
                                    {
                                        continue;
                                    }
                                }
                                else
                                {
                                    //NSH
                                    if (dtNgayCKy.CompareTo(dtHLucNSH) < 0 || dtNgayCKy.CompareTo(dtHetHLucNSH) >= 0)
                                    {
                                        continue;
                                    }
                                }
                            }
                            else
                            {
                                if (!config.lstTNamHTro.Contains(thang + "/" + nam))
                                {
                                    continue;
                                }
                            }
                            if (config.lstTNamHTroSH.Contains(thang + "/" + nam))
                            {
                                if (config.lstSoHTro.Count > 0)
                                    for (int i = 0; i < config.lstSoHTro.Count; i++)
                                    {
                                        //string strMaSoGCS = drCT["MA_SOGCS"].ToString().Trim();
                                        if (strMaSoGCS.Equals(config.lstSoHTro[i]))
                                        {
                                            isGTruSHoat = true;
                                            break;
                                        }
                                    }
                                if (!isGTruSHoat && config.lstDViHTro.Count > 0)
                                    for (int i = 0; i < config.lstDViHTro.Count; i++)
                                    {
                                        string strMaDVTemp = drCT["MA_DVIQLY"].ToString().Trim();
                                        if (strMaDVTemp.StartsWith(config.lstDViHTro[i]))
                                        {
                                            isGTruSHoat = true;
                                            break;
                                        }
                                    }
                                if (isGTruSHoat)
                                {
                                    //Kiểm tra tồn tại nhóm giá SHBT
                                    dvCTiet.RowFilter = "MA_KHANG='" + drCT["MA_KHANG"].ToString().Trim() + "' and BCS<>'VC' and MA_NHOMNN='SHBT'";
                                    if (dvCTiet != null && dvCTiet.Count > 0)
                                    {
                                        //Tính lại tổng sản lượng SHBT trong chi tiết
                                        decimal sumSL_SHBT = Convert.ToDecimal(dsInvoiceData.Tables["HDN_HDONCTIET"].Compute("SUM(DIEN_TTHU)", "MA_KHANG='" + drCT["MA_KHANG"].ToString().Trim() + "' and BCS<>'VC' and MA_NHOMNN='SHBT'"));
                                        decTyLeSHBT = sumSL_SHBT > 200 ? dec10 : dec15;
                                    }
                                    dvCTiet.RowFilter = "";
                                }
                            }
                            dvCTiet.RowFilter = "MA_DDO='" + drCT["MA_DDO"].ToString().Trim() + "' and BCS<>'VC'";
                            if (dvCTiet.Count > 0)
                            {
                                if (dsCustomerData.Tables.Contains("HDG_DDO_GTRU") && dsCustomerData.Tables["HDG_DDO_GTRU"].Rows.Count > 0)
                                {
                                    foreach (DataRow drGTru in dsCustomerData.Tables["HDG_DDO_GTRU"].Rows)
                                    {
                                        if (drGTru["MA_DDO"].ToString().Trim().Equals(strMaDDo) && drGTru["MANHOM_KHANG"].ToString().Trim().Equals("COVID"))
                                        {
                                            decTyLe = Convert.ToDecimal(drGTru["TY_LE"]);
                                            strLoaiDMuc = drGTru["LOAI_DMUC"].ToString().Trim();
                                            break;
                                        }
                                    }
                                }


                                if (!decTyLe.Equals(100))
                                {
                                    //Tỷ lệ dưới 100%, quá 100% coi như lỗi, ko tính
                                    decTongTien = Convert.ToDecimal(dsInvoiceData.Tables["HDN_HDONCTIET"].Compute("SUM(SO_TIEN)", "MA_DDO='" + drCT["MA_DDO"].ToString().Trim() + "' and BCS<>'VC'"));
                                    decTongGTru = Math.Round(decTongTien * decTyLe / 100, 0, MidpointRounding.AwayFromZero);
                                    //Tính thêm tổng tiền giảm trừ SHBT


                                    for (int i = 0; i < dvCTiet.Count; i++)
                                    {
                                        decTienGoc = 0;
                                        decTienGTru = 0;
                                        strLoaiDMuc = "%";

                                        //bắt đầu tính lại các giá trị
                                        decTienGoc = Convert.ToDecimal(dvCTiet[i]["SO_TIEN"]);
                                        decTienGTru = Math.Round(decTienGoc * decTyLe / 100, 0, MidpointRounding.AwayFromZero);
                                        dvCTiet[i]["TY_LE"] = decTyLe;
                                        dvCTiet[i]["TIEN_GOC"] = decTienGoc;
                                        if (i < dvCTiet.Count - 1)
                                        {
                                            dvCTiet[i]["TIEN_GTRU"] = decTienGTru;
                                            dvCTiet[i]["SO_TIEN"] = decTienGoc - decTienGTru;
                                            decTongGTru -= decTienGTru;
                                        }
                                        else
                                        {
                                            //Dòng cuối tiền giảm trừ = phần còn lại
                                            dvCTiet[i]["TIEN_GTRU"] = decTongGTru;
                                            dvCTiet[i]["SO_TIEN"] = decTienGoc - decTongGTru;
                                        }

                                    }

                                    //Xử lý riêng nhóm SHBT
                                    if (decTyLeSHBT > 0 && decTyLe <= 0)
                                    {
                                        dvCTiet.RowFilter = "MA_DDO='" + drCT["MA_DDO"].ToString().Trim() + "' and BCS<>'VC' and MA_NHOMNN='SHBT'";
                                        if (dvCTiet.Count > 0)
                                        {
                                            decimal decTongTien_SHBT = Convert.ToDecimal(dsInvoiceData.Tables["HDN_HDONCTIET"].Compute("SUM(SO_TIEN)", "MA_DDO='" + drCT["MA_DDO"].ToString().Trim() + "' and BCS<>'VC' and MA_NHOMNN='SHBT'"));
                                            decTongGTru_SHBT = Math.Round(decTongTien_SHBT * decTyLeSHBT / 100, 0, MidpointRounding.AwayFromZero);
                                        }
                                        else
                                            decTongGTru_SHBT = 0;
                                        //dvCTiet.RowFilter = "MA_DDO='" + drCT["MA_DDO"].ToString().Trim() + "' and BCS<>'VC' and MA_NHOMNN='SHBT'";
                                        for (int i = 0; i < dvCTiet.Count; i++)
                                        {
                                            decTienGoc = 0;
                                            decTienGTru = 0;
                                            strLoaiDMuc = "%";

                                            //bắt đầu tính lại các giá trị
                                            decTienGoc = Convert.ToDecimal(dvCTiet[i]["SO_TIEN"]);
                                            decTienGTru = Math.Round(decTienGoc * decTyLeSHBT / 100, 0, MidpointRounding.AwayFromZero);
                                            dvCTiet[i]["TY_LE"] = decTyLeSHBT;
                                            dvCTiet[i]["TIEN_GOC"] = decTienGoc;
                                            if (i < dvCTiet.Count - 1)
                                            {
                                                dvCTiet[i]["TIEN_GTRU"] = decTienGTru;
                                                dvCTiet[i]["SO_TIEN"] = decTienGoc - decTienGTru;
                                                decTongGTru_SHBT -= decTienGTru;
                                            }
                                            else
                                            {
                                                //Dòng cuối tiền giảm trừ = phần còn lại
                                                dvCTiet[i]["TIEN_GTRU"] = decTongGTru_SHBT;
                                                dvCTiet[i]["SO_TIEN"] = decTienGoc - decTongGTru_SHBT;
                                            }

                                        }
                                    }


                                }
                                else
                                {
                                    //tỷ lệ 100%, trừ sạch
                                    for (int i = 0; i < dvCTiet.Count; i++)
                                    {
                                        dvCTiet[i]["TY_LE"] = decTyLe;
                                        dvCTiet[i]["TIEN_GOC"] = dvCTiet[i]["SO_TIEN"];
                                        dvCTiet[i]["TIEN_GTRU"] = dvCTiet[i]["SO_TIEN"];
                                        dvCTiet[i]["SO_TIEN"] = 0;
                                    }
                                }
                            }
                            dvCTiet.RowFilter = "";
                            lstDDoDaTinh.Add(strMaDDo);
                        }


                    }

                }
                else
                {
                    foreach (DataRow drCT in dsInvoiceData.Tables["HDN_HDONCTIET"].Rows)
                    {

                        drCT["TIEN_GTRU"] = 0;
                        drCT["TIEN_GOC"] = drCT["SO_TIEN"];
                        drCT["TY_LE"] = 0;
                    }
                }
                return "";
            }
            catch (Exception ex)
            {
                return "PriceSpecification_4: " + ex.Message + strDebug;
            }
        }
        //Xu ly lam tron ngang doi voi cac nhom nganh nghe bac thang trong ky doi gia
        public string PriceSpecification_12(DS_CALCULATIONTABLES dsCalculation, DataSet dsStaticCatalog, DataSet dsCustomerData)
        {
            try
            {
                if (dsCalculation == null)
                    return "dsCalculationIsNull";
                else
                    if (dsCalculation.Tables.Contains("SL_4") == false)
                    return "NotExistsSL_4";

                if (dsStaticCatalog == null)
                    return "dsStaticCatalogIsNull";
                else
                    if (dsStaticCatalog.Tables.Contains("D_BAC_THANG") == false)
                    return "NotExistsD_BAC_THANG";

                DataView dwSL4 = new DataView(dsCalculation.Tables["SL_4"]);
                dwSL4.RowFilter = "BTHANG_ID <> 0";
                dwSL4.Sort = "MA_NHOMNN, MA_NGIA, MA_DDO, BTHANG_ID, NGAY_CUOI, NGAY_DAU";

                DataView dwSL4_1 = new DataView(dsCalculation.Tables["SL_4"]);
                dwSL4_1.RowFilter = "BTHANG_ID <> 0";
                dwSL4_1.Sort = "MA_DDO, NGAY_DAU, NGAY_CUOI";

                if (dwSL4.Count == 0)
                {
                    return "";
                }

                DataView dwBThang = new DataView(dsStaticCatalog.Tables["D_BAC_THANG"]);
                //dwBThang.RowFilter = "MA_NHOMNN = 'SHBT' AND MA_NGIA = 'A'";
                dwBThang.Sort = "NGAY_HLUC DESC, BTHANG_ID";

                string strMa_NhomNN = "", strMa_NGia = "";
                DataTable dt1 = dsStaticCatalog.Tables["D_BAC_THANG"].Clone();
                DataTable dt2 = dsStaticCatalog.Tables["D_BAC_THANG"].Clone();
                DataRow dr;

                DateTime ngay_dkymin = Convert.ToDateTime(dsCalculation.Tables["SL_4"].Compute("MIN(NGAY_DAU)", "BTHANG_ID <> 0"));
                DateTime ngay_ckymax = Convert.ToDateTime(dsCalculation.Tables["SL_4"].Compute("MAX(NGAY_CUOI)", "BTHANG_ID <> 0"));
                DateTime ngay_hlucgia1 = DateTime.Parse("01/01/1900", new System.Globalization.CultureInfo("vi-VN"));
                DateTime ngay_hlucgia2 = DateTime.Parse("01/01/1900", new System.Globalization.CultureInfo("vi-VN"));
                DateTime ngay_hlucBT = DateTime.Parse("01/01/1900", new System.Globalization.CultureInfo("vi-VN"));

                //Bang gia moi
                foreach (DataRowView drwBThang in dwBThang)
                {
                    ngay_hlucBT = Convert.ToDateTime(drwBThang["NGAY_HLUC"]);
                    if (ngay_hlucBT > ngay_ckymax)
                    {
                        continue;
                    }
                    else
                    {
                        ngay_hlucgia2 = ngay_hlucBT;
                        break;
                    }
                }

                //Bang gia cu
                foreach (DataRowView drwBThang in dwBThang)
                {
                    ngay_hlucBT = Convert.ToDateTime(drwBThang["NGAY_HLUC"]);
                    if (ngay_hlucBT >= ngay_dkymin)
                    {
                        continue;
                    }
                    else
                    {
                        ngay_hlucgia1 = ngay_hlucBT;
                        break;
                    }
                }

                //Khong co doi gia nha nuoc, khong can phai thuc hien
                if (ngay_hlucgia1 == ngay_hlucgia2)
                {
                    return "";
                }
                else
                {
                    if (ngay_ckymax <= ngay_hlucgia2)
                    {
                        return "";
                    }
                }

                //Co doi gia nha nuoc trong bang bac thang, kiem tra so ban ghi va tung dinh muc bac thang
                foreach (DataRowView drwSL4 in dwSL4)
                {
                    if (strMa_NhomNN != Convert.ToString(drwSL4["MA_NHOMNN"]) || strMa_NGia != Convert.ToString(drwSL4["MA_NGIA"]))
                    {

                        if (Convert.ToString(drwSL4["MA_DDO"]) == "PA07HH0025782001")
                        {
                            drwSL4["MA_DDO"] = "PA07HH0025782001";
                        }

                        strMa_NhomNN = Convert.ToString(drwSL4["MA_NHOMNN"]);
                        strMa_NGia = Convert.ToString(drwSL4["MA_NGIA"]);

                        dwBThang = new DataView(dsStaticCatalog.Tables["D_BAC_THANG"]);
                        dwBThang.RowFilter = "MA_NHOMNN = '" + strMa_NhomNN + "' AND MA_NGIA = '" + strMa_NGia + "'";
                        dwBThang.Sort = "NGAY_HLUC DESC, BTHANG_ID";
                        //Tính lại ngay_hlucgia1 và ngay_hlucgia2 theo mã nhóm ngành nghề mới
                        //Bang gia moi
                        foreach (DataRowView drwBThang in dwBThang)
                        {

                            ngay_hlucBT = Convert.ToDateTime(drwBThang["NGAY_HLUC"]);
                            if (ngay_hlucBT > ngay_ckymax)
                            {
                                continue;
                            }
                            else
                            {
                                ngay_hlucgia2 = ngay_hlucBT;
                                break;
                            }

                        }

                        //Bang gia cu
                        foreach (DataRowView drwBThang in dwBThang)
                        {

                            ngay_hlucBT = Convert.ToDateTime(drwBThang["NGAY_HLUC"]);
                            if (ngay_hlucBT >= ngay_dkymin)
                            {
                                continue;
                            }
                            else
                            {
                                ngay_hlucgia1 = ngay_hlucBT;
                                break;
                            }

                        }
                        if (ngay_hlucgia1 == ngay_hlucgia2)
                        {
                            //Không đổi giá cho nhóm giá này
                            continue;
                        }
                        else
                        {
                            if (ngay_ckymax <= ngay_hlucgia2)
                            {
                                //Không đổi giá cho nhóm giá này
                                continue;
                            }
                        }
                        if (dwBThang.Count == 0)
                            continue;

                        dt1.Rows.Clear();
                        dt2.Rows.Clear();

                        foreach (DataRowView drwBT in dwBThang)
                        {
                            if (ngay_hlucgia1 == Convert.ToDateTime(drwBT["NGAY_HLUC"]))
                            {
                                dr = dt1.NewRow();
                                dr.ItemArray = drwBT.Row.ItemArray;
                                dt1.Rows.Add(dr);
                            }
                            else
                            {
                                if (ngay_hlucgia2 == Convert.ToDateTime(drwBT["NGAY_HLUC"]))
                                {
                                    dr = dt2.NewRow();
                                    dr.ItemArray = drwBT.Row.ItemArray;
                                    dt2.Rows.Add(dr);
                                }
                            }
                        }

                        if (dt1.Rows.Count != dt2.Rows.Count)
                        {
                            //2 he thong bac thang khac so dong
                            continue;
                        }
                        else
                        {
                            //2 he thong bac thang cung so dong
                            Int16 iCheck = 0;
                            for (int i = 0; i < dt1.Rows.Count; i++)
                            {
                                if (Convert.ToString(dt1.Rows[i]["DINH_MUC"]) != Convert.ToString(dt2.Rows[i]["DINH_MUC"]))
                                {
                                    //Khac dinh muc
                                    iCheck = 1;
                                    break;
                                }
                            }

                            if (iCheck == 1)
                                continue;

                            //strGiaBacThang = strGiaBacThang & strMa_NhomNN & "|" & strMa_NGia & "|";

                            //Decimal idBThang1_Old = 0, idBThang2_Old = 0;

                            dwSL4_1 = new DataView(dsCalculation.Tables["SL_4"]);
                            dwSL4_1.RowFilter = "BTHANG_ID <> 0 AND MA_NHOMNN = '" + strMa_NhomNN + "' AND MA_NGIA = '" + strMa_NGia + "'";
                            dwSL4_1.Sort = "MA_DDO, NGAY_DAU, NGAY_CUOI";

                            for (int i = 0; i < dt1.Rows.Count; i++)
                            {
                                Decimal idBThang1 = 0, idBThang2 = 0, idBThang1_Old = 0, idBThang2_Old = 0;
                                idBThang1 = Convert.ToDecimal(dt1.Rows[i]["BTHANG_ID"]);
                                idBThang2 = Convert.ToDecimal(dt2.Rows[i]["BTHANG_ID"]);

                                DataView dw1 = new DataView(dsCalculation.Tables["SL_4"]);
                                dw1.RowFilter = "BTHANG_ID = " + idBThang1;
                                dw1.Sort = "MA_DDO, NGAY_DAU, NGAY_CUOI";

                                string strMa_DDo = "";
                                Decimal sl = 0, sl1 = 0, sl2 = 0, sldm = 0, sldm1 = 0, sldm2 = 0, sl1_Save = 0;
                                DateTime ngay_dau = DateTime.Parse("01/01/3000", new System.Globalization.CultureInfo("vi-VN"));
                                DateTime ngay_cuoi = DateTime.Parse("01/01/1900", new System.Globalization.CultureInfo("vi-VN"));
                                DateTime ngay_dkymin_ddo = DateTime.Parse("01/01/1900", new System.Globalization.CultureInfo("vi-VN"));
                                DateTime ngay_ckymax_ddo = DateTime.Parse("01/01/1900", new System.Globalization.CultureInfo("vi-VN"));
                                decimal songaydmuc = 0, soho = 0;

                                foreach (DataRowView drwSL4_1 in dwSL4_1)
                                {
                                    if (strMa_DDo != Convert.ToString(drwSL4_1["MA_DDO"]))
                                    {
                                        strMa_DDo = Convert.ToString(drwSL4_1["MA_DDO"]);

                                        if (strMa_DDo == "PA07HH0025782001")
                                        {
                                            strMa_DDo = "PA07HH0025782001";
                                        }

                                        ngay_dkymin_ddo = Convert.ToDateTime(dsCalculation.Tables["SL_4"].Compute("MIN(NGAY_DAU)", "MA_DDO = '" + strMa_DDo + "' AND BTHANG_ID <> 0 AND MA_NHOMNN = '" + strMa_NhomNN + "' AND MA_NGIA = '" + strMa_NGia + "'"));
                                        ngay_ckymax_ddo = Convert.ToDateTime(dsCalculation.Tables["SL_4"].Compute("MAX(NGAY_CUOI)", "MA_DDO = '" + strMa_DDo + "' AND BTHANG_ID <> 0 AND MA_NHOMNN = '" + strMa_NhomNN + "' AND MA_NGIA = '" + strMa_NGia + "'"));

                                        #region Xu ly voi bac thang truoc doi gia
                                        sl1 = 0;
                                        sl2 = 0;
                                        sldm1 = 0;
                                        sl1_Save = 0;

                                        songaydmuc = Convert.ToDecimal(drwSL4_1["SO_NGAYDMUC"]);
                                        soho = Convert.ToDecimal(drwSL4_1["SO_HO"]);
                                        sldm = Convert.ToDecimal(dt1.Rows[i]["DINH_MUC"]);

                                        dw1 = new DataView(dsCalculation.Tables["SL_4"]);
                                        dw1.RowFilter = "BTHANG_ID = " + Convert.ToString(dt1.Rows[0]["BTHANG_ID"]) + " AND MA_DDO = '" + strMa_DDo + "'";
                                        dw1.Sort = "NGAY_DAU, NGAY_CUOI";

                                        foreach (DataRowView drw1 in dw1)
                                        {
                                            if (Convert.ToDateTime(drw1["NGAY_DAU"]) <= Convert.ToDateTime(dt2.Rows[i]["NGAY_HLUC"]).AddDays(-1.0) && Convert.ToDateTime(drw1["NGAY_CUOI"]) <= Convert.ToDateTime(dt2.Rows[i]["NGAY_HLUC"]).AddDays(-1.0))
                                            {
                                                if (Convert.ToDateTime(drw1["NGAY_CUOI"]) < Convert.ToDateTime(dt2.Rows[i]["NGAY_HLUC"]).AddDays(-1.0))
                                                {
                                                    sl1 += this.DeltaDinhMuc(Convert.ToDateTime(drw1["NGAY_DAU"]), Convert.ToDateTime(drw1["NGAY_CUOI"]), songaydmuc, sldm, soho, dsCustomerData, strMa_DDo);
                                                }
                                            }
                                            else
                                            {
                                                break;
                                            }
                                        }

                                        dw1 = new DataView(dsCalculation.Tables["SL_4"]);
                                        dw1.RowFilter = "BTHANG_ID = " + idBThang1 + " AND MA_DDO = '" + strMa_DDo + "'";
                                        dw1.Sort = "NGAY_DAU, NGAY_CUOI";

                                        foreach (DataRowView drw1 in dw1)
                                        {
                                            if (Convert.ToDateTime(drw1["NGAY_DAU"]) <= Convert.ToDateTime(dt2.Rows[i]["NGAY_HLUC"]).AddDays(-1.0) && Convert.ToDateTime(drw1["NGAY_CUOI"]) <= Convert.ToDateTime(dt2.Rows[i]["NGAY_HLUC"]).AddDays(-1.0))
                                            {
                                                if (ngay_dau > Convert.ToDateTime(drw1["NGAY_DAU"]))
                                                {
                                                    ngay_dau = Convert.ToDateTime(drw1["NGAY_DAU"]);
                                                }
                                                ngay_cuoi = Convert.ToDateTime(drw1["NGAY_CUOI"]);

                                                if (ngay_cuoi == Convert.ToDateTime(dt2.Rows[i]["NGAY_HLUC"]).AddDays(-1.0))
                                                {
                                                    sl2 = Convert.ToDecimal(drw1["SLCT"]);
                                                }
                                            }
                                            else
                                            {
                                                break;
                                            }
                                        }

                                        //Tinh dinh muc tu ngay dau den ngay doi gia
                                        sldm1 = this.DeltaDinhMuc(ngay_dkymin_ddo, Convert.ToDateTime(dt2.Rows[i]["NGAY_HLUC"]).AddDays(-1.0), songaydmuc, sldm, soho, dsCustomerData, strMa_DDo);

                                        if (ngay_cuoi == Convert.ToDateTime(dt2.Rows[i]["NGAY_HLUC"]).AddDays(-1.0) && sldm != 0)
                                        {
                                            //So sanh voi tong sl1 + sl2, neu chenh lech thi se cap nhat lai sl2
                                            if (sl1 + sl2 != sldm1 && sl1 != 0)
                                            {
                                                sl2 = sl2 + sldm1 - (sl1 + sl2);
                                            }

                                            //Gan lai vao slct neu co, khong thi se don vao sai so theo chieu doc                                        
                                            foreach (DataRowView drw1 in dw1)
                                            {
                                                if (ngay_cuoi == Convert.ToDateTime(drw1["NGAY_CUOI"]))
                                                {
                                                    drw1["SLCT"] = sl2;
                                                    break;
                                                }
                                            }

                                            sl1_Save = sl2;
                                            //idBThang1_Old = idBThang1;
                                        }
                                        #endregion

                                        #region Xu ly voi bac thang cuoi cung truoc doi gia
                                        if (ngay_cuoi != Convert.ToDateTime(dt2.Rows[i]["NGAY_HLUC"]).AddDays(-1.0) || i == dt1.Rows.Count - 1)
                                        {
                                            if (strMa_DDo == "PA07HH0025782001")
                                            {
                                                strMa_DDo = "PA07HH0025782001";
                                            }

                                            //Bac thang truoc co lam tron
                                            sl1 = 0;
                                            sl2 = 0;
                                            sl = 0;

                                            dw1 = new DataView(dsCalculation.Tables["SL_4"]);
                                            dw1.RowFilter = "MA_DDO = '" + strMa_DDo + "' AND BTHANG_ID <> 0 AND MA_NHOMNN = '" + strMa_NhomNN + "' AND MA_NGIA = '" + strMa_NGia + "'";
                                            dw1.Sort = "NGAY_DAU, NGAY_CUOI";

                                            idBThang1_Old = 0;
                                            foreach (DataRowView drw1 in dw1)
                                            {
                                                //La khoang cuoi cung truoc doi gia
                                                if (Convert.ToDateTime(dt2.Rows[i]["NGAY_HLUC"]).AddDays(-1.0) == Convert.ToDateTime(drw1["NGAY_CUOI"]))
                                                {
                                                    if (idBThang1_Old < Convert.ToDecimal(drw1["BTHANG_ID"]))
                                                    {
                                                        idBThang1_Old = Convert.ToDecimal(drw1["BTHANG_ID"]);
                                                    }
                                                }
                                            }

                                            if (idBThang1_Old != 0)
                                            {
                                                foreach (DataRowView drw1 in dw1)
                                                {
                                                    //La khoang cuoi cung truoc doi gia
                                                    if (Convert.ToDateTime(dt2.Rows[i]["NGAY_HLUC"]).AddDays(-1.0) == Convert.ToDateTime(drw1["NGAY_CUOI"]))
                                                    {
                                                        if (Convert.ToDecimal(drw1["SLCT"]) == -100) continue;
                                                        sl1 += Convert.ToDecimal(drw1["SLCT"]);
                                                        sl2 = Convert.ToDecimal(drw1["SLCT"]);
                                                        sl = Convert.ToDecimal(drw1["SAN_LUONG"]);
                                                    }
                                                }

                                                sl2 = sl2 + (sl - sl1);

                                                if (sl2 > 0)
                                                {
                                                    foreach (DataRowView drw1 in dw1)
                                                    {
                                                        if (Convert.ToDateTime(dt2.Rows[i]["NGAY_HLUC"]).AddDays(-1.0) == Convert.ToDateTime(drw1["NGAY_CUOI"]) && Convert.ToDecimal(drw1["BTHANG_ID"]) == idBThang1_Old)
                                                        {
                                                            if (Convert.ToDecimal(drw1["SLCT"]) == -100) continue;
                                                            drw1["SLCT"] = sl2;
                                                            break;
                                                        }
                                                    }
                                                }
                                                else
                                                {

                                                    bool isAm = true;
                                                    while (isAm && idBThang2_Old > 0)
                                                    {
                                                        if (sl2 == 0)
                                                        {
                                                            //Xoa dong dinh muc hien tai
                                                            foreach (DataRowView drw1 in dw1)
                                                            {
                                                                if (Convert.ToDateTime(dt2.Rows[i]["NGAY_HLUC"]).AddDays(-1.0) == Convert.ToDateTime(drw1["NGAY_CUOI"]) && Convert.ToDecimal(drw1["BTHANG_ID"]) == idBThang1_Old)
                                                                {
                                                                    drw1["SLCT"] = -100;
                                                                    isAm = false;
                                                                    break;
                                                                }
                                                            }
                                                        }
                                                        else
                                                        {
                                                            //Cap nhat lai gia tri lam tron len dinh muc phia truoc
                                                            Decimal id_BThangTmp = 0;
                                                            foreach (DataRowView drw1 in dw1)
                                                            {
                                                                if (Convert.ToDateTime(dt2.Rows[i]["NGAY_HLUC"]).AddDays(-1.0) == Convert.ToDateTime(drw1["NGAY_CUOI"]) && Convert.ToDecimal(drw1["BTHANG_ID"]) < idBThang1_Old)
                                                                {
                                                                    if (id_BThangTmp < Convert.ToDecimal(drw1["BTHANG_ID"]))
                                                                    {
                                                                        id_BThangTmp = Convert.ToDecimal(drw1["BTHANG_ID"]);
                                                                    }
                                                                }
                                                            }

                                                            foreach (DataRowView drw1 in dw1)
                                                            {
                                                                if (Convert.ToDateTime(dt2.Rows[i]["NGAY_HLUC"]).AddDays(-1.0) == Convert.ToDateTime(drw1["NGAY_CUOI"]) && Convert.ToDecimal(drw1["BTHANG_ID"]) == id_BThangTmp)
                                                                {
                                                                    if (Convert.ToDecimal(drw1["SLCT"]) == -100) continue;
                                                                    drw1["SLCT"] = Convert.ToDecimal(drw1["SLCT"]) + sl2;
                                                                    sl2 = Convert.ToDecimal(drw1["SLCT"]);
                                                                    if (sl2 == 0)
                                                                    {
                                                                        drw1["SLCT"] = -100;
                                                                        isAm = false;

                                                                    }
                                                                    else if (sl2 > 0)
                                                                        isAm = false;
                                                                    break;
                                                                }
                                                            }

                                                            //Xoa dong dinh muc hien tai
                                                            foreach (DataRowView drw1 in dw1)
                                                            {
                                                                if (Convert.ToDateTime(dt2.Rows[i]["NGAY_HLUC"]).AddDays(-1.0) == Convert.ToDateTime(drw1["NGAY_CUOI"]) && Convert.ToDecimal(drw1["BTHANG_ID"]) == idBThang1_Old)
                                                                {
                                                                    drw1["SLCT"] = -100;
                                                                    break;
                                                                }
                                                            }
                                                            if (sl2 < 0)
                                                                //Tiếp tục cập nhật
                                                                idBThang1_Old = id_BThangTmp;
                                                        }
                                                    }
                                                }

                                                sl1_Save = sl2;
                                            }
                                        }
                                        #endregion

                                        #region Xu ly voi bac thang sau doi gia
                                        sl1 = 0;
                                        sl2 = 0;
                                        ngay_dau = DateTime.Parse("01/01/3000", new System.Globalization.CultureInfo("vi-VN"));
                                        ngay_cuoi = DateTime.Parse("01/01/1900", new System.Globalization.CultureInfo("vi-VN"));
                                        sldm = 0;
                                        sldm2 = 0;

                                        songaydmuc = Convert.ToDecimal(drwSL4_1["SO_NGAYDMUC"]);
                                        soho = Convert.ToDecimal(drwSL4_1["SO_HO"]);
                                        sldm = Convert.ToDecimal(dt2.Rows[i]["DINH_MUC"]);

                                        dw1 = new DataView(dsCalculation.Tables["SL_4"]);
                                        dw1.RowFilter = "BTHANG_ID = " + Convert.ToString(dt2.Rows[0]["BTHANG_ID"]) + " AND MA_DDO = '" + strMa_DDo + "'";
                                        dw1.Sort = "NGAY_DAU, NGAY_CUOI";

                                        foreach (DataRowView drw1 in dw1)
                                        {
                                            if (Convert.ToDateTime(drw1["NGAY_DAU"]) >= Convert.ToDateTime(dt2.Rows[i]["NGAY_HLUC"]) && Convert.ToDateTime(drw1["NGAY_CUOI"]) >= Convert.ToDateTime(dt2.Rows[i]["NGAY_HLUC"]))
                                            {
                                                if (Convert.ToDateTime(drw1["NGAY_CUOI"]) < ngay_ckymax_ddo)
                                                {
                                                    sl1 += this.DeltaDinhMuc(Convert.ToDateTime(drw1["NGAY_DAU"]), Convert.ToDateTime(drw1["NGAY_CUOI"]), songaydmuc, sldm, soho, dsCustomerData, strMa_DDo);
                                                }
                                            }
                                            else
                                            {
                                                break;
                                            }
                                        }

                                        dw1 = new DataView(dsCalculation.Tables["SL_4"]);
                                        dw1.RowFilter = "BTHANG_ID = " + idBThang2 + " AND MA_DDO = '" + strMa_DDo + "'";
                                        dw1.Sort = "NGAY_DAU, NGAY_CUOI";

                                        foreach (DataRowView drw1 in dw1)
                                        {
                                            if (Convert.ToDateTime(drw1["NGAY_DAU"]) >= Convert.ToDateTime(dt2.Rows[i]["NGAY_HLUC"]) && Convert.ToDateTime(drw1["NGAY_CUOI"]) >= Convert.ToDateTime(dt2.Rows[i]["NGAY_HLUC"]))
                                            {
                                                if (ngay_dau > Convert.ToDateTime(drw1["NGAY_DAU"]))
                                                {
                                                    ngay_dau = Convert.ToDateTime(drw1["NGAY_DAU"]);
                                                }
                                                ngay_cuoi = Convert.ToDateTime(drw1["NGAY_CUOI"]);

                                                if (ngay_cuoi == ngay_ckymax_ddo)
                                                {
                                                    sl2 = Convert.ToDecimal(drw1["SLCT"]);
                                                }
                                            }
                                            else
                                            {
                                                break;
                                            }
                                        }

                                        //Tinh dinh muc tu ngay dau den ngay cuoi
                                        sldm2 = this.DeltaDinhMuc(ngay_dkymin_ddo, ngay_ckymax_ddo, songaydmuc, sldm, soho, dsCustomerData, strMa_DDo);
                                        //Tinh dinh muc tu ngay doi gia den ngay cuoi                                            
                                        sldm1 = sldm2 - sldm1;

                                        if (ngay_cuoi == ngay_ckymax_ddo && sldm != 0)
                                        {
                                            //So sanh voi tong sl1 + sl2, neu chenh lech thi se cap nhat lai sl2
                                            if (sl1 + sl2 != sldm1)
                                            {
                                                if (sl1 != 0)
                                                {
                                                    sl2 = sl2 + sldm1 - (sl1 + sl2);
                                                }
                                                else
                                                {
                                                    if (sldm1 != 0)
                                                    {
                                                        //Xử lý lỗi vụ SHBT đứt quãng giữa
                                                        sl2 = sl2 + sldm1 - (sl1 + sl2);
                                                    }
                                                }
                                            }

                                            //Gan lai vao slct neu co, khong thi se don vao sai so theo chieu doc

                                            foreach (DataRowView drw1 in dw1)
                                            {
                                                if (ngay_cuoi == Convert.ToDateTime(drw1["NGAY_CUOI"]))
                                                {
                                                    drw1["SLCT"] = sl2;
                                                    break;
                                                }
                                            }
                                        }
                                        #endregion

                                        #region Xu ly voi bac thang cuoi cung sau doi gia
                                        if (ngay_cuoi != ngay_ckymax_ddo || i == dt1.Rows.Count - 1)
                                        {
                                            if (strMa_DDo == "PA07HH0025782001")
                                            {
                                                strMa_DDo = "PA07HH0025782001";
                                            }

                                            //Bac thang truoc co lam tron
                                            sl1 = 0;
                                            sl2 = 0;
                                            sl = 0;

                                            dw1 = new DataView(dsCalculation.Tables["SL_4"]);
                                            dw1.RowFilter = "MA_DDO = '" + strMa_DDo + "' AND BTHANG_ID <> 0 AND MA_NHOMNN = '" + strMa_NhomNN + "' AND MA_NGIA = '" + strMa_NGia + "'";
                                            dw1.Sort = "NGAY_DAU, NGAY_CUOI";

                                            idBThang2_Old = 0;
                                            foreach (DataRowView drw1 in dw1)
                                            {
                                                //La khoang cuoi cung truoc doi gia
                                                if (ngay_ckymax_ddo == Convert.ToDateTime(drw1["NGAY_CUOI"]))
                                                {
                                                    if (idBThang2_Old < Convert.ToDecimal(drw1["BTHANG_ID"]))
                                                    {
                                                        idBThang2_Old = Convert.ToDecimal(drw1["BTHANG_ID"]);
                                                    }
                                                }
                                            }

                                            if (idBThang2_Old != 0)
                                            {
                                                foreach (DataRowView drw1 in dw1)
                                                {
                                                    //La khoang cuoi cung truoc doi gia
                                                    if (ngay_ckymax_ddo == Convert.ToDateTime(drw1["NGAY_CUOI"]))
                                                    {
                                                        if (Convert.ToDecimal(drw1["SLCT"]) == -100) continue;
                                                        
                                                        sl1 += Convert.ToDecimal(drw1["SLCT"]);
                                                        sl2 = Convert.ToDecimal(drw1["SLCT"]);
                                                        sl = Convert.ToDecimal(drw1["SAN_LUONG"]);
                                                    }
                                                }
                                                sl2 = sl2 + (sl - sl1);

                                                if (sl2 > 0)
                                                {
                                                    //Cap nhat gia tri sai so
                                                    foreach (DataRowView drw1 in dw1)
                                                    {
                                                        if (ngay_ckymax_ddo == Convert.ToDateTime(drw1["NGAY_CUOI"]) && Convert.ToDecimal(drw1["BTHANG_ID"]) == idBThang2_Old)
                                                        {
                                                            if (Convert.ToDecimal(drw1["SLCT"]) == -100) continue;
                                                            drw1["SLCT"] = sl2;
                                                            break;
                                                        }
                                                    }
                                                }
                                                else
                                                {
                                                    bool isAm = true;
                                                    while (isAm && idBThang2_Old>0)
                                                    {
                                                        if (sl2 == 0)
                                                        {
                                                            //Xoa dong dinh muc hien tai                                                        
                                                            foreach (DataRowView drw1 in dw1)
                                                            {
                                                                if (ngay_ckymax_ddo == Convert.ToDateTime(drw1["NGAY_CUOI"]) && Convert.ToDecimal(drw1["BTHANG_ID"]) == idBThang2_Old)
                                                                {
                                                                    drw1["SLCT"] = -100;
                                                                    isAm = false;
                                                                    break;
                                                                }
                                                            }
                                                        }
                                                        else
                                                        {
                                                            //Cap nhat lai gia tri lam tron len dinh muc phia truoc


                                                            Decimal id_BThangTmp = 0;
                                                            foreach (DataRowView drw1 in dw1)
                                                            {
                                                                if (ngay_ckymax_ddo == Convert.ToDateTime(drw1["NGAY_CUOI"]) && Convert.ToDecimal(drw1["BTHANG_ID"]) < idBThang2_Old)
                                                                {
                                                                    if (id_BThangTmp < Convert.ToDecimal(drw1["BTHANG_ID"]))
                                                                    {
                                                                        id_BThangTmp = Convert.ToDecimal(drw1["BTHANG_ID"]);
                                                                    }
                                                                }
                                                            }

                                                            foreach (DataRowView drw1 in dw1)
                                                            {
                                                                if (ngay_ckymax_ddo == Convert.ToDateTime(drw1["NGAY_CUOI"]) && Convert.ToDecimal(drw1["BTHANG_ID"]) == id_BThangTmp)
                                                                {
                                                                    if (Convert.ToDecimal(drw1["SLCT"]) == -100) continue;
                                                                    drw1["SLCT"] = Convert.ToDecimal(drw1["SLCT"]) + sl2;
                                                                    sl2 = Convert.ToDecimal(drw1["SLCT"]);
                                                                    if(sl2==0)
                                                                    {
                                                                        drw1["SLCT"] = -100;
                                                                        isAm = false;
                                                                       
                                                                    } 
                                                                    else if(sl2>0)                                                                    
                                                                        isAm = false;
                                                                    break;
                                                                }
                                                            }

                                                            //Xoa dong dinh muc hien tai                                                        
                                                            foreach (DataRowView drw1 in dw1)
                                                            {
                                                                if (ngay_ckymax_ddo == Convert.ToDateTime(drw1["NGAY_CUOI"]) && Convert.ToDecimal(drw1["BTHANG_ID"]) == idBThang2_Old)
                                                                {
                                                                    drw1["SLCT"] = -100;
                                                                    break;
                                                                }
                                                            }
                                                            if(sl2<0)                                                            
                                                                //Tiếp tục cập nhật
                                                                idBThang2_Old = id_BThangTmp;
                                                            
                                                                   
                                                        }
                                                    }    

                                                        
                                                }
                                            }
                                        }
                                        #endregion

                                        
                                    }
                                }
                            }
                        }
                    }
                }

                foreach (DataRowView drwSL4 in dwSL4)
                {
                    if (Convert.ToDecimal(drwSL4["SLCT"]) == -100)
                    {
                        drwSL4.Row.Delete();
                    }
                }
                return "";
            }
            catch (Exception ex)
            {
                return "PriceSpecification_12: " + ex.Message;
            }
        }
        public void updateNoi_Dung(DataRow drSL4, DataRow drHDN_CTCF, int i32DoiLichT12, DateTime dtNgayGhiCu, string strNgayCKy)
        {
            if (Convert.ToDateTime(drSL4["NGAY_CUOI"]) <= dtNgayGhiCu) drHDN_CTCF["NOI_DUNG"] = dtNgayGhiCu.ToString("dd/MM/yyyy");
            else drHDN_CTCF["NOI_DUNG"] = strNgayCKy;
        }

    }
}
