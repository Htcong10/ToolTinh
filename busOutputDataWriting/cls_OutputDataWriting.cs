using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using OutputDataObject;
using busBillingWF;
using System.Windows.Forms;
using System.Reflection;
using DbConnect.DB;
using CMISLibrary;
using Newtonsoft.Json;
using System.IO;
using System.Net;
using System.Configuration;
using Newtonsoft.Json.Linq;
using BillingLibrary;
using System.Diagnostics;

namespace busOutputDataWriting
{
    public class cls_OutputDataWriting
    {
        #region Attributes
        private List<HDN_HDON> lstHDon;
        private List<HDN_HDONCOSFI> lstHDonCosfi;
        private List<HDN_HDONCTIET> lstHDonCTiet;
        private List<GCS_CHISO> lstChiSo;
        private cls_HDN_HDON_Controller obj_HDN_HDON_Controller;
        private cls_HDN_HDONCOSFI_Controller obj_HDN_HDONCOSFI_Controller;
        private cls_HDN_HDONCTIET_Controller obj_HDN_HDONCTIET_Controller;
        private cls_GCS_CHISO_Controller obj_GCS_CHISO_Controller;
        private cls_GCS_CHISO_Controller objDKy;
        cls_HDN_BBAN_APGIA_DC_Controller obj_HDN_BBAN_APGIA_DC_Controller;
        cls_HDN_BBAN_DCHINH_Controller obj_HDN_BBAN_DCHINH_Controller;
        cls_HDN_BCS_CTO_DC_Controller obj_HDN_BCS_CTO_DC_Controller;
        cls_HDN_CHISO_DC_Controller obj_HDN_CHISO_DC_Controller;
        cls_HDN_DIEMDO_DC_Controller obj_HDN_DIEMDO_DC_Controller;
        cls_HDN_HDON_DC_Controller obj_HDN_HDON_DC_Controller;
        cls_HDN_HDONCOSFI_DC_Controller obj_HDN_HDONCOSFI_DC_Controller;
        cls_HDN_HDONCTIET_DC_Controller obj_HDN_HDONCTIET_DC_Controller;
        cls_HDN_KHANG_DC_Controller obj_HDN_KHANG_DC_Controller;
        cls_HDN_QHE_DDO_DC_Controller obj_HDN_QHE_DDO_DC_Controller;
        cls_Workflows_GCS objWF;

        #endregion
        public cls_OutputDataWriting()
        {
            obj_HDN_HDON_Controller = new cls_HDN_HDON_Controller();
            obj_HDN_HDONCOSFI_Controller = new cls_HDN_HDONCOSFI_Controller();
            obj_HDN_HDONCTIET_Controller = new cls_HDN_HDONCTIET_Controller();
            obj_GCS_CHISO_Controller = new cls_GCS_CHISO_Controller();
        }

        public static DataSet createDSInvoiceData()
        {
            DataSet ds = new DS_INVOICE_DATA();
            return ds;
        }
        public string InsertInvoiceData_PGT(string strMa_DViQLy, string strMa_SoGCS, string strTenDNhap, long lngCurrentLibID, long lngWorkflowID, short ky, short thang, short nam)
        {
            //Dùng để cập nhật trạng thái sổ trong trường hợp sổ toàn điểm đo phụ ghép tổng
            CMIS2 db = new CMIS2();
            try
            {
                DataSet ds = new GCS_SOGCS_XULY_Entity();
                DataRow dr = ds.Tables["GCS_SOGCS_XULY"].NewRow();
                dr["CURRENTLIBID"] = lngCurrentLibID;
                dr["KY"] = ky;
                dr["MA_CNANG"] = lngCurrentLibID.ToString();
                dr["MA_DVIQLY"] = strMa_DViQLy;
                dr["MA_SOGCS"] = strMa_SoGCS;
                dr["NAM"] = nam;
                dr["NGAY_SUA"] = DateTime.Now;
                dr["NGAY_TAO"] = DateTime.Now;
                dr["NGUOI_SUA"] = strTenDNhap;
                dr["NGUOI_TAO"] = strTenDNhap;
                dr["NGUOI_THIEN"] = strTenDNhap;
                dr["THANG"] = thang;
                dr["WORKFLOWID"] = lngWorkflowID;
                ds.Tables["GCS_SOGCS_XULY"].Rows.Add(dr);
                ds.AcceptChanges();
                objWF = new cls_Workflows_GCS(ds);
                objWF.CMIS2 = db;
                objWF.getCurrentState();
                objWF.getConfigState();

                if (objWF.insertOnSuccessOrDestroy("THD") == false)
                {
                    return "Lỗi khi Insert dữ liệu hóa đơn với mã sổ= " + strMa_SoGCS + ": " + objWF.strError;
                }
                obj_HDN_HDON_Controller = new cls_HDN_HDON_Controller();
                obj_HDN_HDON_Controller.CMIS2 = db;
                //obj_HDN_HDON_Controller.InsertNhatKy(strMa_DViQLy, "PSINH_HDON", "MA_SOGCS", strMa_SoGCS, ky, thang, nam, "Tính hóa đơn cho sổ", strTenDNhap, lngCurrentLibID.ToString());

                //Thuc hien submitchange
                db.DB.SubmitChanges(1);
                return "";
            }
            catch (Exception ex)
            {
                return "Lỗi khi Insert dữ liệu hóa đơn với mã sổ= " + strMa_SoGCS + ": " + ex.Message;
            }
            finally
            {
                db.ReleaseConnection();
            }
        }
        public string InsertInvoiceData(DataSet dsInvoiceData, DataSet dsCustomerData, string strMa_DViQLy, string strMa_SoGCS, string strTenDNhap, long lngCurrentLibID, long lngWorkflowID, short ky, short thang, short nam)
        {
            CMIS2 db = new CMIS2();
            try
            {
                DataSet ds = new GCS_SOGCS_XULY_Entity();
                DataRow dr = ds.Tables["GCS_SOGCS_XULY"].NewRow();
                dr["CURRENTLIBID"] = lngCurrentLibID;
                dr["KY"] = ky;
                dr["MA_CNANG"] = lngCurrentLibID.ToString();
                dr["MA_DVIQLY"] = strMa_DViQLy;
                dr["MA_SOGCS"] = strMa_SoGCS;
                dr["NAM"] = nam;
                dr["NGAY_SUA"] = DateTime.Now;
                dr["NGAY_TAO"] = DateTime.Now;
                dr["NGUOI_SUA"] = strTenDNhap;
                dr["NGUOI_TAO"] = strTenDNhap;
                dr["NGUOI_THIEN"] = strTenDNhap;
                dr["THANG"] = thang;
                dr["WORKFLOWID"] = lngWorkflowID;
                ds.Tables["GCS_SOGCS_XULY"].Rows.Add(dr);
                ds.AcceptChanges();
                string strResult = SetPropertiesForObject(dsInvoiceData, dsCustomerData, db);
                if (strResult != "") return strResult;

                if (lstHDon == null) return "Danh sach hoa don khong co";
                if (lstHDonCTiet == null) return "Danh sach hoa don chi tiet khong co";
                //Viet lai doan code nay de submitchange 1 lan
                var arrMaKHang = lstHDon.Select(c => c.MA_KHANG).Distinct();
                obj_HDN_HDON_Controller.CMIS2 = db;
                obj_HDN_HDONCTIET_Controller.CMIS2 = db;
                obj_HDN_HDONCOSFI_Controller.CMIS2 = db;
                obj_GCS_CHISO_Controller.CMIS2 = db;

                //Day gop theo tung bang 1
                //obj_HDN_HDON_Controller.lstHDon = lstHDon;
                //strResult = obj_HDN_HDON_Controller.InsertHDN_HDON();
                //if (strResult.Trim() != "")
                //{
                //    return "Lỗi khi tạo dữ liệu hoá đơn: " + strResult;
                //}

                //obj_HDN_HDONCTIET_Controller.lstCTiet = lstHDonCTiet;
                //strResult = obj_HDN_HDONCTIET_Controller.InsertHDN_HDONCTIET();
                //if (strResult.Trim() != "")
                //{
                //    return "Lỗi khi tạo dữ liệu hoá đơn chi tiết: " + strResult;
                //}

                //if (lstHDonCosfi != null && lstHDonCosfi.Count > 0)
                //{
                //    obj_HDN_HDONCOSFI_Controller.lstCosfi = lstHDonCosfi;
                //    strResult = obj_HDN_HDONCOSFI_Controller.InsertHDN_HDONCOSFI();
                //    if (strResult.Trim() != "")
                //    {
                //        return "Lỗi khi tạo dữ liệu hoá đơn cosfi: " + strResult;
                //    }
                //}

                //obj_GCS_CHISO_Controller.LstInfo = lstChiSo;
                //strResult = obj_GCS_CHISO_Controller.UpdateList();
                //if (strResult.Trim() != "")
                //{
                //    return "Lỗi khi tạo dữ liệu chỉ số: " + strResult;
                //}

                //Tach ra theo tung ma khach hang
                foreach (var x in arrMaKHang)
                {
                    if (x == "PD01000010568")
                    {
                    }
                    string strTemp = "";
                    List<HDN_HDON> lstHDon_Temp = lstHDon.Where(c => c.MA_KHANG == x).ToList();
                    List<HDN_HDONCTIET> lstHDonCTiet_Temp = lstHDonCTiet.Where(c => c.MA_KHANG == x).ToList();
                    List<GCS_CHISO> lstChiSo_Temp = lstChiSo.Where(c => c.MA_KHANG == x).ToList();
                    obj_HDN_HDON_Controller.lstHDon = lstHDon_Temp;
                    obj_HDN_HDONCTIET_Controller.lstCTiet = lstHDonCTiet_Temp;
                    obj_GCS_CHISO_Controller.LstInfo = new List<GCS_CHISO>();
                    obj_GCS_CHISO_Controller.LstInfo = lstChiSo_Temp;
                    strTemp += obj_HDN_HDON_Controller.InsertHDN_HDON();
                    strTemp += obj_HDN_HDONCTIET_Controller.InsertHDN_HDONCTIET();
                    if (lstHDonCosfi != null && lstHDonCosfi.Count > 0)
                    {

                        List<HDN_HDONCOSFI> lstHDonCosfi_Temp = lstHDonCosfi.Where(c => c.MA_KHANG == x).ToList();
                        if (lstHDonCosfi_Temp != null && lstHDonCosfi_Temp.Count > 0)
                        {
                            foreach (HDN_HDONCOSFI hcosfi in lstHDonCosfi_Temp)
                            {
                                if (lstHDon_Temp != null && lstHDon_Temp.Count > 0)
                                {
                                    var hdon = lstHDon_Temp.Where(c => c.LOAI_HDON == "VC");
                                    if (hdon != null && hdon.Count() > 0)
                                    {
                                        hcosfi.ID_HDON = hdon.First().ID_HDON;
                                    }
                                }
                            }
                        }
                        obj_HDN_HDONCOSFI_Controller.lstCosfi = lstHDonCosfi_Temp;
                        strTemp = strTemp + obj_HDN_HDONCOSFI_Controller.InsertHDN_HDONCOSFI();
                        lstHDonCosfi = lstHDonCosfi.Where(c => c.MA_KHANG != x).ToList();
                    }
                    strTemp += obj_GCS_CHISO_Controller.UpdateList();

                    if (strTemp != "")
                    {
                        //Bao loi insert hoa don
                        return "Lỗi Insert hóa đơn, mã sổ = " + strMa_SoGCS + " mã khách hàng = " + x + ": " + strTemp;
                    }

                    lstHDon = lstHDon.Where(c => c.MA_KHANG != x).ToList();
                    lstHDonCTiet = lstHDonCTiet.Where(c => c.MA_KHANG != x).ToList();
                    lstChiSo = lstChiSo.Where(c => c.MA_KHANG != x).ToList();
                }
                if (lstChiSo != null && lstChiSo.Count > 0)
                {
                    obj_GCS_CHISO_Controller.LstInfo = lstChiSo;
                    obj_GCS_CHISO_Controller.UpdateList();
                }
                //Insert luong so ghi chi so
                objWF = new cls_Workflows_GCS(ds);
                objWF.CMIS2 = db;
                objWF.getCurrentState();
                objWF.getConfigState();

                if (objWF.insertOnSuccessOrDestroy("THD") == false)
                {
                    return "Lỗi khi Insert dữ liệu hóa đơn với mã sổ= " + strMa_SoGCS + ": " + objWF.strError;
                }
                //obj_HDN_HDON_Controller.InsertNhatKy(strMa_DViQLy, "PSINH_HDON", "MA_SOGCS", strMa_SoGCS, ky, thang, nam, "Tính hóa đơn cho sổ", strTenDNhap, lngCurrentLibID.ToString());

                //Thuc hien submitchange
                db.DB.SubmitChanges(1);
                return "";
            }
            catch (Exception ex)
            {
                return "Lỗi khi Insert dữ liệu hóa đơn với mã sổ= " + strMa_SoGCS + ": " + ex.Message;
            }
            finally
            {
                db.ReleaseConnection();
            }
        }

        public string InsertInvoiceData_1(DataSet dsInvoiceData, DataSet dsCustomerData)
        {
            CMIS2 db = new CMIS2();
            try
            {
                string strResult = SetPropertiesForMultiObject(dsInvoiceData, dsCustomerData, db);
                if (strResult.Trim().Length > 0) return strResult;

                obj_HDN_HDON_DC_Controller.CMIS2 = db;
                strResult += obj_HDN_HDON_DC_Controller.Insert();

                obj_HDN_BBAN_DCHINH_Controller.CMIS2 = db;
                strResult += obj_HDN_BBAN_DCHINH_Controller.Insert();

                obj_HDN_BCS_CTO_DC_Controller.CMIS2 = db;
                strResult += obj_HDN_BCS_CTO_DC_Controller.InsertList();

                obj_HDN_CHISO_DC_Controller.CMIS2 = db;
                strResult += obj_HDN_CHISO_DC_Controller.InsertList();

                obj_HDN_DIEMDO_DC_Controller.CMIS2 = db;
                strResult += obj_HDN_DIEMDO_DC_Controller.InsertList();

                obj_HDN_KHANG_DC_Controller.CMIS2 = db;
                strResult += obj_HDN_KHANG_DC_Controller.InsertList();

                obj_HDN_BBAN_APGIA_DC_Controller.CMIS2 = db;
                strResult += obj_HDN_BBAN_APGIA_DC_Controller.InsertList();

                obj_HDN_HDONCTIET_DC_Controller.CMIS2 = db;
                strResult += obj_HDN_HDONCTIET_DC_Controller.InsertList();


                if (obj_HDN_HDONCOSFI_DC_Controller != null && obj_HDN_HDONCOSFI_DC_Controller.LstInfo.Count > 0)
                {
                    obj_HDN_HDONCOSFI_DC_Controller.CMIS2 = db;
                    strResult += obj_HDN_HDONCOSFI_DC_Controller.InsertList();
                }

                if (obj_HDN_QHE_DDO_DC_Controller != null && obj_HDN_QHE_DDO_DC_Controller.LstInfo.Count > 0)
                {
                    obj_HDN_QHE_DDO_DC_Controller.CMIS2 = db;
                    strResult += obj_HDN_QHE_DDO_DC_Controller.InsertList();
                }

                if (objDKy != null && objDKy.LstInfo.Count > 0)
                {
                    objDKy.CMIS2 = db;
                    strResult += objDKy.InsertList();
                }

                if (strResult.Trim().Length == 0)
                {
                    db.DB.SubmitChanges(1);
                }
                return strResult;
            }
            catch (Exception ex)
            {
                return "Lỗi khi Insert dữ liệu: " + ex.Message;
            }
            finally
            {
                db.ReleaseConnection();
            }
        }

        public string InsertInvoiceData_2(DataSet dsInvoiceData, DataSet dsCustomerData, string strMa_DViQLy, string strMa_SoGCS, short ky, short thang, short nam)
        {
            CMIS2 db = new CMIS2();
            try
            {
                //DataSet ds = new GCS_SOGCS_XULY_Entity();
                //DataRow dr = ds.Tables["GCS_SOGCS_XULY"].NewRow();
                //dr["CURRENTLIBID"] = lngCurrentLibID;
                //dr["KY"] = ky;
                //dr["MA_CNANG"] = lngCurrentLibID.ToString();
                //dr["MA_DVIQLY"] = strMa_DViQLy;
                //dr["MA_SOGCS"] = strMa_SoGCS;
                //dr["NAM"] = nam;
                //dr["NGAY_SUA"] = DateTime.Now;
                //dr["NGAY_TAO"] = DateTime.Now;
                //dr["NGUOI_SUA"] = strTenDNhap;
                //dr["NGUOI_TAO"] = strTenDNhap;
                //dr["NGUOI_THIEN"] = strTenDNhap;
                //dr["THANG"] = thang;
                //dr["WORKFLOWID"] = lngWorkflowID;
                //ds.Tables["GCS_SOGCS_XULY"].Rows.Add(dr);
                //ds.AcceptChanges();
                string strResult = SetPropertiesForObject(dsInvoiceData, dsCustomerData, db);
                if (strResult != "") return strResult;

                if (lstHDon == null) return "Danh sach hoa don khong co";
                if (lstHDonCTiet == null) return "Danh sach hoa don chi tiet khong co";

                //Viet lai doan code nay de submitchange 1 lan
                var arrMaKHang = lstHDon.Select(c => c.MA_KHANG).Distinct();
                obj_HDN_HDON_Controller.CMIS2 = db;
                obj_HDN_HDONCTIET_Controller.CMIS2 = db;
                obj_HDN_HDONCOSFI_Controller.CMIS2 = db;
                obj_GCS_CHISO_Controller.CMIS2 = db;

                //Tach ra theo tung ma khach hang
                foreach (var x in arrMaKHang)
                {
                    string strTemp = "";
                    List<HDN_HDON> lstHDon_Temp = lstHDon.Where(c => c.MA_KHANG == x).ToList();
                    List<HDN_HDONCTIET> lstHDonCTiet_Temp = lstHDonCTiet.Where(c => c.MA_KHANG == x).ToList();
                    List<GCS_CHISO> lstChiSo_Temp = lstChiSo.Where(c => c.MA_KHANG == x).ToList();
                    obj_HDN_HDON_Controller.lstHDon = lstHDon_Temp;
                    obj_HDN_HDONCTIET_Controller.lstCTiet = lstHDonCTiet_Temp;
                    obj_GCS_CHISO_Controller.LstInfo = new List<GCS_CHISO>();
                    obj_GCS_CHISO_Controller.LstInfo = lstChiSo_Temp;
                    strTemp += obj_HDN_HDON_Controller.InsertHDN_HDON();
                    strTemp += obj_HDN_HDONCTIET_Controller.InsertHDN_HDONCTIET();
                    if (lstHDonCosfi != null && lstHDonCosfi.Count > 0)
                    {
                        List<HDN_HDONCOSFI> lstHDonCosfi_Temp = lstHDonCosfi.Where(c => c.MA_KHANG == x).ToList();
                        if (lstHDonCosfi_Temp != null && lstHDonCosfi_Temp.Count > 0)
                        {
                            foreach (HDN_HDONCOSFI hcosfi in lstHDonCosfi_Temp)
                            {
                                if (lstHDon_Temp != null && lstHDon_Temp.Count > 0)
                                {
                                    var hdon = lstHDon_Temp.Where(c => c.LOAI_HDON == "VC");
                                    if (hdon != null && hdon.Count() > 0)
                                    {
                                        hcosfi.ID_HDON = hdon.First().ID_HDON;
                                    }
                                }
                            }
                        }
                        obj_HDN_HDONCOSFI_Controller.lstCosfi = lstHDonCosfi_Temp;
                        strTemp = strTemp + obj_HDN_HDONCOSFI_Controller.InsertHDN_HDONCOSFI();
                        lstHDonCosfi = lstHDonCosfi.Where(c => c.MA_KHANG != x).ToList();
                    }
                    strTemp += obj_GCS_CHISO_Controller.UpdateList();

                    if (strTemp != "")
                    {
                        //Bao loi insert hoa don
                        return "Lỗi Insert hóa đơn, mã sổ = " + strMa_SoGCS + " mã khách hàng = " + x + ": " + strTemp;
                    }

                    lstHDon = lstHDon.Where(c => c.MA_KHANG != x).ToList();
                    lstHDonCTiet = lstHDonCTiet.Where(c => c.MA_KHANG != x).ToList();
                    lstChiSo = lstChiSo.Where(c => c.MA_KHANG != x).ToList();
                }

                //Insert luong so ghi chi so
                //objWF = new cls_Workflows_GCS(ds);
                //objWF.CMIS2 = db;
                //objWF.getCurrentState();
                //objWF.getConfigState();

                //if (objWF.insertOnSuccessOrDestroy("THD") == false)
                //{
                //    return "Lỗi khi Insert dữ liệu hóa đơn với mã sổ= " + strMa_SoGCS + ": " + objWF.strError;
                //}

                //Thuc hien submitchange
                db.DB.SubmitChanges(1);
                return "";
            }
            catch (Exception ex)
            {
                return "Lỗi khi Insert dữ liệu hóa đơn với mã sổ= " + strMa_SoGCS + ": " + ex.Message;
            }
            finally
            {
                db.ReleaseConnection();
            }
        }

        protected string SetPropertiesForObject(DataSet dsInvoiceData, DataSet dsCustomerData, CMIS2 db)
        {
            //String strddo = "";
            //HDN_HDON
            if (dsInvoiceData == null) return "NoDataFound!- Không tìm thấy dữ liệu!";
            if (dsInvoiceData.Tables["HDN_HDON"] == null)
            {
                return "NoInvoiceDataFound!- Không tìm thấy dữ liệu hóa đơn!";
            }
            if (dsInvoiceData.Tables["HDN_HDONCTIET"] == null)
            {
                return "NoInvoiceDataFound!- Không tìm thấy dữ liệu hóa đơn chi tiết!";
            }
            //if (dsInvoiceData.Tables["HDN_HDON"] == null || dsInvoiceData.Tables["HDN_HDON"].Rows.Count == 0) return "NoInvoiceDataFound!- Không tìm thấy dữ liệu hóa đơn!";
            //if (dsInvoiceData.Tables["HDN_HDONCTIET"] == null || dsInvoiceData.Tables["HDN_HDONCTIET"].Rows.Count == 0) return "NoInvoiceDataFound!- Không tìm thấy dữ liệu hóa đơn chi tiết!";
            lstHDon = new List<HDN_HDON>();
            lstHDonCosfi = new List<HDN_HDONCOSFI>();
            lstHDonCTiet = new List<HDN_HDONCTIET>();
            lstChiSo = new List<GCS_CHISO>();
            try
            {
                long lngIDHDon = 0;
                DataTable dtTemp = dsInvoiceData.Tables["HDN_HDONCTIET"].Copy();
                DataTable dtCosfi = dsInvoiceData.Tables["HDN_HDONCOSFI"].Copy();
                foreach (DataRow row in dsInvoiceData.Tables["HDN_HDON"].Rows)
                {
                    #region HDN_HDON
                    HDN_HDON info = new HDN_HDON();

                    if (row["CHI_TIET"].ToString().Trim().Length > 0)
                        info.CHI_TIET = Convert.ToInt16(row["CHI_TIET"].ToString());
                    if (row["COSFI"].ToString().Trim().Length > 0)
                        info.COSFI = Convert.ToDecimal(row["COSFI"].ToString());
                    info.DCHI_KHANG = row["DCHI_KHANG"].ToString();
                    info.DCHI_KHANGTT = row["DCHI_KHANGTT"].ToString();
                    if (row["DIEN_TTHU"].ToString().Trim().Length > 0)
                        info.DIEN_TTHU = Convert.ToDecimal(row["DIEN_TTHU"].ToString());
                    else
                        return "Lỗi dữ liệu HDN_HDON: DIEN_TTHU = null";
                    if (row["ID_BBANPHANH"].ToString().Trim().Length > 0)
                        info.ID_BBANPHANH = Convert.ToInt64(row["ID_BBANPHANH"].ToString());
                    //if (row["ID_HDON"].ToString().Trim().Length > 0 || row["ID_HDON"].ToString() != "0")
                    //    info.ID_HDON = Convert.ToInt64(row["ID_HDON"].ToString());
                    //else
                    obj_HDN_HDON_Controller.CMIS2 = db;
                    info.ID_HDON = obj_HDN_HDON_Controller.getID_HDON();
                    lngIDHDon = info.ID_HDON;
                    if (row["KCOSFI"].ToString().Trim().Length > 0)
                        info.KCOSFI = Convert.ToDecimal(row["KCOSFI"].ToString());
                    info.KIHIEU_SERY = row["KIHIEU_SERY"].ToString();
                    if (row["KY"].ToString().Trim().Length > 0)
                        info.KY = Convert.ToInt16(row["KY"].ToString());
                    else
                        return "Lỗi dữ liệu HDN_HDON: KY = null";
                    if (row["LOAI_KHANG"].ToString().Trim().Length > 0)
                        info.LOAI_KHANG = Convert.ToInt16(row["LOAI_KHANG"]);
                    else
                        return "Lỗi dữ liệu HDN_HDON: LOAI_KHANG = null";
                    info.DCHI_TTOAN = row["DCHI_TTOAN"].ToString();
                    info.MANHOM_KHANG = row["MANHOM_KHANG"].ToString();
                    info.MA_LOAIDN = row["MA_LOAIDN"].ToString();
                    info.MA_PTTT = row["MA_PTTT"].ToString();
                    info.LOAI_HDON = row["LOAI_HDON"].ToString();
                    info.MA_CNANG = row["MA_CNANG"].ToString();
                    info.MA_DVIQLY = row["MA_DVIQLY"].ToString();
                    info.MA_HTTT = row["MA_HTTT"].ToString();
                    info.MA_KHANG = row["MA_KHANG"].ToString();
                    info.MA_KHANGTT = row["MA_KHANGTT"].ToString();
                    info.MA_KVUC = row["MA_KVUC"].ToString();
                    info.MA_NHANG = row["MA_NHANG"].ToString();
                    info.MA_NVIN = row["MA_NVIN"].ToString();
                    info.MA_NVPHANH = row["MA_NVPHANH"].ToString();
                    info.MA_SOGCS = row["MA_SOGCS"].ToString();
                    info.MA_TO = row["MA_TO"].ToString();
                    info.MASO_THUE = row["MASO_THUE"].ToString();
                    if (row["NAM"].ToString().Trim().Length > 0)
                        info.NAM = Convert.ToInt16(row["NAM"].ToString());
                    else
                        return "Lỗi dữ liệu HDN_HDON: NAM = null";
                    if (row["NGAY_CKY"].ToString().Trim().Length > 0)
                        info.NGAY_CKY = Convert.ToDateTime(row["NGAY_CKY"].ToString());
                    else
                        return "Lỗi dữ liệu HDN_HDON: NGAY_CKY = null";
                    if (row["NGAY_DKY"].ToString().Trim().Length > 0)
                        info.NGAY_DKY = Convert.ToDateTime(row["NGAY_DKY"].ToString());
                    else
                        return "Lỗi dữ liệu HDN_HDON: NGAY_DKY = null";
                    if (row["NGAY_IN"].ToString().Trim().Length > 0)
                        info.NGAY_IN = Convert.ToDateTime(row["NGAY_IN"].ToString());
                    if (row["NGAY_PHANH"].ToString().Trim().Length > 0)
                        info.NGAY_PHANH = Convert.ToDateTime(row["NGAY_PHANH"].ToString());
                    if (row["NGAY_SUA"].ToString().Trim().Length > 0)
                        info.NGAY_SUA = Convert.ToDateTime(row["NGAY_SUA"].ToString());
                    else
                        return "Lỗi dữ liệu HDN_HDON: NGAY_SUA = null";
                    if (row["NGAY_TAO"].ToString().Trim().Length > 0)
                        info.NGAY_TAO = Convert.ToDateTime(row["NGAY_TAO"].ToString());
                    else
                        return "Lỗi dữ liệu HDN_HDON: NGAY_TAO = null";
                    info.NGUOI_SUA = row["NGUOI_SUA"].ToString();
                    info.NGUOI_TAO = row["NGUOI_TAO"].ToString();
                    info.SO_CTO = row["SO_CTO"].ToString();
                    if (row["SO_HO"].ToString().Trim().Length > 0)
                        info.SO_HO = Convert.ToDecimal(row["SO_HO"].ToString());
                    else
                        return "Lỗi dữ liệu HDN_HDON: SO_HO = null";
                    if (row["SO_LANIN"].ToString().Trim().Length > 0)
                        info.SO_LANIN = Convert.ToInt16(row["SO_LANIN"].ToString());
                    info.SO_SERY = row["SO_SERY"].ToString();
                    if (row["SO_TIEN"].ToString().Trim().Length > 0)
                        info.SO_TIEN = Convert.ToDecimal(row["SO_TIEN"].ToString());
                    else
                        return "Lỗi dữ liệu HDN_HDON: SO_TIEN = null";
                    info.STT = row["STT"].ToString();
                    if (row["STT_IN"].ToString().Trim().Length > 0)
                        info.STT_IN = Convert.ToInt32(row["STT_IN"].ToString());
                    info.TEN_KHANG = row["TEN_KHANG"].ToString();
                    info.TEN_KHANGTT = row["TEN_KHANGTT"].ToString();
                    if (row["THANG"].ToString().Trim().Length > 0)
                        info.THANG = Convert.ToInt16(row["THANG"].ToString());
                    else
                        return "Lỗi dữ liệu HDN_HDON: THANG = null";
                    if (row["TIEN_GTGT"].ToString().Trim().Length > 0)
                        info.TIEN_GTGT = Convert.ToDecimal(row["TIEN_GTGT"].ToString());
                    else
                        return "Lỗi dữ liệu HDN_HDON: TIEN_GTGT = null";
                    info.TKHOAN_KHANG = row["TKHOAN_KHANG"].ToString();
                    if (row["TONG_TIEN"].ToString().Trim().Length > 0)
                        info.TONG_TIEN = Convert.ToDecimal(row["TONG_TIEN"].ToString());
                    else
                        return "Lỗi dữ liệu HDN_HDON: TONG_TIEN = null";
                    if (row["TYLE_THUE"].ToString().Trim().Length > 0)
                        info.TYLE_THUE = Convert.ToDecimal(row["TYLE_THUE"].ToString());
                    else
                        return "Lỗi dữ liệu HDN_HDON: TYLE_THUE = null";
                    //long lngIDHDong = Convert.ToInt64(dsCustomerData.Tables["HDG_KHACH_HANG"].Select("MA_KHANG='" + info.MA_KHANG + "'")[0]["ID_HDONG"]);
                    //string strDChiTToan = (new cls_HDG_HOP_DONG_Controller()).getDCHI_TTOAN(info.MA_DVIQLY, lngIDHDong, info.NGAY_CKY.Date);
                    //if (strDChiTToan != null && strDChiTToan.Trim().Length > 0)
                    //    info.DCHI_TTOAN = strDChiTToan;
                    //else
                    //    return "Không tìm thấy hợp đồng tương ứng với mã khách hàng " + info.MA_KHANG;
                    if (info.DCHI_TTOAN == null || info.DCHI_TTOAN.Trim().Length == 0)
                        return "Không tìm thấy hợp đồng tương ứng với mã khách hàng " + info.MA_KHANG;
                    info.TIEN_TD = Utility.DecimalDbnull(row["TIEN_TD"]);
                    info.THUE_TD = Utility.DecimalDbnull(row["THUE_TD"]);
                    info.TIEN_VC = Utility.DecimalDbnull(row["TIEN_VC"]);
                    info.THUE_VC = Utility.DecimalDbnull(row["THUE_VC"]);
                    info.DTHOAI = row["DTHOAI"].ToString();
                    lstHDon.Add(info);

                    #endregion

                    #region HDN_HDONCTIET
                    if (info.MA_KHANG == "PD01000010568")
                    {
                    }
                    DataRow[] arrHDonCTiet = null;
                    if (dtTemp != null && dtTemp.Rows.Count != 0)
                    {
                        List<DataRow> arrTemp = new List<DataRow>();
                        if (info.LOAI_HDON == "TD")
                        {
                            foreach (DataRow drTemp in dtTemp.Rows)
                            {
                                if (drTemp["MA_KHANG"].ToString().Trim() == info.MA_KHANG && "BT;CD;TD;KT".Contains(drTemp["BCS"].ToString().Trim()))
                                {
                                    arrTemp.Add(drTemp);
                                }
                            }
                            //arrHDonCTiet = dtTemp.Select("MA_KHANG='" + info.MA_KHANG + "' AND BCS IN ('BT','CD','TD','KT')");
                        }
                        if (info.LOAI_HDON == "VC")
                        {
                            foreach (DataRow drTemp in dtTemp.Rows)
                            {
                                if (drTemp["MA_KHANG"].ToString().Trim() == info.MA_KHANG && drTemp["BCS"].ToString().Trim() == "VC")
                                {
                                    arrTemp.Add(drTemp);
                                }
                            }
                            //arrHDonCTiet = dtTemp.Select("MA_KHANG='" + info.MA_KHANG + "' AND BCS ='VC'");
                        }

                        if (info.LOAI_HDON == "TC")
                        {
                            foreach (DataRow drTemp in dtTemp.Rows)
                            {
                                if (drTemp["MA_KHANG"].ToString().Trim() == info.MA_KHANG && "BT;CD;TD;KT;VC".Contains(drTemp["BCS"].ToString().Trim()))
                                {
                                    arrTemp.Add(drTemp);
                                }
                            }
                            //arrHDonCTiet = dtTemp.Select("MA_KHANG='" + info.MA_KHANG + "' AND BCS IN ('BT','CD','TD','KT','VC')");
                        }
                        arrHDonCTiet = arrTemp.ToArray();
                        var chitiet = dtTemp.AsEnumerable().Except(arrHDonCTiet);
                        dtTemp = (chitiet != null && chitiet.Count() > 0) ? chitiet.CopyToDataTable() : null;
                    }
                    if (arrHDonCTiet != null && arrHDonCTiet.Length > 0)
                    {
                        obj_HDN_HDONCTIET_Controller.CMIS2 = db;
                        foreach (DataRow dr in arrHDonCTiet)
                        {
                            HDN_HDONCTIET infoHDCT = new HDN_HDONCTIET();
                            infoHDCT.BCS = dr["BCS"].ToString();
                            if (dr["DIEN_TTHU"].ToString().Trim().Length > 0)
                                infoHDCT.DIEN_TTHU = Convert.ToDecimal(dr["DIEN_TTHU"]);
                            else
                                return "Lỗi dữ liệu HDN_HDONCTIET: DIEN_TTHU = null";
                            infoHDCT.DINH_MUC = dr["DINH_MUC"].ToString();
                            if (dr["DON_GIA"].ToString().Trim().Length > 0)
                                infoHDCT.DON_GIA = Convert.ToDecimal(dr["DON_GIA"]);
                            if (dr["ID_CHISO"].ToString().Trim().Length > 0)
                                infoHDCT.ID_CHISO = Convert.ToInt64(dr["ID_CHISO"]);
                            else
                                return "Lỗi dữ liệu HDN_HDONCTIET: ID_CHISO = null";
                            infoHDCT.ID_HDON = info.ID_HDON;
                            //if (dr["ID_HDONCTIET"].ToString().Trim().Length > 0)
                            //    infoHDCT.ID_HDONCTIET = Convert.ToInt64(dr["ID_HDONCTIET"]);
                            //else
                            //    return "Lỗi dữ liệu HDN_HDONCTIET: ID_HDONCTIET = null";
                            infoHDCT.ID_HDONCTIET = obj_HDN_HDONCTIET_Controller.getID_HDON();
                            if (dr["KY"].ToString().Trim().Length > 0)
                                infoHDCT.KY = Convert.ToInt16(dr["KY"]);
                            else
                                return "Lỗi dữ liệu HDN_HDONCTIET: KY = null";
                            if (dr["LOAI_KHANG"].ToString().Trim().Length > 0)
                                infoHDCT.LOAI_KHANG = Convert.ToInt16(dr["LOAI_KHANG"].ToString());
                            if (dr["LOAI_DDO"].ToString().Trim().Length > 0)
                                infoHDCT.LOAI_DDO = Convert.ToInt16(dr["LOAI_DDO"].ToString());
                            if (dr["SO_PHA"].ToString().Trim().Length > 0)
                                infoHDCT.SO_PHA = Convert.ToInt16(dr["SO_PHA"].ToString());
                            infoHDCT.LOAI_DMUC = dr["LOAI_DMUC"].ToString();
                            infoHDCT.MA_CAPDA = dr["MA_CAPDA"].ToString();
                            infoHDCT.MA_CNANG = dr["MA_CNANG"].ToString();
                            infoHDCT.MA_DDO = dr["MA_DDO"].ToString();
                            infoHDCT.MA_DVIQLY = dr["MA_DVIQLY"].ToString();
                            infoHDCT.MA_KHANG = dr["MA_KHANG"].ToString();
                            infoHDCT.MA_KVUC = dr["MA_KVUC"].ToString();
                            infoHDCT.MA_LO = dr["MA_LO"].ToString();
                            infoHDCT.MA_NGIA = dr["MA_NGIA"].ToString();
                            infoHDCT.MA_NHOMNN = dr["MA_NHOMNN"].ToString();
                            infoHDCT.MA_NN = dr["MA_NN"].ToString();
                            infoHDCT.MA_SOGCS = dr["MA_SOGCS"].ToString();
                            infoHDCT.MA_TO = dr["MA_TO"].ToString();
                            infoHDCT.MA_TRAM = dr["MA_TRAM"].ToString();
                            if (dr["NAM"].ToString().Trim().Length > 0)
                                infoHDCT.NAM = Convert.ToInt16(dr["NAM"]);
                            else
                                return "Lỗi dữ liệu HDN_HDONCTIET: NAM = null";
                            if (dr["NGAY_APDUNG"].ToString().Trim().Length > 0)
                                infoHDCT.NGAY_APDUNG = Convert.ToDateTime(dr["NGAY_APDUNG"]);
                            if (dr["NGAY_SUA"].ToString().Trim().Length > 0)
                                infoHDCT.NGAY_SUA = Convert.ToDateTime(dr["NGAY_SUA"]);
                            else
                                return "Lỗi dữ liệu HDN_HDONCTIET: NGAY_SUA = null";
                            if (dr["NGAY_TAO"].ToString().Trim().Length > 0)
                                infoHDCT.NGAY_TAO = Convert.ToDateTime(dr["NGAY_TAO"]);
                            else
                                return "Lỗi dữ liệu HDN_HDONCTIET: NGAY_TAO = null";
                            infoHDCT.NGUOI_SUA = dr["NGUOI_SUA"].ToString();
                            infoHDCT.NGUOI_TAO = dr["NGUOI_TAO"].ToString();
                            infoHDCT.SO_CTO = dr["SO_CTO"].ToString();
                            if (dr["SO_TIEN"].ToString().Trim().Length > 0)
                                infoHDCT.SO_TIEN = Convert.ToDecimal(dr["SO_TIEN"]);
                            else
                                return "Lỗi dữ liệu HDN_HDONCTIET: SO_TIEN = null";
                            infoHDCT.SO_TIEN = Convert.ToDecimal(dr["SO_TIEN"]);
                            infoHDCT.STT = dr["STT"].ToString();
                            infoHDCT.TGIAN_BDIEN = dr["TGIAN_BDIEN"].ToString();
                            if (dr["THANG"].ToString().Trim().Length > 0)
                                infoHDCT.THANG = Convert.ToInt16(dr["THANG"]);
                            else
                                return "Lỗi dữ liệu HDN_HDONCTIET: THANG = null";
                            lstHDonCTiet.Add(infoHDCT);
                        }
                    }
                    #endregion

                    #region HDN_HDONCOSFI
                    DataRow[] arrHDonCosfi = null;
                    if (dtCosfi != null && dtCosfi.Rows.Count > 0)
                    {
                        arrHDonCosfi = dtCosfi.Select("MA_KHANG='" + info.MA_KHANG + "'");
                    }

                    //MessageBox.Show("Số bản ghi: " + Convert.ToString(arrHDonCosfi.Count()), "Thông báo", MessageBoxButtons.OK, MessageBoxIcon.Information);
                    if (arrHDonCosfi != null && arrHDonCosfi.Length > 0)
                    {
                        var cosfi = dtCosfi.AsEnumerable().Except(arrHDonCosfi);
                        dtCosfi = (cosfi != null && cosfi.Count() > 0) ? cosfi.CopyToDataTable() : null;

                        obj_HDN_HDONCOSFI_Controller.CMIS2 = db;
                        foreach (DataRow drCF in arrHDonCosfi)
                        {
                            //strddo = Convert.ToString(drCF["MA_DDO"]);
                            //Bổ sung đoạn kiểm soát MA_SOGCS null
                            if (drCF["MA_SOGCS"].ToString().Trim().Length == 0)
                            {
                                var other = arrHDonCosfi.Where(c => c.Field<string>("MA_DDO") != drCF["MA_DDO"] && c.Field<string>("MA_SOGCS") != "").ToArray();
                                if (other != null && other.Length > 0)
                                {
                                    foreach (DataColumn col in drCF.Table.Columns)
                                    {
                                        drCF[col.ColumnName] = drCF[col.ColumnName].ToString().Trim().Length == 0 ? other[0][col.ColumnName] : drCF[col.ColumnName];
                                    }
                                }
                            }
                            HDN_HDONCOSFI infoHDCF = new HDN_HDONCOSFI();
                            if (drCF["COSFI"].ToString().Trim().Length > 0)
                                infoHDCF.COSFI = Convert.ToDecimal(drCF["COSFI"].ToString());
                            else
                                return "Lỗi dữ liệu HDN_HDONCOSFI: COSFI = null";
                            //MessageBox.Show("1");
                            if (drCF["HUU_CONG"].ToString().Trim().Length > 0)
                                infoHDCF.HUU_CONG = Convert.ToDecimal(drCF["HUU_CONG"].ToString());
                            else
                                return "Lỗi dữ liệu HDN_HDONCOSFI: HUU_CONG = null";
                            //MessageBox.Show("2");
                            infoHDCF.ID_HDON = info.ID_HDON;
                            //if (drCF["ID_HDONCOSFI"].ToString().Trim().Length > 0)
                            //    infoHDCF.ID_HDONCOSFI = Convert.ToInt64(drCF["ID_HDONCOSFI"].ToString());
                            //else
                            //    return "Lỗi dữ liệu HDN_HDONCOSFI: ID_HDONCOSFI = null";
                            infoHDCF.ID_HDONCOSFI = obj_HDN_HDONCOSFI_Controller.getID_HDON();
                            if (drCF["KCOSFI"].ToString().Trim().Length > 0)
                                infoHDCF.KCOSFI = Convert.ToDecimal(drCF["KCOSFI"].ToString());
                            else
                                return "Lỗi dữ liệu HDN_HDONCOSFI: KCOSFI = null";
                            //MessageBox.Show("3");
                            if (drCF["KIMUA_CSPK"].ToString().Trim().Length > 0)
                                infoHDCF.KIMUA_CSPK = Convert.ToInt16(drCF["KIMUA_CSPK"].ToString());
                            else
                                return "Lỗi dữ liệu HDN_HDONCOSFI: KIMUA_CSPK = null";
                            if (drCF["KY"].ToString().Trim().Length > 0)
                                infoHDCF.KY = Convert.ToInt16(drCF["KY"].ToString());
                            else
                                return "Lỗi dữ liệu HDN_HDONCOSFI: KY = null";
                            //MessageBox.Show("4");
                            infoHDCF.MA_CNANG = drCF["MA_CNANG"].ToString();
                            infoHDCF.MA_DDO = drCF["MA_DDO"].ToString();
                            infoHDCF.MA_DVIQLY = drCF["MA_DVIQLY"].ToString();
                            infoHDCF.MA_KHANG = drCF["MA_KHANG"].ToString();
                            infoHDCF.MA_KVUC = drCF["MA_KVUC"].ToString();
                            infoHDCF.MA_LO = drCF["MA_LO"].ToString();
                            infoHDCF.MA_SOGCS = drCF["MA_SOGCS"].ToString();
                            infoHDCF.MA_TO = drCF["MA_TO"].ToString();
                            infoHDCF.MA_TRAM = drCF["MA_TRAM"].ToString();
                            //MessageBox.Show("5");
                            if (drCF["NAM"].ToString().Trim().Length > 0)
                                infoHDCF.NAM = Convert.ToInt16(drCF["NAM"].ToString());
                            else
                                return "Lỗi dữ liệu HDN_HDONCOSFI: NAM = null";
                            if (drCF["NGAY_SUA"].ToString().Trim().Length > 0)
                                infoHDCF.NGAY_SUA = Convert.ToDateTime(drCF["NGAY_SUA"].ToString());
                            else
                                return "Lỗi dữ liệu HDN_HDONCOSFI: NGAY_SUA = null";
                            if (drCF["NGAY_TAO"].ToString().Trim().Length > 0)
                                infoHDCF.NGAY_TAO = Convert.ToDateTime(drCF["NGAY_TAO"].ToString());
                            else
                                return "Lỗi dữ liệu HDN_HDONCOSFI: NGAY_TAO = null";
                            //MessageBox.Show("6");
                            infoHDCF.NGUOI_SUA = drCF["NGUOI_SUA"].ToString();
                            infoHDCF.NGUOI_TAO = drCF["NGUOI_TAO"].ToString();
                            infoHDCF.STT = drCF["STT"].ToString();
                            if (drCF["ID_CHISO"].ToString().Trim().Length > 0)
                                infoHDCF.ID_CHISO = Convert.ToInt64(drCF["ID_CHISO"]);
                            //MessageBox.Show("7");
                            if (drCF["THANG"].ToString().Trim().Length > 0)
                                infoHDCF.THANG = Convert.ToInt16(drCF["THANG"].ToString());
                            else
                                return "Lỗi dữ liệu HDN_HDONCOSFI: THANG = null";
                            if (drCF["TIEN_HUUCONG"].ToString().Trim().Length > 0)
                                infoHDCF.TIEN_HUUCONG = Convert.ToDecimal(drCF["TIEN_HUUCONG"].ToString());
                            else
                                return "Lỗi dữ liệu HDN_HDONCOSFI: TIEN_HUUCONG = null";
                            if (drCF["TIEN_VOCONG"].ToString().Trim().Length > 0)
                                infoHDCF.TIEN_VOCONG = Convert.ToDecimal(drCF["TIEN_VOCONG"].ToString());
                            else
                                return "Lỗi dữ liệu HDN_HDONCOSFI: TIEN_VOCONG = null";
                            if (drCF["VO_CONG"].ToString().Trim().Length > 0)
                                infoHDCF.VO_CONG = Convert.ToDecimal(drCF["VO_CONG"].ToString());
                            else
                                return "Lỗi dữ liệu HDN_HDONCOSFI: VO_CONG = null";
                            //MessageBox.Show("8");
                            lstHDonCosfi.Add(infoHDCF);
                            //MessageBox.Show("9");
                        }
                    }
                    #endregion
                    //MessageBox.Show("End", "Thông báo", MessageBoxButtons.OK, MessageBoxIcon.Information);
                }
                #region DũngNT hiệu chỉnh - các chi tiết VC(không có hóa đơn VC)
                if (dtTemp != null && dtTemp.Rows.Count > 0)
                {
                    obj_HDN_HDON_Controller.CMIS2 = db;
                    obj_HDN_HDONCTIET_Controller.CMIS2 = db;
                    foreach (DataRow dr in dtTemp.Rows)
                    {
                        HDN_HDONCTIET infoHDCT = new HDN_HDONCTIET();
                        infoHDCT.BCS = dr["BCS"].ToString();
                        if (dr["DIEN_TTHU"].ToString().Trim().Length > 0)
                            infoHDCT.DIEN_TTHU = Convert.ToDecimal(dr["DIEN_TTHU"]);
                        else
                            return "Lỗi dữ liệu HDN_HDONCTIET: DIEN_TTHU = null";
                        infoHDCT.DINH_MUC = dr["DINH_MUC"].ToString();
                        if (dr["DON_GIA"].ToString().Trim().Length > 0)
                            infoHDCT.DON_GIA = Convert.ToDecimal(dr["DON_GIA"]);
                        if (dr["ID_CHISO"].ToString().Trim().Length > 0)
                            infoHDCT.ID_CHISO = Convert.ToInt64(dr["ID_CHISO"]);
                        else
                            return "Lỗi dữ liệu HDN_HDONCTIET: ID_CHISO = null";
                        infoHDCT.ID_HDON = obj_HDN_HDON_Controller.getID_HDON();
                        //if (dr["ID_HDONCTIET"].ToString().Trim().Length > 0)
                        //    infoHDCT.ID_HDONCTIET = Convert.ToInt64(dr["ID_HDONCTIET"]);
                        //else
                        //    return "Lỗi dữ liệu HDN_HDONCTIET: ID_HDONCTIET = null";
                        infoHDCT.ID_HDONCTIET = obj_HDN_HDONCTIET_Controller.getID_HDON();
                        if (dr["KY"].ToString().Trim().Length > 0)
                            infoHDCT.KY = Convert.ToInt16(dr["KY"]);
                        else
                            return "Lỗi dữ liệu HDN_HDONCTIET: KY = null";
                        if (dr["LOAI_KHANG"].ToString().Trim().Length > 0)
                            infoHDCT.LOAI_KHANG = Convert.ToInt16(dr["LOAI_KHANG"].ToString());
                        if (dr["LOAI_DDO"].ToString().Trim().Length > 0)
                            infoHDCT.LOAI_DDO = Convert.ToInt16(dr["LOAI_DDO"].ToString());
                        if (dr["SO_PHA"].ToString().Trim().Length > 0)
                            infoHDCT.SO_PHA = Convert.ToInt16(dr["SO_PHA"].ToString());
                        infoHDCT.LOAI_DMUC = dr["LOAI_DMUC"].ToString();
                        infoHDCT.MA_CAPDA = dr["MA_CAPDA"].ToString();
                        infoHDCT.MA_CNANG = dr["MA_CNANG"].ToString();
                        infoHDCT.MA_DDO = dr["MA_DDO"].ToString();
                        infoHDCT.MA_DVIQLY = dr["MA_DVIQLY"].ToString();
                        infoHDCT.MA_KHANG = dr["MA_KHANG"].ToString();
                        infoHDCT.MA_KVUC = dr["MA_KVUC"].ToString();
                        infoHDCT.MA_LO = dr["MA_LO"].ToString();
                        infoHDCT.MA_NGIA = dr["MA_NGIA"].ToString();
                        infoHDCT.MA_NHOMNN = dr["MA_NHOMNN"].ToString();
                        infoHDCT.MA_NN = dr["MA_NN"].ToString();
                        infoHDCT.MA_SOGCS = dr["MA_SOGCS"].ToString();
                        infoHDCT.MA_TO = dr["MA_TO"].ToString();
                        infoHDCT.MA_TRAM = dr["MA_TRAM"].ToString();
                        if (dr["NAM"].ToString().Trim().Length > 0)
                            infoHDCT.NAM = Convert.ToInt16(dr["NAM"]);
                        else
                            return "Lỗi dữ liệu HDN_HDONCTIET: NAM = null";
                        if (dr["NGAY_APDUNG"].ToString().Trim().Length > 0)
                            infoHDCT.NGAY_APDUNG = Convert.ToDateTime(dr["NGAY_APDUNG"]);
                        if (dr["NGAY_SUA"].ToString().Trim().Length > 0)
                            infoHDCT.NGAY_SUA = Convert.ToDateTime(dr["NGAY_SUA"]);
                        else
                            return "Lỗi dữ liệu HDN_HDONCTIET: NGAY_SUA = null";
                        if (dr["NGAY_TAO"].ToString().Trim().Length > 0)
                            infoHDCT.NGAY_TAO = Convert.ToDateTime(dr["NGAY_TAO"]);
                        else
                            return "Lỗi dữ liệu HDN_HDONCTIET: NGAY_TAO = null";
                        infoHDCT.NGUOI_SUA = dr["NGUOI_SUA"].ToString();
                        infoHDCT.NGUOI_TAO = dr["NGUOI_TAO"].ToString();
                        infoHDCT.SO_CTO = dr["SO_CTO"].ToString();
                        if (dr["SO_TIEN"].ToString().Trim().Length > 0)
                            infoHDCT.SO_TIEN = Convert.ToDecimal(dr["SO_TIEN"]);
                        else
                            return "Lỗi dữ liệu HDN_HDONCTIET: SO_TIEN = null";
                        infoHDCT.SO_TIEN = Convert.ToDecimal(dr["SO_TIEN"]);
                        infoHDCT.STT = dr["STT"].ToString();
                        infoHDCT.TGIAN_BDIEN = dr["TGIAN_BDIEN"].ToString();
                        if (dr["THANG"].ToString().Trim().Length > 0)
                            infoHDCT.THANG = Convert.ToInt16(dr["THANG"]);
                        else
                            return "Lỗi dữ liệu HDN_HDONCTIET: THANG = null";
                        lstHDonCTiet.Add(infoHDCT);
                    }
                }
                #endregion

                #region GCS_CHISO & GCS_CHISO_GT
                string strError = "";
                //obj_GCS_CHISO_Controller.LstInfo = new List<GCS_CHISO>();
                if (dsCustomerData.Tables["GCS_CHISO"] == null || dsCustomerData.Tables["GCS_CHISO"].Rows.Count == 0)
                    return "Không tìm thấy bảng GCS_CHISO để cập nhật dữ liệu.";
                foreach (DataRow row in dsCustomerData.Tables["GCS_CHISO"].Rows)
                {
                    GCS_CHISO info = BillingLibrary.BillingLibrary.MapDatarowToObject<GCS_CHISO>(row, ref strError);
                    if (strError.Trim().Length > 0)
                        return strError;
                    //obj_GCS_CHISO_Controller.LstInfo.Add(info);
                    lstChiSo.Add(info);
                }
                if (dsCustomerData.Tables.Contains("GCS_CHISO_GT") == true)
                {
                    foreach (DataRow row in dsCustomerData.Tables["GCS_CHISO_GT"].Rows)
                    {
                        GCS_CHISO info = BillingLibrary.BillingLibrary.MapDatarowToObject<GCS_CHISO>(row, ref strError);
                        if (strError.Trim().Length > 0)
                            return strError;
                        //obj_GCS_CHISO_Controller.LstInfo.Add(info);
                        lstChiSo.Add(info);
                    }
                }
                #endregion
                return "";

            }
            catch (Exception e)
            {
                //MessageBox.Show(strddo, "Thông báo", MessageBoxButtons.OK, MessageBoxIcon.Information);
                return "Error In SetPropertiesForObject Method: " + e.Message;
            }
        }

        protected string SetPropertiesForMultiObject(DataSet dsInvoiceData, DataSet dsCustomerData, CMIS2 db)
        {
            //DũngNT viết cho chức năng Tính hóa đơn điều chỉnh
            try
            {
                string strError = "";
                //Kiểm tra trong dataset chứa thông tin điều chỉnh
                if (!dsInvoiceData.Tables.Contains("HDN_HDON"))
                    return "Lỗi không tìm thấy bảng dsInvoiceData.HDN_HDON";
                if (!dsInvoiceData.Tables.Contains("HDN_HDONCTIET"))
                    return "Lỗi không tìm thấy bảng dsInvoiceData.HDN_HDONCTIET";
                //Kiểm tra trong dataset chứa thông tin khách hàng điều chỉnh và thông tin hóa đơn cũ
                if (!dsCustomerData.Tables.Contains("HDN_HDON_TIEPNHAN"))
                    return "Lỗi không tìm thấy bảng dsCustomerData.HDN_HDON_TIEPNHAN";
                if (!dsCustomerData.Tables.Contains("HDN_HDON"))
                    return "Lỗi không tìm thấy bảng dsCustomerData.HDN_HDON";
                if (!dsCustomerData.Tables.Contains("HDN_HDONCTIET"))
                    return "Lỗi không tìm thấy bảng dsCustomerData.HDN_HDONCTIET";
                if (dsCustomerData.Tables["HDN_BBAN_DCHINH"].Rows.Count <= 0)
                    return "Lỗi không tìm thấy dữ liệu bảng dsCustomerData.HDN_BBAN_DCHINH";
                if (!dsCustomerData.Tables.Contains("HDN_BBAN_DCHINH"))
                    return "Lỗi không tìm thấy bảng dsCustomerData.HDN_BBAN_DCHINH";
                if (!dsCustomerData.Tables.Contains("HDN_KHANG_DC"))
                    return "Lỗi không tìm thấy bảng dsCustomerData.HDN_KHANG_DC";
                if (!dsCustomerData.Tables.Contains("HDN_DIEMDO_DC"))
                    return "Lỗi không tìm thấy bảng dsCustomerData.HDN_DIEMDO_DC";
                if (!dsCustomerData.Tables.Contains("HDN_CHISO_DC"))
                    return "Lỗi không tìm thấy bảng dsCustomerData.HDN_CHISO_DC";
                if (!dsCustomerData.Tables.Contains("HDN_BCS_CTO_DC"))
                    return "Lỗi không tìm thấy bảng dsCustomerData.HDN_BCS_CTO_DC";
                if (!dsCustomerData.Tables.Contains("HDN_BBAN_APGIA_DC"))
                    return "Lỗi không tìm thấy bảng dsCustomerData.HDN_BBAN_APGIA_DC";

                //Đổi tên bảng tránh nhầm lẫn
                dsInvoiceData.Tables["HDN_HDON"].TableName = "HDN_HDON_DC";
                dsInvoiceData.Tables["HDN_HDONCTIET"].TableName = "HDN_HDONCTIET_DC";
                if (dsInvoiceData.Tables.Contains("HDN_HDONCOSFI"))
                    dsInvoiceData.Tables["HDN_HDONCOSFI"].TableName = "HDN_HDONCOSFI_DC";
                dsInvoiceData.Tables["HDN_HDONCTIET_DC"].Columns["ID_CHISO"].ColumnName = "ID_CHISO_DC";
                dsInvoiceData.Tables["HDN_HDONCTIET_DC"].Columns["ID_HDON"].ColumnName = "ID_HDON_DC";
                dsInvoiceData.Tables["HDN_HDONCTIET_DC"].Columns["ID_HDONCTIET"].ColumnName = "ID_HDONCTIET_DC";
                dsInvoiceData.Tables["HDN_HDONCOSFI_DC"].Columns["ID_HDON"].ColumnName = "ID_HDON_DC";
                dsInvoiceData.Tables["HDN_HDONCOSFI_DC"].Columns["ID_HDONCOSFI"].ColumnName = "ID_HDONCOSFI_DC";
                dsInvoiceData.Tables["HDN_HDON_DC"].Columns.Add("ID_HDON_DC", typeof(long));
                dsInvoiceData.Tables["HDN_HDON_DC"].Columns.Add("LOAI_DCHINH", typeof(string));
                DataRow drBBan = dsCustomerData.Tables["HDN_BBAN_DCHINH"].Rows[0];
                //DũngNT bổ sung so sánh các trường hợp có hóa đơn VC - có hiệu chỉnh
                DataRow rowDC_TD = dsInvoiceData.Tables["HDN_HDON_DC"].Select("LOAI_HDON='TD' OR LOAI_HDON='TC'").FirstOrDefault();
                DataRow rowDC_VC = dsInvoiceData.Tables["HDN_HDON_DC"].Select("LOAI_HDON='VC'").FirstOrDefault();
                DataRow rowPS_TD = dsCustomerData.Tables["HDN_HDON"].Select("LOAI_HDON='TD' OR LOAI_HDON='TC'").FirstOrDefault();
                DataRow rowPS_VC = dsCustomerData.Tables["HDN_HDON"].Select("LOAI_HDON='VC'").FirstOrDefault();
                rowDC_TD["ID_HDON"] = rowPS_TD["ID_HDON"];
                if (rowDC_VC != null)
                    rowDC_VC["ID_HDON"] = rowPS_VC != null ? rowPS_VC["ID_HDON"] : rowPS_TD["ID_HDON"];
                //Kiểm tra loại điều chỉnh của hóa đơn điều chỉnh
                decimal decDaTra = Utility.DecimalDbnull(dsCustomerData.Tables["HDN_HDON_TIEPNHAN"].Rows[0]["DA_TRA"]);
                decimal decTongNo = Utility.DecimalDbnull(dsCustomerData.Tables["HDN_HDON_TIEPNHAN"].Rows[0]["SO_TIEN"]) + Utility.DecimalDbnull(dsCustomerData.Tables["HDN_HDON_TIEPNHAN"].Rows[0]["TIEN_GTGT"]);
                if (decTongNo > decDaTra)
                    decDaTra = 0;
                decimal decTongTienDC = Utility.DecimalDbnull(rowDC_TD["TONG_TIEN"]);
                decimal decTongTien = Utility.DecimalDbnull(rowPS_TD["TONG_TIEN"]);
                string strLoaiDC = "";
                long lngIDHDonDC_HuyBo = 0;
                long lngIDHDonDC_HuyBo_VC = 0;
                obj_HDN_HDON_DC_Controller = new cls_HDN_HDON_DC_Controller();
                long lngIDHDonDC = 0;
                long lngIDHDonDC_VC = 0;
                Int16 ky = Convert.ToInt16(drBBan["KY_DC"]);
                Int16 thang = Convert.ToInt16(drBBan["THANG_DC"]);
                Int16 nam = Convert.ToInt16(drBBan["NAM_DC"]);

                //if (rowDC_TD == null)
                //{
                //    ky = Convert.ToInt16(rowDC_VC["KY"]);
                //    thang = Convert.ToInt16(rowDC_VC["THANG"]);
                //    nam = Convert.ToInt16(rowDC_VC["NAM"]);
                //}
                //else
                //{
                //    ky = Convert.ToInt16(rowDC_TD["KY"]);
                //    thang = Convert.ToInt16(rowDC_TD["THANG"]);
                //    nam = Convert.ToInt16(rowDC_TD["NAM"]);
                //}
                if (decDaTra != 0)
                {
                    //Đã thu tiền của khách hàng => có thể là hóa đơn truy thu TT hoặc hóa đơn thoái hoàn TH
                    if (decTongTienDC > decTongTien) strLoaiDC = "TT";
                    else if (decTongTienDC < decTongTien) strLoaiDC = "TH";
                    else strLoaiDC = "RA"; //Re Ask: hỏi lại sau
                    //if (strLoaiDC == "RA") return "HĐ điều chỉnh có tổng tiền bằng với HĐ phát sinh. Không lập hóa đơn điều chỉnh.";
                    rowDC_TD["LOAI_DCHINH"] = strLoaiDC;

                    obj_HDN_HDON_DC_Controller.CMIS2 = db;
                    lngIDHDonDC = obj_HDN_HDON_DC_Controller.getMaxID();
                    lngIDHDonDC_VC = obj_HDN_HDON_DC_Controller.getMaxID();
                    //Tính lại các giá trị tiền
                    rowDC_TD["DIEN_TTHU"] = Utility.DecimalDbnull(rowDC_TD["DIEN_TTHU"]) - Utility.DecimalDbnull(rowPS_TD["DIEN_TTHU"]);
                    rowDC_TD["SO_TIEN"] = Utility.DecimalDbnull(rowDC_TD["SO_TIEN"]) - Utility.DecimalDbnull(rowPS_TD["SO_TIEN"]);
                    rowDC_TD["TIEN_GTGT"] = Utility.DecimalDbnull(rowDC_TD["TIEN_GTGT"]) - Utility.DecimalDbnull(rowPS_TD["TIEN_GTGT"]);
                    rowDC_TD["TONG_TIEN"] = Utility.DecimalDbnull(rowDC_TD["TONG_TIEN"]) - Utility.DecimalDbnull(rowPS_TD["TONG_TIEN"]);

                    rowDC_TD["TIEN_TD"] = Utility.DecimalDbnull(rowDC_TD["TIEN_TD"]) - Utility.DecimalDbnull(rowPS_TD["TIEN_TD"]);
                    rowDC_TD["THUE_TD"] = Utility.DecimalDbnull(rowDC_TD["THUE_TD"]) - Utility.DecimalDbnull(rowPS_TD["THUE_TD"]);
                    rowDC_TD["TIEN_VC"] = Utility.DecimalDbnull(rowDC_TD["TIEN_VC"]) - Utility.DecimalDbnull(rowPS_TD["TIEN_VC"]);
                    rowDC_TD["THUE_VC"] = Utility.DecimalDbnull(rowDC_TD["THUE_VC"]) - Utility.DecimalDbnull(rowPS_TD["THUE_VC"]);
                    rowDC_TD["KY"] = ky;
                    rowDC_TD["THANG"] = thang;
                    rowDC_TD["NAM"] = nam;
                    if (rowDC_VC != null)
                    {
                        if (rowPS_VC == null) //Không có hóa đơn vô công phát sinh
                            rowDC_VC["LOAI_DCHINH"] = "TT";
                        else
                        {
                            decimal decTongTienDC_VC = Utility.DecimalDbnull(rowDC_VC["TONG_TIEN"]);
                            decimal decTongTien_VC = Utility.DecimalDbnull(rowPS_VC["TONG_TIEN"]);
                            rowDC_VC["DIEN_TTHU"] = Utility.DecimalDbnull(rowDC_VC["DIEN_TTHU"]) - Utility.DecimalDbnull(rowPS_VC["DIEN_TTHU"]);
                            rowDC_VC["SO_TIEN"] = Utility.DecimalDbnull(rowDC_VC["SO_TIEN"]) - Utility.DecimalDbnull(rowPS_VC["SO_TIEN"]);
                            rowDC_VC["TIEN_GTGT"] = Utility.DecimalDbnull(rowDC_VC["TIEN_GTGT"]) - Utility.DecimalDbnull(rowPS_VC["TIEN_GTGT"]);
                            rowDC_VC["TONG_TIEN"] = Utility.DecimalDbnull(rowDC_VC["TONG_TIEN"]) - Utility.DecimalDbnull(rowPS_VC["TONG_TIEN"]);
                            rowDC_VC["TIEN_TD"] = Utility.DecimalDbnull(rowDC_VC["TIEN_TD"]) - Utility.DecimalDbnull(rowPS_VC["TIEN_TD"]);
                            rowDC_VC["THUE_TD"] = Utility.DecimalDbnull(rowDC_VC["THUE_TD"]) - Utility.DecimalDbnull(rowPS_VC["THUE_TD"]);
                            rowDC_VC["TIEN_VC"] = Utility.DecimalDbnull(rowDC_VC["TIEN_VC"]) - Utility.DecimalDbnull(rowPS_VC["TIEN_VC"]);
                            rowDC_VC["THUE_VC"] = Utility.DecimalDbnull(rowDC_VC["THUE_VC"]) - Utility.DecimalDbnull(rowPS_VC["THUE_VC"]);
                            rowDC_VC["KY"] = ky;
                            rowDC_VC["THANG"] = thang;
                            rowDC_VC["NAM"] = nam;
                            if (decTongTienDC_VC > decTongTien_VC) rowDC_VC["LOAI_DCHINH"] = "TT";
                            else if (decTongTienDC_VC < decTongTien_VC) rowDC_VC["LOAI_DCHINH"] = "TH";
                            else
                            {
                                //Tiền TD DC = tiền TD PS, tiền VC DC = tiền VC PS
                                if (strLoaiDC == "RA")
                                    return "HĐ điều chỉnh có tổng tiền bằng với HĐ phát sinh. Không lập hóa đơn điều chỉnh.";
                                //Không ghi hóa đơn VC DC vào DB
                                dsInvoiceData.Tables["HDN_HDON_DC"].Rows.Remove(rowDC_VC);
                            }
                        }
                    }
                    var arrID_HDON_DC_TD = (from a in dsInvoiceData.Tables["HDN_HDON_DC"].AsEnumerable()
                                            where (a.Field<string>("LOAI_HDON") == "TD" || a.Field<string>("LOAI_HDON") == "TC")
                                            && a.Field<string>("LOAI_DCHINH") != "HB"
                                            && a.Field<string>("LOAI_DCHINH") != "RA"
                                            select a);
                    var arrID_HDON_DC_VC = (from a in dsInvoiceData.Tables["HDN_HDON_DC"].AsEnumerable()
                                            where a.Field<string>("LOAI_HDON") == "VC"
                                            && a.Field<string>("LOAI_DCHINH") != "HB"
                                            && a.Field<string>("LOAI_DCHINH") != "RA"
                                            select a);
                    if ((arrID_HDON_DC_TD == null || arrID_HDON_DC_TD.Count() == 0) && (arrID_HDON_DC_VC == null || arrID_HDON_DC_VC.Count() == 0))
                        return "Hóa đơn điều chỉnh có tổng tiền bằng hóa đơn sai. Không thực hiện điều chỉnh";


                    foreach (DataRow row in dsCustomerData.Tables["HDN_HDONCTIET"].Rows)
                    {
                        DataRow drCTiet = dsInvoiceData.Tables["HDN_HDONCTIET_DC"].NewRow();
                        foreach (DataColumn column in dsCustomerData.Tables["HDN_HDONCTIET"].Columns)
                        {
                            if (column.ColumnName != "ID_HDON" && column.ColumnName != "ID_HDONCTIET" && column.ColumnName != "ID_CHISO")
                            {
                                if (column.ColumnName == "DIEN_TTHU" || column.ColumnName == "SO_TIEN")
                                    drCTiet[column.ColumnName] = 0 - Convert.ToDecimal(row[column.ColumnName]);
                                else
                                    drCTiet[column.ColumnName] = row[column.ColumnName];
                            }
                            //Kiểm tra chi tiết 
                            //BT,CD,TD,KT: ID_HDON_DC = ID_HDON_DC của hóa đơn TD (nếu không có TD lấy = VC)



                            drCTiet["ID_HDONCTIET_DC"] = 1;
                            drCTiet["ID_CHISO_DC"] = row["ID_CHISO"];
                        }
                        drCTiet["KY"] = ky;
                        drCTiet["THANG"] = thang;
                        drCTiet["NAM"] = nam;
                        if ("KT;BT;CD;TD".Contains(drCTiet["BCS"].ToString().Trim()))
                        {
                            if (arrID_HDON_DC_TD == null || arrID_HDON_DC_TD.Count() == 0)
                                drCTiet["ID_HDON_DC"] = lngIDHDonDC_VC;
                            else drCTiet["ID_HDON_DC"] = lngIDHDonDC;
                        }
                        else
                        {
                            if (arrID_HDON_DC_VC == null || arrID_HDON_DC_VC.Count() == 0)
                                drCTiet["ID_HDON_DC"] = lngIDHDonDC;
                            else drCTiet["ID_HDON_DC"] = lngIDHDonDC_VC;
                        }
                        dsInvoiceData.Tables["HDN_HDONCTIET_DC"].Rows.Add(drCTiet);
                        dsInvoiceData.AcceptChanges();
                    }
                    if (dsCustomerData.Tables.Contains("HDN_HDONCOSFI"))
                    {
                        foreach (DataRow row in dsCustomerData.Tables["HDN_HDONCOSFI"].Rows)
                        {
                            DataRow drCosfi = dsInvoiceData.Tables["HDN_HDONCOSFI_DC"].NewRow();
                            foreach (DataColumn column in dsCustomerData.Tables["HDN_HDONCOSFI"].Columns)
                            {
                                if (column.ColumnName != "ID_HDON" && column.ColumnName != "ID_HDONCOSFI")//
                                {
                                    if (column.ColumnName == "TIEN_HUUCONG" || column.ColumnName == "TIEN_VOCONG" || column.ColumnName == "VO_CONG")
                                        drCosfi[column.ColumnName] = 0 - Convert.ToDecimal(row[column.ColumnName]);
                                    else
                                        drCosfi[column.ColumnName] = row[column.ColumnName];
                                }

                                drCosfi["ID_HDONCOSFI_DC"] = 1;
                            }
                            drCosfi["KY"] = ky;
                            drCosfi["THANG"] = thang;
                            drCosfi["NAM"] = nam;
                            if (arrID_HDON_DC_VC == null || arrID_HDON_DC_VC.Count() == 0)
                                drCosfi["ID_HDON_DC"] = lngIDHDonDC;
                            else drCosfi["ID_HDON_DC"] = lngIDHDonDC_VC;
                            dsInvoiceData.Tables["HDN_HDONCOSFI_DC"].Rows.Add(drCosfi);
                            dsInvoiceData.AcceptChanges();
                        }
                    }
                }
                else
                {
                    //Chưa thu tiền khách hàng  
                    //Hóa đơn điều chỉnh mới là hóa đơn lập lại LL
                    foreach (DataRow row in dsInvoiceData.Tables["HDN_HDON_DC"].Rows)
                    {
                        row["LOAI_DCHINH"] = "LL";
                        //set lai ky thang nam dieu chinh
                        row["KY"] = ky;
                        row["THANG"] = thang;
                        row["NAM"] = nam;
                    }
                    dsInvoiceData.AcceptChanges();

                    obj_HDN_HDON_DC_Controller.CMIS2 = db;
                    lngIDHDonDC_HuyBo = obj_HDN_HDON_DC_Controller.getMaxID();
                    lngIDHDonDC_HuyBo_VC = obj_HDN_HDON_DC_Controller.getMaxID();
                    lngIDHDonDC = obj_HDN_HDON_DC_Controller.getMaxID();
                    lngIDHDonDC_VC = obj_HDN_HDON_DC_Controller.getMaxID();
                    //Tạo thêm hóa đơn hủy bỏ HB
                    foreach (DataRow rowHD in dsCustomerData.Tables["HDN_HDON"].Rows)
                    {
                        DataRow drHDon_HuyBo = dsInvoiceData.Tables["HDN_HDON_DC"].NewRow();
                        foreach (DataColumn column in dsCustomerData.Tables["HDN_HDON"].Columns)
                        {
                            if (column.ColumnName != "ID_HDON")
                            {
                                if (column.ColumnName == "DIEN_TTHU" || column.ColumnName == "SO_TIEN" || column.ColumnName == "TIEN_GTGT" || column.ColumnName == "TONG_TIEN" || column.ColumnName == "TIEN_TD" || column.ColumnName == "THUE_TD" || column.ColumnName == "TIEN_VC" || column.ColumnName == "THUE_VC")
                                    drHDon_HuyBo[column.ColumnName] = 0 - Utility.DecimalDbnull(rowHD[column.ColumnName]);
                                else
                                    drHDon_HuyBo[column.ColumnName] = rowHD[column.ColumnName];
                            }
                            drHDon_HuyBo["ID_HDON"] = rowHD["ID_HDON"];
                            drHDon_HuyBo["ID_HDON_DC"] = lngIDHDonDC_HuyBo;
                            drHDon_HuyBo["LOAI_DCHINH"] = "HB";
                        }
                        drHDon_HuyBo["KY"] = ky;
                        drHDon_HuyBo["THANG"] = thang;
                        drHDon_HuyBo["NAM"] = nam;
                        dsInvoiceData.Tables["HDN_HDON_DC"].Rows.Add(drHDon_HuyBo);
                        dsInvoiceData.AcceptChanges();
                        if (dsInvoiceData.Tables.Contains("HDN_HDONCTIET_DC") && dsInvoiceData.Tables["HDN_HDONCTIET_DC"].Rows.Count > 0)
                        {
                            foreach (DataRow row in dsInvoiceData.Tables["HDN_HDONCTIET_DC"].Rows)
                            {
                                row["ID_HDON_DC"] = lngIDHDonDC;
                            }
                            dsInvoiceData.Tables["HDN_HDONCTIET_DC"].AcceptChanges();
                        }
                        foreach (DataRow row in dsCustomerData.Tables["HDN_HDONCTIET"].Rows)
                        {
                            if (row["ID_HDON"].ToString().Trim() != rowHD["ID_HDON"].ToString().Trim()) continue;
                            DataRow drCTiet = dsInvoiceData.Tables["HDN_HDONCTIET_DC"].NewRow();
                            foreach (DataColumn column in dsCustomerData.Tables["HDN_HDONCTIET"].Columns)
                            {
                                if (column.ColumnName != "ID_HDON" && column.ColumnName != "ID_HDONCTIET" && column.ColumnName != "ID_CHISO")
                                {
                                    if (column.ColumnName == "DIEN_TTHU" || column.ColumnName == "SO_TIEN")
                                        drCTiet[column.ColumnName] = 0 - Convert.ToDecimal(row[column.ColumnName]);
                                    else
                                        drCTiet[column.ColumnName] = row[column.ColumnName];
                                }
                                drCTiet["ID_HDON_DC"] = lngIDHDonDC_HuyBo;
                                drCTiet["ID_HDONCTIET_DC"] = 1;
                                drCTiet["ID_CHISO_DC"] = row["ID_CHISO"];
                            }
                            drCTiet["KY"] = ky;
                            drCTiet["THANG"] = thang;
                            drCTiet["NAM"] = nam;
                            dsInvoiceData.Tables["HDN_HDONCTIET_DC"].Rows.Add(drCTiet);
                            dsInvoiceData.AcceptChanges();
                        }

                        if (dsInvoiceData.Tables.Contains("HDN_HDONCOSFI_DC") && dsInvoiceData.Tables["HDN_HDONCOSFI_DC"].Rows.Count > 0)
                        {
                            foreach (DataRow row in dsInvoiceData.Tables["HDN_HDONCOSFI_DC"].Rows)
                            {
                                row["ID_HDON_DC"] = lngIDHDonDC;
                            }
                            dsInvoiceData.Tables["HDN_HDONCOSFI_DC"].AcceptChanges();
                        }
                        if (dsCustomerData.Tables.Contains("HDN_HDONCOSFI"))
                        {

                            foreach (DataRow row in dsCustomerData.Tables["HDN_HDONCOSFI"].Rows)
                            {
                                if (row["ID_HDON"].ToString().Trim() != rowHD["ID_HDON"].ToString().Trim()) continue;
                                DataRow drCosfi = dsInvoiceData.Tables["HDN_HDONCOSFI_DC"].NewRow();
                                foreach (DataColumn column in dsCustomerData.Tables["HDN_HDONCOSFI"].Columns)
                                {
                                    if (column.ColumnName != "ID_HDON" && column.ColumnName != "ID_HDONCOSFI")//
                                    {
                                        if (column.ColumnName == "TIEN_HUUCONG" || column.ColumnName == "TIEN_VOCONG" || column.ColumnName == "VO_CONG")
                                            drCosfi[column.ColumnName] = 0 - Convert.ToDecimal(row[column.ColumnName]);
                                        else
                                            drCosfi[column.ColumnName] = row[column.ColumnName];
                                    }
                                    drCosfi["ID_HDON_DC"] = lngIDHDonDC_HuyBo;
                                    drCosfi["ID_HDONCOSFI_DC"] = 1;
                                }
                                drCosfi["KY"] = ky;
                                drCosfi["THANG"] = thang;
                                drCosfi["NAM"] = nam;
                                dsInvoiceData.Tables["HDN_HDONCOSFI_DC"].Rows.Add(drCosfi);
                                dsInvoiceData.AcceptChanges();
                            }
                        }
                    }
                }

                #region HDN_HDON_DC

                string macnang = dsCustomerData.Tables["HDN_BBAN_DCHINH"].Rows[0]["MA_CNANG"].ToString();
                foreach (DataRow dr1 in dsInvoiceData.Tables["HDN_HDON_DC"].Rows)
                {
                    HDN_HDON_DC infoHDDC = new HDN_HDON_DC();

                    infoHDDC = BillingLibrary.BillingLibrary.MapDatarowToObject<HDN_HDON_DC>(dr1, ref strError);
                    infoHDDC.MA_CNANG = macnang;
                    if (strError.Trim().Length == 0)
                    {
                        //Nếu hóa đơn điều chỉnh có loại hóa đơn ="RA"(Quy ước khi không có thay đổi về tiền)
                        //Không ghi vào CSDL
                        if (infoHDDC.LOAI_DCHINH == "RA") continue;
                        if (infoHDDC.LOAI_DCHINH == "LL" || infoHDDC.LOAI_DCHINH == "TT" || infoHDDC.LOAI_DCHINH == "TH")
                        {
                            if (infoHDDC.LOAI_HDON == "VC")
                            {
                                infoHDDC.ID_HDON_DC = lngIDHDonDC_VC;
                                if (rowPS_VC == null) //Nếu không có hóa đơn phát sinh VC, ID_HDON_DC = ID_HDON
                                {
                                    if (rowPS_TD == null)
                                        infoHDDC.ID_HDON = infoHDDC.ID_HDON_DC;
                                    else
                                        infoHDDC.ID_HDON = Convert.ToInt64(rowPS_TD["ID_HDON"]);
                                }
                                else
                                    infoHDDC.ID_HDON = Convert.ToInt64(rowPS_VC["ID_HDON"]);
                            }
                            else
                            {
                                infoHDDC.ID_HDON_DC = lngIDHDonDC;
                                if (rowPS_TD == null) //Nếu không có hóa đơn phát sinh TD, ID_HDON_DC = ID_HDON
                                    infoHDDC.ID_HDON = infoHDDC.ID_HDON_DC;
                                else
                                    infoHDDC.ID_HDON = Convert.ToInt64(rowPS_TD["ID_HDON"]);
                            }
                        }
                        else
                        {
                            if (infoHDDC.LOAI_HDON == "VC")
                            {
                                infoHDDC.ID_HDON_DC = lngIDHDonDC_HuyBo_VC;
                            }
                            else
                            {
                                infoHDDC.ID_HDON_DC = lngIDHDonDC_HuyBo;
                            }
                        }

                        obj_HDN_HDON_DC_Controller.LstInfo.Add(infoHDDC);
                    }
                    else return strError;
                }
                //Kiểm tra xem hóa đơn điều chỉnh mới có ID_HDON_DC = lngIDHDonDC không, hay bằng lngIDHDonDC_VC
                #endregion

                #region HDN_BBAN_DCHINH
                obj_HDN_BBAN_DCHINH_Controller = new cls_HDN_BBAN_DCHINH_Controller();
                obj_HDN_BBAN_DCHINH_Controller.CMIS2 = db;
                long lngIDBBanDC = obj_HDN_BBAN_DCHINH_Controller.getMaxID();
                foreach (HDN_HDON_DC dcInfo in obj_HDN_HDON_DC_Controller.LstInfo)
                {
                    DataRow dr2 = dsCustomerData.Tables["HDN_BBAN_DCHINH"].Rows[0];
                    HDN_BBAN_DCHINH infoBBDC = new HDN_BBAN_DCHINH();
                    infoBBDC = BillingLibrary.BillingLibrary.MapDatarowToObject<HDN_BBAN_DCHINH>(dr2, ref strError);
                    if (strError.Trim().Length == 0)
                    {
                        infoBBDC.ID_BBAN = lngIDBBanDC;
                        infoBBDC.ID_HDON_DC = dcInfo.ID_HDON_DC;
                        infoBBDC.ID_HDON = dcInfo.ID_HDON;
                        obj_HDN_BBAN_DCHINH_Controller.LstInfo.Add(infoBBDC);
                    }
                    else return strError;
                }
                #endregion

                #region HDN_BCS_CTO_DC
                obj_HDN_BCS_CTO_DC_Controller = new cls_HDN_BCS_CTO_DC_Controller();
                HDN_HDON_DC dcTemp = obj_HDN_HDON_DC_Controller.LstInfo.Where(c => c.LOAI_DCHINH != "HB" && c.LOAI_HDON == "TC").FirstOrDefault();
                //lngIDHDonDC_Tem là ID_HDON_DC của hóa đơn điều chỉnh vừa tạo ra
                //Trường hợp không có HDDC tiền điện thì lấy bằng ID của HDDC vô công. 
                long lngIDHDonDC_Tem = 0;
                long lngIDHDonDC_Tem_VC = 0;
                if (dcTemp == null)
                {
                    //Không phải TC
                    dcTemp = obj_HDN_HDON_DC_Controller.LstInfo.Where(c => c.LOAI_DCHINH != "HB" && c.LOAI_HDON == "TD").FirstOrDefault();
                    if (dcTemp != null)  //Nếu không có báo lỗi
                    {
                        lngIDHDonDC_Tem = dcTemp.ID_HDON_DC;
                        lngIDHDonDC_Tem_VC = dcTemp.ID_HDON_DC;
                    }
                    else
                    {
                        dcTemp = obj_HDN_HDON_DC_Controller.LstInfo.Where(c => c.LOAI_DCHINH != "HB" && c.LOAI_HDON == "VC").FirstOrDefault();
                        if (dcTemp != null)
                        {
                            lngIDHDonDC_Tem_VC = dcTemp.ID_HDON_DC;
                            lngIDHDonDC_Tem = dcTemp.ID_HDON_DC;
                        }
                    }
                    dcTemp = obj_HDN_HDON_DC_Controller.LstInfo.Where(c => c.LOAI_DCHINH != "HB" && c.LOAI_HDON == "VC").FirstOrDefault();
                    if (dcTemp != null)
                        lngIDHDonDC_Tem_VC = dcTemp.ID_HDON_DC;


                    //else
                    //    return "Không tồn tại dữ liệu hóa đơn điều chỉnh truy thu, thoái hoàn hoặc lập lại";

                }
                else
                {
                    lngIDHDonDC_Tem = dcTemp.ID_HDON_DC;
                    lngIDHDonDC_Tem_VC = dcTemp.ID_HDON_DC;
                }
                foreach (DataRow dr5 in dsCustomerData.Tables["HDN_BCS_CTO_DC"].Rows)
                //DataRow dr2 = dsInvoiceData.Tables["HDN_BCS_CTO_DC"].Rows[0];
                {
                    HDN_BCS_CTO_DC infoBCSDC = new HDN_BCS_CTO_DC();
                    //long lngIDBCSDC = 0;

                    infoBCSDC = BillingLibrary.BillingLibrary.MapDatarowToObject<HDN_BCS_CTO_DC>(dr5, ref strError);
                    infoBCSDC.MA_CNANG = macnang;
                    if (strError.Trim().Length == 0)
                    {
                        if (infoBCSDC.BCS != "VC")
                            infoBCSDC.ID_HDON_DC = lngIDHDonDC_Tem;
                        else
                            infoBCSDC.ID_HDON_DC = lngIDHDonDC_Tem_VC;
                        obj_HDN_BCS_CTO_DC_Controller.LstInfo.Add(infoBCSDC);
                    }
                    else return strError;
                }
                #endregion

                #region HDN_CHISO_DC
                obj_HDN_CHISO_DC_Controller = new cls_HDN_CHISO_DC_Controller();
                obj_HDN_BCS_CTO_DC_Controller.CMIS2 = db;
                obj_HDN_CHISO_DC_Controller.CMIS2 = db;
                //Dũng NT sửa để fix ID_CHISO tương ứng với ID_CHISO phát sinh
                if (!dsCustomerData.Tables["HDN_CHISO_DC"].Columns.Contains("ID_CHISO_PS"))
                    dsCustomerData.Tables["HDN_CHISO_DC"].Columns.Add("ID_CHISO_PS", typeof(Int64));


                //End


                foreach (DataRow dr4 in dsCustomerData.Tables["HDN_CHISO_DC"].Rows)
                {
                    dr4["ID_CHISO_PS"] = dr4["ID_CHISO"];
                    HDN_CHISO_DC infoCSDC = new HDN_CHISO_DC();
                    //dr4["MA_CNANG"] = macnang;
                    infoCSDC = BillingLibrary.BillingLibrary.MapDatarowToObject<HDN_CHISO_DC>(dr4, ref strError);
                    infoCSDC.MA_CNANG = macnang;
                    if (strError.Trim().Length == 0)
                    {

                        if (infoCSDC.BCS != "VC")
                            infoCSDC.ID_HDON_DC = lngIDHDonDC_Tem;
                        else
                            infoCSDC.ID_HDON_DC = lngIDHDonDC_Tem_VC;

                        if (infoCSDC.SAN_LUONG != null)
                            infoCSDC.SAN_LUONG = Math.Round(infoCSDC.SAN_LUONG.Value);
                        if (infoCSDC.SLUONG_TTIEP != null)
                            infoCSDC.SLUONG_TTIEP = Math.Round(infoCSDC.SLUONG_TTIEP.Value);
                        //Gán lại ID_BCS_DC
                        HDN_BCS_CTO_DC infoBCS = obj_HDN_BCS_CTO_DC_Controller.LstInfo.Where(c => c.MA_DVIQLY == infoCSDC.MA_DVIQLY && c.ID_BCS_DC == infoCSDC.ID_BCS_DC).FirstOrDefault();
                        if (infoBCS == null) return "Không tìm thấy bộ chỉ số tương ứng MA_DVIQLY=" + infoCSDC.MA_DVIQLY + " và ID_BCS_DC=" + infoCSDC.ID_BCS_DC.ToString() + "";
                        infoBCS.ID_BCS_DC = obj_HDN_BCS_CTO_DC_Controller.getMaxID();
                        infoCSDC.ID_BCS_DC = infoBCS.ID_BCS_DC;
                        infoCSDC.ID_CHISO = obj_HDN_CHISO_DC_Controller.getMaxID();
                        dr4["ID_CHISO"] = infoCSDC.ID_CHISO;
                        obj_HDN_CHISO_DC_Controller.LstInfo.Add(infoCSDC);
                    }
                    else return strError;
                }
                #endregion

                #region HDN_HDONCTIET_DC
                obj_HDN_HDONCTIET_DC_Controller = new cls_HDN_HDONCTIET_DC_Controller();
                obj_HDN_HDONCTIET_DC_Controller.CMIS2 = db;
                var arrHDonTD = obj_HDN_HDON_DC_Controller.LstInfo.Where(c => "TD;TC".Contains(c.LOAI_HDON) && c.LOAI_DCHINH != "HB");
                var arrHDonVC = obj_HDN_HDON_DC_Controller.LstInfo.Where(c => c.LOAI_HDON == "VC" && c.LOAI_DCHINH != "HB");
                foreach (DataRow dr3 in dsInvoiceData.Tables["HDN_HDONCTIET_DC"].Rows)
                //DataRow dr2 = dsInvoiceData.Tables["HDN_HDONCTIET_DC"].Rows[0];
                {
                    HDN_HDONCTIET_DC infoHDCT = new HDN_HDONCTIET_DC();

                    long lngIDHDCTietDC = 0;
                    infoHDCT = BillingLibrary.BillingLibrary.MapDatarowToObject<HDN_HDONCTIET_DC>(dr3, ref strError);
                    infoHDCT.MA_CNANG = macnang;
                    if (strError.Trim().Length == 0)
                    {
                        lngIDHDCTietDC = obj_HDN_HDONCTIET_DC_Controller.getMaxID();
                        infoHDCT.ID_HDONCTIET_DC = lngIDHDCTietDC;
                        if (decDaTra != 0)
                        {
                            if ("KT;BT;CD;TD".Contains(infoHDCT.BCS))
                            {
                                if (arrHDonTD == null || arrHDonTD.Count() == 0)
                                    infoHDCT.ID_HDON_DC = lngIDHDonDC_VC;
                                else infoHDCT.ID_HDON_DC = lngIDHDonDC;
                            }
                            else
                            {
                                int intTC = obj_HDN_HDON_DC_Controller.LstInfo.Where(c => c.LOAI_HDON == "TC" && c.LOAI_DCHINH != "HB").Count();
                                if (intTC > 0)
                                {
                                    infoHDCT.ID_HDON_DC = lngIDHDonDC;
                                }
                                else
                                {
                                    if (arrHDonVC == null || arrHDonVC.Count() == 0)
                                        infoHDCT.ID_HDON_DC = lngIDHDonDC;
                                    else infoHDCT.ID_HDON_DC = lngIDHDonDC_VC;
                                }
                            }
                        }
                        else
                        {
                            if ("KT;BT;CD;TD".Contains(infoHDCT.BCS))
                            {
                                if (infoHDCT.SO_TIEN <= 0)
                                    infoHDCT.ID_HDON_DC = lngIDHDonDC_HuyBo;
                                else
                                {
                                    infoHDCT.ID_HDON_DC = infoHDCT.ID_HDON_DC == lngIDHDonDC_HuyBo ? lngIDHDonDC_HuyBo : lngIDHDonDC;
                                }
                            }
                            else
                            {
                                int intTC = obj_HDN_HDON_DC_Controller.LstInfo.Where(c => c.LOAI_HDON == "TC" && c.LOAI_DCHINH != "HB").Count();
                                if (intTC > 0)
                                {
                                    if (infoHDCT.SO_TIEN <= 0)
                                        infoHDCT.ID_HDON_DC = infoHDCT.ID_HDON_DC == lngIDHDonDC_HuyBo_VC ? lngIDHDonDC_HuyBo_VC : lngIDHDonDC_HuyBo;
                                    else
                                        infoHDCT.ID_HDON_DC = lngIDHDonDC;
                                }
                                else
                                {
                                    if (infoHDCT.SO_TIEN < 0)
                                        infoHDCT.ID_HDON_DC = lngIDHDonDC_HuyBo_VC;
                                    else
                                    {
                                        infoHDCT.ID_HDON_DC = infoHDCT.ID_HDON_DC == lngIDHDonDC_HuyBo_VC ? lngIDHDonDC_HuyBo_VC : lngIDHDonDC_VC;
                                    }
                                }


                            }
                        }

                        var chiso = from a in obj_HDN_CHISO_DC_Controller.LstInfo
                                    where a.BCS == infoHDCT.BCS
                                    && a.ID_HDON_DC == infoHDCT.ID_HDON_DC
                                    && a.MA_DDO == infoHDCT.MA_DDO
                                    && a.SO_CTO == infoHDCT.SO_CTO
                                    select a;
                        if (chiso != null && chiso.Count() > 0)
                        {
                            foreach (DataRow drChiSoTemp in dsCustomerData.Tables["HDN_CHISO_DC"].Rows)
                            {
                                if (Convert.ToInt64(drChiSoTemp["ID_CHISO_PS"]) == infoHDCT.ID_CHISO_DC)
                                {
                                    chiso = chiso.Where(c => c.ID_CHISO == Convert.ToInt64(drChiSoTemp["ID_CHISO"]));
                                    break;
                                }
                            }
                            if (chiso != null && chiso.Count() > 0)
                            {
                                infoHDCT.ID_CHISO_DC = chiso.First().ID_CHISO;
                            }
                        }

                        infoHDCT.KY = ky;
                        infoHDCT.THANG = thang;
                        infoHDCT.NAM = nam;
                        obj_HDN_HDONCTIET_DC_Controller.LstInfo.Add(infoHDCT);
                    }
                    else return strError;
                }
                //Thêm chi tiết âm của hóa đơn phát sinh gốc
                if (dsCustomerData.Tables.Contains("HDN_HDONCTIET_PS") && dsCustomerData.Tables["HDN_HDONCTIET_PS"].Rows.Count > 0)
                {
                    foreach (DataRow row in dsCustomerData.Tables["HDN_HDONCTIET_PS"].Rows)
                    {
                        //if (row["ID_HDON"].ToString().Trim() != rowHD["ID_HDON"].ToString().Trim()) continue;
                        DataRow drCTiet = dsInvoiceData.Tables["HDN_HDONCTIET_DC"].NewRow();
                        foreach (DataColumn column in dsCustomerData.Tables["HDN_HDONCTIET"].Columns)
                        {
                            if (column.ColumnName != "ID_HDON" && column.ColumnName != "ID_HDONCTIET" && column.ColumnName != "ID_CHISO")
                            {
                                if (column.ColumnName == "DIEN_TTHU" || column.ColumnName == "SO_TIEN")
                                    drCTiet[column.ColumnName] = 0 - Convert.ToDecimal(row[column.ColumnName]);
                                else
                                    drCTiet[column.ColumnName] = row[column.ColumnName];
                            }
                            drCTiet["ID_HDON_DC"] = lngIDHDonDC;
                            drCTiet["ID_HDONCTIET_DC"] = 1;
                            drCTiet["ID_CHISO_DC"] = row["ID_CHISO"];
                        }
                        drCTiet["KY"] = ky;
                        drCTiet["THANG"] = thang;
                        drCTiet["NAM"] = nam;
                        dsInvoiceData.Tables["HDN_HDONCTIET_DC"].Rows.Add(drCTiet);
                        HDN_HDONCTIET_DC infoHDCT = new HDN_HDONCTIET_DC();

                        long lngIDHDCTietDC = 0;
                        infoHDCT = BillingLibrary.BillingLibrary.MapDatarowToObject<HDN_HDONCTIET_DC>(drCTiet, ref strError);
                        infoHDCT.MA_CNANG = macnang;
                        if (strError.Trim().Length == 0)
                        {
                            lngIDHDCTietDC = obj_HDN_HDONCTIET_DC_Controller.getMaxID();
                            infoHDCT.ID_HDONCTIET_DC = lngIDHDCTietDC;
                            obj_HDN_HDONCTIET_DC_Controller.LstInfo.Add(infoHDCT);
                        }
                        else return strError;
                        dsInvoiceData.AcceptChanges();
                    }
                }
                //Kêt thúc
                #endregion

                #region HDN_KHANG_DC
                DataRow dr6 = dsCustomerData.Tables["HDN_KHANG_DC"].Rows[0];
                obj_HDN_KHANG_DC_Controller = new cls_HDN_KHANG_DC_Controller();
                HDN_KHANG_DC infoKHDC = new HDN_KHANG_DC();
                List<HDN_KHANG_DC> lstKH = new List<HDN_KHANG_DC>();
                infoKHDC = BillingLibrary.BillingLibrary.MapDatarowToObject<HDN_KHANG_DC>(dr6, ref strError);
                infoKHDC.MA_CNANG = macnang;
                if (strError.Trim().Length == 0)
                {
                    infoKHDC.ID_HDON_DC = lngIDHDonDC_Tem;
                    lstKH.Add(infoKHDC);
                    //obj_HDN_KHANG_DC_Controller.pInfor = infoKHDC;
                }
                else return strError;
                if (lngIDHDonDC_Tem != 0 && lngIDHDonDC_Tem_VC != 0 && lngIDHDonDC_Tem != lngIDHDonDC_Tem_VC)
                {
                    //Có 2 hóa đơn
                    HDN_KHANG_DC infoKHDC_VC = new HDN_KHANG_DC();
                    infoKHDC_VC = BillingLibrary.BillingLibrary.MapDatarowToObject<HDN_KHANG_DC>(dr6, ref strError);
                    infoKHDC_VC.MA_CNANG = macnang;
                    if (strError.Trim().Length == 0)
                    {
                        infoKHDC_VC.ID_HDON_DC = lngIDHDonDC_Tem_VC;
                        lstKH.Add(infoKHDC_VC);
                    }
                    else return strError;
                }
                obj_HDN_KHANG_DC_Controller.LstInfo = lstKH;

                #endregion

                #region HDN_DIEMDO_DC
                obj_HDN_DIEMDO_DC_Controller = new cls_HDN_DIEMDO_DC_Controller();
                foreach (DataRow dr7 in dsCustomerData.Tables["HDN_DIEMDO_DC"].Rows)
                {
                    HDN_DIEMDO_DC infoDDoDC = new HDN_DIEMDO_DC();
                    infoDDoDC = BillingLibrary.BillingLibrary.MapDatarowToObject<HDN_DIEMDO_DC>(dr7, ref strError);
                    infoDDoDC.MA_CNANG = macnang;
                    if (strError.Trim().Length == 0)
                    {
                        infoDDoDC.ID_HDON_DC = lngIDHDonDC_Tem;
                        obj_HDN_DIEMDO_DC_Controller.LstInfo.Add(infoDDoDC);
                    }
                    else return strError;
                    if (lngIDHDonDC_Tem != 0 && lngIDHDonDC_Tem_VC != 0 && lngIDHDonDC_Tem != lngIDHDonDC_Tem_VC)
                    {
                        HDN_DIEMDO_DC infoDDoDC_VC = new HDN_DIEMDO_DC();
                        infoDDoDC_VC = BillingLibrary.BillingLibrary.MapDatarowToObject<HDN_DIEMDO_DC>(dr7, ref strError);
                        infoDDoDC_VC.MA_CNANG = macnang;
                        if (strError.Trim().Length == 0)
                        {
                            infoDDoDC_VC.ID_HDON_DC = lngIDHDonDC_Tem_VC;
                            obj_HDN_DIEMDO_DC_Controller.LstInfo.Add(infoDDoDC_VC);
                        }
                        else return strError;
                    }
                }
                #endregion

                #region HDN_BBAN_APGIA_DC
                obj_HDN_BBAN_APGIA_DC_Controller = new cls_HDN_BBAN_APGIA_DC_Controller();
                obj_HDN_BBAN_APGIA_DC_Controller.CMIS2 = db;
                foreach (DataRow dr8 in dsCustomerData.Tables["HDN_BBAN_APGIA_DC"].Rows)
                {
                    HDN_BBAN_APGIA_DC infoApGiaDC = new HDN_BBAN_APGIA_DC();
                    long lngIDBBanApGia = 0;
                    infoApGiaDC = BillingLibrary.BillingLibrary.MapDatarowToObject<HDN_BBAN_APGIA_DC>(dr8, ref strError);
                    infoApGiaDC.MA_CNANG = macnang;
                    if (strError.Trim().Length == 0)
                    {
                        lngIDBBanApGia = obj_HDN_BBAN_APGIA_DC_Controller.getMaxID();
                        infoApGiaDC.ID_BBANAGIA = lngIDBBanApGia;
                        infoApGiaDC.ID_HDON_DC = lngIDHDonDC_Tem;
                        infoApGiaDC.ID_BBANAGIA = obj_HDN_BBAN_APGIA_DC_Controller.getMaxID();
                        obj_HDN_BBAN_APGIA_DC_Controller.LstInfo.Add(infoApGiaDC);
                    }
                    else return strError;
                }
                #endregion

                if (dsInvoiceData.Tables.Contains("HDN_HDONCOSFI_DC") && dsInvoiceData.Tables["HDN_HDONCOSFI_DC"].Rows.Count > 0)
                {
                    #region HDN_HDONCOSFI_DC
                    obj_HDN_HDONCOSFI_DC_Controller = new cls_HDN_HDONCOSFI_DC_Controller();
                    obj_HDN_HDONCOSFI_DC_Controller.CMIS2 = db;
                    foreach (DataRow dr9 in dsInvoiceData.Tables["HDN_HDONCOSFI_DC"].Rows)
                    //DataRow dr2 = dsInvoiceData.Tables["HDN_HDONCOSFI_DC"].Rows[0];
                    {
                        HDN_HDONCOSFI_DC infoHDCF = new HDN_HDONCOSFI_DC();
                        long lngIDHDCosfiDC = 0;
                        infoHDCF = BillingLibrary.BillingLibrary.MapDatarowToObject<HDN_HDONCOSFI_DC>(dr9, ref strError);
                        infoHDCF.MA_CNANG = macnang;
                        if (strError.Trim().Length == 0)
                        {
                            lngIDHDCosfiDC = obj_HDN_HDONCOSFI_DC_Controller.getMaxID();
                            infoHDCF.ID_HDONCOSFI_DC = lngIDHDCosfiDC;
                            if (arrHDonVC == null || arrHDonVC.Count() == 0)
                                infoHDCF.ID_HDON_DC = lngIDHDonDC;
                            else
                            {
                                if (arrHDonVC.First().LOAI_DCHINH == "LL")
                                {
                                    if (infoHDCF.VO_CONG < 0)
                                    {
                                        var arrHDonVC_HB = obj_HDN_HDON_DC_Controller.LstInfo.Where(c => c.LOAI_HDON == "VC" && c.LOAI_DCHINH == "HB");
                                        if (arrHDonVC_HB == null || arrHDonVC_HB.Count() == 0)
                                            //Ko có hóa đơn hủy bỏ vô công, dùng ID_HDON_DC hủy bỏ của TD
                                            infoHDCF.ID_HDON_DC = lngIDHDonDC_HuyBo;
                                        else
                                            infoHDCF.ID_HDON_DC = lngIDHDonDC_HuyBo_VC;
                                    }
                                    else
                                        infoHDCF.ID_HDON_DC = lngIDHDonDC_VC;
                                }
                                else
                                    infoHDCF.ID_HDON_DC = lngIDHDonDC_VC;
                            }
                            var chiso = from a in obj_HDN_CHISO_DC_Controller.LstInfo
                                        where a.BCS == "VC"
                                        && a.ID_HDON_DC == infoHDCF.ID_HDON_DC
                                        && a.MA_DDO == infoHDCF.MA_DDO
                                        select a;
                            if (chiso != null && chiso.Count() > 0)
                            {
                                foreach (DataRow drChiSoTemp in dsCustomerData.Tables["HDN_CHISO_DC"].Rows)
                                {
                                    if (Convert.ToInt64(drChiSoTemp["ID_CHISO_PS"]) == infoHDCF.ID_CHISO)
                                    {
                                        chiso = chiso.Where(c => c.ID_CHISO == Convert.ToInt64(drChiSoTemp["ID_CHISO"]));
                                        break;
                                    }
                                }
                                if (chiso != null && chiso.Count() > 0)
                                {
                                    infoHDCF.ID_CHISO = chiso.First().ID_CHISO;
                                }
                            }
                            infoHDCF.KY = ky;
                            infoHDCF.THANG = thang;
                            infoHDCF.NAM = nam;

                            obj_HDN_HDONCOSFI_DC_Controller.LstInfo.Add(infoHDCF);
                        }
                        else return strError;
                    }
                    #endregion
                }
                if (dsCustomerData.Tables.Contains("HDN_QHE_DDO_DC") && dsCustomerData.Tables["HDN_QHE_DDO_DC"].Rows.Count > 0)
                {
                    #region HDN_QHE_DDO_DC
                    obj_HDN_QHE_DDO_DC_Controller = new cls_HDN_QHE_DDO_DC_Controller();
                    obj_HDN_QHE_DDO_DC_Controller.CMIS2 = db;
                    foreach (DataRow dr10 in dsCustomerData.Tables["HDN_QHE_DDO_DC"].Rows)
                    //DataRow dr2 = dsInvoiceData.Tables["HDN_QHE_DDO_DC"].Rows[0];
                    {
                        HDN_QHE_DDO_DC infoQHDDo = new HDN_QHE_DDO_DC();
                        long lngIDQHeDDoDC = 0;
                        infoQHDDo = BillingLibrary.BillingLibrary.MapDatarowToObject<HDN_QHE_DDO_DC>(dr10, ref strError);
                        infoQHDDo.MA_CNANG = macnang;
                        if (strError.Trim().Length == 0)
                        {
                            lngIDQHeDDoDC = obj_HDN_QHE_DDO_DC_Controller.getMaxID();
                            infoQHDDo.ID_QHE_DC = lngIDQHeDDoDC;
                            infoQHDDo.ID_HDON_DC = lngIDHDonDC_Tem;
                            obj_HDN_QHE_DDO_DC_Controller.LstInfo.Add(infoQHDDo);
                        }
                        else return strError;
                    }
                    #endregion
                }
                if (dsCustomerData.Tables.Contains("GCS_CHISO_DKY") && dsCustomerData.Tables["GCS_CHISO_DKY"].Rows.Count > 0)
                {
                    #region GCS_CHISO Dau ky sau
                    objDKy = new cls_GCS_CHISO_Controller();
                    obj_HDN_CHISO_DC_Controller.CMIS2 = db;
                    foreach (DataRow dr11 in dsCustomerData.Tables["GCS_CHISO_DKY"].Rows)
                    //DataRow dr2 = dsInvoiceData.Tables["HDN_QHE_DDO_DC"].Rows[0];
                    {
                        GCS_CHISO infoCSDKy = new GCS_CHISO();
                        infoCSDKy = BillingLibrary.BillingLibrary.MapDatarowToObject<GCS_CHISO>(dr11, ref strError);
                        infoCSDKy.MA_CNANG = macnang;
                        if (strError.Trim().Length == 0)
                        {
                            infoCSDKy.ID_CHISO = obj_HDN_CHISO_DC_Controller.getMaxID();
                            objDKy.LstInfo.Add(infoCSDKy);
                        }
                        else return strError;
                    }
                    #endregion
                }

                return "";
            }
            catch (Exception ex)
            {
                return "Lỗi khi gán dữ liệu cho Object: " + ex.Message;
            }
        }


        public DataSet GetHDon(string strMa_DViQLy, string strMa_SoGCS, Int16 ky, Int16 thang, Int16 nam)
        {
            CMIS2 db = new CMIS2();
            try
            {
                DataSet ds = new DataSet();

                cls_HDN_HDON_Controller obj_HDN_HDON_Controller = new cls_HDN_HDON_Controller();
                cls_HDN_HDONCTIET_Controller obj_HDN_HDONCTIET_Controller = new cls_HDN_HDONCTIET_Controller();
                cls_GCS_CHISO_Controller obj_GCS_CHISO_Controller = new cls_GCS_CHISO_Controller();

                obj_HDN_HDON_Controller.CMIS2 = db;
                obj_HDN_HDONCTIET_Controller.CMIS2 = db;
                obj_GCS_CHISO_Controller.CMIS2 = db;

                var hdon = from a in obj_HDN_HDON_Controller.GetHDon(strMa_DViQLy, strMa_SoGCS, ky, thang, nam)
                           select new
                           {
                               a.MA_DVIQLY,
                               a.MA_KHANG,
                               a.TEN_KHANG,
                               STT = a.MA_KVUC + "-" + a.STT,
                               a.ID_HDON,
                               a.DIEN_TTHU,
                               a.SO_TIEN,
                               a.TIEN_GTGT,
                               a.TONG_TIEN,
                               a.LOAI_HDON,
                               a.SO_HO,
                               a.SO_CTO,
                           };

                if (hdon != null && hdon.Take(1).Count() > 0)
                {
                    DataTable dtHDon = new DataTable();
                    dtHDon = Utility.LinqToDataTable_WithoutFixSchema(hdon);
                    dtHDon.TableName = "HDN_HDON";
                    ds.Tables.Add(dtHDon);

                    long[] lngID_HDon = (from a in hdon select a.ID_HDON).ToArray();

                    var hdonctiet = from a in obj_HDN_HDONCTIET_Controller.getHDonCTiet(strMa_DViQLy, lngID_HDon)
                                    select new
                                    {
                                        a.MA_DDO,
                                        a.BCS,
                                        a.DIEN_TTHU,
                                        a.DON_GIA,
                                        a.SO_TIEN,
                                        a.NGAY_APDUNG,
                                        a.ID_HDON,
                                        a.MA_DVIQLY,
                                        a.ID_HDONCTIET,
                                        a.ID_CHISO,
                                        a.MA_KHANG,
                                    };

                    if (hdonctiet != null && hdonctiet.Take(1).Count() != 0)
                    {
                        DataTable dtHDonCTiet = new DataTable();
                        dtHDonCTiet = Utility.LinqToDataTable_WithoutFixSchema(hdonctiet);
                        dtHDonCTiet.TableName = "HDN_HDONCTIET";
                        ds.Tables.Add(dtHDonCTiet);

                        string[] arrMaDDo = (from a in hdonctiet select a.MA_DDO).ToArray();
                        var chiso = from a in obj_GCS_CHISO_Controller.getChiSo(strMa_DViQLy, arrMaDDo, ky, thang, nam)
                                    select new
                                    {
                                        a.MA_DDO,
                                        a.BCS,
                                        a.CHISO_CU,
                                        a.CHISO_MOI,
                                        a.HS_NHAN,
                                        a.SAN_LUONG,
                                        a.NGAY_DKY,
                                        a.NGAY_CKY,
                                        a.SLUONG_TTIEP,
                                        a.SLUONG_TRPHU,
                                        a.LOAI_CHISO,
                                        a.MA_TTCTO,
                                        a.MA_DVIQLY,
                                        a.MA_KHANG,
                                    };
                        if (chiso != null && chiso.Take(1).Count() > 0)
                        {
                            DataTable dtChiSo = new DataTable();
                            dtChiSo = Utility.LinqToDataTable_WithoutFixSchema(chiso);
                            dtChiSo.TableName = "GCS_CHISO";
                            ds.Tables.Add(dtChiSo);
                        }
                    }
                    return ds;
                }
                else
                {
                    return null;
                }
            }
            catch
            {
                return null;
            }
            finally
            {
                db.ReleaseConnection();
            }
        }

        public DataSet GetSoGCS_DaTinhHDon(string strMa_DViQLy, Int16 ky, Int16 thang, Int16 nam)
        {
            CMIS2 db = new CMIS2();
            try
            {
                DataSet ds = new DataSet();
                cls_GCS_CHISO_Controller obj_GCS_CHISO_Controller = new cls_GCS_CHISO_Controller();
                obj_GCS_CHISO_Controller.CMIS2 = db;

                var sogcs = from a in obj_GCS_CHISO_Controller.getSoGCS_DaTinhHoaDon(strMa_DViQLy, ky, thang, nam)
                            select new
                            {
                                a.ID_LICHGCS,
                                a.KY,
                                a.MA_DVIQLY,
                                a.MA_SOGCS,
                                a.NAM,
                                a.NGAY_CKY,
                                a.NGAY_DKY,
                                a.THANG,
                                a.TRANG_THAI,
                            };
                if (sogcs != null && sogcs.Take(1).Count() > 0)
                {
                    DataTable dtSoGCS = new DataTable();
                    dtSoGCS = Utility.LinqToDataTable_WithoutFixSchema(sogcs);
                    dtSoGCS.TableName = "GCS_LICHGCS";
                    ds.Tables.Add(dtSoGCS);
                }
                return ds;
            }
            catch
            {
                return null;
            }
            finally
            {
                db.ReleaseConnection();
            }
        }

        public string CheckExistInvoice(string strMa_DViQLy, string strMa_SoGCS, Int16 ky, Int16 thang, Int16 nam)
        {
            CMIS2 db = new CMIS2();
            try
            {
                cls_HDN_HDON_Controller obj_HDN_HDON_Controller = new cls_HDN_HDON_Controller();
                obj_HDN_HDON_Controller.CMIS2 = db;

                var hdon = from a in obj_HDN_HDON_Controller.GetHDon(strMa_DViQLy, strMa_SoGCS, ky, thang, nam)
                           select new
                           {
                               a.MA_DVIQLY,
                               a.MA_KHANG,
                               a.TEN_KHANG,
                               STT = a.MA_KVUC + "-" + a.STT,
                               a.ID_HDON,
                               a.DIEN_TTHU,
                               a.SO_TIEN,
                               a.TIEN_GTGT,
                               a.TONG_TIEN,
                               a.LOAI_HDON,
                               a.SO_HO,
                               a.SO_CTO,
                           };

                if (hdon != null && hdon.Take(1).Count() > 0)
                {
                    //So da tinh hoa don
                    return "Sổ " + strMa_SoGCS + " đã có hoá đơn kỳ " + ky.ToString() + " - tháng " + thang.ToString().PadLeft(2, Convert.ToChar("0")) + "/" + nam.ToString().PadLeft(4, Convert.ToChar("0"));
                }
                else
                {
                    //So chua tinh hoa don
                    return "";
                }
            }
            catch
            {
                //Loi khi kiem tra so da tinh hoa don hay chua
                return "Lỗi khi kiểm tra tình trạng tính hoá đơn của sổ " + strMa_SoGCS + " kỳ " + ky.ToString() + " - tháng " + thang.ToString().PadLeft(2, Convert.ToChar("0")) + "/" + nam.ToString().PadLeft(4, Convert.ToChar("0"));
            }
            finally
            {
                db.ReleaseConnection();
            }
        }

        #region Tinh hoa don cmis 3
        public string InsertInvoiceDataPlus(DataSet dsInvoiceData, DataSet dsCustomerData, string strMa_DViQLy, string strMa_SoGCS, string strTenDNhap, long lngCurrentLibID, long lngWorkflowID, short ky, short thang, short nam)
        {
            //CMIS2 db = new CMIS2();
            try
            {
                //Debug.WriteLine(DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff") + " InsertInvoiceDataPlus Start");
                List<GCS_CHISO_PLUS> lstChiSo = new List<GCS_CHISO_PLUS>();
                List<HDN_HDON_PLUS> lstHDon = new List<HDN_HDON_PLUS>();
                List<HDN_HDONCOSFI_PLUS> lstHDonCosfi = new List<HDN_HDONCOSFI_PLUS>();
                List<HDN_HDONCTIET_PLUS> lstHDonCTiet = new List<HDN_HDONCTIET_PLUS>();
                string strResult = SetPropertiesForObjectPlus(dsInvoiceData, dsCustomerData, lstChiSo, lstHDon, lstHDonCTiet, lstHDonCosfi, strTenDNhap, lngCurrentLibID.ToString());
                //Debug.WriteLine(DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff") + " InsertInvoiceDataPlus SetPropertiesForObjectPlusComplete");
                if (strResult != "") return strResult;
                if (lstHDon == null || lstHDon.Count <= 0) return "Danh sach hoa don khong co";
                if (lstHDonCTiet == null || lstHDonCTiet.Count <= 0) return "Danh sach hoa don chi tiet khong co";
                //Viet lai doan code nay de submitchange 1 lan
                var arrMaKHang = lstHDon.Select(c => c.MA_KHANG).Distinct();
                //obj_HDN_HDON_Controller.CMIS2 = db;
                //obj_HDN_HDONCTIET_Controller.CMIS2 = db;
                //obj_HDN_HDONCOSFI_Controller.CMIS2 = db;
                //obj_GCS_CHISO_Controller.CMIS2 = db;
                inpDLieuHDon inpTHD = new inpDLieuHDon();
                inpTHD.LST_CSO = new List<GCS_CHISO_PLUS>();
                inpTHD.LST_HDCF = new List<HDN_HDONCOSFI_PLUS>();
                inpTHD.LST_HDCT = new List<HDN_HDONCTIET_PLUS>();
                inpTHD.LST_HDON = new List<HDN_HDON_PLUS>();
                //Tach ra theo tung ma khach hang
                foreach (var x in arrMaKHang)
                {
                    if (x == "PD01000010568")
                    {
                    }
                    string strTemp = "";
                    List<HDN_HDON_PLUS> lstHDon_Temp = lstHDon.Where(c => c.MA_KHANG == x).ToList();
                    List<HDN_HDONCTIET_PLUS> lstHDonCTiet_Temp = lstHDonCTiet.Where(c => c.MA_KHANG == x).ToList();
                    List<GCS_CHISO_PLUS> lstChiSo_Temp = lstChiSo.Where(c => c.MA_KHANG == x).ToList();
                    inpTHD.LST_HDON.AddRange(lstHDon_Temp);
                    inpTHD.LST_HDCT.AddRange(lstHDonCTiet_Temp);
                    inpTHD.LST_CSO.AddRange(lstChiSo_Temp);
                    //obj_HDN_HDON_Controller.lstHDon = lstHDon_Temp;
                    //obj_HDN_HDONCTIET_Controller.lstCTiet = lstHDonCTiet_Temp;
                    //obj_GCS_CHISO_Controller.LstInfo = new List<GCS_CHISO>();
                    //obj_GCS_CHISO_Controller.LstInfo = lstChiSo_Temp;
                    //strTemp += obj_HDN_HDON_Controller.InsertHDN_HDON();
                    //strTemp += obj_HDN_HDONCTIET_Controller.InsertHDN_HDONCTIET();
                    if (lstHDonCosfi != null && lstHDonCosfi.Count > 0)
                    {

                        List<HDN_HDONCOSFI_PLUS> lstHDonCosfi_Temp = lstHDonCosfi.Where(c => c.MA_KHANG == x).ToList();
                        if (lstHDonCosfi_Temp != null && lstHDonCosfi_Temp.Count > 0)
                        {
                            foreach (HDN_HDONCOSFI_PLUS hcosfi in lstHDonCosfi_Temp)
                            {
                                if (lstHDon_Temp != null && lstHDon_Temp.Count > 0)
                                {
                                    var hdon = lstHDon_Temp.Where(c => c.LOAI_HDON == "VC");
                                    if (hdon != null && hdon.Count() > 0)
                                    {
                                        hcosfi.ID_HDON = hdon.First().ID_HDON;
                                    }
                                }
                            }
                        }
                        inpTHD.LST_HDCF.AddRange(lstHDonCosfi_Temp);
                        //obj_HDN_HDONCOSFI_Controller.lstCosfi = lstHDonCosfi_Temp;
                        //strTemp = strTemp + obj_HDN_HDONCOSFI_Controller.InsertHDN_HDONCOSFI();
                        lstHDonCosfi = lstHDonCosfi.Where(c => c.MA_KHANG != x).ToList();
                    }

                    //strTemp += obj_GCS_CHISO_Controller.UpdateList();

                    //if (strTemp != "")
                    //{
                    //    //Bao loi insert hoa don
                    //    return "Lỗi Insert hóa đơn, mã sổ = " + strMa_SoGCS + " mã khách hàng = " + x + ": " + strTemp;
                    //}

                    lstHDon = lstHDon.Where(c => c.MA_KHANG != x).ToList();
                    lstHDonCTiet = lstHDonCTiet.Where(c => c.MA_KHANG != x).ToList();
                    lstChiSo = lstChiSo.Where(c => c.MA_KHANG != x).ToList();
                }
                if (lstChiSo != null && lstChiSo.Count > 0)
                {
                    //obj_GCS_CHISO_Controller.LstInfo = lstChiSo;
                    //obj_GCS_CHISO_Controller.UpdateList();
                    inpTHD.LST_CSO.AddRange(lstChiSo);
                }
                //Debug.WriteLine(DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff") + " InsertInvoiceDataPlus TachKH");

                //Insert luong so ghi chi so
                string strInput = JsonConvert.SerializeObject(inpTHD);
                //Debug.WriteLine(DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff") + " InsertInvoiceDataPlus SerializeObject");
                //Gọi hàm insert
                string strIP = ConfigurationManager.AppSettings["URI"];
                var baseAddress = "http://" + strIP + "/ServiceHDonPSinh-HDonPSinh-context-root/resources/serviceHDonPSinh/insertTHD";
                var http = (HttpWebRequest)WebRequest.Create(new Uri(baseAddress));
                http.Accept = "application/json";
                http.ContentType = "application/json";
                http.Method = "POST";

                //string parsedContent = "{'MA_DVIQLY':'PC06AA','TEN_DANH_MUC':'D_SOGCS'}";
                string parsedContent = strInput;
                //ASCIIEncoding encoding = new ASCIIEncoding();
                Byte[] bytes = System.Text.Encoding.UTF8.GetBytes(parsedContent);

                Stream newStream = http.GetRequestStream();
                newStream.Write(bytes, 0, bytes.Length);
                newStream.Close();

                var response = http.GetResponse();
                //Debug.WriteLine(DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff") + " InsertInvoiceDataPlus GetResponse");
                var stream = response.GetResponseStream();
                var sr = new StreamReader(stream);
                var content = sr.ReadToEnd();
                ReturnObject obj = JsonConvert.DeserializeObject<ReturnObject>(content);
                //Debug.WriteLine(DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff") + " InsertInvoiceDataPlus DeserializeObject");
                if (obj.TYPE == "ERROR") return obj.MESSAGE;
                else return "";

            }
            catch (Exception ex)
            {
                return "Lỗi khi Insert dữ liệu hóa đơn với mã sổ= " + strMa_SoGCS + ": " + ex.Message;
            }
            finally
            {
                //db.ReleaseConnection();
            }
        }
        protected string SetPropertiesForObjectPlus(DataSet dsInvoiceData, DataSet dsCustomerData, List<GCS_CHISO_PLUS> lstChiSo, List<HDN_HDON_PLUS> lstHDon, List<HDN_HDONCTIET_PLUS> lstHDonCTiet, List<HDN_HDONCOSFI_PLUS> lstHDonCosfi, string strTenDNhap, string strMaCNang)
        {
            //String strddo = "";
            //HDN_HDON
            if (dsInvoiceData == null) return "NoDataFound!- Không tìm thấy dữ liệu!";
            if (dsInvoiceData.Tables["HDN_HDON"] == null)
            {
                return "NoInvoiceDataFound!- Không tìm thấy dữ liệu hóa đơn!";
            }
            if (dsInvoiceData.Tables["HDN_HDONCTIET"] == null)
            {
                return "NoInvoiceDataFound!- Không tìm thấy dữ liệu hóa đơn chi tiết!";
            }
            //if (dsInvoiceData.Tables["HDN_HDON"] == null || dsInvoiceData.Tables["HDN_HDON"].Rows.Count == 0) return "NoInvoiceDataFound!- Không tìm thấy dữ liệu hóa đơn!";
            //if (dsInvoiceData.Tables["HDN_HDONCTIET"] == null || dsInvoiceData.Tables["HDN_HDONCTIET"].Rows.Count == 0) return "NoInvoiceDataFound!- Không tìm thấy dữ liệu hóa đơn chi tiết!";
            //lstHDon = new List<HDN_HDON_PLUS>();
            //lstHDonCosfi = new List<HDN_HDONCOSFI_PLUS>();
            //lstHDonCTiet = new List<HDN_HDONCTIET_PLUS>();
            //List<GCS_CHISO_PLUS> lstChiSo = new List<GCS_CHISO_PLUS>();
            try
            {
                long lngIDHDon = 0;
                DataTable dtTemp = dsInvoiceData.Tables["HDN_HDONCTIET"].Copy();
                DataTable dtCosfi = dsInvoiceData.Tables["HDN_HDONCOSFI"].Copy();
                int id_hdon_sequence = 0;
                int id_hdct_sequence = 0;
                int id_hdcf_sequence = 0;
                String strTemp = JsonConvert.SerializeObject(dsInvoiceData.Tables["HDN_HDON"]);
                strTemp = JsonConvert.SerializeObject(dsInvoiceData.Tables["HDN_HDONCTIET"]);
                strTemp = JsonConvert.SerializeObject(dsInvoiceData.Tables["HDN_HDONCOSFI"]);
                foreach (DataRow row in dsInvoiceData.Tables["HDN_HDON"].Rows)
                {
                    #region HDN_HDON
                    HDN_HDON_PLUS info = new HDN_HDON_PLUS();

                    if (row["CHI_TIET"].ToString().Trim().Length > 0)
                        info.CHI_TIET = Convert.ToInt16(row["CHI_TIET"].ToString());
                    if (row["COSFI"].ToString().Trim().Length > 0)
                        info.COSFI = Convert.ToDecimal(row["COSFI"].ToString());
                    info.DCHI_KHANG = row["DCHI_KHANG"].ToString();
                    info.DCHI_KHANGTT = row["DCHI_KHANGTT"].ToString();
                    if (row["DIEN_TTHU"].ToString().Trim().Length > 0)
                        info.DIEN_TTHU = Convert.ToDecimal(row["DIEN_TTHU"].ToString());
                    else
                        return "Lỗi dữ liệu HDN_HDON: DIEN_TTHU = null";
                    if (row["ID_BBANPHANH"].ToString().Trim().Length > 0)
                        info.ID_BBANPHANH = Convert.ToInt64(row["ID_BBANPHANH"].ToString());
                    //if (row["ID_HDON"].ToString().Trim().Length > 0 || row["ID_HDON"].ToString() != "0")
                    //    info.ID_HDON = Convert.ToInt64(row["ID_HDON"].ToString());
                    //else
                    //obj_HDN_HDON_Controller.CMIS2 = db;
                    info.ID_HDON = id_hdon_sequence++;
                    lngIDHDon = info.ID_HDON;
                    if (row["KCOSFI"].ToString().Trim().Length > 0)
                        info.KCOSFI = Convert.ToDecimal(row["KCOSFI"].ToString());
                    info.KIHIEU_SERY = row["KIHIEU_SERY"].ToString();
                    if (row["KY"].ToString().Trim().Length > 0)
                        info.KY = Convert.ToInt16(row["KY"].ToString());
                    else
                        return "Lỗi dữ liệu HDN_HDON: KY = null";
                    if (row["LOAI_KHANG"].ToString().Trim().Length > 0)
                        info.LOAI_KHANG = Convert.ToInt16(row["LOAI_KHANG"]);
                    else
                        return "Lỗi dữ liệu HDN_HDON: LOAI_KHANG = null";
                    info.DCHI_TTOAN = row["DCHI_TTOAN"].ToString();
                    info.MANHOM_KHANG = row["MANHOM_KHANG"].ToString();
                    info.MA_LOAIDN = row["MA_LOAIDN"].ToString();
                    info.MA_PTTT = row["MA_PTTT"].ToString();
                    info.LOAI_HDON = row["LOAI_HDON"].ToString();
                    info.MA_CNANG = strMaCNang;
                    info.MA_DVIQLY = row["MA_DVIQLY"].ToString();
                    info.MA_HTTT = row["MA_HTTT"].ToString();
                    info.MA_KHANG = row["MA_KHANG"].ToString();
                    info.MA_KHANGTT = row["MA_KHANGTT"].ToString();
                    info.MA_KVUC = row["MA_KVUC"].ToString();
                    info.MA_NHANG = row["MA_NHANG"].ToString();
                    info.MA_NVIN = row["MA_NVIN"].ToString();
                    info.MA_NVPHANH = row["MA_NVPHANH"].ToString();
                    info.MA_SOGCS = row["MA_SOGCS"].ToString();
                    info.MA_TO = row["MA_TO"].ToString();
                    info.MASO_THUE = row["MASO_THUE"].ToString();
                    if (row["NAM"].ToString().Trim().Length > 0)
                        info.NAM = Convert.ToInt16(row["NAM"].ToString());
                    else
                        return "Lỗi dữ liệu HDN_HDON: NAM = null";
                    if (row["NGAY_CKY"].ToString().Trim().Length > 0)
                        info.NGAY_CKY = Convert.ToDateTime(row["NGAY_CKY"].ToString()).ToString("dd/MM/yyyy");
                    else
                        return "Lỗi dữ liệu HDN_HDON: NGAY_CKY = null";
                    if (row["NGAY_DKY"].ToString().Trim().Length > 0)
                        info.NGAY_DKY = Convert.ToDateTime(row["NGAY_DKY"].ToString()).ToString("dd/MM/yyyy");
                    else
                        return "Lỗi dữ liệu HDN_HDON: NGAY_DKY = null";
                    if (row["NGAY_IN"].ToString().Trim().Length > 0)
                        info.NGAY_IN = row["NGAY_IN"].ToString();
                    if (row["NGAY_PHANH"].ToString().Trim().Length > 0)
                        info.NGAY_PHANH = row["NGAY_PHANH"].ToString();
                    if (row["NGAY_SUA"].ToString().Trim().Length > 0)
                        info.NGAY_SUA = Convert.ToDateTime(row["NGAY_SUA"].ToString());
                    else
                        return "Lỗi dữ liệu HDN_HDON: NGAY_SUA = null";
                    if (row["NGAY_TAO"].ToString().Trim().Length > 0)
                        info.NGAY_TAO = Convert.ToDateTime(row["NGAY_TAO"].ToString());
                    else
                        return "Lỗi dữ liệu HDN_HDON: NGAY_TAO = null";
                    info.NGUOI_SUA = strTenDNhap;
                    info.NGUOI_TAO = strTenDNhap;
                    info.SO_CTO = row["SO_CTO"].ToString();
                    if (row["SO_HO"].ToString().Trim().Length > 0)
                        info.SO_HO = Convert.ToDecimal(row["SO_HO"].ToString());
                    else
                        return "Lỗi dữ liệu HDN_HDON: SO_HO = null";
                    if (row["SO_LANIN"].ToString().Trim().Length > 0)
                        info.SO_LANIN = Convert.ToInt16(row["SO_LANIN"].ToString());
                    info.SO_SERY = row["SO_SERY"].ToString();
                    if (row["SO_TIEN"].ToString().Trim().Length > 0)
                        info.SO_TIEN = Convert.ToDecimal(row["SO_TIEN"].ToString());
                    else
                        return "Lỗi dữ liệu HDN_HDON: SO_TIEN = null";
                    info.STT = row["STT"].ToString();
                    if (row["STT_IN"].ToString().Trim().Length > 0)
                        info.STT_IN = Convert.ToInt32(row["STT_IN"].ToString());
                    info.TEN_KHANG = row["TEN_KHANG"].ToString();
                    info.TEN_KHANGTT = row["TEN_KHANGTT"].ToString();
                    if (row["THANG"].ToString().Trim().Length > 0)
                        info.THANG = Convert.ToInt16(row["THANG"].ToString());
                    else
                        return "Lỗi dữ liệu HDN_HDON: THANG = null";
                    if (row["TIEN_GTGT"].ToString().Trim().Length > 0)
                        info.TIEN_GTGT = Convert.ToDecimal(row["TIEN_GTGT"].ToString());
                    else
                        return "Lỗi dữ liệu HDN_HDON: TIEN_GTGT = null";
                    info.TKHOAN_KHANG = row["TKHOAN_KHANG"].ToString();
                    if (row["TONG_TIEN"].ToString().Trim().Length > 0)
                        info.TONG_TIEN = Convert.ToDecimal(row["TONG_TIEN"].ToString());
                    else
                        return "Lỗi dữ liệu HDN_HDON: TONG_TIEN = null";
                    if (row["TYLE_THUE"].ToString().Trim().Length > 0)
                        info.TYLE_THUE = Convert.ToDecimal(row["TYLE_THUE"].ToString());
                    else
                        return "Lỗi dữ liệu HDN_HDON: TYLE_THUE = null";
                    //long lngIDHDong = Convert.ToInt64(dsCustomerData.Tables["HDG_KHACH_HANG"].Select("MA_KHANG='" + info.MA_KHANG + "'")[0]["ID_HDONG"]);
                    //string strDChiTToan = (new cls_HDG_HOP_DONG_Controller()).getDCHI_TTOAN(info.MA_DVIQLY, lngIDHDong, info.NGAY_CKY.Date);
                    //if (strDChiTToan != null && strDChiTToan.Trim().Length > 0)
                    //    info.DCHI_TTOAN = strDChiTToan;
                    //else
                    //    return "Không tìm thấy hợp đồng tương ứng với mã khách hàng " + info.MA_KHANG;
                    //if (info.DCHI_TTOAN == null || info.DCHI_TTOAN.Trim().Length == 0)
                    //    return "Không tìm thấy hợp đồng tương ứng với mã khách hàng " + info.MA_KHANG;
                    info.TIEN_TD = Utility.DecimalDbnull(row["TIEN_TD"]);
                    info.THUE_TD = Utility.DecimalDbnull(row["THUE_TD"]);
                    info.TIEN_VC = Utility.DecimalDbnull(row["TIEN_VC"]);
                    info.THUE_VC = Utility.DecimalDbnull(row["THUE_VC"]);
                    info.DTHOAI = row["DTHOAI"].ToString();


                    info.TIEN_GTRU = Utility.DecimalDbnull(row["TIEN_GTRU"]);

                    if (row["TIEN_GOC"].ToString().Trim().Length > 0)
                        info.TIEN_GOC = Convert.ToDecimal(row["TIEN_GOC"].ToString());
                    else
                        info.TIEN_GOC = Convert.ToDecimal(row["SO_TIEN"].ToString());
                    info.TIEN_TD_GTRU = Utility.DecimalDbnull(row["TIEN_TD_GTRU"]);

                    if (row["TIEN_TD_GOC"].ToString().Trim().Length > 0)
                        info.TIEN_TD_GOC = Convert.ToDecimal(row["TIEN_TD_GOC"].ToString());
                    else
                        info.TIEN_TD_GOC = Utility.DecimalDbnull(row["TIEN_TD"]);

                    info.TIEN_VC_GTRU = Utility.DecimalDbnull(row["TIEN_VC_GTRU"]);

                    if (row["TIEN_VC_GOC"].ToString().Trim().Length > 0)
                        info.TIEN_VC_GOC = Convert.ToDecimal(row["TIEN_VC_GOC"].ToString());
                    else
                        info.TIEN_VC_GOC = Utility.DecimalDbnull(row["TIEN_VC"]);
                    lstHDon.Add(info);

                    #endregion

                    #region HDN_HDONCTIET
                    if (info.MA_KHANG == "PD01000010568")
                    {
                    }
                    DataRow[] arrHDonCTiet = null;
                    if (dtTemp != null && dtTemp.Rows.Count != 0)
                    {
                        List<DataRow> arrTemp = new List<DataRow>();
                        if (info.LOAI_HDON == "TD")
                        {
                            foreach (DataRow drTemp in dtTemp.Rows)
                            {
                                if (drTemp["MA_KHANG"].ToString().Trim() == info.MA_KHANG && "BT;CD;TD;KT".Contains(drTemp["BCS"].ToString().Trim()))
                                {
                                    arrTemp.Add(drTemp);
                                }
                            }
                            //arrHDonCTiet = dtTemp.Select("MA_KHANG='" + info.MA_KHANG + "' AND BCS IN ('BT','CD','TD','KT')");
                        }
                        if (info.LOAI_HDON == "VC")
                        {
                            foreach (DataRow drTemp in dtTemp.Rows)
                            {
                                if (drTemp["MA_KHANG"].ToString().Trim() == info.MA_KHANG && drTemp["BCS"].ToString().Trim() == "VC")
                                {
                                    arrTemp.Add(drTemp);
                                }
                            }
                            //arrHDonCTiet = dtTemp.Select("MA_KHANG='" + info.MA_KHANG + "' AND BCS ='VC'");
                        }

                        if (info.LOAI_HDON == "TC")
                        {
                            foreach (DataRow drTemp in dtTemp.Rows)
                            {
                                if (drTemp["MA_KHANG"].ToString().Trim() == info.MA_KHANG && "BT;CD;TD;KT;VC".Contains(drTemp["BCS"].ToString().Trim()))
                                {
                                    arrTemp.Add(drTemp);
                                }
                            }
                            //arrHDonCTiet = dtTemp.Select("MA_KHANG='" + info.MA_KHANG + "' AND BCS IN ('BT','CD','TD','KT','VC')");
                        }
                        arrHDonCTiet = arrTemp.ToArray();
                        var chitiet = dtTemp.AsEnumerable().Except(arrHDonCTiet);
                        dtTemp = (chitiet != null && chitiet.Count() > 0) ? chitiet.CopyToDataTable() : null;
                    }
                    if (arrHDonCTiet != null && arrHDonCTiet.Length > 0)
                    {
                        //obj_HDN_HDONCTIET_Controller.CMIS2 = db;
                        foreach (DataRow dr in arrHDonCTiet)
                        {
                            HDN_HDONCTIET_PLUS infoHDCT = new HDN_HDONCTIET_PLUS();
                            infoHDCT.BCS = dr["BCS"].ToString();
                            if (dr["DIEN_TTHU"].ToString().Trim().Length > 0)
                                infoHDCT.DIEN_TTHU = Convert.ToDecimal(dr["DIEN_TTHU"]);
                            else
                                return "Lỗi dữ liệu HDN_HDONCTIET: DIEN_TTHU = null";
                            infoHDCT.DINH_MUC = dr["DINH_MUC"].ToString();
                            if (dr["DON_GIA"].ToString().Trim().Length > 0)
                                infoHDCT.DON_GIA = Convert.ToDecimal(dr["DON_GIA"]);
                            if (dr["ID_CHISO"].ToString().Trim().Length > 0)
                                infoHDCT.ID_CHISO = Convert.ToInt64(dr["ID_CHISO"]);
                            else
                                return "Lỗi dữ liệu HDN_HDONCTIET: ID_CHISO = null";
                            infoHDCT.ID_HDON = info.ID_HDON;
                            //if (dr["ID_HDONCTIET"].ToString().Trim().Length > 0)
                            //    infoHDCT.ID_HDONCTIET = Convert.ToInt64(dr["ID_HDONCTIET"]);
                            //else
                            //    return "Lỗi dữ liệu HDN_HDONCTIET: ID_HDONCTIET = null";
                            infoHDCT.ID_HDONCTIET = id_hdct_sequence++;//obj_HDN_HDONCTIET_Controller.getID_HDON();
                            if (dr["KY"].ToString().Trim().Length > 0)
                                infoHDCT.KY = Convert.ToInt16(dr["KY"]);
                            else
                                return "Lỗi dữ liệu HDN_HDONCTIET: KY = null";
                            if (dr["LOAI_KHANG"].ToString().Trim().Length > 0)
                                infoHDCT.LOAI_KHANG = Convert.ToInt16(dr["LOAI_KHANG"].ToString());
                            if (dr["LOAI_DDO"].ToString().Trim().Length > 0)
                                infoHDCT.LOAI_DDO = Convert.ToInt16(dr["LOAI_DDO"].ToString());
                            if (dr["SO_PHA"].ToString().Trim().Length > 0)
                                infoHDCT.SO_PHA = Convert.ToInt16(dr["SO_PHA"].ToString());
                            infoHDCT.LOAI_DMUC = dr["LOAI_DMUC"].ToString();
                            infoHDCT.MA_CAPDA = dr["MA_CAPDA"].ToString();
                            infoHDCT.MA_CNANG = strMaCNang;
                            infoHDCT.MA_DDO = dr["MA_DDO"].ToString();
                            infoHDCT.MA_DVIQLY = dr["MA_DVIQLY"].ToString();
                            infoHDCT.MA_KHANG = dr["MA_KHANG"].ToString();
                            infoHDCT.MA_KVUC = dr["MA_KVUC"].ToString();
                            infoHDCT.MA_LO = dr["MA_LO"].ToString();
                            infoHDCT.MA_NGIA = dr["MA_NGIA"].ToString();
                            infoHDCT.MA_NHOMNN = dr["MA_NHOMNN"].ToString();
                            infoHDCT.MA_NN = dr["MA_NN"].ToString();
                            infoHDCT.MA_SOGCS = dr["MA_SOGCS"].ToString();
                            infoHDCT.MA_TO = dr["MA_TO"].ToString();
                            infoHDCT.MA_TRAM = dr["MA_TRAM"].ToString();
                            if (dr["NAM"].ToString().Trim().Length > 0)
                                infoHDCT.NAM = Convert.ToInt16(dr["NAM"]);
                            else
                                return "Lỗi dữ liệu HDN_HDONCTIET: NAM = null";
                            if (dr["NGAY_APDUNG"].ToString().Trim().Length > 0)
                                infoHDCT.NGAY_APDUNG = Convert.ToDateTime(dr["NGAY_APDUNG"]).ToString("dd/MM/yyyy");
                            if (dr["NGAY_SUA"].ToString().Trim().Length > 0)
                                infoHDCT.NGAY_SUA = Convert.ToDateTime(dr["NGAY_SUA"]);
                            else
                                return "Lỗi dữ liệu HDN_HDONCTIET: NGAY_SUA = null";
                            if (dr["NGAY_TAO"].ToString().Trim().Length > 0)
                                infoHDCT.NGAY_TAO = Convert.ToDateTime(dr["NGAY_TAO"]);
                            else
                                return "Lỗi dữ liệu HDN_HDONCTIET: NGAY_TAO = null";
                            infoHDCT.NGUOI_SUA = strTenDNhap;
                            infoHDCT.NGUOI_TAO = strTenDNhap;
                            infoHDCT.SO_CTO = dr["SO_CTO"].ToString();
                            if (dr["SO_TIEN"].ToString().Trim().Length > 0)
                                infoHDCT.SO_TIEN = Convert.ToDecimal(dr["SO_TIEN"]);
                            else
                                return "Lỗi dữ liệu HDN_HDONCTIET: SO_TIEN = null";
                            infoHDCT.SO_TIEN = Convert.ToDecimal(dr["SO_TIEN"]);
                            infoHDCT.STT = dr["STT"].ToString();
                            infoHDCT.TGIAN_BDIEN = dr["TGIAN_BDIEN"].ToString();
                            if (dr["THANG"].ToString().Trim().Length > 0)
                                infoHDCT.THANG = Convert.ToInt16(dr["THANG"]);
                            else
                                return "Lỗi dữ liệu HDN_HDONCTIET: THANG = null";

                            infoHDCT.TY_LE = Utility.DecimalDbnull(dr["TY_LE"]);
                            infoHDCT.TIEN_GTRU = Utility.DecimalDbnull(dr["TIEN_GTRU"]);
                            if (dr["TIEN_GOC"].ToString().Trim().Length > 0)
                                infoHDCT.TIEN_GOC = Convert.ToDecimal(dr["TIEN_GOC"].ToString());
                            else
                                infoHDCT.TIEN_GOC = Utility.DecimalDbnull(dr["SO_TIEN"]);
                            infoHDCT.NOI_DUNG = dr["NOI_DUNG"].ToString();
                            lstHDonCTiet.Add(infoHDCT);
                        }
                    }
                    #endregion

                    #region HDN_HDONCOSFI
                    DataRow[] arrHDonCosfi = null;
                    if (dtCosfi != null && dtCosfi.Rows.Count > 0)
                    {
                        arrHDonCosfi = dtCosfi.Select("MA_KHANG='" + info.MA_KHANG + "'");
                    }

                    //MessageBox.Show("Số bản ghi: " + Convert.ToString(arrHDonCosfi.Count()), "Thông báo", MessageBoxButtons.OK, MessageBoxIcon.Information);
                    if (arrHDonCosfi != null && arrHDonCosfi.Length > 0)
                    {
                        var cosfi = dtCosfi.AsEnumerable().Except(arrHDonCosfi);
                        dtCosfi = (cosfi != null && cosfi.Count() > 0) ? cosfi.CopyToDataTable() : null;

                        //obj_HDN_HDONCOSFI_Controller.CMIS2 = db;
                        foreach (DataRow drCF in arrHDonCosfi)
                        {
                            //strddo = Convert.ToString(drCF["MA_DDO"]);
                            //Bổ sung đoạn kiểm soát MA_SOGCS null
                            if (drCF["MA_SOGCS"].ToString().Trim().Length == 0)
                            {
                                var other = arrHDonCosfi.Where(c => c.Field<string>("MA_DDO") != drCF["MA_DDO"] && c.Field<string>("MA_SOGCS") != "").ToArray();
                                if (other != null && other.Length > 0)
                                {
                                    foreach (DataColumn col in drCF.Table.Columns)
                                    {
                                        drCF[col.ColumnName] = drCF[col.ColumnName].ToString().Trim().Length == 0 ? other[0][col.ColumnName] : drCF[col.ColumnName];
                                    }
                                }
                            }
                            HDN_HDONCOSFI_PLUS infoHDCF = new HDN_HDONCOSFI_PLUS();
                            if (drCF["COSFI"].ToString().Trim().Length > 0)
                                infoHDCF.COSFI = Convert.ToDecimal(drCF["COSFI"].ToString());
                            else
                                return "Lỗi dữ liệu HDN_HDONCOSFI: COSFI = null";
                            //MessageBox.Show("1");
                            if (drCF["HUU_CONG"].ToString().Trim().Length > 0)
                                infoHDCF.HUU_CONG = Convert.ToDecimal(drCF["HUU_CONG"].ToString());
                            else
                                return "Lỗi dữ liệu HDN_HDONCOSFI: HUU_CONG = null";
                            //MessageBox.Show("2");
                            infoHDCF.ID_HDON = info.ID_HDON;
                            //if (drCF["ID_HDONCOSFI"].ToString().Trim().Length > 0)
                            //    infoHDCF.ID_HDONCOSFI = Convert.ToInt64(drCF["ID_HDONCOSFI"].ToString());
                            //else
                            //    return "Lỗi dữ liệu HDN_HDONCOSFI: ID_HDONCOSFI = null";
                            infoHDCF.ID_HDONCOSFI = id_hdcf_sequence++;//obj_HDN_HDONCOSFI_Controller.getID_HDON();
                            if (drCF["KCOSFI"].ToString().Trim().Length > 0)
                                infoHDCF.KCOSFI = Convert.ToDecimal(drCF["KCOSFI"].ToString());
                            else
                                return "Lỗi dữ liệu HDN_HDONCOSFI: KCOSFI = null";
                            //MessageBox.Show("3");
                            if (drCF["KIMUA_CSPK"].ToString().Trim().Length > 0)
                                infoHDCF.KIMUA_CSPK = Convert.ToInt16(drCF["KIMUA_CSPK"].ToString());
                            else
                                return "Lỗi dữ liệu HDN_HDONCOSFI: KIMUA_CSPK = null";
                            if (drCF["KY"].ToString().Trim().Length > 0)
                                infoHDCF.KY = Convert.ToInt16(drCF["KY"].ToString());
                            else
                                return "Lỗi dữ liệu HDN_HDONCOSFI: KY = null";
                            //MessageBox.Show("4");
                            infoHDCF.MA_CNANG = strMaCNang;
                            infoHDCF.MA_DDO = drCF["MA_DDO"].ToString();
                            infoHDCF.MA_DVIQLY = drCF["MA_DVIQLY"].ToString();
                            infoHDCF.MA_KHANG = drCF["MA_KHANG"].ToString();
                            infoHDCF.MA_KVUC = drCF["MA_KVUC"].ToString();
                            infoHDCF.MA_LO = drCF["MA_LO"].ToString();
                            infoHDCF.MA_SOGCS = drCF["MA_SOGCS"].ToString();
                            infoHDCF.MA_TO = drCF["MA_TO"].ToString();
                            infoHDCF.MA_TRAM = drCF["MA_TRAM"].ToString();
                            //MessageBox.Show("5");
                            if (drCF["NAM"].ToString().Trim().Length > 0)
                                infoHDCF.NAM = Convert.ToInt16(drCF["NAM"].ToString());
                            else
                                return "Lỗi dữ liệu HDN_HDONCOSFI: NAM = null";
                            if (drCF["NGAY_SUA"].ToString().Trim().Length > 0)
                                infoHDCF.NGAY_SUA = Convert.ToDateTime(drCF["NGAY_SUA"].ToString());
                            else
                                return "Lỗi dữ liệu HDN_HDONCOSFI: NGAY_SUA = null";
                            if (drCF["NGAY_TAO"].ToString().Trim().Length > 0)
                                infoHDCF.NGAY_TAO = Convert.ToDateTime(drCF["NGAY_TAO"].ToString());
                            else
                                return "Lỗi dữ liệu HDN_HDONCOSFI: NGAY_TAO = null";
                            //MessageBox.Show("6");
                            infoHDCF.NGUOI_SUA = strTenDNhap;
                            infoHDCF.NGUOI_TAO = strTenDNhap;
                            infoHDCF.STT = drCF["STT"].ToString();
                            if (drCF["ID_CHISO"].ToString().Trim().Length > 0)
                                infoHDCF.ID_CHISO = Convert.ToInt64(drCF["ID_CHISO"]);
                            //MessageBox.Show("7");
                            if (drCF["THANG"].ToString().Trim().Length > 0)
                                infoHDCF.THANG = Convert.ToInt16(drCF["THANG"].ToString());
                            else
                                return "Lỗi dữ liệu HDN_HDONCOSFI: THANG = null";
                            if (drCF["TIEN_HUUCONG"].ToString().Trim().Length > 0)
                                infoHDCF.TIEN_HUUCONG = Convert.ToDecimal(drCF["TIEN_HUUCONG"].ToString());
                            else
                                return "Lỗi dữ liệu HDN_HDONCOSFI: TIEN_HUUCONG = null";
                            if (drCF["TIEN_VOCONG"].ToString().Trim().Length > 0)
                                infoHDCF.TIEN_VOCONG = Convert.ToDecimal(drCF["TIEN_VOCONG"].ToString());
                            else
                                return "Lỗi dữ liệu HDN_HDONCOSFI: TIEN_VOCONG = null";
                            if (drCF["VO_CONG"].ToString().Trim().Length > 0)
                                infoHDCF.VO_CONG = Convert.ToDecimal(drCF["VO_CONG"].ToString());
                            else
                                return "Lỗi dữ liệu HDN_HDONCOSFI: VO_CONG = null";

                            infoHDCF.TIEN_HC_GTRU = Utility.DecimalDbnull(drCF["TIEN_HC_GTRU"]);
                            if (drCF["TIEN_HC_GOC"].ToString().Trim().Length > 0)
                                infoHDCF.TIEN_HC_GOC = Convert.ToDecimal(drCF["TIEN_HC_GOC"].ToString());
                            else
                                infoHDCF.TIEN_HC_GOC = Utility.DecimalDbnull(drCF["TIEN_HUUCONG"]);
                            infoHDCF.TIEN_VC_GTRU = Utility.DecimalDbnull(drCF["TIEN_VC_GTRU"]);
                            if (drCF["TIEN_VC_GOC"].ToString().Trim().Length > 0)
                                infoHDCF.TIEN_VC_GOC = Convert.ToDecimal(drCF["TIEN_VC_GOC"].ToString());
                            else
                                infoHDCF.TIEN_VC_GOC = Utility.DecimalDbnull(drCF["TIEN_VOCONG"]);
                            infoHDCF.NOI_DUNG = drCF["NOI_DUNG"].ToString();
                            //MessageBox.Show("8");
                            lstHDonCosfi.Add(infoHDCF);
                            //MessageBox.Show("9");
                        }
                    }
                    #endregion
                    //MessageBox.Show("End", "Thông báo", MessageBoxButtons.OK, MessageBoxIcon.Information);
                }
                #region DũngNT hiệu chỉnh - các chi tiết VC(không có hóa đơn VC)
                if (dtTemp != null && dtTemp.Rows.Count > 0)
                {
                    //obj_HDN_HDON_Controller.CMIS2 = db;
                    //obj_HDN_HDONCTIET_Controller.CMIS2 = db;
                    foreach (DataRow dr in dtTemp.Rows)
                    {
                        HDN_HDONCTIET_PLUS infoHDCT = new HDN_HDONCTIET_PLUS();
                        infoHDCT.BCS = dr["BCS"].ToString();
                        if (dr["DIEN_TTHU"].ToString().Trim().Length > 0)
                            infoHDCT.DIEN_TTHU = Convert.ToDecimal(dr["DIEN_TTHU"]);
                        else
                            return "Lỗi dữ liệu HDN_HDONCTIET: DIEN_TTHU = null";
                        infoHDCT.DINH_MUC = dr["DINH_MUC"].ToString();
                        if (dr["DON_GIA"].ToString().Trim().Length > 0)
                            infoHDCT.DON_GIA = Convert.ToDecimal(dr["DON_GIA"]);
                        if (dr["ID_CHISO"].ToString().Trim().Length > 0)
                            infoHDCT.ID_CHISO = Convert.ToInt64(dr["ID_CHISO"]);
                        else
                            return "Lỗi dữ liệu HDN_HDONCTIET: ID_CHISO = null";
                        infoHDCT.ID_HDON = id_hdon_sequence++;//obj_HDN_HDON_Controller.getID_HDON();
                        //if (dr["ID_HDONCTIET"].ToString().Trim().Length > 0)
                        //    infoHDCT.ID_HDONCTIET = Convert.ToInt64(dr["ID_HDONCTIET"]);
                        //else
                        //    return "Lỗi dữ liệu HDN_HDONCTIET: ID_HDONCTIET = null";
                        infoHDCT.ID_HDONCTIET = id_hdct_sequence++;//obj_HDN_HDONCTIET_Controller.getID_HDON();
                        if (dr["KY"].ToString().Trim().Length > 0)
                            infoHDCT.KY = Convert.ToInt16(dr["KY"]);
                        else
                            return "Lỗi dữ liệu HDN_HDONCTIET: KY = null";
                        if (dr["LOAI_KHANG"].ToString().Trim().Length > 0)
                            infoHDCT.LOAI_KHANG = Convert.ToInt16(dr["LOAI_KHANG"].ToString());
                        if (dr["LOAI_DDO"].ToString().Trim().Length > 0)
                            infoHDCT.LOAI_DDO = Convert.ToInt16(dr["LOAI_DDO"].ToString());
                        if (dr["SO_PHA"].ToString().Trim().Length > 0)
                            infoHDCT.SO_PHA = Convert.ToInt16(dr["SO_PHA"].ToString());
                        infoHDCT.LOAI_DMUC = dr["LOAI_DMUC"].ToString();
                        infoHDCT.MA_CAPDA = dr["MA_CAPDA"].ToString();
                        infoHDCT.MA_CNANG = strMaCNang;
                        infoHDCT.MA_DDO = dr["MA_DDO"].ToString();
                        infoHDCT.MA_DVIQLY = dr["MA_DVIQLY"].ToString();
                        infoHDCT.MA_KHANG = dr["MA_KHANG"].ToString();
                        infoHDCT.MA_KVUC = dr["MA_KVUC"].ToString();
                        infoHDCT.MA_LO = dr["MA_LO"].ToString();
                        infoHDCT.MA_NGIA = dr["MA_NGIA"].ToString();
                        infoHDCT.MA_NHOMNN = dr["MA_NHOMNN"].ToString();
                        infoHDCT.MA_NN = dr["MA_NN"].ToString();
                        infoHDCT.MA_SOGCS = dr["MA_SOGCS"].ToString();
                        infoHDCT.MA_TO = dr["MA_TO"].ToString();
                        infoHDCT.MA_TRAM = dr["MA_TRAM"].ToString();
                        if (dr["NAM"].ToString().Trim().Length > 0)
                            infoHDCT.NAM = Convert.ToInt16(dr["NAM"]);
                        else
                            return "Lỗi dữ liệu HDN_HDONCTIET: NAM = null";
                        if (dr["NGAY_APDUNG"].ToString().Trim().Length > 0)
                            infoHDCT.NGAY_APDUNG = Convert.ToDateTime(dr["NGAY_APDUNG"]).ToString("dd/MM/yyyy");
                        if (dr["NGAY_SUA"].ToString().Trim().Length > 0)
                            infoHDCT.NGAY_SUA = Convert.ToDateTime(dr["NGAY_SUA"]);
                        else
                            return "Lỗi dữ liệu HDN_HDONCTIET: NGAY_SUA = null";
                        if (dr["NGAY_TAO"].ToString().Trim().Length > 0)
                            infoHDCT.NGAY_TAO = Convert.ToDateTime(dr["NGAY_TAO"]);
                        else
                            return "Lỗi dữ liệu HDN_HDONCTIET: NGAY_TAO = null";
                        infoHDCT.NGUOI_SUA = strTenDNhap;
                        infoHDCT.NGUOI_TAO = strTenDNhap;
                        infoHDCT.SO_CTO = dr["SO_CTO"].ToString();
                        if (dr["SO_TIEN"].ToString().Trim().Length > 0)
                            infoHDCT.SO_TIEN = Convert.ToDecimal(dr["SO_TIEN"]);
                        else
                            return "Lỗi dữ liệu HDN_HDONCTIET: SO_TIEN = null";
                        infoHDCT.SO_TIEN = Convert.ToDecimal(dr["SO_TIEN"]);
                        infoHDCT.STT = dr["STT"].ToString();
                        infoHDCT.TGIAN_BDIEN = dr["TGIAN_BDIEN"].ToString();
                        if (dr["THANG"].ToString().Trim().Length > 0)
                            infoHDCT.THANG = Convert.ToInt16(dr["THANG"]);
                        else
                            return "Lỗi dữ liệu HDN_HDONCTIET: THANG = null";
                        infoHDCT.NOI_DUNG = dr["NOI_DUNG"].ToString();
                        lstHDonCTiet.Add(infoHDCT);
                    }
                }
                #endregion

                #region GCS_CHISO & GCS_CHISO_GT
                string strError = "";
                //obj_GCS_CHISO_Controller.LstInfo = new List<GCS_CHISO>();
                if (dsCustomerData.Tables["GCS_CHISO"] == null || dsCustomerData.Tables["GCS_CHISO"].Rows.Count == 0)
                    return "Không tìm thấy bảng GCS_CHISO để cập nhật dữ liệu.";
                foreach (DataRow row in dsCustomerData.Tables["GCS_CHISO"].Rows)
                {
                    GCS_CHISO_PLUS info = BillingLibrary.BillingLibrary.MapDatarowToObject<GCS_CHISO_PLUS>(row, ref strError);
                    if (strError.Trim().Length > 0)
                        return strError;
                    //obj_GCS_CHISO_Controller.LstInfo.Add(info);
                    lstChiSo.Add(info);
                }
                if (dsCustomerData.Tables.Contains("GCS_CHISO_GT") == true)
                {
                    foreach (DataRow row in dsCustomerData.Tables["GCS_CHISO_GT"].Rows)
                    {
                        GCS_CHISO_PLUS info = BillingLibrary.BillingLibrary.MapDatarowToObject<GCS_CHISO_PLUS>(row, ref strError);
                        if (strError.Trim().Length > 0)
                            return strError;
                        //obj_GCS_CHISO_Controller.LstInfo.Add(info);
                        lstChiSo.Add(info);
                    }
                }
                #endregion
                return "";

            }
            catch (Exception e)
            {
                //MessageBox.Show(strddo, "Thông báo", MessageBoxButtons.OK, MessageBoxIcon.Information);
                return "Error In SetPropertiesForObject Method: " + e.Message;
            }
        }

        public string InsertInvoiceData_PGT_Plus(string strMa_DViQLy, string strMa_SoGCS, string strTenDNhap, long lngCurrentLibID, long lngWorkflowID, short ky, short thang, short nam)
        {
            //Dùng để cập nhật trạng thái sổ trong trường hợp sổ toàn điểm đo phụ ghép tổng
            //CMIS2 db = new CMIS2();
            try
            {
                DataSet ds = new GCS_SOGCS_XULY_Entity();
                DataRow dr = ds.Tables["GCS_SOGCS_XULY"].NewRow();
                dr["CURRENTLIBID"] = lngCurrentLibID;
                dr["NEXTLIBID"] = -1;
                dr["PATH"] = "-1";
                dr["RESULTSTATE"] = "THD";
                dr["ID_BUOCXLY"] = -1;
                dr["KY"] = ky;
                dr["MA_CNANG"] = lngCurrentLibID.ToString();
                dr["MA_DVIQLY"] = strMa_DViQLy;
                dr["MA_SOGCS"] = strMa_SoGCS;
                dr["NAM"] = nam;
                dr["NGAY_BDAU"] = DateTime.Now;
                dr["NGAY_KTHUC"] = DateTime.Now;
                dr["NGAY_SUA"] = DateTime.Now;
                dr["NGAY_TAO"] = DateTime.Now;
                dr["NGUOI_SUA"] = strTenDNhap;
                dr["NGUOI_TAO"] = strTenDNhap;
                dr["NGUOI_THIEN"] = strTenDNhap;
                dr["THANG"] = thang;
                dr["WORKFLOWID"] = lngWorkflowID;
                ds.Tables["GCS_SOGCS_XULY"].Rows.Add(dr);
                ds.AcceptChanges();
                String strInput = JsonConvert.SerializeObject(ds.Tables[0].Rows[0]);
                string strIP = ConfigurationManager.AppSettings["URI"];
                var baseAddress = "http://" + strIP + "/ServiceHDonPSinh-HDonPSinh-context-root/resources/serviceHDonPSinh/insertPGT";
                var http = (HttpWebRequest)WebRequest.Create(new Uri(baseAddress));
                http.Accept = "application/json";
                http.ContentType = "application/json";
                http.Method = "POST";

                //string parsedContent = "{'MA_DVIQLY':'PC06AA','TEN_DANH_MUC':'D_SOGCS'}";
                string parsedContent = strInput;
                //ASCIIEncoding encoding = new ASCIIEncoding();
                Byte[] bytes = System.Text.Encoding.UTF8.GetBytes(parsedContent);
                //Byte[] bytes = encoding.GetBytes(parsedContent);

                Stream newStream = http.GetRequestStream();
                newStream.Write(bytes, 0, bytes.Length);
                newStream.Close();

                var response = http.GetResponse();

                var stream = response.GetResponseStream();
                var sr = new StreamReader(stream);
                var content = sr.ReadToEnd();
                ReturnObject obj = JsonConvert.DeserializeObject<ReturnObject>(content);
                if (obj.TYPE == "ERROR") return obj.MESSAGE;
                else return "";

            }
            catch (Exception ex)
            {
                return "Lỗi khi Insert dữ liệu hóa đơn với mã sổ= " + strMa_SoGCS + ": " + ex.Message;
            }
            finally
            {

            }
        }

        public class ReturnObject
        {
            public string TYPE { get; set; }
            public string MESSAGE { get; set; }

        }

        public string InsertInvoiceData_DC_Plus(DataSet dsInvoiceData, DataSet dsCustomerData)
        {
            CMIS2 db = null;// new CMIS2();
            try
            {
                string strLoaiTThuan = "";
                if (dsCustomerData.Tables.Contains("LST_TIEN_TRINH") && dsCustomerData.Tables["LST_TIEN_TRINH"].Columns.Contains("LOAI_TTHUAN") && dsCustomerData.Tables["HDN_BBAN_DCHINH"].Rows.Count > 0)
                {
                    strLoaiTThuan = dsCustomerData.Tables["LST_TIEN_TRINH"].Rows[0]["LOAI_TTHUAN"].ToString().Trim();
                }
                inpDLieuHDonDC dataInsert = new inpDLieuHDonDC();
                dataInsert.HDN_BBAN_APGIA_DC = new List<HDN_BBAN_APGIA_DC_PLUS>();
                dataInsert.HDN_BBAN_DCHINH = new List<HDN_BBAN_DCHINH_PLUS>();
                dataInsert.HDN_CHISO_DC = new List<HDN_CHISO_DC_PLUS>();
                dataInsert.HDN_DIEMDO_DC = new List<HDN_DIEMDO_DC_PLUS>();
                dataInsert.HDN_HDONCOSFI_DC = new List<HDN_HDONCOSFI_DC_PLUS>();
                dataInsert.HDN_HDONCTIET_DC = new List<HDN_HDONCTIET_DC_PLUS>();
                dataInsert.HDN_HDON_DC = new List<HDN_HDON_DC_PLUS>();
                dataInsert.HDN_KHANG_DC = new List<HDN_KHANG_DC_PLUS>();
                dataInsert.GCS_CHISO = new List<GCS_CHISO_DUP1>();

                string strResult = SetPropertiesForMultiObjectPlus(dsInvoiceData, dsCustomerData, ref dataInsert, db);

                if (strResult.Trim().Length > 0) return strResult;
                //string strInput = "";
                inpListLapDC lstDC = new inpListLapDC();
                lstDC.LST_OBJ = new List<inpLapDC>();
                //List<inpLapDC> lstDC = new List<inpLapDC>();
                //kiểm tra, tách nhóm dữ liệu hóa đơn
                if (dataInsert.HDN_HDON_DC.Count > 1)
                {

                    foreach (HDN_HDON_DC_PLUS dc in dataInsert.HDN_HDON_DC)
                    {
                        if (dataInsert.LST_TIEN_TRINH != null && dataInsert.LST_TIEN_TRINH.Count > 0)
                            dc.KIEU_PSINH = "GS";
                        else
                            dc.KIEU_PSINH = "DC";
                        dc.LOAI_TTHUAN = strLoaiTThuan;
                        inpLapDC temp = new inpLapDC();
                        temp.HDN_HDON_DC = dc;
                        temp.HDN_BBAN_DCHINH = dataInsert.HDN_BBAN_DCHINH.Where(c => c.ID_HDON_DC == dc.ID_HDON_DC).First();
                        temp.HDN_BBAN_APGIA_DC = dataInsert.HDN_BBAN_APGIA_DC.Where(c => c.ID_HDON_DC == dc.ID_HDON_DC).ToList();
                        temp.HDN_KHANG_DC = dataInsert.HDN_KHANG_DC.Where(c => c.ID_HDON_DC == dc.ID_HDON_DC).First();
                        temp.HDN_DIEMDO_DC = dataInsert.HDN_DIEMDO_DC.Where(c => c.ID_HDON_DC == dc.ID_HDON_DC).ToList();
                        temp.HDN_CHISO_DC = dataInsert.HDN_CHISO_DC.Where(c => c.ID_HDON_DC == dc.ID_HDON_DC).ToList();
                        temp.HDN_HDONCOSFI_DC = dataInsert.HDN_HDONCOSFI_DC.Where(c => c.ID_HDON_DC == dc.ID_HDON_DC).ToList();
                        temp.HDN_HDONCTIET_DC = dataInsert.HDN_HDONCTIET_DC.Where(c => c.ID_HDON_DC == dc.ID_HDON_DC).ToList();
                        lstDC.LST_OBJ.Add(temp);
                    }
                    if (lstDC.LST_OBJ[0].HDN_BBAN_DCHINH.IS_DUP1 == 1)
                        lstDC.LST_OBJ[0].GCS_CHISO = dataInsert.GCS_CHISO;

                }
                else
                {
                    inpLapDC temp = new inpLapDC();

                    temp.HDN_HDON_DC = dataInsert.HDN_HDON_DC[0];
                    temp.HDN_HDON_DC.LOAI_TTHUAN = strLoaiTThuan;
                    if (dataInsert.LST_TIEN_TRINH != null && dataInsert.LST_TIEN_TRINH.Count > 0)
                        temp.HDN_HDON_DC.KIEU_PSINH = "GS";
                    else
                        temp.HDN_HDON_DC.KIEU_PSINH = "DC";
                    temp.HDN_BBAN_DCHINH = dataInsert.HDN_BBAN_DCHINH[0];
                    temp.HDN_BBAN_APGIA_DC = dataInsert.HDN_BBAN_APGIA_DC;
                    temp.HDN_KHANG_DC = dataInsert.HDN_KHANG_DC[0];
                    temp.HDN_DIEMDO_DC = dataInsert.HDN_DIEMDO_DC;
                    temp.HDN_CHISO_DC = dataInsert.HDN_CHISO_DC;
                    temp.HDN_HDONCOSFI_DC = dataInsert.HDN_HDONCOSFI_DC;
                    temp.HDN_HDONCTIET_DC = dataInsert.HDN_HDONCTIET_DC;
                    //strInput = JsonConvert.SerializeObject(temp);
                    if (temp.HDN_BBAN_DCHINH.IS_DUP1 == 1)
                        temp.GCS_CHISO = dataInsert.GCS_CHISO;
                    lstDC.LST_OBJ.Add(temp);
                }
                #region Bổ sung lại các bảng bên KTGSMBĐ nếu có
                if (dataInsert.LST_TIEN_TRINH != null && dataInsert.LST_TIEN_TRINH.Count > 0)
                {
                    lstDC.LST_TIEN_TRINH = dataInsert.LST_TIEN_TRINH;
                }
                if (dataInsert.LST_PAN_PHAT != null && dataInsert.LST_PAN_PHAT.Count > 0)
                {
                    lstDC.LST_PAN_PHAT = dataInsert.LST_PAN_PHAT;
                }
                if (dataInsert.LST_KHANG_DDO != null && dataInsert.LST_KHANG_DDO.Count > 0)
                {
                    lstDC.LST_KHANG_DDO = dataInsert.LST_KHANG_DDO;
                }
                if (dataInsert.LST_BBAN_PLUC != null && dataInsert.LST_BBAN_PLUC.Count > 0)
                {
                    lstDC.LST_BBAN_PLUC = dataInsert.LST_BBAN_PLUC;
                }
                #endregion
                string strInput = JsonConvert.SerializeObject(lstDC);
                //strInput = strInput.Replace("null", "\"\"");
                //Gọi hàm insert
                string strIP = ConfigurationManager.AppSettings["URI"];
                var baseAddress = "";
                if (dataInsert.LST_TIEN_TRINH != null && dataInsert.LST_TIEN_TRINH.Count > 0)
                {
                    baseAddress = "http://" + strIP + "/ServiceKTraGSatMBD-KTraGSatMBD-context-root/resources/serviceKTraGSatMBD/InsertTimHdonBthuong";
                }
                else
                {
                    baseAddress = "http://" + strIP + "/ServiceHDonDChinh-HDonDChinh-context-root/resources/serviceHDonDChinh/ghiDuLieuDC";
                }
                //MessageBox.Show(baseAddress);
                
                var http = (HttpWebRequest)WebRequest.Create(new Uri(baseAddress));
                http.Accept = "application/json";
                http.ContentType = "application/json";
                http.Method = "POST";

                //string parsedContent = "{'MA_DVIQLY':'PC06AA','TEN_DANH_MUC':'D_SOGCS'}";
                string parsedContent = strInput;
                //File.WriteAllText("D:/Input_App.txt", strInput);
                //ASCIIEncoding encoding = new ASCIIEncoding();
                Byte[] bytes = System.Text.Encoding.UTF8.GetBytes(parsedContent);

                Stream newStream = http.GetRequestStream();
                newStream.Write(bytes, 0, bytes.Length);
                newStream.Close();

                var response = http.GetResponse();

                var stream = response.GetResponseStream();
                var sr = new StreamReader(stream);
                var content = sr.ReadToEnd();
                ReturnObject obj = JsonConvert.DeserializeObject<ReturnObject>(content);
                if (obj.TYPE == "ERROR") return obj.MESSAGE;
                else return "";

                return strResult;
            }
            catch (Exception ex)
            {
                return "Lỗi khi Insert dữ liệu: " + ex.Message;
            }
            finally
            {
                //db.ReleaseConnection();
            }
        }

        public object InsertInvoiceData_DC_Plus_Mobile(DataSet dsInvoiceData, DataSet dsCustomerData, ref string strResult)
        {
            CMIS2 db = null;// new CMIS2();
            try
            {
                string strLoaiTThuan = "";
                if (dsCustomerData.Tables.Contains("LST_TIEN_TRINH") && dsCustomerData.Tables["LST_TIEN_TRINH"].Columns.Contains("LOAI_TTHUAN") && dsCustomerData.Tables["HDN_BBAN_DCHINH"].Rows.Count > 0)
                {
                    strLoaiTThuan = dsCustomerData.Tables["LST_TIEN_TRINH"].Rows[0]["LOAI_TTHUAN"].ToString().Trim();
                }
                inpDLieuHDonDC dataInsert = new inpDLieuHDonDC();
                dataInsert.HDN_BBAN_APGIA_DC = new List<HDN_BBAN_APGIA_DC_PLUS>();
                dataInsert.HDN_BBAN_DCHINH = new List<HDN_BBAN_DCHINH_PLUS>();
                dataInsert.HDN_CHISO_DC = new List<HDN_CHISO_DC_PLUS>();
                dataInsert.HDN_DIEMDO_DC = new List<HDN_DIEMDO_DC_PLUS>();
                dataInsert.HDN_HDONCOSFI_DC = new List<HDN_HDONCOSFI_DC_PLUS>();
                dataInsert.HDN_HDONCTIET_DC = new List<HDN_HDONCTIET_DC_PLUS>();
                dataInsert.HDN_HDON_DC = new List<HDN_HDON_DC_PLUS>();
                dataInsert.HDN_KHANG_DC = new List<HDN_KHANG_DC_PLUS>();
                dataInsert.GCS_CHISO = new List<GCS_CHISO_DUP1>();

                strResult = SetPropertiesForMultiObjectPlus_Mobile(dsInvoiceData, dsCustomerData, ref dataInsert, db);

                if (strResult.Trim().Length > 0) return null;
                //string strInput = "";
                inpListLapDC lstDC = new inpListLapDC();
                lstDC.LST_OBJ = new List<inpLapDC>();
                //List<inpLapDC> lstDC = new List<inpLapDC>();
                //kiểm tra, tách nhóm dữ liệu hóa đơn
                if (dataInsert.HDN_HDON_DC.Count > 1)
                {

                    foreach (HDN_HDON_DC_PLUS dc in dataInsert.HDN_HDON_DC)
                    {
                        if (dataInsert.LST_TIEN_TRINH != null && dataInsert.LST_TIEN_TRINH.Count > 0)
                            dc.KIEU_PSINH = "GS";
                        else
                            dc.KIEU_PSINH = "DC";
                        dc.LOAI_TTHUAN = strLoaiTThuan;
                        inpLapDC temp = new inpLapDC();
                        temp.HDN_HDON_DC = dc;
                        temp.HDN_BBAN_DCHINH = dataInsert.HDN_BBAN_DCHINH.Where(c => c.ID_HDON_DC == dc.ID_HDON_DC).First();
                        //temp.HDN_BBAN_APGIA_DC = dataInsert.HDN_BBAN_APGIA_DC.Where(c => c.ID_HDON_DC == dc.ID_HDON_DC).ToList();
                        //temp.HDN_KHANG_DC = dataInsert.HDN_KHANG_DC.Where(c => c.ID_HDON_DC == dc.ID_HDON_DC).First();
                        //temp.HDN_DIEMDO_DC = dataInsert.HDN_DIEMDO_DC.Where(c => c.ID_HDON_DC == dc.ID_HDON_DC).ToList();
                        //temp.HDN_CHISO_DC = dataInsert.HDN_CHISO_DC.Where(c => c.ID_HDON_DC == dc.ID_HDON_DC).ToList();
                        temp.HDN_HDONCOSFI_DC = dataInsert.HDN_HDONCOSFI_DC.Where(c => c.ID_HDON_DC == dc.ID_HDON_DC).ToList();
                        temp.HDN_HDONCTIET_DC = dataInsert.HDN_HDONCTIET_DC.Where(c => c.ID_HDON_DC == dc.ID_HDON_DC).ToList();
                        lstDC.LST_OBJ.Add(temp);
                    }
                    if (lstDC.LST_OBJ[0].HDN_BBAN_DCHINH.IS_DUP1 == 1)
                        lstDC.LST_OBJ[0].GCS_CHISO = dataInsert.GCS_CHISO;

                }
                else
                {
                    inpLapDC temp = new inpLapDC();

                    temp.HDN_HDON_DC = dataInsert.HDN_HDON_DC[0];
                    temp.HDN_HDON_DC.LOAI_TTHUAN = strLoaiTThuan;
                    if (dataInsert.LST_TIEN_TRINH != null && dataInsert.LST_TIEN_TRINH.Count > 0)
                        temp.HDN_HDON_DC.KIEU_PSINH = "GS";
                    else
                        temp.HDN_HDON_DC.KIEU_PSINH = "DC";
                    temp.HDN_BBAN_DCHINH = dataInsert.HDN_BBAN_DCHINH[0];
                    //temp.HDN_BBAN_APGIA_DC = dataInsert.HDN_BBAN_APGIA_DC;
                    //temp.HDN_KHANG_DC = dataInsert.HDN_KHANG_DC[0];
                    //temp.HDN_DIEMDO_DC = dataInsert.HDN_DIEMDO_DC;
                    //temp.HDN_CHISO_DC = dataInsert.HDN_CHISO_DC;
                    temp.HDN_HDONCOSFI_DC = dataInsert.HDN_HDONCOSFI_DC;
                    temp.HDN_HDONCTIET_DC = dataInsert.HDN_HDONCTIET_DC;
                    //strInput = JsonConvert.SerializeObject(temp);
                    if (temp.HDN_BBAN_DCHINH.IS_DUP1 == 1)
                        temp.GCS_CHISO = dataInsert.GCS_CHISO;
                    lstDC.LST_OBJ.Add(temp);
                }



                return lstDC.LST_OBJ;
            }
            catch (Exception ex)
            {
                strResult = "Lỗi khi Insert dữ liệu: " + ex.Message;
                return null;
            }
            finally
            {
                //db.ReleaseConnection();
            }
        }
        protected string SetPropertiesForMultiObjectPlus(DataSet dsInvoiceData, DataSet dsCustomerData, ref inpDLieuHDonDC dataInsert, CMIS2 db)
        {
            //DũngNT viết cho chức năng Tính hóa đơn điều chỉnh
            try
            {
                string strError = "";
                //Kiểm tra trong dataset chứa thông tin điều chỉnh
                if (!dsInvoiceData.Tables.Contains("HDN_HDON"))
                    return "Lỗi không tìm thấy bảng dsInvoiceData.HDN_HDON";
                if (!dsInvoiceData.Tables.Contains("HDN_HDONCTIET"))
                    return "Lỗi không tìm thấy bảng dsInvoiceData.HDN_HDONCTIET";
                //Kiểm tra trong dataset chứa thông tin khách hàng điều chỉnh và thông tin hóa đơn cũ
                if (!dsCustomerData.Tables.Contains("HDN_HDON_TIEPNHAN"))
                    return "Lỗi không tìm thấy bảng dsCustomerData.HDN_HDON_TIEPNHAN";
                if (!dsCustomerData.Tables.Contains("HDN_HDON"))
                    return "Lỗi không tìm thấy bảng dsCustomerData.HDN_HDON";
                if (!dsCustomerData.Tables.Contains("HDN_HDONCTIET"))
                    return "Lỗi không tìm thấy bảng dsCustomerData.HDN_HDONCTIET";
                if (dsCustomerData.Tables["HDN_BBAN_DCHINH"].Rows.Count <= 0)
                    return "Lỗi không tìm thấy dữ liệu bảng dsCustomerData.HDN_BBAN_DCHINH";
                if (!dsCustomerData.Tables.Contains("HDN_BBAN_DCHINH"))
                    return "Lỗi không tìm thấy bảng dsCustomerData.HDN_BBAN_DCHINH";
                if (!dsCustomerData.Tables.Contains("HDN_KHANG_DC"))
                    return "Lỗi không tìm thấy bảng dsCustomerData.HDN_KHANG_DC";
                if (!dsCustomerData.Tables.Contains("HDN_DIEMDO_DC"))
                    return "Lỗi không tìm thấy bảng dsCustomerData.HDN_DIEMDO_DC";
                if (!dsCustomerData.Tables.Contains("HDN_CHISO_DC"))
                    return "Lỗi không tìm thấy bảng dsCustomerData.HDN_CHISO_DC";
                //if (!dsCustomerData.Tables.Contains("HDN_BCS_CTO_DC"))
                //    return "Lỗi không tìm thấy bảng dsCustomerData.HDN_BCS_CTO_DC";
                if (!dsCustomerData.Tables.Contains("HDN_BBAN_APGIA_DC"))
                    return "Lỗi không tìm thấy bảng dsCustomerData.HDN_BBAN_APGIA_DC";

                //Đổi tên bảng tránh nhầm lẫn
                dsInvoiceData.Tables["HDN_HDON"].TableName = "HDN_HDON_DC";
                dsInvoiceData.Tables["HDN_HDONCTIET"].TableName = "HDN_HDONCTIET_DC";
                if (dsInvoiceData.Tables.Contains("HDN_HDONCOSFI"))
                    dsInvoiceData.Tables["HDN_HDONCOSFI"].TableName = "HDN_HDONCOSFI_DC";
                dsInvoiceData.Tables["HDN_HDONCTIET_DC"].Columns["ID_CHISO"].ColumnName = "ID_CHISO_DC";
                dsInvoiceData.Tables["HDN_HDONCTIET_DC"].Columns["ID_HDON"].ColumnName = "ID_HDON_DC";
                dsInvoiceData.Tables["HDN_HDONCTIET_DC"].Columns["ID_HDONCTIET"].ColumnName = "ID_HDONCTIET_DC";
                dsInvoiceData.Tables["HDN_HDONCOSFI_DC"].Columns["ID_HDON"].ColumnName = "ID_HDON_DC";
                dsInvoiceData.Tables["HDN_HDONCOSFI_DC"].Columns["ID_HDONCOSFI"].ColumnName = "ID_HDONCOSFI_DC";
                dsInvoiceData.Tables["HDN_HDON_DC"].Columns.Add("ID_HDON_DC", typeof(long));
                dsInvoiceData.Tables["HDN_HDON_DC"].Columns.Add("LOAI_DCHINH", typeof(string));
                DataRow drBBan = dsCustomerData.Tables["HDN_BBAN_DCHINH"].Rows[0];
                //DũngNT bổ sung so sánh các trường hợp có hóa đơn VC - có hiệu chỉnh
                DataRow rowDC_TD = dsInvoiceData.Tables["HDN_HDON_DC"].Select("LOAI_HDON='TD' OR LOAI_HDON='TC'").FirstOrDefault();
                DataRow rowDC_VC = dsInvoiceData.Tables["HDN_HDON_DC"].Select("LOAI_HDON='VC'").FirstOrDefault();
                DataRow rowPS_TD = dsCustomerData.Tables["HDN_HDON"].Select("LOAI_HDON='TD' OR LOAI_HDON='TC'").FirstOrDefault();
                DataRow rowPS_VC = dsCustomerData.Tables["HDN_HDON"].Select("LOAI_HDON='VC'").FirstOrDefault();
                rowDC_TD["ID_HDON"] = rowPS_TD["ID_HDON"];
                if (rowDC_VC != null)
                    rowDC_VC["ID_HDON"] = rowPS_VC != null ? rowPS_VC["ID_HDON"] : rowPS_TD["ID_HDON"];
                //Kiểm tra loại điều chỉnh của hóa đơn điều chỉnh
                decimal decTyLeThue = Utility.DecimalDbnull(dsInvoiceData.Tables["HDN_HDON_DC"].Rows[0]["TYLE_THUE"]);
                decimal decDaTra = Utility.DecimalDbnull(dsCustomerData.Tables["HDN_HDON_TIEPNHAN"].Rows[0]["DA_TRA"]);
                decimal decTongNo = Utility.DecimalDbnull(dsCustomerData.Tables["HDN_HDON_TIEPNHAN"].Rows[0]["SO_TIEN"]) + Utility.DecimalDbnull(dsCustomerData.Tables["HDN_HDON_TIEPNHAN"].Rows[0]["TIEN_GTGT"]);
                if (decTongNo > decDaTra)
                    decDaTra = 0;
                bool isDCKhongPS = false;
                if (dsCustomerData != null && dsCustomerData.Tables["LST_TIEN_TRINH"] != null && dsCustomerData.Tables["LST_TIEN_TRINH"].Rows.Count > 0 && dsCustomerData.Tables["LST_TIEN_TRINH"].Columns.Contains("IS_PSINH") && dsCustomerData.Tables["LST_TIEN_TRINH"].Rows[0]["IS_PSINH"].ToString().Trim() == "0")
                {
                    isDCKhongPS = true;
                }
                decimal decTongTienDC = Utility.DecimalDbnull(rowDC_TD["TONG_TIEN"]);
                decimal decTongTien = Utility.DecimalDbnull(rowPS_TD["TONG_TIEN"]);
                string strLoaiDC = "";
                long i64SequenceHDon = 0;
                long i64SequenceCTiet = 0;
                long i64SequenceCosfi = 0;
                long i64SequenceCSo = 0;
                long i64SequenceBBan = 0;
                long i64SequenceApGia = 0;

                long lngIDHDonDC_HuyBo = 0;
                long lngIDHDonDC_HuyBo_VC = 0;
                decimal dec100 = 100;
                long lngIDHDonDC = 0;
                long lngIDHDonDC_VC = 0;
                Int16 ky = Convert.ToInt16(drBBan["KY_DC"]);
                Int16 thang = Convert.ToInt16(drBBan["THANG_DC"]);
                Int16 nam = Convert.ToInt16(drBBan["NAM_DC"]);

                if (decDaTra != 0 || isDCKhongPS == true)
                {
                    //Đã thu tiền của khách hàng => có thể là hóa đơn truy thu TT hoặc hóa đơn thoái hoàn TH
                    if (decTongTienDC > decTongTien) strLoaiDC = "TT";
                    else if (decTongTienDC < decTongTien) strLoaiDC = "TH";
                    else strLoaiDC = "RA"; //Re Ask: hỏi lại sau                    
                    //if (strLoaiDC == "RA") return "HĐ điều chỉnh có tổng tiền bằng với HĐ phát sinh. Không lập hóa đơn điều chỉnh.";
                    rowDC_TD["LOAI_DCHINH"] = strLoaiDC;

                    //obj_HDN_HDON_DC_Controller.CMIS2 = db;
                    i64SequenceHDon++;
                    lngIDHDonDC = i64SequenceHDon;//obj_HDN_HDON_DC_Controller.getMaxID();
                    i64SequenceHDon++;
                    lngIDHDonDC_VC = i64SequenceHDon;// obj_HDN_HDON_DC_Controller.getMaxID();
                    //Tính lại các giá trị tiền
                    rowDC_TD["DIEN_TTHU"] = Utility.DecimalDbnull(rowDC_TD["DIEN_TTHU"]) - Utility.DecimalDbnull(rowPS_TD["DIEN_TTHU"]);
                    rowDC_TD["SO_TIEN"] = Utility.DecimalDbnull(rowDC_TD["SO_TIEN"]) - Utility.DecimalDbnull(rowPS_TD["SO_TIEN"]);
                    //rowDC_TD["TIEN_GTGT"] = Utility.DecimalDbnull(rowDC_TD["TIEN_GTGT"]) - Utility.DecimalDbnull(rowPS_TD["TIEN_GTGT"]);
                    rowDC_TD["TIEN_GTGT"] = Math.Round(decTyLeThue * Utility.DecimalDbnull(rowDC_TD["SO_TIEN"]) / dec100, 0, MidpointRounding.AwayFromZero);
                    rowDC_TD["TONG_TIEN"] = Utility.DecimalDbnull(rowDC_TD["SO_TIEN"]) + Utility.DecimalDbnull(rowDC_TD["TIEN_GTGT"]);

                    rowDC_TD["TIEN_TD"] = Utility.DecimalDbnull(rowDC_TD["TIEN_TD"]) - Utility.DecimalDbnull(rowPS_TD["TIEN_TD"]);
                    rowDC_TD["THUE_TD"] = Math.Round(decTyLeThue * Utility.DecimalDbnull(rowDC_TD["TIEN_TD"]) / dec100, 0, MidpointRounding.AwayFromZero);
                    rowDC_TD["TIEN_VC"] = Utility.DecimalDbnull(rowDC_TD["SO_TIEN"]) - Utility.DecimalDbnull(rowDC_TD["TIEN_TD"]);
                    rowDC_TD["THUE_VC"] = Utility.DecimalDbnull(rowDC_TD["TIEN_GTGT"]) - Utility.DecimalDbnull(rowDC_TD["THUE_TD"]);

                    rowDC_TD["KY"] = ky;
                    rowDC_TD["THANG"] = thang;
                    rowDC_TD["NAM"] = nam;
                    if (rowDC_VC != null)
                    {
                        if (rowPS_VC == null) //Không có hóa đơn vô công phát sinh
                            rowDC_VC["LOAI_DCHINH"] = "TT";
                        else
                        {
                            decimal decTongTienDC_VC = Utility.DecimalDbnull(rowDC_VC["TONG_TIEN"]);
                            decimal decTongTien_VC = Utility.DecimalDbnull(rowPS_VC["TONG_TIEN"]);
                            rowDC_VC["DIEN_TTHU"] = Utility.DecimalDbnull(rowDC_VC["DIEN_TTHU"]) - Utility.DecimalDbnull(rowPS_VC["DIEN_TTHU"]);
                            rowDC_VC["SO_TIEN"] = Utility.DecimalDbnull(rowDC_VC["SO_TIEN"]) - Utility.DecimalDbnull(rowPS_VC["SO_TIEN"]);
                            rowDC_VC["TIEN_GTGT"] = Math.Round(decTyLeThue * Utility.DecimalDbnull(rowDC_VC["SO_TIEN"]) / dec100, 0, MidpointRounding.AwayFromZero);
                            rowDC_VC["TONG_TIEN"] = Utility.DecimalDbnull(rowDC_VC["SO_TIEN"]) + Utility.DecimalDbnull(rowDC_VC["TIEN_GTGT"]);
                            rowDC_VC["TIEN_TD"] = Utility.DecimalDbnull(rowDC_VC["TIEN_TD"]) - Utility.DecimalDbnull(rowPS_VC["TIEN_TD"]);
                            rowDC_VC["THUE_TD"] = Math.Round(decTyLeThue * Utility.DecimalDbnull(rowDC_VC["TIEN_TD"]) / dec100, 0, MidpointRounding.AwayFromZero);
                            rowDC_VC["TIEN_VC"] = Utility.DecimalDbnull(rowDC_VC["SO_TIEN"]) - Utility.DecimalDbnull(rowDC_VC["TIEN_TD"]);
                            rowDC_VC["THUE_VC"] = Utility.DecimalDbnull(rowDC_VC["TIEN_GTGT"]) - Utility.DecimalDbnull(rowDC_VC["THUE_TD"]);
                            rowDC_VC["KY"] = ky;
                            rowDC_VC["THANG"] = thang;
                            rowDC_VC["NAM"] = nam;
                            if (decTongTienDC_VC > decTongTien_VC) rowDC_VC["LOAI_DCHINH"] = "TT";
                            else if (decTongTienDC_VC < decTongTien_VC) rowDC_VC["LOAI_DCHINH"] = "TH";
                            else
                            {
                                //Tiền TD DC = tiền TD PS, tiền VC DC = tiền VC PS
                                if (strLoaiDC == "RA")
                                    return "HĐ điều chỉnh có tổng tiền bằng với HĐ phát sinh. Không lập hóa đơn điều chỉnh.";
                                //Không ghi hóa đơn VC DC vào DB
                                dsInvoiceData.Tables["HDN_HDON_DC"].Rows.Remove(rowDC_VC);
                            }
                        }
                    }
                    var arrID_HDON_DC_TD = (from a in dsInvoiceData.Tables["HDN_HDON_DC"].AsEnumerable()
                                            where (a.Field<string>("LOAI_HDON") == "TD" || a.Field<string>("LOAI_HDON") == "TC")
                                            && a.Field<string>("LOAI_DCHINH") != "HB"
                                            && a.Field<string>("LOAI_DCHINH") != "RA"
                                            select a);
                    var arrID_HDON_DC_VC = (from a in dsInvoiceData.Tables["HDN_HDON_DC"].AsEnumerable()
                                            where a.Field<string>("LOAI_HDON") == "VC"
                                            && a.Field<string>("LOAI_DCHINH") != "HB"
                                            && a.Field<string>("LOAI_DCHINH") != "RA"
                                            select a);
                    if ((arrID_HDON_DC_TD == null || arrID_HDON_DC_TD.Count() == 0) && (arrID_HDON_DC_VC == null || arrID_HDON_DC_VC.Count() == 0))
                        return "Hóa đơn điều chỉnh có tổng tiền bằng hóa đơn sai. Không thực hiện điều chỉnh";

                    if (isDCKhongPS)
                    {
                        //Không có hóa đơn phát sinh
                    }
                    else
                    {
                        //Có hóa đơn phát sinh, làm như bình thường
                        foreach (DataRow row in dsCustomerData.Tables["HDN_HDONCTIET"].Rows)
                        {
                            DataRow drCTiet = dsInvoiceData.Tables["HDN_HDONCTIET_DC"].NewRow();
                            foreach (DataColumn column in dsCustomerData.Tables["HDN_HDONCTIET"].Columns)
                            {
                                if (column.ColumnName != "ID_HDON" && column.ColumnName != "ID_HDONCTIET" && column.ColumnName != "ID_CHISO")
                                {
                                    if (column.ColumnName == "DIEN_TTHU" || column.ColumnName == "SO_TIEN")
                                        drCTiet[column.ColumnName] = 0 - Convert.ToDecimal(row[column.ColumnName]);
                                    else
                                        drCTiet[column.ColumnName] = row[column.ColumnName];
                                }
                                //Kiểm tra chi tiết 
                                //BT,CD,TD,KT: ID_HDON_DC = ID_HDON_DC của hóa đơn TD (nếu không có TD lấy = VC)



                                drCTiet["ID_HDONCTIET_DC"] = 1;
                                drCTiet["ID_CHISO_DC"] = row["ID_CHISO"];
                            }
                            drCTiet["KY"] = ky;
                            drCTiet["THANG"] = thang;
                            drCTiet["NAM"] = nam;
                            if ("KT;BT;CD;TD".Contains(drCTiet["BCS"].ToString().Trim()))
                            {
                                if (arrID_HDON_DC_TD == null || arrID_HDON_DC_TD.Count() == 0)
                                    drCTiet["ID_HDON_DC"] = lngIDHDonDC_VC;
                                else drCTiet["ID_HDON_DC"] = lngIDHDonDC;
                            }
                            else
                            {
                                if (arrID_HDON_DC_VC == null || arrID_HDON_DC_VC.Count() == 0)
                                    drCTiet["ID_HDON_DC"] = lngIDHDonDC;
                                else drCTiet["ID_HDON_DC"] = lngIDHDonDC_VC;
                            }
                            dsInvoiceData.Tables["HDN_HDONCTIET_DC"].Rows.Add(drCTiet);
                            dsInvoiceData.AcceptChanges();
                        }
                    }

                    if (dsCustomerData.Tables.Contains("HDN_HDONCOSFI"))
                    {
                        foreach (DataRow row in dsCustomerData.Tables["HDN_HDONCOSFI"].Rows)
                        {
                            DataRow drCosfi = dsInvoiceData.Tables["HDN_HDONCOSFI_DC"].NewRow();
                            foreach (DataColumn column in dsCustomerData.Tables["HDN_HDONCOSFI"].Columns)
                            {
                                if (column.ColumnName != "ID_HDON" && column.ColumnName != "ID_HDONCOSFI")//
                                {
                                    if (column.ColumnName == "TIEN_HUUCONG" || column.ColumnName == "TIEN_VOCONG" || column.ColumnName == "VO_CONG")
                                        drCosfi[column.ColumnName] = 0 - Convert.ToDecimal(row[column.ColumnName]);
                                    else
                                        drCosfi[column.ColumnName] = row[column.ColumnName];
                                }

                                drCosfi["ID_HDONCOSFI_DC"] = 1;
                            }
                            drCosfi["KY"] = ky;
                            drCosfi["THANG"] = thang;
                            drCosfi["NAM"] = nam;
                            if (arrID_HDON_DC_VC == null || arrID_HDON_DC_VC.Count() == 0)
                                drCosfi["ID_HDON_DC"] = lngIDHDonDC;
                            else drCosfi["ID_HDON_DC"] = lngIDHDonDC_VC;
                            dsInvoiceData.Tables["HDN_HDONCOSFI_DC"].Rows.Add(drCosfi);
                            dsInvoiceData.AcceptChanges();
                        }
                    }
                }
                else
                {
                    //Chưa thu tiền khách hàng  
                    //Hóa đơn điều chỉnh mới là hóa đơn lập lại LL
                    foreach (DataRow row in dsInvoiceData.Tables["HDN_HDON_DC"].Rows)
                    {
                        row["LOAI_DCHINH"] = "LL";
                        //set lai ky thang nam dieu chinh
                        row["KY"] = ky;
                        row["THANG"] = thang;
                        row["NAM"] = nam;
                    }
                    dsInvoiceData.AcceptChanges();

                    //obj_HDN_HDON_DC_Controller.CMIS2 = db;
                    i64SequenceHDon++;
                    lngIDHDonDC_HuyBo = i64SequenceHDon;// obj_HDN_HDON_DC_Controller.getMaxID();
                    i64SequenceHDon++;
                    lngIDHDonDC_HuyBo_VC = i64SequenceHDon;// obj_HDN_HDON_DC_Controller.getMaxID();
                    i64SequenceHDon++;
                    lngIDHDonDC = i64SequenceHDon;// obj_HDN_HDON_DC_Controller.getMaxID();
                    i64SequenceHDon++;
                    lngIDHDonDC_VC = i64SequenceHDon;// obj_HDN_HDON_DC_Controller.getMaxID();
                    //Tạo thêm hóa đơn hủy bỏ HB
                    foreach (DataRow rowHD in dsCustomerData.Tables["HDN_HDON"].Rows)
                    {
                        DataRow drHDon_HuyBo = dsInvoiceData.Tables["HDN_HDON_DC"].NewRow();
                        foreach (DataColumn column in dsCustomerData.Tables["HDN_HDON"].Columns)
                        {
                            if (column.ColumnName != "ID_HDON")
                            {
                                if (column.ColumnName == "DIEN_TTHU" || column.ColumnName == "SO_TIEN" || column.ColumnName == "TIEN_GTGT" || column.ColumnName == "TONG_TIEN" || column.ColumnName == "TIEN_TD" || column.ColumnName == "THUE_TD" || column.ColumnName == "TIEN_VC" || column.ColumnName == "THUE_VC")
                                    drHDon_HuyBo[column.ColumnName] = 0 - Utility.DecimalDbnull(rowHD[column.ColumnName]);
                                else
                                    drHDon_HuyBo[column.ColumnName] = rowHD[column.ColumnName];
                            }
                            drHDon_HuyBo["ID_HDON"] = rowHD["ID_HDON"];
                            drHDon_HuyBo["ID_HDON_DC"] = lngIDHDonDC_HuyBo;
                            drHDon_HuyBo["LOAI_DCHINH"] = "HB";
                        }
                        drHDon_HuyBo["KY"] = ky;
                        drHDon_HuyBo["THANG"] = thang;
                        drHDon_HuyBo["NAM"] = nam;
                        dsInvoiceData.Tables["HDN_HDON_DC"].Rows.Add(drHDon_HuyBo);
                        dsInvoiceData.AcceptChanges();
                        if (dsInvoiceData.Tables.Contains("HDN_HDONCTIET_DC") && dsInvoiceData.Tables["HDN_HDONCTIET_DC"].Rows.Count > 0)
                        {
                            foreach (DataRow row in dsInvoiceData.Tables["HDN_HDONCTIET_DC"].Rows)
                            {
                                row["ID_HDON_DC"] = lngIDHDonDC;
                            }
                            dsInvoiceData.Tables["HDN_HDONCTIET_DC"].AcceptChanges();
                        }
                        foreach (DataRow row in dsCustomerData.Tables["HDN_HDONCTIET"].Rows)
                        {
                            if (row["ID_HDON"].ToString().Trim() != rowHD["ID_HDON"].ToString().Trim()) continue;
                            DataRow drCTiet = dsInvoiceData.Tables["HDN_HDONCTIET_DC"].NewRow();
                            foreach (DataColumn column in dsCustomerData.Tables["HDN_HDONCTIET"].Columns)
                            {
                                if (column.ColumnName != "ID_HDON" && column.ColumnName != "ID_HDONCTIET" && column.ColumnName != "ID_CHISO")
                                {
                                    if (column.ColumnName == "DIEN_TTHU" || column.ColumnName == "SO_TIEN")
                                        drCTiet[column.ColumnName] = 0 - Convert.ToDecimal(row[column.ColumnName]);
                                    else
                                        drCTiet[column.ColumnName] = row[column.ColumnName];
                                }
                                drCTiet["ID_HDON_DC"] = lngIDHDonDC_HuyBo;
                                drCTiet["ID_HDONCTIET_DC"] = 1;
                                drCTiet["ID_CHISO_DC"] = row["ID_CHISO"];
                            }
                            drCTiet["KY"] = ky;
                            drCTiet["THANG"] = thang;
                            drCTiet["NAM"] = nam;
                            dsInvoiceData.Tables["HDN_HDONCTIET_DC"].Rows.Add(drCTiet);
                            dsInvoiceData.AcceptChanges();
                        }

                        if (dsInvoiceData.Tables.Contains("HDN_HDONCOSFI_DC") && dsInvoiceData.Tables["HDN_HDONCOSFI_DC"].Rows.Count > 0)
                        {
                            foreach (DataRow row in dsInvoiceData.Tables["HDN_HDONCOSFI_DC"].Rows)
                            {
                                row["ID_HDON_DC"] = lngIDHDonDC;
                            }
                            dsInvoiceData.Tables["HDN_HDONCOSFI_DC"].AcceptChanges();
                        }
                        if (dsCustomerData.Tables.Contains("HDN_HDONCOSFI"))
                        {

                            foreach (DataRow row in dsCustomerData.Tables["HDN_HDONCOSFI"].Rows)
                            {
                                if (row["ID_HDON"].ToString().Trim() != rowHD["ID_HDON"].ToString().Trim()) continue;
                                DataRow drCosfi = dsInvoiceData.Tables["HDN_HDONCOSFI_DC"].NewRow();
                                foreach (DataColumn column in dsCustomerData.Tables["HDN_HDONCOSFI"].Columns)
                                {
                                    if (column.ColumnName != "ID_HDON" && column.ColumnName != "ID_HDONCOSFI")//
                                    {
                                        if (column.ColumnName == "TIEN_HUUCONG" || column.ColumnName == "TIEN_VOCONG" || column.ColumnName == "VO_CONG")
                                            drCosfi[column.ColumnName] = 0 - Convert.ToDecimal(row[column.ColumnName]);
                                        else
                                            drCosfi[column.ColumnName] = row[column.ColumnName];
                                    }
                                    drCosfi["ID_HDON_DC"] = lngIDHDonDC_HuyBo;
                                    drCosfi["ID_HDONCOSFI_DC"] = 1;
                                }
                                drCosfi["KY"] = ky;
                                drCosfi["THANG"] = thang;
                                drCosfi["NAM"] = nam;
                                dsInvoiceData.Tables["HDN_HDONCOSFI_DC"].Rows.Add(drCosfi);
                                dsInvoiceData.AcceptChanges();
                            }
                        }
                    }
                }

                #region HDN_HDON_DC

                string macnang = dsCustomerData.Tables["HDN_BBAN_DCHINH"].Rows[0]["MA_CNANG"].ToString();
                foreach (DataRow dr1 in dsInvoiceData.Tables["HDN_HDON_DC"].Rows)
                {
                    HDN_HDON_DC_PLUS infoHDDC = new HDN_HDON_DC_PLUS();

                    infoHDDC = BillingLibrary.BillingLibrary.MapDatarowToObjectCDPlus<HDN_HDON_DC_PLUS>(dr1, ref strError);
                    infoHDDC.MA_CNANG = macnang;
                    if (strError.Trim().Length == 0)
                    {
                        //Nếu hóa đơn điều chỉnh có loại hóa đơn ="RA"(Quy ước khi không có thay đổi về tiền)
                        //Không ghi vào CSDL
                        if (infoHDDC.LOAI_DCHINH == "RA") continue;
                        if (infoHDDC.LOAI_DCHINH == "LL" || infoHDDC.LOAI_DCHINH == "TT" || infoHDDC.LOAI_DCHINH == "TH")
                        {
                            if (infoHDDC.LOAI_HDON == "VC")
                            {
                                infoHDDC.ID_HDON_DC = lngIDHDonDC_VC;
                                if (rowPS_VC == null) //Nếu không có hóa đơn phát sinh VC, ID_HDON_DC = ID_HDON
                                {
                                    if (rowPS_TD == null)
                                        infoHDDC.ID_HDON = infoHDDC.ID_HDON_DC;
                                    else
                                        infoHDDC.ID_HDON = Convert.ToInt64(rowPS_TD["ID_HDON"]);
                                }
                                else
                                    infoHDDC.ID_HDON = Convert.ToInt64(rowPS_VC["ID_HDON"]);
                            }
                            else
                            {
                                infoHDDC.ID_HDON_DC = lngIDHDonDC;
                                if (rowPS_TD == null) //Nếu không có hóa đơn phát sinh TD, ID_HDON_DC = ID_HDON
                                    infoHDDC.ID_HDON = infoHDDC.ID_HDON_DC;
                                else
                                    infoHDDC.ID_HDON = Convert.ToInt64(rowPS_TD["ID_HDON"]);
                            }
                        }
                        else
                        {
                            if (infoHDDC.LOAI_HDON == "VC")
                            {
                                infoHDDC.ID_HDON_DC = lngIDHDonDC_HuyBo_VC;
                            }
                            else
                            {
                                infoHDDC.ID_HDON_DC = lngIDHDonDC_HuyBo;
                            }
                        }
                        #region Bổ sung cột MA_YCAU_KNAI
                        if (dsCustomerData.Tables.Contains("LST_TIEN_TRINH") && dsCustomerData.Tables["LST_TIEN_TRINH"].Rows.Count > 0)
                        {
                            infoHDDC.MA_YCAU_KNAI = dsCustomerData.Tables["LST_TIEN_TRINH"].Rows[0]["MA_YCAU_KNAI"].ToString().Trim();
                        }
                        #endregion
                        dataInsert.HDN_HDON_DC.Add(infoHDDC);
                    }
                    else return strError;
                }
                //Kiểm tra xem hóa đơn điều chỉnh mới có ID_HDON_DC = lngIDHDonDC không, hay bằng lngIDHDonDC_VC
                #endregion

                #region HDN_BBAN_DCHINH
                //obj_HDN_BBAN_DCHINH_Controller = new cls_HDN_BBAN_DCHINH_Controller();
                //obj_HDN_BBAN_DCHINH_Controller.CMIS2 = db;
                i64SequenceBBan++;
                long lngIDBBanDC = i64SequenceBBan;// obj_HDN_BBAN_DCHINH_Controller.getMaxID();
                short isDup1 = 0;

                foreach (HDN_HDON_DC_PLUS dcInfo in dataInsert.HDN_HDON_DC)
                {
                    DataRow dr2 = dsCustomerData.Tables["HDN_BBAN_DCHINH"].Rows[0];
                    HDN_BBAN_DCHINH_PLUS infoBBDC = new HDN_BBAN_DCHINH_PLUS();
                    infoBBDC = BillingLibrary.BillingLibrary.MapDatarowToObjectCDPlus<HDN_BBAN_DCHINH_PLUS>(dr2, ref strError);
                    if (strError.Trim().Length == 0)
                    {
                        infoBBDC.ID_BBAN = lngIDBBanDC;
                        infoBBDC.ID_HDON_DC = dcInfo.ID_HDON_DC;
                        infoBBDC.ID_HDON = dcInfo.ID_HDON;
                        isDup1 = infoBBDC.IS_DUP1;
                        dataInsert.HDN_BBAN_DCHINH.Add(infoBBDC);
                    }
                    else return strError;
                }
                #endregion


                #region HDN_CHISO_DC
                //obj_HDN_CHISO_DC_Controller = new cls_HDN_CHISO_DC_Controller();
                ////obj_HDN_BCS_CTO_DC_Controller.CMIS2 = db;
                //obj_HDN_CHISO_DC_Controller.CMIS2 = db;
                //Dũng NT sửa để fix ID_CHISO tương ứng với ID_CHISO phát sinh
                if (!dsCustomerData.Tables["HDN_CHISO_DC"].Columns.Contains("ID_CHISO_PS"))
                    dsCustomerData.Tables["HDN_CHISO_DC"].Columns.Add("ID_CHISO_PS", typeof(Int64));
                //End
                HDN_HDON_DC_PLUS dcTemp = dataInsert.HDN_HDON_DC.Where(c => c.LOAI_DCHINH != "HB" && c.LOAI_HDON == "TC").FirstOrDefault();
                //lngIDHDonDC_Tem là ID_HDON_DC của hóa đơn điều chỉnh vừa tạo ra
                //Trường hợp không có HDDC tiền điện thì lấy bằng ID của HDDC vô công. 

                if (dcTemp == null)
                {
                    //Không phải TC
                    dcTemp = dataInsert.HDN_HDON_DC.Where(c => c.LOAI_DCHINH != "HB" && c.LOAI_HDON == "TD").FirstOrDefault();
                    if (dcTemp != null)  //Nếu không có báo lỗi
                    {
                        lngIDHDonDC = dcTemp.ID_HDON_DC;
                        lngIDHDonDC_VC = dcTemp.ID_HDON_DC;
                    }
                    else
                    {
                        dcTemp = dataInsert.HDN_HDON_DC.Where(c => c.LOAI_DCHINH != "HB" && c.LOAI_HDON == "VC").FirstOrDefault();
                        if (dcTemp != null)
                        {
                            lngIDHDonDC_VC = dcTemp.ID_HDON_DC;
                            lngIDHDonDC = dcTemp.ID_HDON_DC;
                        }
                    }
                    dcTemp = dataInsert.HDN_HDON_DC.Where(c => c.LOAI_DCHINH != "HB" && c.LOAI_HDON == "VC").FirstOrDefault();
                    if (dcTemp != null)
                        lngIDHDonDC_VC = dcTemp.ID_HDON_DC;


                    //else
                    //    return "Không tồn tại dữ liệu hóa đơn điều chỉnh truy thu, thoái hoàn hoặc lập lại";

                }
                else
                {
                    lngIDHDonDC = dcTemp.ID_HDON_DC;
                    lngIDHDonDC_VC = dcTemp.ID_HDON_DC;
                }



                foreach (DataRow dr4 in dsCustomerData.Tables["HDN_CHISO_DC"].Rows)
                {
                    dr4["ID_CHISO_PS"] = dr4["ID_CHISO"];
                    HDN_CHISO_DC_PLUS infoCSDC = new HDN_CHISO_DC_PLUS();
                    //dr4["MA_CNANG"] = macnang;
                    infoCSDC = BillingLibrary.BillingLibrary.MapDatarowToObjectCDPlus<HDN_CHISO_DC_PLUS>(dr4, ref strError);
                    infoCSDC.MA_CNANG = macnang;
                    if (strError.Trim().Length == 0)
                    {

                        if (infoCSDC.BCS != "VC")
                            infoCSDC.ID_HDON_DC = lngIDHDonDC;
                        else
                            infoCSDC.ID_HDON_DC = lngIDHDonDC_VC;

                        if (infoCSDC.SAN_LUONG != null)
                            infoCSDC.SAN_LUONG = Math.Round(infoCSDC.SAN_LUONG.Value);
                        if (infoCSDC.SLUONG_TTIEP != null)
                            infoCSDC.SLUONG_TTIEP = Math.Round(infoCSDC.SLUONG_TTIEP.Value);
                        //Gán lại ID_BCS_DC
                        //HDN_BCS_CTO_DC infoBCS = obj_HDN_BCS_CTO_DC_Controller.LstInfo.Where(c => c.MA_DVIQLY == infoCSDC.MA_DVIQLY && c.ID_BCS_DC == infoCSDC.ID_BCS_DC).FirstOrDefault();
                        //if (infoBCS == null) return "Không tìm thấy bộ chỉ số tương ứng MA_DVIQLY=" + infoCSDC.MA_DVIQLY + " và ID_BCS_DC=" + infoCSDC.ID_BCS_DC.ToString() + "";
                        //infoBCS.ID_BCS_DC = obj_HDN_BCS_CTO_DC_Controller.getMaxID();
                        infoCSDC.ID_BCS_DC = -1;
                        i64SequenceCSo++;
                        infoCSDC.ID_CHISO = i64SequenceCSo;// obj_HDN_CHISO_DC_Controller.getMaxID();
                        dr4["ID_CHISO"] = infoCSDC.ID_CHISO;
                        dataInsert.HDN_CHISO_DC.Add(infoCSDC);
                    }
                    else return strError;
                }
                #endregion

                #region HDN_HDONCTIET_DC
                //obj_HDN_HDONCTIET_DC_Controller = new cls_HDN_HDONCTIET_DC_Controller();
                //obj_HDN_HDONCTIET_DC_Controller.CMIS2 = db;
                var arrHDonTD = dataInsert.HDN_HDON_DC.Where(c => "TD;TC".Contains(c.LOAI_HDON) && c.LOAI_DCHINH != "HB");
                var arrHDonVC = dataInsert.HDN_HDON_DC.Where(c => c.LOAI_HDON == "VC" && c.LOAI_DCHINH != "HB");
                foreach (DataRow dr3 in dsInvoiceData.Tables["HDN_HDONCTIET_DC"].Rows)
                //DataRow dr2 = dsInvoiceData.Tables["HDN_HDONCTIET_DC"].Rows[0];
                {
                    HDN_HDONCTIET_DC_PLUS infoHDCT = new HDN_HDONCTIET_DC_PLUS();

                    long lngIDHDCTietDC = 0;
                    infoHDCT = BillingLibrary.BillingLibrary.MapDatarowToObjectCDPlus<HDN_HDONCTIET_DC_PLUS>(dr3, ref strError);
                    infoHDCT.MA_CNANG = macnang;
                    if (strError.Trim().Length == 0)
                    {
                        i64SequenceCTiet++;
                        lngIDHDCTietDC = i64SequenceCTiet;// obj_HDN_HDONCTIET_DC_Controller.getMaxID();
                        infoHDCT.ID_HDONCTIET_DC = lngIDHDCTietDC;
                        if (decDaTra != 0)
                        {
                            if ("KT;BT;CD;TD".Contains(infoHDCT.BCS))
                            {
                                if (arrHDonTD == null || arrHDonTD.Count() == 0)
                                    infoHDCT.ID_HDON_DC = lngIDHDonDC_VC;
                                else infoHDCT.ID_HDON_DC = lngIDHDonDC;
                            }
                            else
                            {
                                int intTC = dataInsert.HDN_HDON_DC.Where(c => c.LOAI_HDON == "TC" && c.LOAI_DCHINH != "HB").Count();
                                if (intTC > 0)
                                {
                                    infoHDCT.ID_HDON_DC = lngIDHDonDC;
                                }
                                else
                                {
                                    if (arrHDonVC == null || arrHDonVC.Count() == 0)
                                        infoHDCT.ID_HDON_DC = lngIDHDonDC;
                                    else infoHDCT.ID_HDON_DC = lngIDHDonDC_VC;
                                }
                            }
                        }
                        else
                        {
                            if ("KT;BT;CD;TD".Contains(infoHDCT.BCS))
                            {
                                if (infoHDCT.SO_TIEN <= 0)
                                    infoHDCT.ID_HDON_DC = lngIDHDonDC_HuyBo;
                                else
                                {
                                    infoHDCT.ID_HDON_DC = infoHDCT.ID_HDON_DC == lngIDHDonDC_HuyBo ? lngIDHDonDC_HuyBo : lngIDHDonDC;
                                }
                            }
                            else
                            {
                                int intTC = dataInsert.HDN_HDON_DC.Where(c => c.LOAI_HDON == "TC" && c.LOAI_DCHINH != "HB").Count();
                                if (intTC > 0)
                                {
                                    if (infoHDCT.SO_TIEN <= 0)
                                        infoHDCT.ID_HDON_DC = infoHDCT.ID_HDON_DC == lngIDHDonDC_HuyBo_VC ? lngIDHDonDC_HuyBo_VC : lngIDHDonDC_HuyBo;
                                    else
                                        infoHDCT.ID_HDON_DC = lngIDHDonDC;
                                }
                                else
                                {
                                    if (infoHDCT.SO_TIEN < 0)
                                        infoHDCT.ID_HDON_DC = lngIDHDonDC_HuyBo_VC;
                                    else
                                    {
                                        infoHDCT.ID_HDON_DC = infoHDCT.ID_HDON_DC == lngIDHDonDC_HuyBo_VC ? lngIDHDonDC_HuyBo_VC : lngIDHDonDC_VC;
                                    }
                                }


                            }
                        }

                        var chiso = from a in dataInsert.HDN_CHISO_DC
                                    where a.BCS == infoHDCT.BCS
                                    && a.ID_HDON_DC == infoHDCT.ID_HDON_DC
                                    && a.MA_DDO == infoHDCT.MA_DDO
                                    && a.SO_CTO == infoHDCT.SO_CTO
                                    select a;
                        if (chiso != null && chiso.Count() > 0)
                        {
                            foreach (DataRow drChiSoTemp in dsCustomerData.Tables["HDN_CHISO_DC"].Rows)
                            {
                                if (Convert.ToInt64(drChiSoTemp["ID_CHISO_PS"]) == infoHDCT.ID_CHISO_DC)
                                {
                                    chiso = chiso.Where(c => c.ID_CHISO == Convert.ToInt64(drChiSoTemp["ID_CHISO"]));
                                    break;
                                }
                            }
                            if (chiso != null && chiso.Count() > 0)
                            {
                                infoHDCT.ID_CHISO_DC = chiso.First().ID_CHISO;
                            }
                        }

                        infoHDCT.KY = ky;
                        infoHDCT.THANG = thang;
                        infoHDCT.NAM = nam;
                        dataInsert.HDN_HDONCTIET_DC.Add(infoHDCT);
                    }
                    else return strError;
                }
                //Thêm chi tiết âm của hóa đơn phát sinh gốc
                if (dsCustomerData.Tables.Contains("HDN_HDONCTIET_PS") && dsCustomerData.Tables["HDN_HDONCTIET_PS"].Rows.Count > 0)
                {
                    foreach (DataRow row in dsCustomerData.Tables["HDN_HDONCTIET_PS"].Rows)
                    {
                        //if (row["ID_HDON"].ToString().Trim() != rowHD["ID_HDON"].ToString().Trim()) continue;
                        DataRow drCTiet = dsInvoiceData.Tables["HDN_HDONCTIET_DC"].NewRow();
                        foreach (DataColumn column in dsCustomerData.Tables["HDN_HDONCTIET"].Columns)
                        {
                            if (column.ColumnName != "ID_HDON" && column.ColumnName != "ID_HDONCTIET" && column.ColumnName != "ID_CHISO")
                            {
                                if (column.ColumnName == "DIEN_TTHU" || column.ColumnName == "SO_TIEN")
                                    drCTiet[column.ColumnName] = 0 - Convert.ToDecimal(row[column.ColumnName]);
                                else
                                    drCTiet[column.ColumnName] = row[column.ColumnName];
                            }
                            drCTiet["ID_HDON_DC"] = lngIDHDonDC;
                            drCTiet["ID_HDONCTIET_DC"] = 1;
                            drCTiet["ID_CHISO_DC"] = row["ID_CHISO"];
                        }
                        drCTiet["KY"] = ky;
                        drCTiet["THANG"] = thang;
                        drCTiet["NAM"] = nam;
                        dsInvoiceData.Tables["HDN_HDONCTIET_DC"].Rows.Add(drCTiet);
                        HDN_HDONCTIET_DC_PLUS infoHDCT = new HDN_HDONCTIET_DC_PLUS();

                        long lngIDHDCTietDC = 0;
                        infoHDCT = BillingLibrary.BillingLibrary.MapDatarowToObjectCDPlus<HDN_HDONCTIET_DC_PLUS>(drCTiet, ref strError);
                        infoHDCT.MA_CNANG = macnang;
                        if (strError.Trim().Length == 0)
                        {
                            i64SequenceCTiet++;
                            lngIDHDCTietDC = i64SequenceCTiet;//obj_HDN_HDONCTIET_DC_Controller.getMaxID();
                            infoHDCT.ID_HDONCTIET_DC = lngIDHDCTietDC;
                            dataInsert.HDN_HDONCTIET_DC.Add(infoHDCT);
                        }
                        else return strError;
                        dsInvoiceData.AcceptChanges();
                    }
                }
                //Kêt thúc
                #endregion

                #region HDN_KHANG_DC
                DataRow dr6 = dsCustomerData.Tables["HDN_KHANG_DC"].Rows[0];
                obj_HDN_KHANG_DC_Controller = new cls_HDN_KHANG_DC_Controller();
                HDN_KHANG_DC_PLUS infoKHDC = new HDN_KHANG_DC_PLUS();
                //List<HDN_KHANG_DC> lstKH = new List<HDN_KHANG_DC>();
                infoKHDC = BillingLibrary.BillingLibrary.MapDatarowToObjectCDPlus<HDN_KHANG_DC_PLUS>(dr6, ref strError);
                infoKHDC.MA_CNANG = macnang;
                if (strError.Trim().Length == 0)
                {
                    infoKHDC.ID_HDON_DC = lngIDHDonDC;
                    dataInsert.HDN_KHANG_DC.Add(infoKHDC);
                    //obj_HDN_KHANG_DC_Controller.pInfor = infoKHDC;
                }
                else return strError;
                //if (lngIDHDonDC != 0 && lngIDHDonDC_VC != 0 && lngIDHDonDC != lngIDHDonDC_VC)
                //{
                //    //Có 2 hóa đơn
                //    HDN_KHANG_DC_PLUS infoKHDC_VC = new HDN_KHANG_DC_PLUS();
                //    infoKHDC_VC = BillingLibrary.BillingLibrary.MapDatarowToObjectCDPlus<HDN_KHANG_DC_PLUS>(dr6, ref strError);
                //    infoKHDC_VC.MA_CNANG = macnang;
                //    if (strError.Trim().Length == 0)
                //    {
                //        infoKHDC_VC.ID_HDON_DC = lngIDHDonDC_VC;
                //        dataInsert.HDN_KHANG_DC.Add(infoKHDC_VC);
                //    }
                //    else return strError;
                //}
                //obj_HDN_KHANG_DC_Controller.LstInfo = lstKH;
                var hdon = dataInsert.HDN_HDON_DC.Where(c => c.ID_HDON_DC != infoKHDC.ID_HDON_DC);
                if (hdon != null && hdon.Count() > 0)
                {
                    foreach (HDN_HDON_DC_PLUS dc in hdon)
                    {
                        HDN_KHANG_DC_PLUS infoKHDC_VC = new HDN_KHANG_DC_PLUS();
                        infoKHDC_VC = BillingLibrary.BillingLibrary.MapDatarowToObjectCDPlus<HDN_KHANG_DC_PLUS>(dr6, ref strError);
                        infoKHDC_VC.MA_CNANG = macnang;
                        if (strError.Trim().Length == 0)
                        {
                            infoKHDC_VC.ID_HDON_DC = dc.ID_HDON_DC;
                            dataInsert.HDN_KHANG_DC.Add(infoKHDC_VC);
                        }
                        else return strError;
                    }
                }
                #endregion

                #region HDN_DIEMDO_DC
                //obj_HDN_DIEMDO_DC_Controller = new cls_HDN_DIEMDO_DC_Controller();
                foreach (DataRow dr7 in dsCustomerData.Tables["HDN_DIEMDO_DC"].Rows)
                {
                    HDN_DIEMDO_DC_PLUS infoDDoDC = new HDN_DIEMDO_DC_PLUS();
                    infoDDoDC = BillingLibrary.BillingLibrary.MapDatarowToObjectCDPlus<HDN_DIEMDO_DC_PLUS>(dr7, ref strError);
                    infoDDoDC.MA_CNANG = macnang;
                    if (strError.Trim().Length == 0)
                    {
                        infoDDoDC.ID_HDON_DC = lngIDHDonDC;
                        dataInsert.HDN_DIEMDO_DC.Add(infoDDoDC);
                    }
                    else return strError;
                    hdon = dataInsert.HDN_HDON_DC.Where(c => c.ID_HDON_DC != infoDDoDC.ID_HDON_DC);
                    if (hdon != null && hdon.Count() > 0)
                    {
                        foreach (HDN_HDON_DC_PLUS dc in hdon)
                        {
                            HDN_DIEMDO_DC_PLUS infoDDoDCTemp = new HDN_DIEMDO_DC_PLUS();
                            infoDDoDCTemp = BillingLibrary.BillingLibrary.MapDatarowToObjectCDPlus<HDN_DIEMDO_DC_PLUS>(dr7, ref strError);
                            infoDDoDCTemp.MA_CNANG = macnang;
                            if (strError.Trim().Length == 0)
                            {
                                infoDDoDCTemp.ID_HDON_DC = dc.ID_HDON_DC;
                                dataInsert.HDN_DIEMDO_DC.Add(infoDDoDCTemp);
                            }
                            else return strError;
                        }
                    }
                }
                #endregion

                #region HDN_BBAN_APGIA_DC
                //obj_HDN_BBAN_APGIA_DC_Controller = new cls_HDN_BBAN_APGIA_DC_Controller();
                //obj_HDN_BBAN_APGIA_DC_Controller.CMIS2 = db;
                foreach (DataRow dr8 in dsCustomerData.Tables["HDN_BBAN_APGIA_DC"].Rows)
                {
                    HDN_BBAN_APGIA_DC_PLUS infoApGiaDC = new HDN_BBAN_APGIA_DC_PLUS();
                    long lngIDBBanApGia = 0;
                    infoApGiaDC = BillingLibrary.BillingLibrary.MapDatarowToObjectCDPlus<HDN_BBAN_APGIA_DC_PLUS>(dr8, ref strError);
                    infoApGiaDC.MA_CNANG = macnang;
                    if (strError.Trim().Length == 0)
                    {
                        i64SequenceApGia++;
                        lngIDBBanApGia = i64SequenceApGia;//obj_HDN_BBAN_APGIA_DC_Controller.getMaxID();
                        infoApGiaDC.ID_BBANAGIA = lngIDBBanApGia;
                        infoApGiaDC.ID_HDON_DC = lngIDHDonDC;
                        //infoApGiaDC.ID_BBANAGIA = obj_HDN_BBAN_APGIA_DC_Controller.getMaxID();
                        dataInsert.HDN_BBAN_APGIA_DC.Add(infoApGiaDC);
                    }
                    else return strError;
                    //var hdon = dataInsert.HDN_HDON_DC.Where(c => c.ID_HDON_DC != infoApGiaDC.ID_HDON_DC);
                    //if (hdon != null && hdon.Count() > 0)
                    //{
                    //    foreach (HDN_HDON_DC_PLUS dc in hdon)
                    //    {
                    //        HDN_BBAN_APGIA_DC_PLUS infoApGiaDC = new HDN_BBAN_APGIA_DC_PLUS();
                    //        long lngIDBBanApGia = 0;
                    //        infoApGiaDC = BillingLibrary.BillingLibrary.MapDatarowToObjectCDPlus<HDN_BBAN_APGIA_DC_PLUS>(dr8, ref strError);
                    //        infoApGiaDC.MA_CNANG = macnang;
                    //        if (strError.Trim().Length == 0)
                    //        {
                    //            i64SequenceApGia++;
                    //            lngIDBBanApGia = i64SequenceApGia;//obj_HDN_BBAN_APGIA_DC_Controller.getMaxID();
                    //            infoApGiaDC.ID_BBANAGIA = lngIDBBanApGia;
                    //            infoApGiaDC.ID_HDON_DC = dc.ID_HDON_DC;
                    //            //infoApGiaDC.ID_BBANAGIA = obj_HDN_BBAN_APGIA_DC_Controller.getMaxID();
                    //            dataInsert.HDN_BBAN_APGIA_DC.Add(infoApGiaDC);
                    //        }
                    //        else return strError;
                    //    }
                    //}
                }
                #endregion

                if (dsInvoiceData.Tables.Contains("HDN_HDONCOSFI_DC") && dsInvoiceData.Tables["HDN_HDONCOSFI_DC"].Rows.Count > 0)
                {
                    #region HDN_HDONCOSFI_DC
                    //obj_HDN_HDONCOSFI_DC_Controller = new cls_HDN_HDONCOSFI_DC_Controller();
                    //obj_HDN_HDONCOSFI_DC_Controller.CMIS2 = db;
                    foreach (DataRow dr9 in dsInvoiceData.Tables["HDN_HDONCOSFI_DC"].Rows)
                    //DataRow dr2 = dsInvoiceData.Tables["HDN_HDONCOSFI_DC"].Rows[0];
                    {
                        HDN_HDONCOSFI_DC_PLUS infoHDCF = new HDN_HDONCOSFI_DC_PLUS();
                        long lngIDHDCosfiDC = 0;
                        infoHDCF = BillingLibrary.BillingLibrary.MapDatarowToObjectCDPlus<HDN_HDONCOSFI_DC_PLUS>(dr9, ref strError);
                        infoHDCF.MA_CNANG = macnang;
                        if (strError.Trim().Length == 0)
                        {
                            i64SequenceCosfi++;
                            lngIDHDCosfiDC = i64SequenceCosfi;//obj_HDN_HDONCOSFI_DC_Controller.getMaxID();
                            infoHDCF.ID_HDONCOSFI_DC = lngIDHDCosfiDC;
                            if (arrHDonVC == null || arrHDonVC.Count() == 0)
                                infoHDCF.ID_HDON_DC = lngIDHDonDC;
                            else
                            {
                                if (arrHDonVC.First().LOAI_DCHINH == "LL")
                                {
                                    if (infoHDCF.VO_CONG < 0)
                                    {
                                        var arrHDonVC_HB = dataInsert.HDN_HDON_DC.Where(c => c.LOAI_HDON == "VC" && c.LOAI_DCHINH == "HB");
                                        if (arrHDonVC_HB == null || arrHDonVC_HB.Count() == 0)
                                            //Ko có hóa đơn hủy bỏ vô công, dùng ID_HDON_DC hủy bỏ của TD
                                            infoHDCF.ID_HDON_DC = lngIDHDonDC_HuyBo;
                                        else
                                            infoHDCF.ID_HDON_DC = lngIDHDonDC_HuyBo_VC;
                                    }
                                    else
                                        infoHDCF.ID_HDON_DC = lngIDHDonDC_VC;
                                }
                                else
                                    infoHDCF.ID_HDON_DC = lngIDHDonDC_VC;
                            }
                            var chiso = from a in dataInsert.HDN_CHISO_DC
                                        where a.BCS == "VC"
                                        && a.ID_HDON_DC == infoHDCF.ID_HDON_DC
                                        && a.MA_DDO == infoHDCF.MA_DDO
                                        select a;
                            if (chiso != null && chiso.Count() > 0)
                            {
                                foreach (DataRow drChiSoTemp in dsCustomerData.Tables["HDN_CHISO_DC"].Rows)
                                {
                                    if (Convert.ToInt64(drChiSoTemp["ID_CHISO_PS"]) == infoHDCF.ID_CHISO_DC)
                                    {
                                        chiso = chiso.Where(c => c.ID_CHISO == Convert.ToInt64(drChiSoTemp["ID_CHISO"]));
                                        break;
                                    }
                                }
                                if (chiso != null && chiso.Count() > 0)
                                {
                                    infoHDCF.ID_CHISO_DC = chiso.First().ID_CHISO;
                                }
                            }
                            infoHDCF.KY = ky;
                            infoHDCF.THANG = thang;
                            infoHDCF.NAM = nam;

                            dataInsert.HDN_HDONCOSFI_DC.Add(infoHDCF);
                        }
                        else return strError;
                    }
                    #endregion
                }
                if (dsCustomerData.Tables.Contains("HDN_QHE_DDO_DC") && dsCustomerData.Tables["HDN_QHE_DDO_DC"].Rows.Count > 0)
                {
                    #region HDN_QHE_DDO_DC
                    //obj_HDN_QHE_DDO_DC_Controller = new cls_HDN_QHE_DDO_DC_Controller();
                    //obj_HDN_QHE_DDO_DC_Controller.CMIS2 = db;
                    //foreach (DataRow dr10 in dsCustomerData.Tables["HDN_QHE_DDO_DC"].Rows)
                    ////DataRow dr2 = dsInvoiceData.Tables["HDN_QHE_DDO_DC"].Rows[0];
                    //{
                    //    HDN_QHE_DDO_DC infoQHDDo = new HDN_QHE_DDO_DC();
                    //    long lngIDQHeDDoDC = 0;
                    //    infoQHDDo = BillingLibrary.BillingLibrary.MapDatarowToObjectCDPlus<HDN_QHE_DDO_DC>(dr10, ref strError);
                    //    infoQHDDo.MA_CNANG = macnang;
                    //    if (strError.Trim().Length == 0)
                    //    {
                    //        lngIDQHeDDoDC = obj_HDN_QHE_DDO_DC_Controller.getMaxID();
                    //        infoQHDDo.ID_QHE_DC = lngIDQHeDDoDC;
                    //        infoQHDDo.ID_HDON_DC = lngIDHDonDC_Tem;
                    //        obj_HDN_QHE_DDO_DC_Controller.LstInfo.Add(infoQHDDo);
                    //    }
                    //    else return strError;
                    //}
                    #endregion
                }
                //if (dsCustomerData.Tables.Contains("GCS_CHISO_DKY") && dsCustomerData.Tables["GCS_CHISO_DKY"].Rows.Count > 0)
                if (isDup1 == 1)
                {
                    #region GCS_CHISO Dau ky sau
                    foreach (DataRow dr10 in dsCustomerData.Tables["GCS_CHISO"].Rows)
                    {
                        if (dr10["LOAI_CHISO"].ToString().Trim() == "DDK")
                        {
                            GCS_CHISO_DUP1 infoDDoGCS = new GCS_CHISO_DUP1();
                            infoDDoGCS = BillingLibrary.BillingLibrary.MapDatarowToObjectCDPlus<GCS_CHISO_DUP1>(dr10, ref strError);
                            //infoDDoGCS.MA_CNANG = macnang;

                            if (strError.Trim().Length == 0)
                            {
                                infoDDoGCS.LOAI_CHISO = "DUP1";
                                //infoDDoDC.ID_HDON_DC = lngIDHDonDC;
                                dataInsert.GCS_CHISO.Add(infoDDoGCS);
                            }
                            else return strError;
                        }
                    }
                    //objDKy = new cls_GCS_CHISO_Controller();
                    //obj_HDN_CHISO_DC_Controller.CMIS2 = db;
                    //foreach (DataRow dr11 in dsCustomerData.Tables["GCS_CHISO_DKY"].Rows)
                    ////DataRow dr2 = dsInvoiceData.Tables["HDN_QHE_DDO_DC"].Rows[0];
                    //{
                    //    GCS_CHISO infoCSDKy = new GCS_CHISO();
                    //    infoCSDKy = BillingLibrary.BillingLibrary.MapDatarowToObjectCDPlus<GCS_CHISO>(dr11, ref strError);
                    //    infoCSDKy.MA_CNANG = macnang;
                    //    if (strError.Trim().Length == 0)
                    //    {
                    //        infoCSDKy.ID_CHISO = obj_HDN_CHISO_DC_Controller.getMaxID();
                    //        objDKy.LstInfo.Add(infoCSDKy);
                    //    }
                    //    else return strError;
                    //}
                    #endregion
                }
                #region LST_TIEN_TRINH
                if (dsCustomerData.Tables.Contains("LST_TIEN_TRINH") && dsCustomerData.Tables["LST_TIEN_TRINH"].Rows.Count > 0)
                {
                    dataInsert.LST_TIEN_TRINH = new List<LST_TIEN_TRINH>();
                    foreach (DataRow dr1 in dsCustomerData.Tables["LST_TIEN_TRINH"].Rows)
                    {
                        LST_TIEN_TRINH infoHDDC = new LST_TIEN_TRINH();

                        infoHDDC = BillingLibrary.BillingLibrary.MapDatarowToObjectCDPlus<LST_TIEN_TRINH>(dr1, ref strError);
                        //infoHDDC.MA_CNANG = macnang;
                        if (strError.Trim().Length == 0)
                        {
                            dataInsert.LST_TIEN_TRINH.Add(infoHDDC);
                        }
                        else return strError;
                    }

                }

                #endregion
                #region LST_PAN_PHAT
                if (dsCustomerData.Tables.Contains("LST_PAN_PHAT") && dsCustomerData.Tables["LST_PAN_PHAT"].Rows.Count > 0)
                {
                    dataInsert.LST_PAN_PHAT = new List<LST_PAN_PHAT>();
                    foreach (DataRow dr1 in dsCustomerData.Tables["LST_PAN_PHAT"].Rows)
                    {
                        LST_PAN_PHAT infoHDDC = new LST_PAN_PHAT();

                        infoHDDC = BillingLibrary.BillingLibrary.MapDatarowToObjectCDPlus<LST_PAN_PHAT>(dr1, ref strError);
                        //infoHDDC.MA_CNANG = macnang;
                        if (strError.Trim().Length == 0)
                        {
                            dataInsert.LST_PAN_PHAT.Add(infoHDDC);
                        }
                        else return strError;
                    }

                }

                #endregion
                #region LST_KHANG_DDO
                if (dsCustomerData.Tables.Contains("LST_KHANG_DDO") && dsCustomerData.Tables["LST_KHANG_DDO"].Rows.Count > 0)
                {
                    dataInsert.LST_KHANG_DDO = new List<LST_KHANG_DDO>();
                    foreach (DataRow dr1 in dsCustomerData.Tables["LST_KHANG_DDO"].Rows)
                    {
                        LST_KHANG_DDO infoHDDC = new LST_KHANG_DDO();

                        infoHDDC = BillingLibrary.BillingLibrary.MapDatarowToObjectCDPlus<LST_KHANG_DDO>(dr1, ref strError);

                        if (strError.Trim().Length == 0)
                        {
                            dataInsert.LST_KHANG_DDO.Add(infoHDDC);
                        }
                        else return strError;
                    }

                }

                #endregion
                #region LST_BBAN_PLUC
                if (dsCustomerData.Tables.Contains("LST_BBAN_PLUC") && dsCustomerData.Tables["LST_BBAN_PLUC"].Rows.Count > 0)
                {
                    dataInsert.LST_BBAN_PLUC = new List<LST_BBAN_PLUC>();
                    foreach (DataRow dr1 in dsCustomerData.Tables["LST_BBAN_PLUC"].Rows)
                    {
                        LST_BBAN_PLUC infoHDDC = new LST_BBAN_PLUC();

                        infoHDDC = BillingLibrary.BillingLibrary.MapDatarowToObjectCDPlus<LST_BBAN_PLUC>(dr1, ref strError);

                        if (strError.Trim().Length == 0)
                        {
                            dataInsert.LST_BBAN_PLUC.Add(infoHDDC);
                        }
                        else return strError;
                    }

                }

                #endregion
                return "";
            }
            catch (Exception ex)
            {
                return "Lỗi khi gán dữ liệu cho Object: " + ex.Message;
            }
        }

        protected string SetPropertiesForMultiObjectPlus_Mobile(DataSet dsInvoiceData, DataSet dsCustomerData, ref inpDLieuHDonDC dataInsert, CMIS2 db)
        {
            //DũngNT viết cho chức năng Tính hóa đơn điều chỉnh Mobile
            try
            {
                string strError = "";
                //Kiểm tra trong dataset chứa thông tin điều chỉnh
                if (!dsInvoiceData.Tables.Contains("HDN_HDON"))
                    return "Lỗi không tìm thấy bảng dsInvoiceData.HDN_HDON";
                if (!dsInvoiceData.Tables.Contains("HDN_HDONCTIET"))
                    return "Lỗi không tìm thấy bảng dsInvoiceData.HDN_HDONCTIET";
                //Kiểm tra trong dataset chứa thông tin khách hàng điều chỉnh và thông tin hóa đơn cũ
                if (!dsCustomerData.Tables.Contains("HDN_HDON_TIEPNHAN"))
                    return "Lỗi không tìm thấy bảng dsCustomerData.HDN_HDON_TIEPNHAN";
                if (!dsCustomerData.Tables.Contains("HDN_HDON"))
                    return "Lỗi không tìm thấy bảng dsCustomerData.HDN_HDON";
                if (!dsCustomerData.Tables.Contains("HDN_HDONCTIET"))
                    return "Lỗi không tìm thấy bảng dsCustomerData.HDN_HDONCTIET";
                if (dsCustomerData.Tables["HDN_BBAN_DCHINH"].Rows.Count <= 0)
                    return "Lỗi không tìm thấy dữ liệu bảng dsCustomerData.HDN_BBAN_DCHINH";
                if (!dsCustomerData.Tables.Contains("HDN_BBAN_DCHINH"))
                    return "Lỗi không tìm thấy bảng dsCustomerData.HDN_BBAN_DCHINH";
                if (!dsCustomerData.Tables.Contains("HDN_KHANG_DC"))
                    return "Lỗi không tìm thấy bảng dsCustomerData.HDN_KHANG_DC";
                if (!dsCustomerData.Tables.Contains("HDN_DIEMDO_DC"))
                    return "Lỗi không tìm thấy bảng dsCustomerData.HDN_DIEMDO_DC";
                if (!dsCustomerData.Tables.Contains("HDN_CHISO_DC"))
                    return "Lỗi không tìm thấy bảng dsCustomerData.HDN_CHISO_DC";
                //if (!dsCustomerData.Tables.Contains("HDN_BCS_CTO_DC"))
                //    return "Lỗi không tìm thấy bảng dsCustomerData.HDN_BCS_CTO_DC";
                if (!dsCustomerData.Tables.Contains("HDN_BBAN_APGIA_DC"))
                    return "Lỗi không tìm thấy bảng dsCustomerData.HDN_BBAN_APGIA_DC";

                //Đổi tên bảng tránh nhầm lẫn
                dsInvoiceData.Tables["HDN_HDON"].TableName = "HDN_HDON_DC";
                dsInvoiceData.Tables["HDN_HDONCTIET"].TableName = "HDN_HDONCTIET_DC";
                if (dsInvoiceData.Tables.Contains("HDN_HDONCOSFI"))
                    dsInvoiceData.Tables["HDN_HDONCOSFI"].TableName = "HDN_HDONCOSFI_DC";
                dsInvoiceData.Tables["HDN_HDONCTIET_DC"].Columns["ID_CHISO"].ColumnName = "ID_CHISO_DC";
                dsInvoiceData.Tables["HDN_HDONCTIET_DC"].Columns["ID_HDON"].ColumnName = "ID_HDON_DC";
                dsInvoiceData.Tables["HDN_HDONCTIET_DC"].Columns["ID_HDONCTIET"].ColumnName = "ID_HDONCTIET_DC";
                dsInvoiceData.Tables["HDN_HDONCOSFI_DC"].Columns["ID_HDON"].ColumnName = "ID_HDON_DC";
                dsInvoiceData.Tables["HDN_HDONCOSFI_DC"].Columns["ID_HDONCOSFI"].ColumnName = "ID_HDONCOSFI_DC";
                dsInvoiceData.Tables["HDN_HDON_DC"].Columns.Add("ID_HDON_DC", typeof(long));
                dsInvoiceData.Tables["HDN_HDON_DC"].Columns.Add("LOAI_DCHINH", typeof(string));
                DataRow drBBan = dsCustomerData.Tables["HDN_BBAN_DCHINH"].Rows[0];
                //DũngNT bổ sung so sánh các trường hợp có hóa đơn VC - có hiệu chỉnh
                DataRow rowDC_TD = dsInvoiceData.Tables["HDN_HDON_DC"].Select("LOAI_HDON='TD' OR LOAI_HDON='TC'").FirstOrDefault();
                DataRow rowDC_VC = dsInvoiceData.Tables["HDN_HDON_DC"].Select("LOAI_HDON='VC'").FirstOrDefault();
                DataRow rowPS_TD = dsCustomerData.Tables["HDN_HDON"].Select("LOAI_HDON='TD' OR LOAI_HDON='TC'").FirstOrDefault();
                DataRow rowPS_VC = dsCustomerData.Tables["HDN_HDON"].Select("LOAI_HDON='VC'").FirstOrDefault();
                rowDC_TD["ID_HDON"] = rowPS_TD["ID_HDON"];
                if (rowDC_VC != null)
                    rowDC_VC["ID_HDON"] = rowPS_VC != null ? rowPS_VC["ID_HDON"] : rowPS_TD["ID_HDON"];
                //Kiểm tra loại điều chỉnh của hóa đơn điều chỉnh
                decimal decTyLeThue = Utility.DecimalDbnull(dsInvoiceData.Tables["HDN_HDON_DC"].Rows[0]["TYLE_THUE"]);
                decimal decDaTra = Utility.DecimalDbnull(dsCustomerData.Tables["HDN_HDON_TIEPNHAN"].Rows[0]["DA_TRA"]);
                decimal decTongNo = Utility.DecimalDbnull(dsCustomerData.Tables["HDN_HDON_TIEPNHAN"].Rows[0]["SO_TIEN"]) + Utility.DecimalDbnull(dsCustomerData.Tables["HDN_HDON_TIEPNHAN"].Rows[0]["TIEN_GTGT"]);
                if (decTongNo > decDaTra)
                    decDaTra = 0;
                bool isDCKhongPS = false;
                if (dsCustomerData != null && dsCustomerData.Tables["LST_TIEN_TRINH"] != null && dsCustomerData.Tables["LST_TIEN_TRINH"].Rows.Count > 0 && dsCustomerData.Tables["LST_TIEN_TRINH"].Columns.Contains("IS_PSINH") && dsCustomerData.Tables["LST_TIEN_TRINH"].Rows[0]["IS_PSINH"].ToString().Trim() == "0")
                {
                    isDCKhongPS = true;
                }
                decimal decTongTienDC = Utility.DecimalDbnull(rowDC_TD["TONG_TIEN"]);
                decimal decTongTien = Utility.DecimalDbnull(rowPS_TD["TONG_TIEN"]);
                string strLoaiDC = "";
                long i64SequenceHDon = 0;
                long i64SequenceCTiet = 0;
                long i64SequenceCosfi = 0;
                long i64SequenceCSo = 0;
                long i64SequenceBBan = 0;
                long i64SequenceApGia = 0;

                long lngIDHDonDC_HuyBo = 0;
                long lngIDHDonDC_HuyBo_VC = 0;
                decimal dec100 = 100;
                long lngIDHDonDC = 0;
                long lngIDHDonDC_VC = 0;
                Int16 ky = Convert.ToInt16(drBBan["KY_DC"]);
                Int16 thang = Convert.ToInt16(drBBan["THANG_DC"]);
                Int16 nam = Convert.ToInt16(drBBan["NAM_DC"]);

                if (decDaTra != 0 || isDCKhongPS == true)
                {
                    //Đã thu tiền của khách hàng => có thể là hóa đơn truy thu TT hoặc hóa đơn thoái hoàn TH
                    if (decTongTienDC > decTongTien) strLoaiDC = "TT";
                    else if (decTongTienDC < decTongTien) strLoaiDC = "TH";
                    else strLoaiDC = "RA"; //Re Ask: hỏi lại sau                    
                    //if (strLoaiDC == "RA") return "HĐ điều chỉnh có tổng tiền bằng với HĐ phát sinh. Không lập hóa đơn điều chỉnh.";
                    rowDC_TD["LOAI_DCHINH"] = strLoaiDC;

                    //obj_HDN_HDON_DC_Controller.CMIS2 = db;
                    i64SequenceHDon++;
                    lngIDHDonDC = i64SequenceHDon;//obj_HDN_HDON_DC_Controller.getMaxID();
                    i64SequenceHDon++;
                    lngIDHDonDC_VC = i64SequenceHDon;// obj_HDN_HDON_DC_Controller.getMaxID();
                    //Tính lại các giá trị tiền
                    rowDC_TD["DIEN_TTHU"] = Utility.DecimalDbnull(rowDC_TD["DIEN_TTHU"]) - Utility.DecimalDbnull(rowPS_TD["DIEN_TTHU"]);
                    rowDC_TD["SO_TIEN"] = Utility.DecimalDbnull(rowDC_TD["SO_TIEN"]) - Utility.DecimalDbnull(rowPS_TD["SO_TIEN"]);
                    //rowDC_TD["TIEN_GTGT"] = Utility.DecimalDbnull(rowDC_TD["TIEN_GTGT"]) - Utility.DecimalDbnull(rowPS_TD["TIEN_GTGT"]);
                    rowDC_TD["TIEN_GTGT"] = Math.Round(decTyLeThue * Utility.DecimalDbnull(rowDC_TD["SO_TIEN"]) / dec100, 0, MidpointRounding.AwayFromZero);
                    rowDC_TD["TONG_TIEN"] = Utility.DecimalDbnull(rowDC_TD["SO_TIEN"]) + Utility.DecimalDbnull(rowDC_TD["TIEN_GTGT"]);

                    rowDC_TD["TIEN_TD"] = Utility.DecimalDbnull(rowDC_TD["TIEN_TD"]) - Utility.DecimalDbnull(rowPS_TD["TIEN_TD"]);
                    rowDC_TD["THUE_TD"] = Math.Round(decTyLeThue * Utility.DecimalDbnull(rowDC_TD["TIEN_TD"]) / dec100, 0, MidpointRounding.AwayFromZero);
                    rowDC_TD["TIEN_VC"] = Utility.DecimalDbnull(rowDC_TD["SO_TIEN"]) - Utility.DecimalDbnull(rowDC_TD["TIEN_TD"]);
                    rowDC_TD["THUE_VC"] = Utility.DecimalDbnull(rowDC_TD["TIEN_GTGT"]) - Utility.DecimalDbnull(rowDC_TD["THUE_TD"]);

                    rowDC_TD["KY"] = ky;
                    rowDC_TD["THANG"] = thang;
                    rowDC_TD["NAM"] = nam;
                    if (rowDC_VC != null)
                    {
                        if (rowPS_VC == null) //Không có hóa đơn vô công phát sinh
                            rowDC_VC["LOAI_DCHINH"] = "TT";
                        else
                        {
                            decimal decTongTienDC_VC = Utility.DecimalDbnull(rowDC_VC["TONG_TIEN"]);
                            decimal decTongTien_VC = Utility.DecimalDbnull(rowPS_VC["TONG_TIEN"]);
                            rowDC_VC["DIEN_TTHU"] = Utility.DecimalDbnull(rowDC_VC["DIEN_TTHU"]) - Utility.DecimalDbnull(rowPS_VC["DIEN_TTHU"]);
                            rowDC_VC["SO_TIEN"] = Utility.DecimalDbnull(rowDC_VC["SO_TIEN"]) - Utility.DecimalDbnull(rowPS_VC["SO_TIEN"]);
                            rowDC_VC["TIEN_GTGT"] = Math.Round(decTyLeThue * Utility.DecimalDbnull(rowDC_VC["SO_TIEN"]) / dec100, 0, MidpointRounding.AwayFromZero);
                            rowDC_VC["TONG_TIEN"] = Utility.DecimalDbnull(rowDC_VC["SO_TIEN"]) + Utility.DecimalDbnull(rowDC_VC["TIEN_GTGT"]);
                            rowDC_VC["TIEN_TD"] = Utility.DecimalDbnull(rowDC_VC["TIEN_TD"]) - Utility.DecimalDbnull(rowPS_VC["TIEN_TD"]);
                            rowDC_VC["THUE_TD"] = Math.Round(decTyLeThue * Utility.DecimalDbnull(rowDC_VC["TIEN_TD"]) / dec100, 0, MidpointRounding.AwayFromZero);
                            rowDC_VC["TIEN_VC"] = Utility.DecimalDbnull(rowDC_VC["SO_TIEN"]) - Utility.DecimalDbnull(rowDC_VC["TIEN_TD"]);
                            rowDC_VC["THUE_VC"] = Utility.DecimalDbnull(rowDC_VC["TIEN_GTGT"]) - Utility.DecimalDbnull(rowDC_VC["THUE_TD"]);
                            rowDC_VC["KY"] = ky;
                            rowDC_VC["THANG"] = thang;
                            rowDC_VC["NAM"] = nam;
                            if (decTongTienDC_VC > decTongTien_VC) rowDC_VC["LOAI_DCHINH"] = "TT";
                            else if (decTongTienDC_VC < decTongTien_VC) rowDC_VC["LOAI_DCHINH"] = "TH";
                            else
                            {
                                //Tiền TD DC = tiền TD PS, tiền VC DC = tiền VC PS
                                if (strLoaiDC == "RA")
                                    return "HĐ điều chỉnh có tổng tiền bằng với HĐ phát sinh. Không lập hóa đơn điều chỉnh.";
                                //Không ghi hóa đơn VC DC vào DB
                                dsInvoiceData.Tables["HDN_HDON_DC"].Rows.Remove(rowDC_VC);
                            }
                        }
                    }
                    var arrID_HDON_DC_TD = (from a in dsInvoiceData.Tables["HDN_HDON_DC"].AsEnumerable()
                                            where (a.Field<string>("LOAI_HDON") == "TD" || a.Field<string>("LOAI_HDON") == "TC")
                                            && a.Field<string>("LOAI_DCHINH") != "HB"
                                            && a.Field<string>("LOAI_DCHINH") != "RA"
                                            select a);
                    var arrID_HDON_DC_VC = (from a in dsInvoiceData.Tables["HDN_HDON_DC"].AsEnumerable()
                                            where a.Field<string>("LOAI_HDON") == "VC"
                                            && a.Field<string>("LOAI_DCHINH") != "HB"
                                            && a.Field<string>("LOAI_DCHINH") != "RA"
                                            select a);
                    if ((arrID_HDON_DC_TD == null || arrID_HDON_DC_TD.Count() == 0) && (arrID_HDON_DC_VC == null || arrID_HDON_DC_VC.Count() == 0))
                        return "Hóa đơn điều chỉnh có tổng tiền bằng hóa đơn sai. Không thực hiện điều chỉnh";

                    if (isDCKhongPS)
                    {
                        //Không có hóa đơn phát sinh
                    }
                    else
                    {
                        //Có hóa đơn phát sinh, làm như bình thường
                        foreach (DataRow row in dsCustomerData.Tables["HDN_HDONCTIET"].Rows)
                        {
                            DataRow drCTiet = dsInvoiceData.Tables["HDN_HDONCTIET_DC"].NewRow();
                            foreach (DataColumn column in dsCustomerData.Tables["HDN_HDONCTIET"].Columns)
                            {
                                if (column.ColumnName != "ID_HDON" && column.ColumnName != "ID_HDONCTIET" && column.ColumnName != "ID_CHISO")
                                {
                                    if (column.ColumnName == "DIEN_TTHU" || column.ColumnName == "SO_TIEN")
                                        drCTiet[column.ColumnName] = 0 - Convert.ToDecimal(row[column.ColumnName]);
                                    else
                                        drCTiet[column.ColumnName] = row[column.ColumnName];
                                }
                                //Kiểm tra chi tiết 
                                //BT,CD,TD,KT: ID_HDON_DC = ID_HDON_DC của hóa đơn TD (nếu không có TD lấy = VC)



                                drCTiet["ID_HDONCTIET_DC"] = 1;
                                drCTiet["ID_CHISO_DC"] = row["ID_CHISO"];
                            }
                            drCTiet["KY"] = ky;
                            drCTiet["THANG"] = thang;
                            drCTiet["NAM"] = nam;
                            if ("KT;BT;CD;TD".Contains(drCTiet["BCS"].ToString().Trim()))
                            {
                                if (arrID_HDON_DC_TD == null || arrID_HDON_DC_TD.Count() == 0)
                                    drCTiet["ID_HDON_DC"] = lngIDHDonDC_VC;
                                else drCTiet["ID_HDON_DC"] = lngIDHDonDC;
                            }
                            else
                            {
                                if (arrID_HDON_DC_VC == null || arrID_HDON_DC_VC.Count() == 0)
                                    drCTiet["ID_HDON_DC"] = lngIDHDonDC;
                                else drCTiet["ID_HDON_DC"] = lngIDHDonDC_VC;
                            }
                            dsInvoiceData.Tables["HDN_HDONCTIET_DC"].Rows.Add(drCTiet);
                            dsInvoiceData.AcceptChanges();
                        }
                    }

                    if (dsCustomerData.Tables.Contains("HDN_HDONCOSFI"))
                    {
                        foreach (DataRow row in dsCustomerData.Tables["HDN_HDONCOSFI"].Rows)
                        {
                            DataRow drCosfi = dsInvoiceData.Tables["HDN_HDONCOSFI_DC"].NewRow();
                            foreach (DataColumn column in dsCustomerData.Tables["HDN_HDONCOSFI"].Columns)
                            {
                                if (column.ColumnName != "ID_HDON" && column.ColumnName != "ID_HDONCOSFI")//
                                {
                                    if (column.ColumnName == "TIEN_HUUCONG" || column.ColumnName == "TIEN_VOCONG" || column.ColumnName == "VO_CONG")
                                        drCosfi[column.ColumnName] = 0 - Convert.ToDecimal(row[column.ColumnName]);
                                    else
                                        drCosfi[column.ColumnName] = row[column.ColumnName];
                                }

                                drCosfi["ID_HDONCOSFI_DC"] = 1;
                            }
                            drCosfi["KY"] = ky;
                            drCosfi["THANG"] = thang;
                            drCosfi["NAM"] = nam;
                            if (arrID_HDON_DC_VC == null || arrID_HDON_DC_VC.Count() == 0)
                                drCosfi["ID_HDON_DC"] = lngIDHDonDC;
                            else drCosfi["ID_HDON_DC"] = lngIDHDonDC_VC;
                            dsInvoiceData.Tables["HDN_HDONCOSFI_DC"].Rows.Add(drCosfi);
                            dsInvoiceData.AcceptChanges();
                        }
                    }
                }
                else
                {
                    //Chưa thu tiền khách hàng  
                    //Hóa đơn điều chỉnh mới là hóa đơn lập lại LL
                    foreach (DataRow row in dsInvoiceData.Tables["HDN_HDON_DC"].Rows)
                    {
                        row["LOAI_DCHINH"] = "LL";
                        //set lai ky thang nam dieu chinh
                        row["KY"] = ky;
                        row["THANG"] = thang;
                        row["NAM"] = nam;
                    }
                    dsInvoiceData.AcceptChanges();

                    //obj_HDN_HDON_DC_Controller.CMIS2 = db;
                    i64SequenceHDon++;
                    lngIDHDonDC_HuyBo = i64SequenceHDon;// obj_HDN_HDON_DC_Controller.getMaxID();
                    i64SequenceHDon++;
                    lngIDHDonDC_HuyBo_VC = i64SequenceHDon;// obj_HDN_HDON_DC_Controller.getMaxID();
                    i64SequenceHDon++;
                    lngIDHDonDC = i64SequenceHDon;// obj_HDN_HDON_DC_Controller.getMaxID();
                    i64SequenceHDon++;
                    lngIDHDonDC_VC = i64SequenceHDon;// obj_HDN_HDON_DC_Controller.getMaxID();
                    //Tạo thêm hóa đơn hủy bỏ HB
                    foreach (DataRow rowHD in dsCustomerData.Tables["HDN_HDON"].Rows)
                    {
                        DataRow drHDon_HuyBo = dsInvoiceData.Tables["HDN_HDON_DC"].NewRow();
                        foreach (DataColumn column in dsCustomerData.Tables["HDN_HDON"].Columns)
                        {
                            if (column.ColumnName != "ID_HDON")
                            {
                                if (column.ColumnName == "DIEN_TTHU" || column.ColumnName == "SO_TIEN" || column.ColumnName == "TIEN_GTGT" || column.ColumnName == "TONG_TIEN" || column.ColumnName == "TIEN_TD" || column.ColumnName == "THUE_TD" || column.ColumnName == "TIEN_VC" || column.ColumnName == "THUE_VC")
                                    drHDon_HuyBo[column.ColumnName] = 0 - Utility.DecimalDbnull(rowHD[column.ColumnName]);
                                else
                                    drHDon_HuyBo[column.ColumnName] = rowHD[column.ColumnName];
                            }
                            drHDon_HuyBo["ID_HDON"] = rowHD["ID_HDON"];
                            drHDon_HuyBo["ID_HDON_DC"] = lngIDHDonDC_HuyBo;
                            drHDon_HuyBo["LOAI_DCHINH"] = "HB";
                        }
                        drHDon_HuyBo["KY"] = ky;
                        drHDon_HuyBo["THANG"] = thang;
                        drHDon_HuyBo["NAM"] = nam;
                        dsInvoiceData.Tables["HDN_HDON_DC"].Rows.Add(drHDon_HuyBo);
                        dsInvoiceData.AcceptChanges();
                        if (dsInvoiceData.Tables.Contains("HDN_HDONCTIET_DC") && dsInvoiceData.Tables["HDN_HDONCTIET_DC"].Rows.Count > 0)
                        {
                            foreach (DataRow row in dsInvoiceData.Tables["HDN_HDONCTIET_DC"].Rows)
                            {
                                row["ID_HDON_DC"] = lngIDHDonDC;
                            }
                            dsInvoiceData.Tables["HDN_HDONCTIET_DC"].AcceptChanges();
                        }
                        foreach (DataRow row in dsCustomerData.Tables["HDN_HDONCTIET"].Rows)
                        {
                            if (row["ID_HDON"].ToString().Trim() != rowHD["ID_HDON"].ToString().Trim()) continue;
                            DataRow drCTiet = dsInvoiceData.Tables["HDN_HDONCTIET_DC"].NewRow();
                            foreach (DataColumn column in dsCustomerData.Tables["HDN_HDONCTIET"].Columns)
                            {
                                if (column.ColumnName != "ID_HDON" && column.ColumnName != "ID_HDONCTIET" && column.ColumnName != "ID_CHISO")
                                {
                                    if (column.ColumnName == "DIEN_TTHU" || column.ColumnName == "SO_TIEN")
                                        drCTiet[column.ColumnName] = 0 - Convert.ToDecimal(row[column.ColumnName]);
                                    else
                                        drCTiet[column.ColumnName] = row[column.ColumnName];
                                }
                                drCTiet["ID_HDON_DC"] = lngIDHDonDC_HuyBo;
                                drCTiet["ID_HDONCTIET_DC"] = 1;
                                drCTiet["ID_CHISO_DC"] = row["ID_CHISO"];
                            }
                            drCTiet["KY"] = ky;
                            drCTiet["THANG"] = thang;
                            drCTiet["NAM"] = nam;
                            dsInvoiceData.Tables["HDN_HDONCTIET_DC"].Rows.Add(drCTiet);
                            dsInvoiceData.AcceptChanges();
                        }

                        if (dsInvoiceData.Tables.Contains("HDN_HDONCOSFI_DC") && dsInvoiceData.Tables["HDN_HDONCOSFI_DC"].Rows.Count > 0)
                        {
                            foreach (DataRow row in dsInvoiceData.Tables["HDN_HDONCOSFI_DC"].Rows)
                            {
                                row["ID_HDON_DC"] = lngIDHDonDC;
                            }
                            dsInvoiceData.Tables["HDN_HDONCOSFI_DC"].AcceptChanges();
                        }
                        if (dsCustomerData.Tables.Contains("HDN_HDONCOSFI"))
                        {

                            foreach (DataRow row in dsCustomerData.Tables["HDN_HDONCOSFI"].Rows)
                            {
                                if (row["ID_HDON"].ToString().Trim() != rowHD["ID_HDON"].ToString().Trim()) continue;
                                DataRow drCosfi = dsInvoiceData.Tables["HDN_HDONCOSFI_DC"].NewRow();
                                foreach (DataColumn column in dsCustomerData.Tables["HDN_HDONCOSFI"].Columns)
                                {
                                    if (column.ColumnName != "ID_HDON" && column.ColumnName != "ID_HDONCOSFI")//
                                    {
                                        if (column.ColumnName == "TIEN_HUUCONG" || column.ColumnName == "TIEN_VOCONG" || column.ColumnName == "VO_CONG")
                                            drCosfi[column.ColumnName] = 0 - Convert.ToDecimal(row[column.ColumnName]);
                                        else
                                            drCosfi[column.ColumnName] = row[column.ColumnName];
                                    }
                                    drCosfi["ID_HDON_DC"] = lngIDHDonDC_HuyBo;
                                    drCosfi["ID_HDONCOSFI_DC"] = 1;
                                }
                                drCosfi["KY"] = ky;
                                drCosfi["THANG"] = thang;
                                drCosfi["NAM"] = nam;
                                dsInvoiceData.Tables["HDN_HDONCOSFI_DC"].Rows.Add(drCosfi);
                                dsInvoiceData.AcceptChanges();
                            }
                        }
                    }
                }

                #region HDN_HDON_DC

                string macnang = dsCustomerData.Tables["HDN_BBAN_DCHINH"].Rows[0]["MA_CNANG"].ToString();
                foreach (DataRow dr1 in dsInvoiceData.Tables["HDN_HDON_DC"].Rows)
                {
                    HDN_HDON_DC_PLUS infoHDDC = new HDN_HDON_DC_PLUS();

                    infoHDDC = BillingLibrary.BillingLibrary.MapDatarowToObjectCDPlus<HDN_HDON_DC_PLUS>(dr1, ref strError);
                    infoHDDC.MA_CNANG = macnang;
                    if (strError.Trim().Length == 0)
                    {
                        //Nếu hóa đơn điều chỉnh có loại hóa đơn ="RA"(Quy ước khi không có thay đổi về tiền)
                        //Không ghi vào CSDL
                        if (infoHDDC.LOAI_DCHINH == "RA") continue;
                        if (infoHDDC.LOAI_DCHINH == "LL" || infoHDDC.LOAI_DCHINH == "TT" || infoHDDC.LOAI_DCHINH == "TH")
                        {
                            if (infoHDDC.LOAI_HDON == "VC")
                            {
                                infoHDDC.ID_HDON_DC = lngIDHDonDC_VC;
                                if (rowPS_VC == null) //Nếu không có hóa đơn phát sinh VC, ID_HDON_DC = ID_HDON
                                {
                                    if (rowPS_TD == null)
                                        infoHDDC.ID_HDON = infoHDDC.ID_HDON_DC;
                                    else
                                        infoHDDC.ID_HDON = Convert.ToInt64(rowPS_TD["ID_HDON"]);
                                }
                                else
                                    infoHDDC.ID_HDON = Convert.ToInt64(rowPS_VC["ID_HDON"]);
                            }
                            else
                            {
                                infoHDDC.ID_HDON_DC = lngIDHDonDC;
                                if (rowPS_TD == null) //Nếu không có hóa đơn phát sinh TD, ID_HDON_DC = ID_HDON
                                    infoHDDC.ID_HDON = infoHDDC.ID_HDON_DC;
                                else
                                    infoHDDC.ID_HDON = Convert.ToInt64(rowPS_TD["ID_HDON"]);
                            }
                        }
                        else
                        {
                            if (infoHDDC.LOAI_HDON == "VC")
                            {
                                infoHDDC.ID_HDON_DC = lngIDHDonDC_HuyBo_VC;
                            }
                            else
                            {
                                infoHDDC.ID_HDON_DC = lngIDHDonDC_HuyBo;
                            }
                        }
                        #region Bổ sung cột MA_YCAU_KNAI
                        if (dsCustomerData.Tables.Contains("LST_TIEN_TRINH") && dsCustomerData.Tables["LST_TIEN_TRINH"].Rows.Count > 0)
                        {
                            infoHDDC.MA_YCAU_KNAI = dsCustomerData.Tables["LST_TIEN_TRINH"].Rows[0]["MA_YCAU_KNAI"].ToString().Trim();
                        }
                        #endregion
                        dataInsert.HDN_HDON_DC.Add(infoHDDC);
                    }
                    else return strError;
                }
                //Kiểm tra xem hóa đơn điều chỉnh mới có ID_HDON_DC = lngIDHDonDC không, hay bằng lngIDHDonDC_VC
                #endregion

                #region HDN_BBAN_DCHINH
                //obj_HDN_BBAN_DCHINH_Controller = new cls_HDN_BBAN_DCHINH_Controller();
                //obj_HDN_BBAN_DCHINH_Controller.CMIS2 = db;
                i64SequenceBBan++;
                long lngIDBBanDC = i64SequenceBBan;// obj_HDN_BBAN_DCHINH_Controller.getMaxID();
                short isDup1 = 0;

                foreach (HDN_HDON_DC_PLUS dcInfo in dataInsert.HDN_HDON_DC)
                {
                    DataRow dr2 = dsCustomerData.Tables["HDN_BBAN_DCHINH"].Rows[0];
                    HDN_BBAN_DCHINH_PLUS infoBBDC = new HDN_BBAN_DCHINH_PLUS();
                    infoBBDC = BillingLibrary.BillingLibrary.MapDatarowToObjectCDPlus<HDN_BBAN_DCHINH_PLUS>(dr2, ref strError);
                    if (strError.Trim().Length == 0)
                    {
                        infoBBDC.ID_BBAN = lngIDBBanDC;
                        infoBBDC.ID_HDON_DC = dcInfo.ID_HDON_DC;
                        infoBBDC.ID_HDON = dcInfo.ID_HDON;
                        isDup1 = infoBBDC.IS_DUP1;
                        dataInsert.HDN_BBAN_DCHINH.Add(infoBBDC);
                    }
                    else return strError;
                }
                #endregion


                #region HDN_CHISO_DC
                //obj_HDN_CHISO_DC_Controller = new cls_HDN_CHISO_DC_Controller();
                ////obj_HDN_BCS_CTO_DC_Controller.CMIS2 = db;
                //obj_HDN_CHISO_DC_Controller.CMIS2 = db;
                //Dũng NT sửa để fix ID_CHISO tương ứng với ID_CHISO phát sinh
                if (!dsCustomerData.Tables["HDN_CHISO_DC"].Columns.Contains("ID_CHISO_PS"))
                    dsCustomerData.Tables["HDN_CHISO_DC"].Columns.Add("ID_CHISO_PS", typeof(Int64));
                //End
                HDN_HDON_DC_PLUS dcTemp = dataInsert.HDN_HDON_DC.Where(c => c.LOAI_DCHINH != "HB" && c.LOAI_HDON == "TC").FirstOrDefault();
                //lngIDHDonDC_Tem là ID_HDON_DC của hóa đơn điều chỉnh vừa tạo ra
                //Trường hợp không có HDDC tiền điện thì lấy bằng ID của HDDC vô công. 

                if (dcTemp == null)
                {
                    //Không phải TC
                    dcTemp = dataInsert.HDN_HDON_DC.Where(c => c.LOAI_DCHINH != "HB" && c.LOAI_HDON == "TD").FirstOrDefault();
                    if (dcTemp != null)  //Nếu không có báo lỗi
                    {
                        lngIDHDonDC = dcTemp.ID_HDON_DC;
                        lngIDHDonDC_VC = dcTemp.ID_HDON_DC;
                    }
                    else
                    {
                        dcTemp = dataInsert.HDN_HDON_DC.Where(c => c.LOAI_DCHINH != "HB" && c.LOAI_HDON == "VC").FirstOrDefault();
                        if (dcTemp != null)
                        {
                            lngIDHDonDC_VC = dcTemp.ID_HDON_DC;
                            lngIDHDonDC = dcTemp.ID_HDON_DC;
                        }
                    }
                    dcTemp = dataInsert.HDN_HDON_DC.Where(c => c.LOAI_DCHINH != "HB" && c.LOAI_HDON == "VC").FirstOrDefault();
                    if (dcTemp != null)
                        lngIDHDonDC_VC = dcTemp.ID_HDON_DC;


                    //else
                    //    return "Không tồn tại dữ liệu hóa đơn điều chỉnh truy thu, thoái hoàn hoặc lập lại";

                }
                else
                {
                    lngIDHDonDC = dcTemp.ID_HDON_DC;
                    lngIDHDonDC_VC = dcTemp.ID_HDON_DC;
                }



                foreach (DataRow dr4 in dsCustomerData.Tables["HDN_CHISO_DC"].Rows)
                {
                    dr4["ID_CHISO_PS"] = dr4["ID_CHISO"];
                    HDN_CHISO_DC_PLUS infoCSDC = new HDN_CHISO_DC_PLUS();
                    //dr4["MA_CNANG"] = macnang;
                    infoCSDC = BillingLibrary.BillingLibrary.MapDatarowToObjectCDPlus<HDN_CHISO_DC_PLUS>(dr4, ref strError);
                    infoCSDC.MA_CNANG = macnang;
                    if (strError.Trim().Length == 0)
                    {

                        if (infoCSDC.BCS != "VC")
                            infoCSDC.ID_HDON_DC = lngIDHDonDC;
                        else
                            infoCSDC.ID_HDON_DC = lngIDHDonDC_VC;

                        if (infoCSDC.SAN_LUONG != null)
                            infoCSDC.SAN_LUONG = Math.Round(infoCSDC.SAN_LUONG.Value);
                        if (infoCSDC.SLUONG_TTIEP != null)
                            infoCSDC.SLUONG_TTIEP = Math.Round(infoCSDC.SLUONG_TTIEP.Value);
                        //Gán lại ID_BCS_DC
                        //HDN_BCS_CTO_DC infoBCS = obj_HDN_BCS_CTO_DC_Controller.LstInfo.Where(c => c.MA_DVIQLY == infoCSDC.MA_DVIQLY && c.ID_BCS_DC == infoCSDC.ID_BCS_DC).FirstOrDefault();
                        //if (infoBCS == null) return "Không tìm thấy bộ chỉ số tương ứng MA_DVIQLY=" + infoCSDC.MA_DVIQLY + " và ID_BCS_DC=" + infoCSDC.ID_BCS_DC.ToString() + "";
                        //infoBCS.ID_BCS_DC = obj_HDN_BCS_CTO_DC_Controller.getMaxID();
                        infoCSDC.ID_BCS_DC = -1;
                        i64SequenceCSo++;
                        infoCSDC.ID_CHISO = i64SequenceCSo;// obj_HDN_CHISO_DC_Controller.getMaxID();
                        dr4["ID_CHISO"] = infoCSDC.ID_CHISO;
                        dataInsert.HDN_CHISO_DC.Add(infoCSDC);
                    }
                    else return strError;
                }
                #endregion

                #region HDN_HDONCTIET_DC
                //obj_HDN_HDONCTIET_DC_Controller = new cls_HDN_HDONCTIET_DC_Controller();
                //obj_HDN_HDONCTIET_DC_Controller.CMIS2 = db;
                var arrHDonTD = dataInsert.HDN_HDON_DC.Where(c => "TD;TC".Contains(c.LOAI_HDON) && c.LOAI_DCHINH != "HB");
                var arrHDonVC = dataInsert.HDN_HDON_DC.Where(c => c.LOAI_HDON == "VC" && c.LOAI_DCHINH != "HB");
                foreach (DataRow dr3 in dsInvoiceData.Tables["HDN_HDONCTIET_DC"].Rows)
                //DataRow dr2 = dsInvoiceData.Tables["HDN_HDONCTIET_DC"].Rows[0];
                {
                    HDN_HDONCTIET_DC_PLUS infoHDCT = new HDN_HDONCTIET_DC_PLUS();

                    long lngIDHDCTietDC = 0;
                    infoHDCT = BillingLibrary.BillingLibrary.MapDatarowToObjectCDPlus<HDN_HDONCTIET_DC_PLUS>(dr3, ref strError);
                    infoHDCT.MA_CNANG = macnang;
                    if (strError.Trim().Length == 0)
                    {
                        i64SequenceCTiet++;
                        lngIDHDCTietDC = i64SequenceCTiet;// obj_HDN_HDONCTIET_DC_Controller.getMaxID();
                        infoHDCT.ID_HDONCTIET_DC = lngIDHDCTietDC;
                        if (decDaTra != 0)
                        {
                            if ("KT;BT;CD;TD".Contains(infoHDCT.BCS))
                            {
                                if (arrHDonTD == null || arrHDonTD.Count() == 0)
                                    infoHDCT.ID_HDON_DC = lngIDHDonDC_VC;
                                else infoHDCT.ID_HDON_DC = lngIDHDonDC;
                            }
                            else
                            {
                                int intTC = dataInsert.HDN_HDON_DC.Where(c => c.LOAI_HDON == "TC" && c.LOAI_DCHINH != "HB").Count();
                                if (intTC > 0)
                                {
                                    infoHDCT.ID_HDON_DC = lngIDHDonDC;
                                }
                                else
                                {
                                    if (arrHDonVC == null || arrHDonVC.Count() == 0)
                                        infoHDCT.ID_HDON_DC = lngIDHDonDC;
                                    else infoHDCT.ID_HDON_DC = lngIDHDonDC_VC;
                                }
                            }
                        }
                        else
                        {
                            if ("KT;BT;CD;TD".Contains(infoHDCT.BCS))
                            {
                                if (infoHDCT.SO_TIEN <= 0)
                                    infoHDCT.ID_HDON_DC = lngIDHDonDC_HuyBo;
                                else
                                {
                                    infoHDCT.ID_HDON_DC = infoHDCT.ID_HDON_DC == lngIDHDonDC_HuyBo ? lngIDHDonDC_HuyBo : lngIDHDonDC;
                                }
                            }
                            else
                            {
                                int intTC = dataInsert.HDN_HDON_DC.Where(c => c.LOAI_HDON == "TC" && c.LOAI_DCHINH != "HB").Count();
                                if (intTC > 0)
                                {
                                    if (infoHDCT.SO_TIEN <= 0)
                                        infoHDCT.ID_HDON_DC = infoHDCT.ID_HDON_DC == lngIDHDonDC_HuyBo_VC ? lngIDHDonDC_HuyBo_VC : lngIDHDonDC_HuyBo;
                                    else
                                        infoHDCT.ID_HDON_DC = lngIDHDonDC;
                                }
                                else
                                {
                                    if (infoHDCT.SO_TIEN < 0)
                                        infoHDCT.ID_HDON_DC = lngIDHDonDC_HuyBo_VC;
                                    else
                                    {
                                        infoHDCT.ID_HDON_DC = infoHDCT.ID_HDON_DC == lngIDHDonDC_HuyBo_VC ? lngIDHDonDC_HuyBo_VC : lngIDHDonDC_VC;
                                    }
                                }


                            }
                        }

                        var chiso = from a in dataInsert.HDN_CHISO_DC
                                    where a.BCS == infoHDCT.BCS
                                    && a.ID_HDON_DC == infoHDCT.ID_HDON_DC
                                    && a.MA_DDO == infoHDCT.MA_DDO
                                    && a.SO_CTO == infoHDCT.SO_CTO
                                    select a;
                        if (chiso != null && chiso.Count() > 0)
                        {
                            foreach (DataRow drChiSoTemp in dsCustomerData.Tables["HDN_CHISO_DC"].Rows)
                            {
                                if (Convert.ToInt64(drChiSoTemp["ID_CHISO_PS"]) == infoHDCT.ID_CHISO_DC)
                                {
                                    chiso = chiso.Where(c => c.ID_CHISO == Convert.ToInt64(drChiSoTemp["ID_CHISO"]));
                                    break;
                                }
                            }
                            if (chiso != null && chiso.Count() > 0)
                            {
                                infoHDCT.ID_CHISO_DC = chiso.First().ID_CHISO;
                            }
                        }

                        infoHDCT.KY = ky;
                        infoHDCT.THANG = thang;
                        infoHDCT.NAM = nam;
                        dataInsert.HDN_HDONCTIET_DC.Add(infoHDCT);
                    }
                    else return strError;
                }
                //Thêm chi tiết âm của hóa đơn phát sinh gốc
                if (dsCustomerData.Tables.Contains("HDN_HDONCTIET_PS") && dsCustomerData.Tables["HDN_HDONCTIET_PS"].Rows.Count > 0)
                {
                    foreach (DataRow row in dsCustomerData.Tables["HDN_HDONCTIET_PS"].Rows)
                    {
                        //if (row["ID_HDON"].ToString().Trim() != rowHD["ID_HDON"].ToString().Trim()) continue;
                        DataRow drCTiet = dsInvoiceData.Tables["HDN_HDONCTIET_DC"].NewRow();
                        foreach (DataColumn column in dsCustomerData.Tables["HDN_HDONCTIET"].Columns)
                        {
                            if (column.ColumnName != "ID_HDON" && column.ColumnName != "ID_HDONCTIET" && column.ColumnName != "ID_CHISO")
                            {
                                if (column.ColumnName == "DIEN_TTHU" || column.ColumnName == "SO_TIEN")
                                    drCTiet[column.ColumnName] = 0 - Convert.ToDecimal(row[column.ColumnName]);
                                else
                                    drCTiet[column.ColumnName] = row[column.ColumnName];
                            }
                            drCTiet["ID_HDON_DC"] = lngIDHDonDC;
                            drCTiet["ID_HDONCTIET_DC"] = 1;
                            drCTiet["ID_CHISO_DC"] = row["ID_CHISO"];
                        }
                        drCTiet["KY"] = ky;
                        drCTiet["THANG"] = thang;
                        drCTiet["NAM"] = nam;
                        dsInvoiceData.Tables["HDN_HDONCTIET_DC"].Rows.Add(drCTiet);
                        HDN_HDONCTIET_DC_PLUS infoHDCT = new HDN_HDONCTIET_DC_PLUS();

                        long lngIDHDCTietDC = 0;
                        infoHDCT = BillingLibrary.BillingLibrary.MapDatarowToObjectCDPlus<HDN_HDONCTIET_DC_PLUS>(drCTiet, ref strError);
                        infoHDCT.MA_CNANG = macnang;
                        if (strError.Trim().Length == 0)
                        {
                            i64SequenceCTiet++;
                            lngIDHDCTietDC = i64SequenceCTiet;//obj_HDN_HDONCTIET_DC_Controller.getMaxID();
                            infoHDCT.ID_HDONCTIET_DC = lngIDHDCTietDC;
                            dataInsert.HDN_HDONCTIET_DC.Add(infoHDCT);
                        }
                        else return strError;
                        dsInvoiceData.AcceptChanges();
                    }
                }
                //Kêt thúc
                #endregion



                if (dsInvoiceData.Tables.Contains("HDN_HDONCOSFI_DC") && dsInvoiceData.Tables["HDN_HDONCOSFI_DC"].Rows.Count > 0)
                {
                    #region HDN_HDONCOSFI_DC
                    //obj_HDN_HDONCOSFI_DC_Controller = new cls_HDN_HDONCOSFI_DC_Controller();
                    //obj_HDN_HDONCOSFI_DC_Controller.CMIS2 = db;
                    foreach (DataRow dr9 in dsInvoiceData.Tables["HDN_HDONCOSFI_DC"].Rows)
                    //DataRow dr2 = dsInvoiceData.Tables["HDN_HDONCOSFI_DC"].Rows[0];
                    {
                        HDN_HDONCOSFI_DC_PLUS infoHDCF = new HDN_HDONCOSFI_DC_PLUS();
                        long lngIDHDCosfiDC = 0;
                        infoHDCF = BillingLibrary.BillingLibrary.MapDatarowToObjectCDPlus<HDN_HDONCOSFI_DC_PLUS>(dr9, ref strError);
                        infoHDCF.MA_CNANG = macnang;
                        if (strError.Trim().Length == 0)
                        {
                            i64SequenceCosfi++;
                            lngIDHDCosfiDC = i64SequenceCosfi;//obj_HDN_HDONCOSFI_DC_Controller.getMaxID();
                            infoHDCF.ID_HDONCOSFI_DC = lngIDHDCosfiDC;
                            if (arrHDonVC == null || arrHDonVC.Count() == 0)
                                infoHDCF.ID_HDON_DC = lngIDHDonDC;
                            else
                            {
                                if (arrHDonVC.First().LOAI_DCHINH == "LL")
                                {
                                    if (infoHDCF.VO_CONG < 0)
                                    {
                                        var arrHDonVC_HB = dataInsert.HDN_HDON_DC.Where(c => c.LOAI_HDON == "VC" && c.LOAI_DCHINH == "HB");
                                        if (arrHDonVC_HB == null || arrHDonVC_HB.Count() == 0)
                                            //Ko có hóa đơn hủy bỏ vô công, dùng ID_HDON_DC hủy bỏ của TD
                                            infoHDCF.ID_HDON_DC = lngIDHDonDC_HuyBo;
                                        else
                                            infoHDCF.ID_HDON_DC = lngIDHDonDC_HuyBo_VC;
                                    }
                                    else
                                        infoHDCF.ID_HDON_DC = lngIDHDonDC_VC;
                                }
                                else
                                    infoHDCF.ID_HDON_DC = lngIDHDonDC_VC;
                            }
                            var chiso = from a in dataInsert.HDN_CHISO_DC
                                        where a.BCS == "VC"
                                        && a.ID_HDON_DC == infoHDCF.ID_HDON_DC
                                        && a.MA_DDO == infoHDCF.MA_DDO
                                        select a;
                            if (chiso != null && chiso.Count() > 0)
                            {
                                foreach (DataRow drChiSoTemp in dsCustomerData.Tables["HDN_CHISO_DC"].Rows)
                                {
                                    if (Convert.ToInt64(drChiSoTemp["ID_CHISO_PS"]) == infoHDCF.ID_CHISO_DC)
                                    {
                                        chiso = chiso.Where(c => c.ID_CHISO == Convert.ToInt64(drChiSoTemp["ID_CHISO"]));
                                        break;
                                    }
                                }
                                if (chiso != null && chiso.Count() > 0)
                                {
                                    infoHDCF.ID_CHISO_DC = chiso.First().ID_CHISO;
                                }
                            }
                            infoHDCF.KY = ky;
                            infoHDCF.THANG = thang;
                            infoHDCF.NAM = nam;

                            dataInsert.HDN_HDONCOSFI_DC.Add(infoHDCF);
                        }
                        else return strError;
                    }
                    #endregion
                }
                if (dsCustomerData.Tables.Contains("HDN_QHE_DDO_DC") && dsCustomerData.Tables["HDN_QHE_DDO_DC"].Rows.Count > 0)
                {
                    #region HDN_QHE_DDO_DC
                    //obj_HDN_QHE_DDO_DC_Controller = new cls_HDN_QHE_DDO_DC_Controller();
                    //obj_HDN_QHE_DDO_DC_Controller.CMIS2 = db;
                    //foreach (DataRow dr10 in dsCustomerData.Tables["HDN_QHE_DDO_DC"].Rows)
                    ////DataRow dr2 = dsInvoiceData.Tables["HDN_QHE_DDO_DC"].Rows[0];
                    //{
                    //    HDN_QHE_DDO_DC infoQHDDo = new HDN_QHE_DDO_DC();
                    //    long lngIDQHeDDoDC = 0;
                    //    infoQHDDo = BillingLibrary.BillingLibrary.MapDatarowToObjectCDPlus<HDN_QHE_DDO_DC>(dr10, ref strError);
                    //    infoQHDDo.MA_CNANG = macnang;
                    //    if (strError.Trim().Length == 0)
                    //    {
                    //        lngIDQHeDDoDC = obj_HDN_QHE_DDO_DC_Controller.getMaxID();
                    //        infoQHDDo.ID_QHE_DC = lngIDQHeDDoDC;
                    //        infoQHDDo.ID_HDON_DC = lngIDHDonDC_Tem;
                    //        obj_HDN_QHE_DDO_DC_Controller.LstInfo.Add(infoQHDDo);
                    //    }
                    //    else return strError;
                    //}
                    #endregion
                }
                //if (dsCustomerData.Tables.Contains("GCS_CHISO_DKY") && dsCustomerData.Tables["GCS_CHISO_DKY"].Rows.Count > 0)


                return "";
            }
            catch (Exception ex)
            {
                return "Lỗi khi gán dữ liệu cho Object: " + ex.Message;
            }
        }


        public string InsertInvoiceDataKH(DataSet dsInvoiceData, DataSet dsCustomerData, string strMa_DViQLy, string strMa_SoGCS, string strTenDNhap, long lngCurrentLibID, long lngWorkflowID, short ky, short thang, short nam)
        {
            //CMIS2 db = new CMIS2();
            string strMaKHang = "";
            try
            {
                List<GCS_CHISO_PLUS> lstChiSo = new List<GCS_CHISO_PLUS>();
                List<HDN_HDON_PLUS> lstHDon = new List<HDN_HDON_PLUS>();
                List<HDN_HDONCOSFI_PLUS> lstHDonCosfi = new List<HDN_HDONCOSFI_PLUS>();
                List<HDN_HDONCTIET_PLUS> lstHDonCTiet = new List<HDN_HDONCTIET_PLUS>();
                string strResult = SetPropertiesForObjectPlus(dsInvoiceData, dsCustomerData, lstChiSo, lstHDon, lstHDonCTiet, lstHDonCosfi, strTenDNhap, lngCurrentLibID.ToString());
                if (strResult != "") return strResult;
                if (lstHDon == null || lstHDon.Count <= 0) return "Danh sach hoa don khong co";
                if (lstHDonCTiet == null || lstHDonCTiet.Count <= 0) return "Danh sach hoa don chi tiet khong co";
                //Viet lai doan code nay de submitchange 1 lan
                var arrMaKHang = lstHDon.Select(c => c.MA_KHANG).Distinct();
                //obj_HDN_HDON_Controller.CMIS2 = db;
                //obj_HDN_HDONCTIET_Controller.CMIS2 = db;
                //obj_HDN_HDONCOSFI_Controller.CMIS2 = db;
                //obj_GCS_CHISO_Controller.CMIS2 = db;
                inpDLieuHDon inpTHD = new inpDLieuHDon();
                inpTHD.LST_CSO = new List<GCS_CHISO_PLUS>();
                inpTHD.LST_HDCF = new List<HDN_HDONCOSFI_PLUS>();
                inpTHD.LST_HDCT = new List<HDN_HDONCTIET_PLUS>();
                inpTHD.LST_HDON = new List<HDN_HDON_PLUS>();
                //Tach ra theo tung ma khach hang
                foreach (var x in arrMaKHang)
                {
                    if (x == "PD01000010568")
                    {
                    }
                    strMaKHang = x;
                    string strTemp = "";
                    List<HDN_HDON_PLUS> lstHDon_Temp = lstHDon.Where(c => c.MA_KHANG == x).ToList();
                    List<HDN_HDONCTIET_PLUS> lstHDonCTiet_Temp = lstHDonCTiet.Where(c => c.MA_KHANG == x).ToList();
                    List<GCS_CHISO_PLUS> lstChiSo_Temp = lstChiSo.Where(c => c.MA_KHANG == x).ToList();
                    inpTHD.LST_HDON.AddRange(lstHDon_Temp);
                    inpTHD.LST_HDCT.AddRange(lstHDonCTiet_Temp);
                    inpTHD.LST_CSO.AddRange(lstChiSo_Temp);
                    //obj_HDN_HDON_Controller.lstHDon = lstHDon_Temp;
                    //obj_HDN_HDONCTIET_Controller.lstCTiet = lstHDonCTiet_Temp;
                    //obj_GCS_CHISO_Controller.LstInfo = new List<GCS_CHISO>();
                    //obj_GCS_CHISO_Controller.LstInfo = lstChiSo_Temp;
                    //strTemp += obj_HDN_HDON_Controller.InsertHDN_HDON();
                    //strTemp += obj_HDN_HDONCTIET_Controller.InsertHDN_HDONCTIET();
                    if (lstHDonCosfi != null && lstHDonCosfi.Count > 0)
                    {

                        List<HDN_HDONCOSFI_PLUS> lstHDonCosfi_Temp = lstHDonCosfi.Where(c => c.MA_KHANG == x).ToList();
                        if (lstHDonCosfi_Temp != null && lstHDonCosfi_Temp.Count > 0)
                        {
                            foreach (HDN_HDONCOSFI_PLUS hcosfi in lstHDonCosfi_Temp)
                            {
                                if (lstHDon_Temp != null && lstHDon_Temp.Count > 0)
                                {
                                    var hdon = lstHDon_Temp.Where(c => c.LOAI_HDON == "VC");
                                    if (hdon != null && hdon.Count() > 0)
                                    {
                                        hcosfi.ID_HDON = hdon.First().ID_HDON;
                                    }
                                }
                            }
                        }
                        inpTHD.LST_HDCF.AddRange(lstHDonCosfi_Temp);
                        //obj_HDN_HDONCOSFI_Controller.lstCosfi = lstHDonCosfi_Temp;
                        //strTemp = strTemp + obj_HDN_HDONCOSFI_Controller.InsertHDN_HDONCOSFI();
                        lstHDonCosfi = lstHDonCosfi.Where(c => c.MA_KHANG != x).ToList();
                    }

                    //strTemp += obj_GCS_CHISO_Controller.UpdateList();

                    //if (strTemp != "")
                    //{
                    //    //Bao loi insert hoa don
                    //    return "Lỗi Insert hóa đơn, mã sổ = " + strMa_SoGCS + " mã khách hàng = " + x + ": " + strTemp;
                    //}

                    lstHDon = lstHDon.Where(c => c.MA_KHANG != x).ToList();
                    lstHDonCTiet = lstHDonCTiet.Where(c => c.MA_KHANG != x).ToList();
                    lstChiSo = lstChiSo.Where(c => c.MA_KHANG != x).ToList();
                }
                if (lstChiSo != null && lstChiSo.Count > 0)
                {
                    //obj_GCS_CHISO_Controller.LstInfo = lstChiSo;
                    //obj_GCS_CHISO_Controller.UpdateList();
                    inpTHD.LST_CSO.AddRange(lstChiSo);
                }
                //Insert luong so ghi chi so
                string strInput = JsonConvert.SerializeObject(inpTHD);
                //Gọi hàm insert
                string strIP = ConfigurationManager.AppSettings["URI"];
                var baseAddress = "http://" + strIP + "/ServiceHDonPSinh-HDonPSinh-context-root/resources/serviceHDonPSinh/insertTHD_KH";
                var http = (HttpWebRequest)WebRequest.Create(new Uri(baseAddress));
                http.Accept = "application/json";
                http.ContentType = "application/json";
                http.Method = "POST";

                //string parsedContent = "{'MA_DVIQLY':'PC06AA','TEN_DANH_MUC':'D_SOGCS'}";
                string parsedContent = strInput;
                //ASCIIEncoding encoding = new ASCIIEncoding();
                Byte[] bytes = System.Text.Encoding.UTF8.GetBytes(parsedContent);

                Stream newStream = http.GetRequestStream();
                newStream.Write(bytes, 0, bytes.Length);
                newStream.Close();

                var response = http.GetResponse();

                var stream = response.GetResponseStream();
                var sr = new StreamReader(stream);
                var content = sr.ReadToEnd();
                ReturnObject obj = JsonConvert.DeserializeObject<ReturnObject>(content);
                if (obj.TYPE == "ERROR") return obj.MESSAGE;
                else return "";

            }
            catch (Exception ex)
            {
                return "Lỗi khi Insert dữ liệu hóa đơn với mã khách hàng= " + strMaKHang + ": " + ex.Message;
            }
            finally
            {
                //db.ReleaseConnection();
            }
        }
        public string InsertInvoiceDataKH_DCN(DataSet dsInvoiceData, DataSet dsInvoiceDataDCN, DataSet dsCustomerData, string strMa_DViQLy, string strMa_SoGCS, string strTenDNhap, long lngCurrentLibID, long lngWorkflowID, short ky, short thang, short nam)
        {
            //CMIS2 db = new CMIS2();
            string strMaKHang = "";
            try
            {
                List<GCS_CHISO_PLUS> lstChiSo = new List<GCS_CHISO_PLUS>();
                List<HDN_HDON_PLUS> lstHDon = new List<HDN_HDON_PLUS>();
                List<HDN_HDONCOSFI_PLUS> lstHDonCosfi = new List<HDN_HDONCOSFI_PLUS>();
                List<HDN_HDONCTIET_PLUS> lstHDonCTiet = new List<HDN_HDONCTIET_PLUS>();
                List<GCS_CHISO_PLUS> lstChiSoDCN = new List<GCS_CHISO_PLUS>();
                List<HDN_HDON_PLUS> lstHDonDCN = new List<HDN_HDON_PLUS>();
                List<HDN_HDONCOSFI_PLUS> lstHDonCosfiDCN = new List<HDN_HDONCOSFI_PLUS>();
                List<HDN_HDONCTIET_PLUS> lstHDonCTietDCN = new List<HDN_HDONCTIET_PLUS>();
                string strResult = SetPropertiesForObjectPlus(dsInvoiceDataDCN, dsCustomerData, lstChiSoDCN, lstHDonDCN, lstHDonCTietDCN, lstHDonCosfiDCN, strTenDNhap, lngCurrentLibID.ToString());
                if (strResult != "") return strResult;
                if (lstHDonDCN == null || lstHDonDCN.Count <= 0) return "Danh sach hoa don khong co";
                if (lstHDonCTietDCN == null || lstHDonCTietDCN.Count <= 0) return "Danh sach hoa don chi tiet khong co";
                //Đổi lại tên bảng về GCS_CHISO để update
                #region Đổi lại tên bảng về GCS_CHISO để update
                dsCustomerData.Tables["GCS_CHISO"].TableName = "GCS_CSO_DCN";
                dsCustomerData.Tables["GCS_CHISO_GT"].TableName = "GCS_CSO_DCN_GT";
                dsCustomerData.Tables["GCS_CHISO_TP"].TableName = "GCS_CSO_DCN_TP"; ;
                dsCustomerData.Tables["GCS_CHISO_BQ"].TableName = "GCS_CSO_DCN_BQ";
                dsCustomerData.Tables["GCS_CHISO_DDK"].TableName = "GCS_CHISO";
                dsCustomerData.Tables["GCS_CHISO_DDK_GT"].TableName = "GCS_CHISO_GT";
                dsCustomerData.Tables["GCS_CHISO_DDK_TP"].TableName = "GCS_CHISO_TP";
                dsCustomerData.Tables["GCS_CHISO_DDK_BQ"].TableName = "GCS_CHISO_BQ";
                #endregion
                strResult = SetPropertiesForObjectPlus(dsInvoiceData, dsCustomerData, lstChiSo, lstHDon, lstHDonCTiet, lstHDonCosfi, strTenDNhap, lngCurrentLibID.ToString());
                //Debug.WriteLine(DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff") + " InsertInvoiceDataPlus SetPropertiesForObjectPlusComplete");
                if (strResult != "") return strResult;
                if (lstHDon == null || lstHDon.Count <= 0) return "Danh sach hoa don khong co";
                if (lstHDonCTiet == null || lstHDonCTiet.Count <= 0) return "Danh sach hoa don chi tiet khong co";



                //Viet lai doan code nay de submitchange 1 lan
                var arrMaKHang = lstHDon.Select(c => c.MA_KHANG).Distinct();
                //obj_HDN_HDON_Controller.CMIS2 = db;
                //obj_HDN_HDONCTIET_Controller.CMIS2 = db;
                //obj_HDN_HDONCOSFI_Controller.CMIS2 = db;
                //obj_GCS_CHISO_Controller.CMIS2 = db;
                inpDLieuHDon inpTHD = new inpDLieuHDon();
                inpTHD.LST_CSO = new List<GCS_CHISO_PLUS>();
                inpTHD.LST_HDCF = new List<HDN_HDONCOSFI_PLUS>();
                inpTHD.LST_HDCT = new List<HDN_HDONCTIET_PLUS>();
                inpTHD.LST_HDON = new List<HDN_HDON_PLUS>();

                inpTHD.LST_HDCF_BT = new List<HDN_HDONCOSFI_PLUS>();
                inpTHD.LST_HDCT_BT = new List<HDN_HDONCTIET_PLUS>();
                inpTHD.LST_HDON_BT = new List<HDN_HDON_PLUS>();
                //Tach ra theo tung ma khach hang
                foreach (var x in arrMaKHang)
                {
                    if (x == "PD01000010568")
                    {
                    }
                    strMaKHang = x;
                    string strTemp = "";
                    List<HDN_HDON_PLUS> lstHDon_Temp = lstHDon.Where(c => c.MA_KHANG == x).ToList();
                    List<HDN_HDONCTIET_PLUS> lstHDonCTiet_Temp = lstHDonCTiet.Where(c => c.MA_KHANG == x).ToList();
                    List<GCS_CHISO_PLUS> lstChiSo_Temp = lstChiSo.Where(c => c.MA_KHANG == x).ToList();
                    inpTHD.LST_HDON.AddRange(lstHDon_Temp);
                    inpTHD.LST_HDCT.AddRange(lstHDonCTiet_Temp);
                    inpTHD.LST_CSO.AddRange(lstChiSo_Temp);
                    //obj_HDN_HDON_Controller.lstHDon = lstHDon_Temp;
                    //obj_HDN_HDONCTIET_Controller.lstCTiet = lstHDonCTiet_Temp;
                    //obj_GCS_CHISO_Controller.LstInfo = new List<GCS_CHISO>();
                    //obj_GCS_CHISO_Controller.LstInfo = lstChiSo_Temp;
                    //strTemp += obj_HDN_HDON_Controller.InsertHDN_HDON();
                    //strTemp += obj_HDN_HDONCTIET_Controller.InsertHDN_HDONCTIET();
                    if (lstHDonCosfi != null && lstHDonCosfi.Count > 0)
                    {

                        List<HDN_HDONCOSFI_PLUS> lstHDonCosfi_Temp = lstHDonCosfi.Where(c => c.MA_KHANG == x).ToList();
                        if (lstHDonCosfi_Temp != null && lstHDonCosfi_Temp.Count > 0)
                        {
                            foreach (HDN_HDONCOSFI_PLUS hcosfi in lstHDonCosfi_Temp)
                            {
                                if (lstHDon_Temp != null && lstHDon_Temp.Count > 0)
                                {
                                    var hdon = lstHDon_Temp.Where(c => c.LOAI_HDON == "VC");
                                    if (hdon != null && hdon.Count() > 0)
                                    {
                                        hcosfi.ID_HDON = hdon.First().ID_HDON;
                                    }
                                }
                            }
                        }
                        inpTHD.LST_HDCF.AddRange(lstHDonCosfi_Temp);
                        //obj_HDN_HDONCOSFI_Controller.lstCosfi = lstHDonCosfi_Temp;
                        //strTemp = strTemp + obj_HDN_HDONCOSFI_Controller.InsertHDN_HDONCOSFI();
                        lstHDonCosfi = lstHDonCosfi.Where(c => c.MA_KHANG != x).ToList();
                    }

                    //strTemp += obj_GCS_CHISO_Controller.UpdateList();

                    //if (strTemp != "")
                    //{
                    //    //Bao loi insert hoa don
                    //    return "Lỗi Insert hóa đơn, mã sổ = " + strMa_SoGCS + " mã khách hàng = " + x + ": " + strTemp;
                    //}

                    lstHDon = lstHDon.Where(c => c.MA_KHANG != x).ToList();
                    lstHDonCTiet = lstHDonCTiet.Where(c => c.MA_KHANG != x).ToList();
                    lstChiSo = lstChiSo.Where(c => c.MA_KHANG != x).ToList();
                }
                if (lstChiSo != null && lstChiSo.Count > 0)
                {
                    //obj_GCS_CHISO_Controller.LstInfo = lstChiSo;
                    //obj_GCS_CHISO_Controller.UpdateList();
                    inpTHD.LST_CSO.AddRange(lstChiSo);
                }
                foreach (var x in arrMaKHang)
                {
                    if (x == "PD01000010568")
                    {
                    }
                    strMaKHang = x;
                    string strTemp = "";
                    List<HDN_HDON_PLUS> lstHDon_Temp = lstHDonDCN.Where(c => c.MA_KHANG == x).ToList();
                    List<HDN_HDONCTIET_PLUS> lstHDonCTiet_Temp = lstHDonCTietDCN.Where(c => c.MA_KHANG == x).ToList();
                    List<GCS_CHISO_PLUS> lstChiSo_Temp = lstChiSoDCN.Where(c => c.MA_KHANG == x).ToList();
                    inpTHD.LST_HDON_BT.AddRange(lstHDon_Temp);
                    inpTHD.LST_HDCT_BT.AddRange(lstHDonCTiet_Temp);
                    //inpTHD.LST_CSO.AddRange(lstChiSo_Temp);
                    //obj_HDN_HDON_Controller.lstHDon = lstHDon_Temp;
                    //obj_HDN_HDONCTIET_Controller.lstCTiet = lstHDonCTiet_Temp;
                    //obj_GCS_CHISO_Controller.LstInfo = new List<GCS_CHISO>();
                    //obj_GCS_CHISO_Controller.LstInfo = lstChiSo_Temp;
                    //strTemp += obj_HDN_HDON_Controller.InsertHDN_HDON();
                    //strTemp += obj_HDN_HDONCTIET_Controller.InsertHDN_HDONCTIET();
                    if (lstHDonCosfiDCN != null && lstHDonCosfiDCN.Count > 0)
                    {

                        List<HDN_HDONCOSFI_PLUS> lstHDonCosfi_Temp = lstHDonCosfiDCN.Where(c => c.MA_KHANG == x).ToList();
                        if (lstHDonCosfi_Temp != null && lstHDonCosfi_Temp.Count > 0)
                        {
                            foreach (HDN_HDONCOSFI_PLUS hcosfi in lstHDonCosfi_Temp)
                            {
                                if (lstHDon_Temp != null && lstHDon_Temp.Count > 0)
                                {
                                    var hdon = lstHDon_Temp.Where(c => c.LOAI_HDON == "VC");
                                    if (hdon != null && hdon.Count() > 0)
                                    {
                                        hcosfi.ID_HDON = hdon.First().ID_HDON;
                                    }
                                }
                            }
                        }
                        inpTHD.LST_HDCF_BT.AddRange(lstHDonCosfi_Temp);
                        //obj_HDN_HDONCOSFI_Controller.lstCosfi = lstHDonCosfi_Temp;
                        //strTemp = strTemp + obj_HDN_HDONCOSFI_Controller.InsertHDN_HDONCOSFI();
                        lstHDonCosfiDCN = lstHDonCosfiDCN.Where(c => c.MA_KHANG != x).ToList();
                    }

                    //strTemp += obj_GCS_CHISO_Controller.UpdateList();

                    //if (strTemp != "")
                    //{
                    //    //Bao loi insert hoa don
                    //    return "Lỗi Insert hóa đơn, mã sổ = " + strMa_SoGCS + " mã khách hàng = " + x + ": " + strTemp;
                    //}

                    lstHDonDCN = lstHDonDCN.Where(c => c.MA_KHANG != x).ToList();
                    lstHDonCTietDCN = lstHDonCTietDCN.Where(c => c.MA_KHANG != x).ToList();
                    lstChiSoDCN = lstChiSoDCN.Where(c => c.MA_KHANG != x).ToList();
                }


                //Insert luong so ghi chi so
                string strInput = JsonConvert.SerializeObject(inpTHD);
                //Gọi hàm insert
                string strIP = ConfigurationManager.AppSettings["URI"];
                var baseAddress = "http://" + strIP + "/ServiceHDonPSinh-HDonPSinh-context-root/resources/serviceHDonPSinh/insertTHD_KH";
                var http = (HttpWebRequest)WebRequest.Create(new Uri(baseAddress));
                http.Accept = "application/json";
                http.ContentType = "application/json";
                http.Method = "POST";

                //string parsedContent = "{'MA_DVIQLY':'PC06AA','TEN_DANH_MUC':'D_SOGCS'}";
                string parsedContent = strInput;
                //ASCIIEncoding encoding = new ASCIIEncoding();
                Byte[] bytes = System.Text.Encoding.UTF8.GetBytes(parsedContent);

                Stream newStream = http.GetRequestStream();
                newStream.Write(bytes, 0, bytes.Length);
                newStream.Close();

                var response = http.GetResponse();

                var stream = response.GetResponseStream();
                var sr = new StreamReader(stream);
                var content = sr.ReadToEnd();
                ReturnObject obj = JsonConvert.DeserializeObject<ReturnObject>(content);
                if (obj.TYPE == "ERROR") return obj.MESSAGE;
                else return "";

            }
            catch (Exception ex)
            {
                return "Lỗi khi Insert dữ liệu hóa đơn với mã khách hàng= " + strMaKHang + ": " + ex.Message;
            }
            finally
            {
                //db.ReleaseConnection();
            }
        }

        #endregion
        #region Socket.io
        public string writeMessage(JObject jout)
        {
            //CMIS2 db = new CMIS2();
            try
            {

                string strInput = JsonConvert.SerializeObject(jout);
                //strInput.Replace("null", "");
                //Gọi hàm insert
                string strIP = ConfigurationManager.AppSettings["URI"];
                var baseAddress = "http://" + strIP + "/ServiceQTriHThong-QTriHThong-context-root/resources/serviceQTriHThong/writeMessage";
                var http = (HttpWebRequest)WebRequest.Create(new Uri(baseAddress));
                http.Accept = "application/json";
                http.ContentType = "application/json";
                http.Method = "POST";

                //string parsedContent = "{'MA_DVIQLY':'PC06AA','TEN_DANH_MUC':'D_SOGCS'}";
                string parsedContent = strInput;
                //ASCIIEncoding encoding = new ASCIIEncoding();
                Byte[] bytes = System.Text.Encoding.UTF8.GetBytes(parsedContent);

                Stream newStream = http.GetRequestStream();
                newStream.Write(bytes, 0, bytes.Length);
                newStream.Close();

                var response = http.GetResponse();

                var stream = response.GetResponseStream();
                var sr = new StreamReader(stream);
                var content = sr.ReadToEnd();
                ReturnObject obj = JsonConvert.DeserializeObject<ReturnObject>(content);
                if (obj.TYPE == "ERROR") return obj.MESSAGE;
                else return "";

                //return strResult;
            }
            catch (Exception ex)
            {
                return "Lỗi khi Insert dữ liệu: " + ex.Message;
            }
            finally
            {
                //db.ReleaseConnection();
            }
        }

        #endregion
        #region Tinh hoa don Covid 20210601
        public string InsertInvoiceDataPlus_Gtru(DataSet dsInvoiceData, DataSet dsInvoiceDataGC, DataSet dsCustomerData, string strMa_DViQLy, string strMa_SoGCS, string strTenDNhap, long lngCurrentLibID, long lngWorkflowID, short ky, short thang, short nam, string strNgayGhi, cls_Config config)
        {
            //CMIS2 db = new CMIS2();
            try
            {
                DateTime dtHLucNSH = config.dtHLucHTroNSH;
                DateTime dtHLucSH = config.dtHLucHTroSH;
                DateTime dtHetHLucNSH = config.dtHetHLucHTroNSH;
                DateTime dtHetHLucSH = config.dtHetHLucHTroSH;
                List<GCS_CHISO_PLUS> lstChiSo = new List<GCS_CHISO_PLUS>();
                List<HDN_HDON_PLUS> lstHDon = new List<HDN_HDON_PLUS>();
                List<HDN_HDONCOSFI_PLUS> lstHDonCosfi = new List<HDN_HDONCOSFI_PLUS>();
                List<HDN_HDONCTIET_PLUS> lstHDonCTiet = new List<HDN_HDONCTIET_PLUS>();

                List<GCS_CHISO_PLUS> lstChiSoGC = new List<GCS_CHISO_PLUS>();
                List<HDN_HDON_PLUS> lstHDonGC = new List<HDN_HDON_PLUS>();
                List<HDN_HDONCOSFI_PLUS> lstHDonCosfiGC = new List<HDN_HDONCOSFI_PLUS>();
                List<HDN_HDONCTIET_PLUS> lstHDonCTietGC = new List<HDN_HDONCTIET_PLUS>();
                List<HDN_HDON_PLUS> lstHDonGTRU = new List<HDN_HDON_PLUS>();
                List<HDN_HDONCOSFI_PLUS> lstHDonCosfiGTRU = new List<HDN_HDONCOSFI_PLUS>();
                List<HDN_HDONCTIET_PLUS> lstHDonCTietGTRU = new List<HDN_HDONCTIET_PLUS>();
                string strResult = SetPropertiesForObjectPlus(dsInvoiceData, dsCustomerData, lstChiSo, lstHDon, lstHDonCTiet, lstHDonCosfi, strTenDNhap, lngCurrentLibID.ToString());
                if (strResult != "") return strResult;
                if (lstHDon == null || lstHDon.Count <= 0) return "Danh sach hoa don khong co";
                if (lstHDonCTiet == null || lstHDonCTiet.Count <= 0) return "Danh sach hoa don chi tiet khong co";
                strResult = SetPropertiesForObjectPlus_GTru(dsInvoiceDataGC, dsCustomerData, lstChiSoGC, lstHDonGC, lstHDonCTietGC, lstHDonCosfiGC, strTenDNhap, lngCurrentLibID.ToString());
                if (strResult != "") return strResult;
                if (lstHDonGC == null || lstHDonGC.Count <= 0) return "Danh sach hoa don khong co";
                if (lstHDonCTietGC == null || lstHDonCTietGC.Count <= 0) return "Danh sach hoa don chi tiet khong co";
                //Viet lai doan code nay de submitchange 1 lan
                var arrMaKHang = lstHDon.Select(c => c.MA_KHANG).Distinct();
                //obj_HDN_HDON_Controller.CMIS2 = db;
                //obj_HDN_HDONCTIET_Controller.CMIS2 = db;
                //obj_HDN_HDONCOSFI_Controller.CMIS2 = db;
                //obj_GCS_CHISO_Controller.CMIS2 = db;
                inpDLieuHDon inpTHD = new inpDLieuHDon();
                inpTHD.LST_CSO = new List<GCS_CHISO_PLUS>();
                inpTHD.LST_HDCF = new List<HDN_HDONCOSFI_PLUS>();
                inpTHD.LST_HDCT = new List<HDN_HDONCTIET_PLUS>();
                inpTHD.LST_HDON = new List<HDN_HDON_PLUS>();
                inpTHD.LST_HDCF_BT = new List<HDN_HDONCOSFI_PLUS>();
                inpTHD.LST_HDCT_BT = new List<HDN_HDONCTIET_PLUS>();
                inpTHD.LST_HDON_BT = new List<HDN_HDON_PLUS>();
                inpTHD.LST_HDN_THOP_GTRU = new List<HDN_THOP_GTRU>();

                //if (config.isHTroThang == true && thang == config.thang_htro && nam == config.nam_htro && ky == config.ky_htro && dsCustomerData.Tables.Contains("HDN_HDON_GTRU") && dsCustomerData.Tables["HDN_HDON_GTRU"].Rows.Count > 0)
                //{
                //    strResult = CreateObject_GTRU(dsCustomerData, lstHDonGTRU, lstHDonCTietGTRU, lstHDonCosfiGTRU, strTenDNhap, lngCurrentLibID.ToString());
                //    if (strResult != "") return strResult;
                //}
                //Tach ra theo tung ma khach hang
                foreach (var x in arrMaKHang)
                {
                    if (x == "PD01000010568")
                    {
                    }
                    string strTemp = "";


                    List<HDN_HDON_PLUS> lstHDonGC_Temp = lstHDonGC.Where(c => c.MA_KHANG == x).ToList();
                    List<HDN_HDON_PLUS> lstHDon_Temp = lstHDon.Where(c => c.MA_KHANG == x).ToList();
                    List<HDN_HDONCTIET_PLUS> lstHDonCTiet_Temp = lstHDonCTiet.Where(c => c.MA_KHANG == x).ToList();
                    List<GCS_CHISO_PLUS> lstChiSo_Temp = lstChiSo.Where(c => c.MA_KHANG == x).ToList();
                    //DateTime dtNgayCKy = DateTime.ParseExact(lstHDon_Temp[0].NGAY_CKY, "dd/MM/yyyy", System.Globalization.CultureInfo.InvariantCulture);
                    DateTime dtNgayCKy = DateTime.ParseExact(strNgayGhi, "dd/MM/yyyy", System.Globalization.CultureInfo.InvariantCulture);

                    //Bổ sung kiểm tra tiền cũ != tiền mới thì mới ghi dữ liệu BT
                    decimal decTienMoi = lstHDon_Temp.Sum(c => c.TIEN_GOC);
                    decimal decTienCu = lstHDonGC_Temp.Sum(c => c.TIEN_GOC);
                    if (config.isHTroThang == false)
                    {
                        #region Hỗ trợ theo khoảng ngày
                        if (lstHDon_Temp[0].LOAI_KHANG == 0)
                        {
                            //SH
                            if (dtNgayCKy.CompareTo(dtHLucSH) >= 0 && dtNgayCKy.CompareTo(dtHetHLucSH) < 0 && decTienMoi != decTienCu)
                            {
                                //Trong khoảng hiệu lực, tính theo đơn giá mới, ghi lại dữ liệu tính BT để hiển thị trên thể hiện HDDT
                                inpTHD.LST_HDON.AddRange(lstHDon_Temp);
                                inpTHD.LST_HDCT.AddRange(lstHDonCTiet_Temp);
                                inpTHD.LST_CSO.AddRange(lstChiSo_Temp);
                                if (lstHDonCosfi != null && lstHDonCosfi.Count > 0)
                                {

                                    List<HDN_HDONCOSFI_PLUS> lstHDonCosfi_Temp = lstHDonCosfi.Where(c => c.MA_KHANG == x).ToList();
                                    if (lstHDonCosfi_Temp != null && lstHDonCosfi_Temp.Count > 0)
                                    {
                                        foreach (HDN_HDONCOSFI_PLUS hcosfi in lstHDonCosfi_Temp)
                                        {
                                            if (lstHDon_Temp != null && lstHDon_Temp.Count > 0)
                                            {
                                                var hdon = lstHDon_Temp.Where(c => c.LOAI_HDON == "VC");
                                                if (hdon != null && hdon.Count() > 0)
                                                {
                                                    hcosfi.ID_HDON = hdon.First().ID_HDON;
                                                }
                                            }
                                        }
                                    }
                                    inpTHD.LST_HDCF.AddRange(lstHDonCosfi_Temp);
                                    //obj_HDN_HDONCOSFI_Controller.lstCosfi = lstHDonCosfi_Temp;
                                    //strTemp = strTemp + obj_HDN_HDONCOSFI_Controller.InsertHDN_HDONCOSFI();
                                    lstHDonCosfi = lstHDonCosfi.Where(c => c.MA_KHANG != x).ToList();
                                }
                                lstHDon = lstHDon.Where(c => c.MA_KHANG != x).ToList();
                                lstHDonCTiet = lstHDonCTiet.Where(c => c.MA_KHANG != x).ToList();
                                lstChiSo = lstChiSo.Where(c => c.MA_KHANG != x).ToList();





                                List<HDN_HDONCTIET_PLUS> lstHDonCTietGC_Temp = lstHDonCTietGC.Where(c => c.MA_KHANG == x).ToList();
                                inpTHD.LST_HDON_BT.AddRange(lstHDonGC_Temp);
                                inpTHD.LST_HDCT_BT.AddRange(lstHDonCTietGC_Temp);
                                if (lstHDonCosfiGC != null && lstHDonCosfiGC.Count > 0)
                                {

                                    List<HDN_HDONCOSFI_PLUS> lstHDonCosfiGC_Temp = lstHDonCosfiGC.Where(c => c.MA_KHANG == x).ToList();
                                    if (lstHDonCosfiGC_Temp != null && lstHDonCosfiGC_Temp.Count > 0)
                                    {
                                        foreach (HDN_HDONCOSFI_PLUS hcosfi in lstHDonCosfiGC_Temp)
                                        {
                                            if (lstHDonGC_Temp != null && lstHDonGC_Temp.Count > 0)
                                            {
                                                var hdon = lstHDonGC_Temp.Where(c => c.LOAI_HDON == "VC");
                                                if (hdon != null && hdon.Count() > 0)
                                                {
                                                    hcosfi.ID_HDON = hdon.First().ID_HDON;
                                                }
                                            }
                                        }
                                    }
                                    inpTHD.LST_HDCF_BT.AddRange(lstHDonCosfiGC_Temp);
                                    //obj_HDN_HDONCOSFI_Controller.lstCosfi = lstHDonCosfi_Temp;
                                    //strTemp = strTemp + obj_HDN_HDONCOSFI_Controller.InsertHDN_HDONCOSFI();
                                    lstHDonCosfiGC = lstHDonCosfiGC.Where(c => c.MA_KHANG != x).ToList();
                                }


                                lstHDonGC = lstHDonGC.Where(c => c.MA_KHANG != x).ToList();
                                lstHDonCTietGC = lstHDonCTietGC.Where(c => c.MA_KHANG != x).ToList();
                            }
                            else
                            {
                                //Ngoài khoảng hiệu lực, tính theo giá cũ
                                inpTHD.LST_CSO.AddRange(lstChiSo_Temp);
                                lstHDon = lstHDon.Where(c => c.MA_KHANG != x).ToList();
                                lstHDonCTiet = lstHDonCTiet.Where(c => c.MA_KHANG != x).ToList();
                                lstChiSo = lstChiSo.Where(c => c.MA_KHANG != x).ToList();
                                //List<HDN_HDON_PLUS> lstHDonGC_Temp = lstHDonGC.Where(c => c.MA_KHANG == x).ToList();
                                List<HDN_HDONCTIET_PLUS> lstHDonCTietGC_Temp = lstHDonCTietGC.Where(c => c.MA_KHANG == x).ToList();
                                inpTHD.LST_HDON.AddRange(lstHDonGC_Temp);
                                inpTHD.LST_HDCT.AddRange(lstHDonCTietGC_Temp);
                                if (lstHDonCosfiGC != null && lstHDonCosfiGC.Count > 0)
                                {

                                    List<HDN_HDONCOSFI_PLUS> lstHDonCosfiGC_Temp = lstHDonCosfiGC.Where(c => c.MA_KHANG == x).ToList();
                                    if (lstHDonCosfiGC_Temp != null && lstHDonCosfiGC_Temp.Count > 0)
                                    {
                                        foreach (HDN_HDONCOSFI_PLUS hcosfi in lstHDonCosfiGC_Temp)
                                        {
                                            if (lstHDonGC_Temp != null && lstHDonGC_Temp.Count > 0)
                                            {
                                                var hdon = lstHDonGC_Temp.Where(c => c.LOAI_HDON == "VC");
                                                if (hdon != null && hdon.Count() > 0)
                                                {
                                                    hcosfi.ID_HDON = hdon.First().ID_HDON;
                                                }
                                            }
                                        }
                                    }
                                    inpTHD.LST_HDCF.AddRange(lstHDonCosfiGC_Temp);
                                    //obj_HDN_HDONCOSFI_Controller.lstCosfi = lstHDonCosfi_Temp;
                                    //strTemp = strTemp + obj_HDN_HDONCOSFI_Controller.InsertHDN_HDONCOSFI();
                                    lstHDonCosfiGC = lstHDonCosfiGC.Where(c => c.MA_KHANG != x).ToList();
                                }
                                lstHDonGC = lstHDonGC.Where(c => c.MA_KHANG != x).ToList();
                                lstHDonCTietGC = lstHDonCTietGC.Where(c => c.MA_KHANG != x).ToList();
                            }
                        }
                        else
                        {

                            //NSH
                            if (dtNgayCKy.CompareTo(dtHLucNSH) >= 0 && dtNgayCKy.CompareTo(dtHetHLucNSH) < 0 && decTienMoi != decTienCu)
                            {
                                //Trong khoảng hiệu lực, tính theo đơn giá mới, ghi lại dữ liệu tính BT để hiển thị trên thể hiện HDDT
                                inpTHD.LST_HDON.AddRange(lstHDon_Temp);
                                inpTHD.LST_HDCT.AddRange(lstHDonCTiet_Temp);
                                inpTHD.LST_CSO.AddRange(lstChiSo_Temp);
                                if (lstHDonCosfi != null && lstHDonCosfi.Count > 0)
                                {

                                    List<HDN_HDONCOSFI_PLUS> lstHDonCosfi_Temp = lstHDonCosfi.Where(c => c.MA_KHANG == x).ToList();
                                    if (lstHDonCosfi_Temp != null && lstHDonCosfi_Temp.Count > 0)
                                    {
                                        foreach (HDN_HDONCOSFI_PLUS hcosfi in lstHDonCosfi_Temp)
                                        {
                                            if (lstHDon_Temp != null && lstHDon_Temp.Count > 0)
                                            {
                                                var hdon = lstHDon_Temp.Where(c => c.LOAI_HDON == "VC");
                                                if (hdon != null && hdon.Count() > 0)
                                                {
                                                    hcosfi.ID_HDON = hdon.First().ID_HDON;
                                                }
                                            }
                                        }
                                    }
                                    inpTHD.LST_HDCF.AddRange(lstHDonCosfi_Temp);
                                    //obj_HDN_HDONCOSFI_Controller.lstCosfi = lstHDonCosfi_Temp;
                                    //strTemp = strTemp + obj_HDN_HDONCOSFI_Controller.InsertHDN_HDONCOSFI();
                                    lstHDonCosfi = lstHDonCosfi.Where(c => c.MA_KHANG != x).ToList();
                                }
                                lstHDon = lstHDon.Where(c => c.MA_KHANG != x).ToList();
                                lstHDonCTiet = lstHDonCTiet.Where(c => c.MA_KHANG != x).ToList();
                                lstChiSo = lstChiSo.Where(c => c.MA_KHANG != x).ToList();
                                //List<HDN_HDON_PLUS> lstHDonGC_Temp = lstHDonGC.Where(c => c.MA_KHANG == x).ToList();
                                List<HDN_HDONCTIET_PLUS> lstHDonCTietGC_Temp = lstHDonCTietGC.Where(c => c.MA_KHANG == x).ToList();
                                inpTHD.LST_HDON_BT.AddRange(lstHDonGC_Temp);
                                inpTHD.LST_HDCT_BT.AddRange(lstHDonCTietGC_Temp);
                                if (lstHDonCosfiGC != null && lstHDonCosfiGC.Count > 0)
                                {

                                    List<HDN_HDONCOSFI_PLUS> lstHDonCosfiGC_Temp = lstHDonCosfiGC.Where(c => c.MA_KHANG == x).ToList();
                                    if (lstHDonCosfiGC_Temp != null && lstHDonCosfiGC_Temp.Count > 0)
                                    {
                                        foreach (HDN_HDONCOSFI_PLUS hcosfi in lstHDonCosfiGC_Temp)
                                        {
                                            if (lstHDonGC_Temp != null && lstHDonGC_Temp.Count > 0)
                                            {
                                                var hdon = lstHDonGC_Temp.Where(c => c.LOAI_HDON == "VC");
                                                if (hdon != null && hdon.Count() > 0)
                                                {
                                                    hcosfi.ID_HDON = hdon.First().ID_HDON;
                                                }
                                            }
                                        }
                                    }
                                    inpTHD.LST_HDCF_BT.AddRange(lstHDonCosfiGC_Temp);
                                    //obj_HDN_HDONCOSFI_Controller.lstCosfi = lstHDonCosfi_Temp;
                                    //strTemp = strTemp + obj_HDN_HDONCOSFI_Controller.InsertHDN_HDONCOSFI();
                                    lstHDonCosfiGC = lstHDonCosfiGC.Where(c => c.MA_KHANG != x).ToList();
                                }


                                lstHDonGC = lstHDonGC.Where(c => c.MA_KHANG != x).ToList();
                                lstHDonCTietGC = lstHDonCTietGC.Where(c => c.MA_KHANG != x).ToList();
                            }
                            else
                            {
                                //Ngoài khoảng hiệu lực, tính theo giá cũ
                                inpTHD.LST_CSO.AddRange(lstChiSo_Temp);
                                lstHDon = lstHDon.Where(c => c.MA_KHANG != x).ToList();
                                lstHDonCTiet = lstHDonCTiet.Where(c => c.MA_KHANG != x).ToList();
                                lstChiSo = lstChiSo.Where(c => c.MA_KHANG != x).ToList();
                                //List<HDN_HDON_PLUS> lstHDonGC_Temp = lstHDonGC.Where(c => c.MA_KHANG == x).ToList();
                                List<HDN_HDONCTIET_PLUS> lstHDonCTietGC_Temp = lstHDonCTietGC.Where(c => c.MA_KHANG == x).ToList();
                                inpTHD.LST_HDON.AddRange(lstHDonGC_Temp);
                                inpTHD.LST_HDCT.AddRange(lstHDonCTietGC_Temp);
                                if (lstHDonCosfiGC != null && lstHDonCosfiGC.Count > 0)
                                {

                                    List<HDN_HDONCOSFI_PLUS> lstHDonCosfiGC_Temp = lstHDonCosfiGC.Where(c => c.MA_KHANG == x).ToList();
                                    if (lstHDonCosfiGC_Temp != null && lstHDonCosfiGC_Temp.Count > 0)
                                    {
                                        foreach (HDN_HDONCOSFI_PLUS hcosfi in lstHDonCosfiGC_Temp)
                                        {
                                            if (lstHDonGC_Temp != null && lstHDonGC_Temp.Count > 0)
                                            {
                                                var hdon = lstHDonGC_Temp.Where(c => c.LOAI_HDON == "VC");
                                                if (hdon != null && hdon.Count() > 0)
                                                {
                                                    hcosfi.ID_HDON = hdon.First().ID_HDON;
                                                }
                                            }
                                        }
                                    }
                                    inpTHD.LST_HDCF.AddRange(lstHDonCosfiGC_Temp);
                                    //obj_HDN_HDONCOSFI_Controller.lstCosfi = lstHDonCosfi_Temp;
                                    //strTemp = strTemp + obj_HDN_HDONCOSFI_Controller.InsertHDN_HDONCOSFI();
                                    lstHDonCosfiGC = lstHDonCosfiGC.Where(c => c.MA_KHANG != x).ToList();
                                }
                                lstHDonGC = lstHDonGC.Where(c => c.MA_KHANG != x).ToList();
                                lstHDonCTietGC = lstHDonCTietGC.Where(c => c.MA_KHANG != x).ToList();
                            }
                        }
                        #endregion
                    }
                    else
                    {
                        #region Hỗ trợ theo tháng   
                        if (config.lstTNamHTro.Contains(thang + "/" + nam))
                        {
                            inpTHD.LST_HDON.AddRange(lstHDon_Temp);
                            inpTHD.LST_HDCT.AddRange(lstHDonCTiet_Temp);
                            inpTHD.LST_CSO.AddRange(lstChiSo_Temp);
                            if (lstHDonCosfi != null && lstHDonCosfi.Count > 0)
                            {
                                List<HDN_HDONCOSFI_PLUS> lstHDonCosfi_Temp = lstHDonCosfi.Where(c => c.MA_KHANG == x).ToList();
                                if (lstHDonCosfi_Temp != null && lstHDonCosfi_Temp.Count > 0)
                                {
                                    foreach (HDN_HDONCOSFI_PLUS hcosfi in lstHDonCosfi_Temp)
                                    {
                                        if (lstHDon_Temp != null && lstHDon_Temp.Count > 0)
                                        {
                                            var hdon = lstHDon_Temp.Where(c => c.LOAI_HDON == "VC");
                                            if (hdon != null && hdon.Count() > 0)
                                            {
                                                hcosfi.ID_HDON = hdon.First().ID_HDON;
                                            }
                                        }
                                    }
                                }
                                inpTHD.LST_HDCF.AddRange(lstHDonCosfi_Temp);
                                //obj_HDN_HDONCOSFI_Controller.lstCosfi = lstHDonCosfi_Temp;
                                //strTemp = strTemp + obj_HDN_HDONCOSFI_Controller.InsertHDN_HDONCOSFI();
                                lstHDonCosfi = lstHDonCosfi.Where(c => c.MA_KHANG != x).ToList();
                            }
                            lstHDon = lstHDon.Where(c => c.MA_KHANG != x).ToList();
                            lstHDonCTiet = lstHDonCTiet.Where(c => c.MA_KHANG != x).ToList();
                            lstChiSo = lstChiSo.Where(c => c.MA_KHANG != x).ToList();

                            //Bổ sung kiểm tra nếu Kh thuộc bảng hdg_DDO_GTRU thì mới đẩy bảng BT
                            bool isBT = false;
                            if (dsCustomerData.Tables.Contains("HDG_DDO_GTRU") && dsCustomerData.Tables["HDG_DDO_GTRU"].Rows.Count > 0)
                            {
                                foreach (DataRow drGTru in dsCustomerData.Tables["HDG_DDO_GTRU"].Rows)
                                {
                                    if (drGTru["MA_KHANG"].ToString().Trim().Equals(x) && (drGTru["MANHOM_KHANG"].ToString().Trim().Equals("LTDL") || drGTru["MANHOM_KHANG"].ToString().Trim().Equals("COVID")))
                                    {
                                        isBT = true;
                                        break;
                                    }
                                }
                            }
                            #region Bổ sung kiểm tra đung tháng hỗ trợ SH và KH có thành phần giá SHBT sẽ vẫn đẩy vào bảng BT
                            if (isBT == false && config.lstTNamHTroSH.Contains(thang + "/" + nam))
                            {
                                bool isGTruSHoat = false, isGiaSHBT = false;
                                if (config.lstSoHTro.Count > 0)
                                    for (int i = 0; i < config.lstSoHTro.Count; i++)
                                    {
                                        //string strMaSoGCS = drCT["MA_SOGCS"].ToString().Trim();
                                        if (strMa_SoGCS.Equals(config.lstSoHTro[i]))
                                        {
                                            isGTruSHoat = true;
                                            break;
                                        }
                                    }
                                if (!isGTruSHoat && config.lstDViHTro.Count > 0)
                                    for (int i = 0; i < config.lstDViHTro.Count; i++)
                                    {
                                        string strMaDVTemp = lstHDonCTiet_Temp.First().MA_DVIQLY;
                                        if (strMaDVTemp.StartsWith(config.lstDViHTro[i]))
                                        {
                                            isGTruSHoat = true;
                                            break;
                                        }
                                    }
                                isGiaSHBT = lstHDonCTiet_Temp.Exists(c => c.MA_NHOMNN == "SHBT");
                                isBT = isGTruSHoat && isGiaSHBT;
                            }
                            #endregion

                            if (isBT)
                            {
                                List<HDN_HDONCTIET_PLUS> lstHDonCTietGC_Temp = lstHDonCTietGC.Where(c => c.MA_KHANG == x).ToList();
                                inpTHD.LST_HDON_BT.AddRange(lstHDonGC_Temp);
                                inpTHD.LST_HDCT_BT.AddRange(lstHDonCTietGC_Temp);
                                if (lstHDonCosfiGC != null && lstHDonCosfiGC.Count > 0)
                                {

                                    List<HDN_HDONCOSFI_PLUS> lstHDonCosfiGC_Temp = lstHDonCosfiGC.Where(c => c.MA_KHANG == x).ToList();
                                    if (lstHDonCosfiGC_Temp != null && lstHDonCosfiGC_Temp.Count > 0)
                                    {
                                        foreach (HDN_HDONCOSFI_PLUS hcosfi in lstHDonCosfiGC_Temp)
                                        {
                                            if (lstHDonGC_Temp != null && lstHDonGC_Temp.Count > 0)
                                            {
                                                var hdon = lstHDonGC_Temp.Where(c => c.LOAI_HDON == "VC");
                                                if (hdon != null && hdon.Count() > 0)
                                                {
                                                    hcosfi.ID_HDON = hdon.First().ID_HDON;
                                                }
                                            }
                                        }
                                    }
                                    inpTHD.LST_HDCF_BT.AddRange(lstHDonCosfiGC_Temp);
                                    //obj_HDN_HDONCOSFI_Controller.lstCosfi = lstHDonCosfi_Temp;
                                    //strTemp = strTemp + obj_HDN_HDONCOSFI_Controller.InsertHDN_HDONCOSFI();
                                    lstHDonCosfiGC = lstHDonCosfiGC.Where(c => c.MA_KHANG != x).ToList();
                                }
                            }



                            lstHDonGC = lstHDonGC.Where(c => c.MA_KHANG != x).ToList();
                            lstHDonCTietGC = lstHDonCTietGC.Where(c => c.MA_KHANG != x).ToList();
                        }
                        else
                        {
                            inpTHD.LST_CSO.AddRange(lstChiSo_Temp);
                            lstHDon = lstHDon.Where(c => c.MA_KHANG != x).ToList();
                            lstHDonCTiet = lstHDonCTiet.Where(c => c.MA_KHANG != x).ToList();
                            lstChiSo = lstChiSo.Where(c => c.MA_KHANG != x).ToList();
                            //List<HDN_HDON_PLUS> lstHDonGC_Temp = lstHDonGC.Where(c => c.MA_KHANG == x).ToList();
                            List<HDN_HDONCTIET_PLUS> lstHDonCTietGC_Temp = lstHDonCTietGC.Where(c => c.MA_KHANG == x).ToList();
                            inpTHD.LST_HDON.AddRange(lstHDonGC_Temp);
                            inpTHD.LST_HDCT.AddRange(lstHDonCTietGC_Temp);
                            if (lstHDonCosfiGC != null && lstHDonCosfiGC.Count > 0)
                            {

                                List<HDN_HDONCOSFI_PLUS> lstHDonCosfiGC_Temp = lstHDonCosfiGC.Where(c => c.MA_KHANG == x).ToList();
                                if (lstHDonCosfiGC_Temp != null && lstHDonCosfiGC_Temp.Count > 0)
                                {
                                    foreach (HDN_HDONCOSFI_PLUS hcosfi in lstHDonCosfiGC_Temp)
                                    {
                                        if (lstHDonGC_Temp != null && lstHDonGC_Temp.Count > 0)
                                        {
                                            var hdon = lstHDonGC_Temp.Where(c => c.LOAI_HDON == "VC");
                                            if (hdon != null && hdon.Count() > 0)
                                            {
                                                hcosfi.ID_HDON = hdon.First().ID_HDON;
                                            }
                                        }
                                    }
                                }
                                inpTHD.LST_HDCF.AddRange(lstHDonCosfiGC_Temp);
                                //obj_HDN_HDONCOSFI_Controller.lstCosfi = lstHDonCosfi_Temp;
                                //strTemp = strTemp + obj_HDN_HDONCOSFI_Controller.InsertHDN_HDONCOSFI();
                                lstHDonCosfiGC = lstHDonCosfiGC.Where(c => c.MA_KHANG != x).ToList();
                            }
                            lstHDonGC = lstHDonGC.Where(c => c.MA_KHANG != x).ToList();
                            lstHDonCTietGC = lstHDonCTietGC.Where(c => c.MA_KHANG != x).ToList();
                        }

                        #endregion

                    }


                }
                if (lstChiSo != null && lstChiSo.Count > 0)
                {
                    //obj_GCS_CHISO_Controller.LstInfo = lstChiSo;
                    //obj_GCS_CHISO_Controller.UpdateList();
                    inpTHD.LST_CSO.AddRange(lstChiSo);
                }
                //Insert luong so ghi chi so
                string strInput = JsonConvert.SerializeObject(inpTHD);
                //Gọi hàm insert
                string strIP = ConfigurationManager.AppSettings["URI"];
                var baseAddress = "http://" + strIP + "/ServiceHDonPSinh-HDonPSinh-context-root/resources/serviceHDonPSinh/insertTHD";
                var http = (HttpWebRequest)WebRequest.Create(new Uri(baseAddress));
                http.Accept = "application/json";
                http.ContentType = "application/json";
                http.Method = "POST";

                //string parsedContent = "{'MA_DVIQLY':'PC06AA','TEN_DANH_MUC':'D_SOGCS'}";
                string parsedContent = strInput;
                //ASCIIEncoding encoding = new ASCIIEncoding();
                Byte[] bytes = System.Text.Encoding.UTF8.GetBytes(parsedContent);

                Stream newStream = http.GetRequestStream();
                newStream.Write(bytes, 0, bytes.Length);
                newStream.Close();

                var response = http.GetResponse();

                var stream = response.GetResponseStream();
                var sr = new StreamReader(stream);
                var content = sr.ReadToEnd();
                ReturnObject obj = JsonConvert.DeserializeObject<ReturnObject>(content);
                if (obj.TYPE == "ERROR") return obj.MESSAGE;
                else return "";

            }
            catch (Exception ex)
            {
                return "Lỗi khi Insert dữ liệu hóa đơn với mã sổ= " + strMa_SoGCS + ": " + ex.Message;
            }
            finally
            {
                //db.ReleaseConnection();
            }
        }
        protected string SetPropertiesForObjectPlus_GTru(DataSet dsInvoiceData, DataSet dsCustomerData, List<GCS_CHISO_PLUS> lstChiSo, List<HDN_HDON_PLUS> lstHDon, List<HDN_HDONCTIET_PLUS> lstHDonCTiet, List<HDN_HDONCOSFI_PLUS> lstHDonCosfi, string strTenDNhap, string strMaCNang)
        {
            //String strddo = "";
            //HDN_HDON
            if (dsInvoiceData == null) return "NoDataFound!- Không tìm thấy dữ liệu!";
            if (dsInvoiceData.Tables["HDN_HDON"] == null)
            {
                return "NoInvoiceDataFound!- Không tìm thấy dữ liệu hóa đơn!";
            }
            if (dsInvoiceData.Tables["HDN_HDONCTIET"] == null)
            {
                return "NoInvoiceDataFound!- Không tìm thấy dữ liệu hóa đơn chi tiết!";
            }
            //if (dsInvoiceData.Tables["HDN_HDON"] == null || dsInvoiceData.Tables["HDN_HDON"].Rows.Count == 0) return "NoInvoiceDataFound!- Không tìm thấy dữ liệu hóa đơn!";
            //if (dsInvoiceData.Tables["HDN_HDONCTIET"] == null || dsInvoiceData.Tables["HDN_HDONCTIET"].Rows.Count == 0) return "NoInvoiceDataFound!- Không tìm thấy dữ liệu hóa đơn chi tiết!";
            //lstHDon = new List<HDN_HDON_PLUS>();
            //lstHDonCosfi = new List<HDN_HDONCOSFI_PLUS>();
            //lstHDonCTiet = new List<HDN_HDONCTIET_PLUS>();
            //List<GCS_CHISO_PLUS> lstChiSo = new List<GCS_CHISO_PLUS>();
            try
            {
                long lngIDHDon = 0;
                DataTable dtTemp = dsInvoiceData.Tables["HDN_HDONCTIET"].Copy();
                DataTable dtCosfi = dsInvoiceData.Tables["HDN_HDONCOSFI"].Copy();
                //Tránh trùng id với bên phát sinh ban đầu
                int id_hdon_sequence = 30000;
                int id_hdct_sequence = 30000;
                int id_hdcf_sequence = 30000;
                String strTemp = JsonConvert.SerializeObject(dsInvoiceData.Tables["HDN_HDON"]);
                strTemp = JsonConvert.SerializeObject(dsInvoiceData.Tables["HDN_HDONCTIET"]);
                strTemp = JsonConvert.SerializeObject(dsInvoiceData.Tables["HDN_HDONCOSFI"]);
                foreach (DataRow row in dsInvoiceData.Tables["HDN_HDON"].Rows)
                {
                    #region HDN_HDON
                    HDN_HDON_PLUS info = new HDN_HDON_PLUS();

                    if (row["CHI_TIET"].ToString().Trim().Length > 0)
                        info.CHI_TIET = Convert.ToInt16(row["CHI_TIET"].ToString());
                    if (row["COSFI"].ToString().Trim().Length > 0)
                        info.COSFI = Convert.ToDecimal(row["COSFI"].ToString());
                    info.DCHI_KHANG = row["DCHI_KHANG"].ToString();
                    info.DCHI_KHANGTT = row["DCHI_KHANGTT"].ToString();
                    if (row["DIEN_TTHU"].ToString().Trim().Length > 0)
                        info.DIEN_TTHU = Convert.ToDecimal(row["DIEN_TTHU"].ToString());
                    else
                        return "Lỗi dữ liệu HDN_HDON: DIEN_TTHU = null";
                    if (row["ID_BBANPHANH"].ToString().Trim().Length > 0)
                        info.ID_BBANPHANH = Convert.ToInt64(row["ID_BBANPHANH"].ToString());
                    //if (row["ID_HDON"].ToString().Trim().Length > 0 || row["ID_HDON"].ToString() != "0")
                    //    info.ID_HDON = Convert.ToInt64(row["ID_HDON"].ToString());
                    //else
                    //obj_HDN_HDON_Controller.CMIS2 = db;
                    info.ID_HDON = id_hdon_sequence++;
                    lngIDHDon = info.ID_HDON;
                    if (row["KCOSFI"].ToString().Trim().Length > 0)
                        info.KCOSFI = Convert.ToDecimal(row["KCOSFI"].ToString());
                    info.KIHIEU_SERY = row["KIHIEU_SERY"].ToString();
                    if (row["KY"].ToString().Trim().Length > 0)
                        info.KY = Convert.ToInt16(row["KY"].ToString());
                    else
                        return "Lỗi dữ liệu HDN_HDON: KY = null";
                    if (row["LOAI_KHANG"].ToString().Trim().Length > 0)
                        info.LOAI_KHANG = Convert.ToInt16(row["LOAI_KHANG"]);
                    else
                        return "Lỗi dữ liệu HDN_HDON: LOAI_KHANG = null";
                    info.DCHI_TTOAN = row["DCHI_TTOAN"].ToString();
                    info.MANHOM_KHANG = row["MANHOM_KHANG"].ToString();
                    info.MA_LOAIDN = row["MA_LOAIDN"].ToString();
                    info.MA_PTTT = row["MA_PTTT"].ToString();
                    info.LOAI_HDON = row["LOAI_HDON"].ToString();
                    info.MA_CNANG = strMaCNang;
                    info.MA_DVIQLY = row["MA_DVIQLY"].ToString();
                    info.MA_HTTT = row["MA_HTTT"].ToString();
                    info.MA_KHANG = row["MA_KHANG"].ToString();
                    info.MA_KHANGTT = row["MA_KHANGTT"].ToString();
                    info.MA_KVUC = row["MA_KVUC"].ToString();
                    info.MA_NHANG = row["MA_NHANG"].ToString();
                    info.MA_NVIN = row["MA_NVIN"].ToString();
                    info.MA_NVPHANH = row["MA_NVPHANH"].ToString();
                    info.MA_SOGCS = row["MA_SOGCS"].ToString();
                    info.MA_TO = row["MA_TO"].ToString();
                    info.MASO_THUE = row["MASO_THUE"].ToString();
                    if (row["NAM"].ToString().Trim().Length > 0)
                        info.NAM = Convert.ToInt16(row["NAM"].ToString());
                    else
                        return "Lỗi dữ liệu HDN_HDON: NAM = null";
                    if (row["NGAY_CKY"].ToString().Trim().Length > 0)
                        info.NGAY_CKY = Convert.ToDateTime(row["NGAY_CKY"].ToString()).ToString("dd/MM/yyyy");
                    else
                        return "Lỗi dữ liệu HDN_HDON: NGAY_CKY = null";
                    if (row["NGAY_DKY"].ToString().Trim().Length > 0)
                        info.NGAY_DKY = Convert.ToDateTime(row["NGAY_DKY"].ToString()).ToString("dd/MM/yyyy");
                    else
                        return "Lỗi dữ liệu HDN_HDON: NGAY_DKY = null";
                    if (row["NGAY_IN"].ToString().Trim().Length > 0)
                        info.NGAY_IN = row["NGAY_IN"].ToString();
                    if (row["NGAY_PHANH"].ToString().Trim().Length > 0)
                        info.NGAY_PHANH = row["NGAY_PHANH"].ToString();
                    if (row["NGAY_SUA"].ToString().Trim().Length > 0)
                        info.NGAY_SUA = Convert.ToDateTime(row["NGAY_SUA"].ToString());
                    else
                        return "Lỗi dữ liệu HDN_HDON: NGAY_SUA = null";
                    if (row["NGAY_TAO"].ToString().Trim().Length > 0)
                        info.NGAY_TAO = Convert.ToDateTime(row["NGAY_TAO"].ToString());
                    else
                        return "Lỗi dữ liệu HDN_HDON: NGAY_TAO = null";
                    info.NGUOI_SUA = strTenDNhap;
                    info.NGUOI_TAO = strTenDNhap;
                    info.SO_CTO = row["SO_CTO"].ToString();
                    if (row["SO_HO"].ToString().Trim().Length > 0)
                        info.SO_HO = Convert.ToDecimal(row["SO_HO"].ToString());
                    else
                        return "Lỗi dữ liệu HDN_HDON: SO_HO = null";
                    if (row["SO_LANIN"].ToString().Trim().Length > 0)
                        info.SO_LANIN = Convert.ToInt16(row["SO_LANIN"].ToString());
                    info.SO_SERY = row["SO_SERY"].ToString();
                    if (row["SO_TIEN"].ToString().Trim().Length > 0)
                        info.SO_TIEN = Convert.ToDecimal(row["SO_TIEN"].ToString());
                    else
                        return "Lỗi dữ liệu HDN_HDON: SO_TIEN = null";
                    info.STT = row["STT"].ToString();
                    if (row["STT_IN"].ToString().Trim().Length > 0)
                        info.STT_IN = Convert.ToInt32(row["STT_IN"].ToString());
                    info.TEN_KHANG = row["TEN_KHANG"].ToString();
                    info.TEN_KHANGTT = row["TEN_KHANGTT"].ToString();
                    if (row["THANG"].ToString().Trim().Length > 0)
                        info.THANG = Convert.ToInt16(row["THANG"].ToString());
                    else
                        return "Lỗi dữ liệu HDN_HDON: THANG = null";
                    if (row["TIEN_GTGT"].ToString().Trim().Length > 0)
                        info.TIEN_GTGT = Convert.ToDecimal(row["TIEN_GTGT"].ToString());
                    else
                        return "Lỗi dữ liệu HDN_HDON: TIEN_GTGT = null";
                    info.TKHOAN_KHANG = row["TKHOAN_KHANG"].ToString();
                    if (row["TONG_TIEN"].ToString().Trim().Length > 0)
                        info.TONG_TIEN = Convert.ToDecimal(row["TONG_TIEN"].ToString());
                    else
                        return "Lỗi dữ liệu HDN_HDON: TONG_TIEN = null";
                    if (row["TYLE_THUE"].ToString().Trim().Length > 0)
                        info.TYLE_THUE = Convert.ToDecimal(row["TYLE_THUE"].ToString());
                    else
                        return "Lỗi dữ liệu HDN_HDON: TYLE_THUE = null";
                    //long lngIDHDong = Convert.ToInt64(dsCustomerData.Tables["HDG_KHACH_HANG"].Select("MA_KHANG='" + info.MA_KHANG + "'")[0]["ID_HDONG"]);
                    //string strDChiTToan = (new cls_HDG_HOP_DONG_Controller()).getDCHI_TTOAN(info.MA_DVIQLY, lngIDHDong, info.NGAY_CKY.Date);
                    //if (strDChiTToan != null && strDChiTToan.Trim().Length > 0)
                    //    info.DCHI_TTOAN = strDChiTToan;
                    //else
                    //    return "Không tìm thấy hợp đồng tương ứng với mã khách hàng " + info.MA_KHANG;
                    //if (info.DCHI_TTOAN == null || info.DCHI_TTOAN.Trim().Length == 0)
                    //    return "Không tìm thấy hợp đồng tương ứng với mã khách hàng " + info.MA_KHANG;
                    info.TIEN_TD = Utility.DecimalDbnull(row["TIEN_TD"]);
                    info.THUE_TD = Utility.DecimalDbnull(row["THUE_TD"]);
                    info.TIEN_VC = Utility.DecimalDbnull(row["TIEN_VC"]);
                    info.THUE_VC = Utility.DecimalDbnull(row["THUE_VC"]);
                    info.DTHOAI = row["DTHOAI"].ToString();


                    info.TIEN_GTRU = Utility.DecimalDbnull(row["TIEN_GTRU"]);

                    if (row["TIEN_GOC"].ToString().Trim().Length > 0)
                        info.TIEN_GOC = Convert.ToDecimal(row["TIEN_GOC"].ToString());
                    else
                        info.TIEN_GOC = Convert.ToDecimal(row["SO_TIEN"].ToString());
                    info.TIEN_TD_GTRU = Utility.DecimalDbnull(row["TIEN_TD_GTRU"]);

                    if (row["TIEN_TD_GOC"].ToString().Trim().Length > 0)
                        info.TIEN_TD_GOC = Convert.ToDecimal(row["TIEN_TD_GOC"].ToString());
                    else
                        info.TIEN_TD_GOC = Utility.DecimalDbnull(row["TIEN_TD"]);

                    info.TIEN_VC_GTRU = Utility.DecimalDbnull(row["TIEN_VC_GTRU"]);

                    if (row["TIEN_VC_GOC"].ToString().Trim().Length > 0)
                        info.TIEN_VC_GOC = Convert.ToDecimal(row["TIEN_VC_GOC"].ToString());
                    else
                        info.TIEN_VC_GOC = Utility.DecimalDbnull(row["TIEN_VC"]);
                    lstHDon.Add(info);

                    #endregion

                    #region HDN_HDONCTIET
                    if (info.MA_KHANG == "PD01000010568")
                    {
                    }
                    DataRow[] arrHDonCTiet = null;
                    if (dtTemp != null && dtTemp.Rows.Count != 0)
                    {
                        List<DataRow> arrTemp = new List<DataRow>();
                        if (info.LOAI_HDON == "TD")
                        {
                            foreach (DataRow drTemp in dtTemp.Rows)
                            {
                                if (drTemp["MA_KHANG"].ToString().Trim() == info.MA_KHANG && "BT;CD;TD;KT".Contains(drTemp["BCS"].ToString().Trim()))
                                {
                                    arrTemp.Add(drTemp);
                                }
                            }
                            //arrHDonCTiet = dtTemp.Select("MA_KHANG='" + info.MA_KHANG + "' AND BCS IN ('BT','CD','TD','KT')");
                        }
                        if (info.LOAI_HDON == "VC")
                        {
                            foreach (DataRow drTemp in dtTemp.Rows)
                            {
                                if (drTemp["MA_KHANG"].ToString().Trim() == info.MA_KHANG && drTemp["BCS"].ToString().Trim() == "VC")
                                {
                                    arrTemp.Add(drTemp);
                                }
                            }
                            //arrHDonCTiet = dtTemp.Select("MA_KHANG='" + info.MA_KHANG + "' AND BCS ='VC'");
                        }

                        if (info.LOAI_HDON == "TC")
                        {
                            foreach (DataRow drTemp in dtTemp.Rows)
                            {
                                if (drTemp["MA_KHANG"].ToString().Trim() == info.MA_KHANG && "BT;CD;TD;KT;VC".Contains(drTemp["BCS"].ToString().Trim()))
                                {
                                    arrTemp.Add(drTemp);
                                }
                            }
                            //arrHDonCTiet = dtTemp.Select("MA_KHANG='" + info.MA_KHANG + "' AND BCS IN ('BT','CD','TD','KT','VC')");
                        }
                        arrHDonCTiet = arrTemp.ToArray();
                        var chitiet = dtTemp.AsEnumerable().Except(arrHDonCTiet);
                        dtTemp = (chitiet != null && chitiet.Count() > 0) ? chitiet.CopyToDataTable() : null;
                    }
                    if (arrHDonCTiet != null && arrHDonCTiet.Length > 0)
                    {
                        //obj_HDN_HDONCTIET_Controller.CMIS2 = db;
                        foreach (DataRow dr in arrHDonCTiet)
                        {
                            HDN_HDONCTIET_PLUS infoHDCT = new HDN_HDONCTIET_PLUS();
                            infoHDCT.BCS = dr["BCS"].ToString();
                            if (dr["DIEN_TTHU"].ToString().Trim().Length > 0)
                                infoHDCT.DIEN_TTHU = Convert.ToDecimal(dr["DIEN_TTHU"]);
                            else
                                return "Lỗi dữ liệu HDN_HDONCTIET: DIEN_TTHU = null";
                            infoHDCT.DINH_MUC = dr["DINH_MUC"].ToString();
                            if (dr["DON_GIA"].ToString().Trim().Length > 0)
                                infoHDCT.DON_GIA = Convert.ToDecimal(dr["DON_GIA"]);
                            if (dr["ID_CHISO"].ToString().Trim().Length > 0)
                                infoHDCT.ID_CHISO = Convert.ToInt64(dr["ID_CHISO"]);
                            else
                                return "Lỗi dữ liệu HDN_HDONCTIET: ID_CHISO = null";
                            infoHDCT.ID_HDON = info.ID_HDON;
                            //if (dr["ID_HDONCTIET"].ToString().Trim().Length > 0)
                            //    infoHDCT.ID_HDONCTIET = Convert.ToInt64(dr["ID_HDONCTIET"]);
                            //else
                            //    return "Lỗi dữ liệu HDN_HDONCTIET: ID_HDONCTIET = null";
                            infoHDCT.ID_HDONCTIET = id_hdct_sequence++;//obj_HDN_HDONCTIET_Controller.getID_HDON();
                            if (dr["KY"].ToString().Trim().Length > 0)
                                infoHDCT.KY = Convert.ToInt16(dr["KY"]);
                            else
                                return "Lỗi dữ liệu HDN_HDONCTIET: KY = null";
                            if (dr["LOAI_KHANG"].ToString().Trim().Length > 0)
                                infoHDCT.LOAI_KHANG = Convert.ToInt16(dr["LOAI_KHANG"].ToString());
                            if (dr["LOAI_DDO"].ToString().Trim().Length > 0)
                                infoHDCT.LOAI_DDO = Convert.ToInt16(dr["LOAI_DDO"].ToString());
                            if (dr["SO_PHA"].ToString().Trim().Length > 0)
                                infoHDCT.SO_PHA = Convert.ToInt16(dr["SO_PHA"].ToString());
                            infoHDCT.LOAI_DMUC = dr["LOAI_DMUC"].ToString();
                            infoHDCT.MA_CAPDA = dr["MA_CAPDA"].ToString();
                            infoHDCT.MA_CNANG = strMaCNang;
                            infoHDCT.MA_DDO = dr["MA_DDO"].ToString();
                            infoHDCT.MA_DVIQLY = dr["MA_DVIQLY"].ToString();
                            infoHDCT.MA_KHANG = dr["MA_KHANG"].ToString();
                            infoHDCT.MA_KVUC = dr["MA_KVUC"].ToString();
                            infoHDCT.MA_LO = dr["MA_LO"].ToString();
                            infoHDCT.MA_NGIA = dr["MA_NGIA"].ToString();
                            infoHDCT.MA_NHOMNN = dr["MA_NHOMNN"].ToString();
                            infoHDCT.MA_NN = dr["MA_NN"].ToString();
                            infoHDCT.MA_SOGCS = dr["MA_SOGCS"].ToString();
                            infoHDCT.MA_TO = dr["MA_TO"].ToString();
                            infoHDCT.MA_TRAM = dr["MA_TRAM"].ToString();
                            if (dr["NAM"].ToString().Trim().Length > 0)
                                infoHDCT.NAM = Convert.ToInt16(dr["NAM"]);
                            else
                                return "Lỗi dữ liệu HDN_HDONCTIET: NAM = null";
                            if (dr["NGAY_APDUNG"].ToString().Trim().Length > 0)
                                infoHDCT.NGAY_APDUNG = Convert.ToDateTime(dr["NGAY_APDUNG"]).ToString("dd/MM/yyyy");
                            if (dr["NGAY_SUA"].ToString().Trim().Length > 0)
                                infoHDCT.NGAY_SUA = Convert.ToDateTime(dr["NGAY_SUA"]);
                            else
                                return "Lỗi dữ liệu HDN_HDONCTIET: NGAY_SUA = null";
                            if (dr["NGAY_TAO"].ToString().Trim().Length > 0)
                                infoHDCT.NGAY_TAO = Convert.ToDateTime(dr["NGAY_TAO"]);
                            else
                                return "Lỗi dữ liệu HDN_HDONCTIET: NGAY_TAO = null";
                            infoHDCT.NGUOI_SUA = strTenDNhap;
                            infoHDCT.NGUOI_TAO = strTenDNhap;
                            infoHDCT.SO_CTO = dr["SO_CTO"].ToString();
                            if (dr["SO_TIEN"].ToString().Trim().Length > 0)
                                infoHDCT.SO_TIEN = Convert.ToDecimal(dr["SO_TIEN"]);
                            else
                                return "Lỗi dữ liệu HDN_HDONCTIET: SO_TIEN = null";
                            infoHDCT.SO_TIEN = Convert.ToDecimal(dr["SO_TIEN"]);
                            infoHDCT.STT = dr["STT"].ToString();
                            infoHDCT.TGIAN_BDIEN = dr["TGIAN_BDIEN"].ToString();
                            if (dr["THANG"].ToString().Trim().Length > 0)
                                infoHDCT.THANG = Convert.ToInt16(dr["THANG"]);
                            else
                                return "Lỗi dữ liệu HDN_HDONCTIET: THANG = null";

                            infoHDCT.TY_LE = Utility.DecimalDbnull(dr["TY_LE"]);
                            infoHDCT.TIEN_GTRU = Utility.DecimalDbnull(dr["TIEN_GTRU"]);
                            if (dr["TIEN_GOC"].ToString().Trim().Length > 0)
                                infoHDCT.TIEN_GOC = Convert.ToDecimal(dr["TIEN_GOC"].ToString());
                            else
                                infoHDCT.TIEN_GOC = Utility.DecimalDbnull(dr["SO_TIEN"]);

                            lstHDonCTiet.Add(infoHDCT);
                        }
                    }
                    #endregion

                    #region HDN_HDONCOSFI
                    DataRow[] arrHDonCosfi = null;
                    if (dtCosfi != null && dtCosfi.Rows.Count > 0)
                    {
                        arrHDonCosfi = dtCosfi.Select("MA_KHANG='" + info.MA_KHANG + "'");
                    }

                    //MessageBox.Show("Số bản ghi: " + Convert.ToString(arrHDonCosfi.Count()), "Thông báo", MessageBoxButtons.OK, MessageBoxIcon.Information);
                    if (arrHDonCosfi != null && arrHDonCosfi.Length > 0)
                    {
                        var cosfi = dtCosfi.AsEnumerable().Except(arrHDonCosfi);
                        dtCosfi = (cosfi != null && cosfi.Count() > 0) ? cosfi.CopyToDataTable() : null;

                        //obj_HDN_HDONCOSFI_Controller.CMIS2 = db;
                        foreach (DataRow drCF in arrHDonCosfi)
                        {
                            //strddo = Convert.ToString(drCF["MA_DDO"]);
                            //Bổ sung đoạn kiểm soát MA_SOGCS null
                            if (drCF["MA_SOGCS"].ToString().Trim().Length == 0)
                            {
                                var other = arrHDonCosfi.Where(c => c.Field<string>("MA_DDO") != drCF["MA_DDO"] && c.Field<string>("MA_SOGCS") != "").ToArray();
                                if (other != null && other.Length > 0)
                                {
                                    foreach (DataColumn col in drCF.Table.Columns)
                                    {
                                        drCF[col.ColumnName] = drCF[col.ColumnName].ToString().Trim().Length == 0 ? other[0][col.ColumnName] : drCF[col.ColumnName];
                                    }
                                }
                            }
                            HDN_HDONCOSFI_PLUS infoHDCF = new HDN_HDONCOSFI_PLUS();
                            if (drCF["COSFI"].ToString().Trim().Length > 0)
                                infoHDCF.COSFI = Convert.ToDecimal(drCF["COSFI"].ToString());
                            else
                                return "Lỗi dữ liệu HDN_HDONCOSFI: COSFI = null";
                            //MessageBox.Show("1");
                            if (drCF["HUU_CONG"].ToString().Trim().Length > 0)
                                infoHDCF.HUU_CONG = Convert.ToDecimal(drCF["HUU_CONG"].ToString());
                            else
                                return "Lỗi dữ liệu HDN_HDONCOSFI: HUU_CONG = null";
                            //MessageBox.Show("2");
                            infoHDCF.ID_HDON = info.ID_HDON;
                            //if (drCF["ID_HDONCOSFI"].ToString().Trim().Length > 0)
                            //    infoHDCF.ID_HDONCOSFI = Convert.ToInt64(drCF["ID_HDONCOSFI"].ToString());
                            //else
                            //    return "Lỗi dữ liệu HDN_HDONCOSFI: ID_HDONCOSFI = null";
                            infoHDCF.ID_HDONCOSFI = id_hdcf_sequence++;//obj_HDN_HDONCOSFI_Controller.getID_HDON();
                            if (drCF["KCOSFI"].ToString().Trim().Length > 0)
                                infoHDCF.KCOSFI = Convert.ToDecimal(drCF["KCOSFI"].ToString());
                            else
                                return "Lỗi dữ liệu HDN_HDONCOSFI: KCOSFI = null";
                            //MessageBox.Show("3");
                            if (drCF["KIMUA_CSPK"].ToString().Trim().Length > 0)
                                infoHDCF.KIMUA_CSPK = Convert.ToInt16(drCF["KIMUA_CSPK"].ToString());
                            else
                                return "Lỗi dữ liệu HDN_HDONCOSFI: KIMUA_CSPK = null";
                            if (drCF["KY"].ToString().Trim().Length > 0)
                                infoHDCF.KY = Convert.ToInt16(drCF["KY"].ToString());
                            else
                                return "Lỗi dữ liệu HDN_HDONCOSFI: KY = null";
                            //MessageBox.Show("4");
                            infoHDCF.MA_CNANG = strMaCNang;
                            infoHDCF.MA_DDO = drCF["MA_DDO"].ToString();
                            infoHDCF.MA_DVIQLY = drCF["MA_DVIQLY"].ToString();
                            infoHDCF.MA_KHANG = drCF["MA_KHANG"].ToString();
                            infoHDCF.MA_KVUC = drCF["MA_KVUC"].ToString();
                            infoHDCF.MA_LO = drCF["MA_LO"].ToString();
                            infoHDCF.MA_SOGCS = drCF["MA_SOGCS"].ToString();
                            infoHDCF.MA_TO = drCF["MA_TO"].ToString();
                            infoHDCF.MA_TRAM = drCF["MA_TRAM"].ToString();
                            //MessageBox.Show("5");
                            if (drCF["NAM"].ToString().Trim().Length > 0)
                                infoHDCF.NAM = Convert.ToInt16(drCF["NAM"].ToString());
                            else
                                return "Lỗi dữ liệu HDN_HDONCOSFI: NAM = null";
                            if (drCF["NGAY_SUA"].ToString().Trim().Length > 0)
                                infoHDCF.NGAY_SUA = Convert.ToDateTime(drCF["NGAY_SUA"].ToString());
                            else
                                return "Lỗi dữ liệu HDN_HDONCOSFI: NGAY_SUA = null";
                            if (drCF["NGAY_TAO"].ToString().Trim().Length > 0)
                                infoHDCF.NGAY_TAO = Convert.ToDateTime(drCF["NGAY_TAO"].ToString());
                            else
                                return "Lỗi dữ liệu HDN_HDONCOSFI: NGAY_TAO = null";
                            //MessageBox.Show("6");
                            infoHDCF.NGUOI_SUA = strTenDNhap;
                            infoHDCF.NGUOI_TAO = strTenDNhap;
                            infoHDCF.STT = drCF["STT"].ToString();
                            if (drCF["ID_CHISO"].ToString().Trim().Length > 0)
                                infoHDCF.ID_CHISO = Convert.ToInt64(drCF["ID_CHISO"]);
                            //MessageBox.Show("7");
                            if (drCF["THANG"].ToString().Trim().Length > 0)
                                infoHDCF.THANG = Convert.ToInt16(drCF["THANG"].ToString());
                            else
                                return "Lỗi dữ liệu HDN_HDONCOSFI: THANG = null";
                            if (drCF["TIEN_HUUCONG"].ToString().Trim().Length > 0)
                                infoHDCF.TIEN_HUUCONG = Convert.ToDecimal(drCF["TIEN_HUUCONG"].ToString());
                            else
                                return "Lỗi dữ liệu HDN_HDONCOSFI: TIEN_HUUCONG = null";
                            if (drCF["TIEN_VOCONG"].ToString().Trim().Length > 0)
                                infoHDCF.TIEN_VOCONG = Convert.ToDecimal(drCF["TIEN_VOCONG"].ToString());
                            else
                                return "Lỗi dữ liệu HDN_HDONCOSFI: TIEN_VOCONG = null";
                            if (drCF["VO_CONG"].ToString().Trim().Length > 0)
                                infoHDCF.VO_CONG = Convert.ToDecimal(drCF["VO_CONG"].ToString());
                            else
                                return "Lỗi dữ liệu HDN_HDONCOSFI: VO_CONG = null";

                            infoHDCF.TIEN_HC_GTRU = Utility.DecimalDbnull(drCF["TIEN_HC_GTRU"]);
                            if (drCF["TIEN_HC_GOC"].ToString().Trim().Length > 0)
                                infoHDCF.TIEN_HC_GOC = Convert.ToDecimal(drCF["TIEN_HC_GOC"].ToString());
                            else
                                infoHDCF.TIEN_HC_GOC = Utility.DecimalDbnull(drCF["TIEN_HUUCONG"]);
                            infoHDCF.TIEN_VC_GTRU = Utility.DecimalDbnull(drCF["TIEN_VC_GTRU"]);
                            if (drCF["TIEN_VC_GOC"].ToString().Trim().Length > 0)
                                infoHDCF.TIEN_VC_GOC = Convert.ToDecimal(drCF["TIEN_VC_GOC"].ToString());
                            else
                                infoHDCF.TIEN_VC_GOC = Utility.DecimalDbnull(drCF["TIEN_VOCONG"]);
                            //MessageBox.Show("8");
                            lstHDonCosfi.Add(infoHDCF);
                            //MessageBox.Show("9");
                        }
                    }
                    #endregion
                    //MessageBox.Show("End", "Thông báo", MessageBoxButtons.OK, MessageBoxIcon.Information);
                }
                #region DũngNT hiệu chỉnh - các chi tiết VC(không có hóa đơn VC)
                if (dtTemp != null && dtTemp.Rows.Count > 0)
                {
                    //obj_HDN_HDON_Controller.CMIS2 = db;
                    //obj_HDN_HDONCTIET_Controller.CMIS2 = db;
                    foreach (DataRow dr in dtTemp.Rows)
                    {
                        HDN_HDONCTIET_PLUS infoHDCT = new HDN_HDONCTIET_PLUS();
                        infoHDCT.BCS = dr["BCS"].ToString();
                        if (dr["DIEN_TTHU"].ToString().Trim().Length > 0)
                            infoHDCT.DIEN_TTHU = Convert.ToDecimal(dr["DIEN_TTHU"]);
                        else
                            return "Lỗi dữ liệu HDN_HDONCTIET: DIEN_TTHU = null";
                        infoHDCT.DINH_MUC = dr["DINH_MUC"].ToString();
                        if (dr["DON_GIA"].ToString().Trim().Length > 0)
                            infoHDCT.DON_GIA = Convert.ToDecimal(dr["DON_GIA"]);
                        if (dr["ID_CHISO"].ToString().Trim().Length > 0)
                            infoHDCT.ID_CHISO = Convert.ToInt64(dr["ID_CHISO"]);
                        else
                            return "Lỗi dữ liệu HDN_HDONCTIET: ID_CHISO = null";
                        infoHDCT.ID_HDON = id_hdon_sequence++;//obj_HDN_HDON_Controller.getID_HDON();
                        //if (dr["ID_HDONCTIET"].ToString().Trim().Length > 0)
                        //    infoHDCT.ID_HDONCTIET = Convert.ToInt64(dr["ID_HDONCTIET"]);
                        //else
                        //    return "Lỗi dữ liệu HDN_HDONCTIET: ID_HDONCTIET = null";
                        infoHDCT.ID_HDONCTIET = id_hdct_sequence++;//obj_HDN_HDONCTIET_Controller.getID_HDON();
                        if (dr["KY"].ToString().Trim().Length > 0)
                            infoHDCT.KY = Convert.ToInt16(dr["KY"]);
                        else
                            return "Lỗi dữ liệu HDN_HDONCTIET: KY = null";
                        if (dr["LOAI_KHANG"].ToString().Trim().Length > 0)
                            infoHDCT.LOAI_KHANG = Convert.ToInt16(dr["LOAI_KHANG"].ToString());
                        if (dr["LOAI_DDO"].ToString().Trim().Length > 0)
                            infoHDCT.LOAI_DDO = Convert.ToInt16(dr["LOAI_DDO"].ToString());
                        if (dr["SO_PHA"].ToString().Trim().Length > 0)
                            infoHDCT.SO_PHA = Convert.ToInt16(dr["SO_PHA"].ToString());
                        infoHDCT.LOAI_DMUC = dr["LOAI_DMUC"].ToString();
                        infoHDCT.MA_CAPDA = dr["MA_CAPDA"].ToString();
                        infoHDCT.MA_CNANG = strMaCNang;
                        infoHDCT.MA_DDO = dr["MA_DDO"].ToString();
                        infoHDCT.MA_DVIQLY = dr["MA_DVIQLY"].ToString();
                        infoHDCT.MA_KHANG = dr["MA_KHANG"].ToString();
                        infoHDCT.MA_KVUC = dr["MA_KVUC"].ToString();
                        infoHDCT.MA_LO = dr["MA_LO"].ToString();
                        infoHDCT.MA_NGIA = dr["MA_NGIA"].ToString();
                        infoHDCT.MA_NHOMNN = dr["MA_NHOMNN"].ToString();
                        infoHDCT.MA_NN = dr["MA_NN"].ToString();
                        infoHDCT.MA_SOGCS = dr["MA_SOGCS"].ToString();
                        infoHDCT.MA_TO = dr["MA_TO"].ToString();
                        infoHDCT.MA_TRAM = dr["MA_TRAM"].ToString();
                        if (dr["NAM"].ToString().Trim().Length > 0)
                            infoHDCT.NAM = Convert.ToInt16(dr["NAM"]);
                        else
                            return "Lỗi dữ liệu HDN_HDONCTIET: NAM = null";
                        if (dr["NGAY_APDUNG"].ToString().Trim().Length > 0)
                            infoHDCT.NGAY_APDUNG = Convert.ToDateTime(dr["NGAY_APDUNG"]).ToString("dd/MM/yyyy");
                        if (dr["NGAY_SUA"].ToString().Trim().Length > 0)
                            infoHDCT.NGAY_SUA = Convert.ToDateTime(dr["NGAY_SUA"]);
                        else
                            return "Lỗi dữ liệu HDN_HDONCTIET: NGAY_SUA = null";
                        if (dr["NGAY_TAO"].ToString().Trim().Length > 0)
                            infoHDCT.NGAY_TAO = Convert.ToDateTime(dr["NGAY_TAO"]);
                        else
                            return "Lỗi dữ liệu HDN_HDONCTIET: NGAY_TAO = null";
                        infoHDCT.NGUOI_SUA = strTenDNhap;
                        infoHDCT.NGUOI_TAO = strTenDNhap;
                        infoHDCT.SO_CTO = dr["SO_CTO"].ToString();
                        if (dr["SO_TIEN"].ToString().Trim().Length > 0)
                            infoHDCT.SO_TIEN = Convert.ToDecimal(dr["SO_TIEN"]);
                        else
                            return "Lỗi dữ liệu HDN_HDONCTIET: SO_TIEN = null";
                        infoHDCT.SO_TIEN = Convert.ToDecimal(dr["SO_TIEN"]);
                        infoHDCT.STT = dr["STT"].ToString();
                        infoHDCT.TGIAN_BDIEN = dr["TGIAN_BDIEN"].ToString();
                        if (dr["THANG"].ToString().Trim().Length > 0)
                            infoHDCT.THANG = Convert.ToInt16(dr["THANG"]);
                        else
                            return "Lỗi dữ liệu HDN_HDONCTIET: THANG = null";
                        lstHDonCTiet.Add(infoHDCT);
                    }
                }
                #endregion

                #region GCS_CHISO & GCS_CHISO_GT
                string strError = "";
                //obj_GCS_CHISO_Controller.LstInfo = new List<GCS_CHISO>();
                if (dsCustomerData.Tables["GCS_CHISO"] == null || dsCustomerData.Tables["GCS_CHISO"].Rows.Count == 0)
                    return "Không tìm thấy bảng GCS_CHISO để cập nhật dữ liệu.";
                foreach (DataRow row in dsCustomerData.Tables["GCS_CHISO"].Rows)
                {
                    GCS_CHISO_PLUS info = BillingLibrary.BillingLibrary.MapDatarowToObject<GCS_CHISO_PLUS>(row, ref strError);
                    if (strError.Trim().Length > 0)
                        return strError;
                    //obj_GCS_CHISO_Controller.LstInfo.Add(info);
                    lstChiSo.Add(info);
                }
                if (dsCustomerData.Tables.Contains("GCS_CHISO_GT") == true)
                {
                    foreach (DataRow row in dsCustomerData.Tables["GCS_CHISO_GT"].Rows)
                    {
                        GCS_CHISO_PLUS info = BillingLibrary.BillingLibrary.MapDatarowToObject<GCS_CHISO_PLUS>(row, ref strError);
                        if (strError.Trim().Length > 0)
                            return strError;
                        //obj_GCS_CHISO_Controller.LstInfo.Add(info);
                        lstChiSo.Add(info);
                    }
                }
                #endregion
                return "";

            }
            catch (Exception e)
            {
                //MessageBox.Show(strddo, "Thông báo", MessageBoxButtons.OK, MessageBoxIcon.Information);
                return "Error In SetPropertiesForObject Method: " + e.Message;
            }
        }
        public string InsertInvoiceDataKH_Gtru(DataSet dsInvoiceData, DataSet dsInvoiceDataGC, DataSet dsCustomerData, string strMa_DViQLy, string strMa_SoGCS, string strTenDNhap, long lngCurrentLibID, long lngWorkflowID, short ky, short thang, short nam)
        {
            //CMIS2 db = new CMIS2();
            try
            {
                DateTime dtHLucNSH = new DateTime(2020, 4, 16);
                DateTime dtHLucSH = new DateTime(2020, 5, 1);
                DateTime dtHetHLucNSH = new DateTime(2020, 7, 16);
                DateTime dtHetHLucSH = new DateTime(2020, 8, 1);
                List<GCS_CHISO_PLUS> lstChiSo = new List<GCS_CHISO_PLUS>();
                List<HDN_HDON_PLUS> lstHDon = new List<HDN_HDON_PLUS>();
                List<HDN_HDONCOSFI_PLUS> lstHDonCosfi = new List<HDN_HDONCOSFI_PLUS>();
                List<HDN_HDONCTIET_PLUS> lstHDonCTiet = new List<HDN_HDONCTIET_PLUS>();

                List<GCS_CHISO_PLUS> lstChiSoGC = new List<GCS_CHISO_PLUS>();
                List<HDN_HDON_PLUS> lstHDonGC = new List<HDN_HDON_PLUS>();
                List<HDN_HDONCOSFI_PLUS> lstHDonCosfiGC = new List<HDN_HDONCOSFI_PLUS>();
                List<HDN_HDONCTIET_PLUS> lstHDonCTietGC = new List<HDN_HDONCTIET_PLUS>();
                string strResult = SetPropertiesForObjectPlus(dsInvoiceData, dsCustomerData, lstChiSo, lstHDon, lstHDonCTiet, lstHDonCosfi, strTenDNhap, lngCurrentLibID.ToString());
                if (strResult != "") return strResult;
                if (lstHDon == null || lstHDon.Count <= 0) return "Danh sach hoa don khong co";
                if (lstHDonCTiet == null || lstHDonCTiet.Count <= 0) return "Danh sach hoa don chi tiet khong co";
                strResult = SetPropertiesForObjectPlus_GTru(dsInvoiceDataGC, dsCustomerData, lstChiSoGC, lstHDonGC, lstHDonCTietGC, lstHDonCosfiGC, strTenDNhap, lngCurrentLibID.ToString());
                if (strResult != "") return strResult;
                if (lstHDonGC == null || lstHDonGC.Count <= 0) return "Danh sach hoa don khong co";
                if (lstHDonCTietGC == null || lstHDonCTietGC.Count <= 0) return "Danh sach hoa don chi tiet khong co";
                //Viet lai doan code nay de submitchange 1 lan
                var arrMaKHang = lstHDon.Select(c => c.MA_KHANG).Distinct();
                //obj_HDN_HDON_Controller.CMIS2 = db;
                //obj_HDN_HDONCTIET_Controller.CMIS2 = db;
                //obj_HDN_HDONCOSFI_Controller.CMIS2 = db;
                //obj_GCS_CHISO_Controller.CMIS2 = db;
                inpDLieuHDon inpTHD = new inpDLieuHDon();
                inpTHD.LST_CSO = new List<GCS_CHISO_PLUS>();
                inpTHD.LST_HDCF = new List<HDN_HDONCOSFI_PLUS>();
                inpTHD.LST_HDCT = new List<HDN_HDONCTIET_PLUS>();
                inpTHD.LST_HDON = new List<HDN_HDON_PLUS>();
                inpTHD.LST_HDCF_BT = new List<HDN_HDONCOSFI_PLUS>();
                inpTHD.LST_HDCT_BT = new List<HDN_HDONCTIET_PLUS>();
                inpTHD.LST_HDON_BT = new List<HDN_HDON_PLUS>();
                //Tach ra theo tung ma khach hang
                foreach (var x in arrMaKHang)
                {
                    if (x == "PD01000010568")
                    {
                    }
                    string strTemp = "";


                    List<HDN_HDON_PLUS> lstHDonGC_Temp = lstHDonGC.Where(c => c.MA_KHANG == x).ToList();
                    List<HDN_HDON_PLUS> lstHDon_Temp = lstHDon.Where(c => c.MA_KHANG == x).ToList();
                    List<HDN_HDONCTIET_PLUS> lstHDonCTiet_Temp = lstHDonCTiet.Where(c => c.MA_KHANG == x).ToList();
                    List<GCS_CHISO_PLUS> lstChiSo_Temp = lstChiSo.Where(c => c.MA_KHANG == x).ToList();
                    DateTime dtNgayCKy = DateTime.ParseExact(lstHDon_Temp[0].NGAY_CKY, "dd/MM/yyyy", System.Globalization.CultureInfo.InvariantCulture);
                    //Bổ sung kiểm tra tiền cũ != tiền mới thì mới ghi dữ liệu BT
                    decimal decTienMoi = lstHDon_Temp.Sum(c => c.TIEN_GOC);
                    decimal decTienCu = lstHDonGC_Temp.Sum(c => c.TIEN_GOC);
                    if (lstHDon_Temp[0].LOAI_KHANG == 0)
                    {
                        //SH
                        if (dtNgayCKy.CompareTo(dtHLucSH) >= 0 && dtNgayCKy.CompareTo(dtHetHLucSH) < 0 && decTienMoi != decTienCu)
                        {
                            //Trong khoảng hiệu lực, tính theo đơn giá mới, ghi lại dữ liệu tính BT để hiển thị trên thể hiện HDDT
                            inpTHD.LST_HDON.AddRange(lstHDon_Temp);
                            inpTHD.LST_HDCT.AddRange(lstHDonCTiet_Temp);
                            inpTHD.LST_CSO.AddRange(lstChiSo_Temp);
                            if (lstHDonCosfi != null && lstHDonCosfi.Count > 0)
                            {

                                List<HDN_HDONCOSFI_PLUS> lstHDonCosfi_Temp = lstHDonCosfi.Where(c => c.MA_KHANG == x).ToList();
                                if (lstHDonCosfi_Temp != null && lstHDonCosfi_Temp.Count > 0)
                                {
                                    foreach (HDN_HDONCOSFI_PLUS hcosfi in lstHDonCosfi_Temp)
                                    {
                                        if (lstHDon_Temp != null && lstHDon_Temp.Count > 0)
                                        {
                                            var hdon = lstHDon_Temp.Where(c => c.LOAI_HDON == "VC");
                                            if (hdon != null && hdon.Count() > 0)
                                            {
                                                hcosfi.ID_HDON = hdon.First().ID_HDON;
                                            }
                                        }
                                    }
                                }
                                inpTHD.LST_HDCF.AddRange(lstHDonCosfi_Temp);
                                //obj_HDN_HDONCOSFI_Controller.lstCosfi = lstHDonCosfi_Temp;
                                //strTemp = strTemp + obj_HDN_HDONCOSFI_Controller.InsertHDN_HDONCOSFI();
                                lstHDonCosfi = lstHDonCosfi.Where(c => c.MA_KHANG != x).ToList();
                            }
                            lstHDon = lstHDon.Where(c => c.MA_KHANG != x).ToList();
                            lstHDonCTiet = lstHDonCTiet.Where(c => c.MA_KHANG != x).ToList();
                            lstChiSo = lstChiSo.Where(c => c.MA_KHANG != x).ToList();





                            List<HDN_HDONCTIET_PLUS> lstHDonCTietGC_Temp = lstHDonCTietGC.Where(c => c.MA_KHANG == x).ToList();
                            inpTHD.LST_HDON_BT.AddRange(lstHDonGC_Temp);
                            inpTHD.LST_HDCT_BT.AddRange(lstHDonCTietGC_Temp);
                            if (lstHDonCosfiGC != null && lstHDonCosfiGC.Count > 0)
                            {

                                List<HDN_HDONCOSFI_PLUS> lstHDonCosfiGC_Temp = lstHDonCosfiGC.Where(c => c.MA_KHANG == x).ToList();
                                if (lstHDonCosfiGC_Temp != null && lstHDonCosfiGC_Temp.Count > 0)
                                {
                                    foreach (HDN_HDONCOSFI_PLUS hcosfi in lstHDonCosfiGC_Temp)
                                    {
                                        if (lstHDonGC_Temp != null && lstHDonGC_Temp.Count > 0)
                                        {
                                            var hdon = lstHDonGC_Temp.Where(c => c.LOAI_HDON == "VC");
                                            if (hdon != null && hdon.Count() > 0)
                                            {
                                                hcosfi.ID_HDON = hdon.First().ID_HDON;
                                            }
                                        }
                                    }
                                }
                                inpTHD.LST_HDCF_BT.AddRange(lstHDonCosfiGC_Temp);
                                //obj_HDN_HDONCOSFI_Controller.lstCosfi = lstHDonCosfi_Temp;
                                //strTemp = strTemp + obj_HDN_HDONCOSFI_Controller.InsertHDN_HDONCOSFI();
                                lstHDonCosfiGC = lstHDonCosfiGC.Where(c => c.MA_KHANG != x).ToList();
                            }


                            lstHDonGC = lstHDonGC.Where(c => c.MA_KHANG != x).ToList();
                            lstHDonCTietGC = lstHDonCTietGC.Where(c => c.MA_KHANG != x).ToList();
                        }
                        else
                        {
                            //Ngoài khoảng hiệu lực, tính theo giá cũ
                            inpTHD.LST_CSO.AddRange(lstChiSo_Temp);
                            lstHDon = lstHDon.Where(c => c.MA_KHANG != x).ToList();
                            lstHDonCTiet = lstHDonCTiet.Where(c => c.MA_KHANG != x).ToList();
                            lstChiSo = lstChiSo.Where(c => c.MA_KHANG != x).ToList();
                            //List<HDN_HDON_PLUS> lstHDonGC_Temp = lstHDonGC.Where(c => c.MA_KHANG == x).ToList();
                            List<HDN_HDONCTIET_PLUS> lstHDonCTietGC_Temp = lstHDonCTietGC.Where(c => c.MA_KHANG == x).ToList();
                            inpTHD.LST_HDON.AddRange(lstHDonGC_Temp);
                            inpTHD.LST_HDCT.AddRange(lstHDonCTietGC_Temp);
                            if (lstHDonCosfiGC != null && lstHDonCosfiGC.Count > 0)
                            {

                                List<HDN_HDONCOSFI_PLUS> lstHDonCosfiGC_Temp = lstHDonCosfiGC.Where(c => c.MA_KHANG == x).ToList();
                                if (lstHDonCosfiGC_Temp != null && lstHDonCosfiGC_Temp.Count > 0)
                                {
                                    foreach (HDN_HDONCOSFI_PLUS hcosfi in lstHDonCosfiGC_Temp)
                                    {
                                        if (lstHDonGC_Temp != null && lstHDonGC_Temp.Count > 0)
                                        {
                                            var hdon = lstHDonGC_Temp.Where(c => c.LOAI_HDON == "VC");
                                            if (hdon != null && hdon.Count() > 0)
                                            {
                                                hcosfi.ID_HDON = hdon.First().ID_HDON;
                                            }
                                        }
                                    }
                                }
                                inpTHD.LST_HDCF.AddRange(lstHDonCosfiGC_Temp);
                                //obj_HDN_HDONCOSFI_Controller.lstCosfi = lstHDonCosfi_Temp;
                                //strTemp = strTemp + obj_HDN_HDONCOSFI_Controller.InsertHDN_HDONCOSFI();
                                lstHDonCosfiGC = lstHDonCosfiGC.Where(c => c.MA_KHANG != x).ToList();
                            }
                            lstHDonGC = lstHDonGC.Where(c => c.MA_KHANG != x).ToList();
                            lstHDonCTietGC = lstHDonCTietGC.Where(c => c.MA_KHANG != x).ToList();
                        }
                    }
                    else
                    {

                        //NSH
                        if (dtNgayCKy.CompareTo(dtHLucNSH) >= 0 && dtNgayCKy.CompareTo(dtHetHLucNSH) < 0 && decTienMoi != decTienCu)
                        {
                            //Trong khoảng hiệu lực, tính theo đơn giá mới, ghi lại dữ liệu tính BT để hiển thị trên thể hiện HDDT
                            inpTHD.LST_HDON.AddRange(lstHDon_Temp);
                            inpTHD.LST_HDCT.AddRange(lstHDonCTiet_Temp);
                            inpTHD.LST_CSO.AddRange(lstChiSo_Temp);
                            if (lstHDonCosfi != null && lstHDonCosfi.Count > 0)
                            {

                                List<HDN_HDONCOSFI_PLUS> lstHDonCosfi_Temp = lstHDonCosfi.Where(c => c.MA_KHANG == x).ToList();
                                if (lstHDonCosfi_Temp != null && lstHDonCosfi_Temp.Count > 0)
                                {
                                    foreach (HDN_HDONCOSFI_PLUS hcosfi in lstHDonCosfi_Temp)
                                    {
                                        if (lstHDon_Temp != null && lstHDon_Temp.Count > 0)
                                        {
                                            var hdon = lstHDon_Temp.Where(c => c.LOAI_HDON == "VC");
                                            if (hdon != null && hdon.Count() > 0)
                                            {
                                                hcosfi.ID_HDON = hdon.First().ID_HDON;
                                            }
                                        }
                                    }
                                }
                                inpTHD.LST_HDCF.AddRange(lstHDonCosfi_Temp);
                                //obj_HDN_HDONCOSFI_Controller.lstCosfi = lstHDonCosfi_Temp;
                                //strTemp = strTemp + obj_HDN_HDONCOSFI_Controller.InsertHDN_HDONCOSFI();
                                lstHDonCosfi = lstHDonCosfi.Where(c => c.MA_KHANG != x).ToList();
                            }
                            lstHDon = lstHDon.Where(c => c.MA_KHANG != x).ToList();
                            lstHDonCTiet = lstHDonCTiet.Where(c => c.MA_KHANG != x).ToList();
                            lstChiSo = lstChiSo.Where(c => c.MA_KHANG != x).ToList();
                            //List<HDN_HDON_PLUS> lstHDonGC_Temp = lstHDonGC.Where(c => c.MA_KHANG == x).ToList();
                            List<HDN_HDONCTIET_PLUS> lstHDonCTietGC_Temp = lstHDonCTietGC.Where(c => c.MA_KHANG == x).ToList();
                            inpTHD.LST_HDON_BT.AddRange(lstHDonGC_Temp);
                            inpTHD.LST_HDCT_BT.AddRange(lstHDonCTietGC_Temp);
                            if (lstHDonCosfiGC != null && lstHDonCosfiGC.Count > 0)
                            {

                                List<HDN_HDONCOSFI_PLUS> lstHDonCosfiGC_Temp = lstHDonCosfiGC.Where(c => c.MA_KHANG == x).ToList();
                                if (lstHDonCosfiGC_Temp != null && lstHDonCosfiGC_Temp.Count > 0)
                                {
                                    foreach (HDN_HDONCOSFI_PLUS hcosfi in lstHDonCosfiGC_Temp)
                                    {
                                        if (lstHDonGC_Temp != null && lstHDonGC_Temp.Count > 0)
                                        {
                                            var hdon = lstHDonGC_Temp.Where(c => c.LOAI_HDON == "VC");
                                            if (hdon != null && hdon.Count() > 0)
                                            {
                                                hcosfi.ID_HDON = hdon.First().ID_HDON;
                                            }
                                        }
                                    }
                                }
                                inpTHD.LST_HDCF_BT.AddRange(lstHDonCosfiGC_Temp);
                                //obj_HDN_HDONCOSFI_Controller.lstCosfi = lstHDonCosfi_Temp;
                                //strTemp = strTemp + obj_HDN_HDONCOSFI_Controller.InsertHDN_HDONCOSFI();
                                lstHDonCosfiGC = lstHDonCosfiGC.Where(c => c.MA_KHANG != x).ToList();
                            }


                            lstHDonGC = lstHDonGC.Where(c => c.MA_KHANG != x).ToList();
                            lstHDonCTietGC = lstHDonCTietGC.Where(c => c.MA_KHANG != x).ToList();
                        }
                        else
                        {
                            //Ngoài khoảng hiệu lực, tính theo giá cũ
                            inpTHD.LST_CSO.AddRange(lstChiSo_Temp);
                            lstHDon = lstHDon.Where(c => c.MA_KHANG != x).ToList();
                            lstHDonCTiet = lstHDonCTiet.Where(c => c.MA_KHANG != x).ToList();
                            lstChiSo = lstChiSo.Where(c => c.MA_KHANG != x).ToList();
                            //List<HDN_HDON_PLUS> lstHDonGC_Temp = lstHDonGC.Where(c => c.MA_KHANG == x).ToList();
                            List<HDN_HDONCTIET_PLUS> lstHDonCTietGC_Temp = lstHDonCTietGC.Where(c => c.MA_KHANG == x).ToList();
                            inpTHD.LST_HDON.AddRange(lstHDonGC_Temp);
                            inpTHD.LST_HDCT.AddRange(lstHDonCTietGC_Temp);
                            if (lstHDonCosfiGC != null && lstHDonCosfiGC.Count > 0)
                            {

                                List<HDN_HDONCOSFI_PLUS> lstHDonCosfiGC_Temp = lstHDonCosfiGC.Where(c => c.MA_KHANG == x).ToList();
                                if (lstHDonCosfiGC_Temp != null && lstHDonCosfiGC_Temp.Count > 0)
                                {
                                    foreach (HDN_HDONCOSFI_PLUS hcosfi in lstHDonCosfiGC_Temp)
                                    {
                                        if (lstHDonGC_Temp != null && lstHDonGC_Temp.Count > 0)
                                        {
                                            var hdon = lstHDonGC_Temp.Where(c => c.LOAI_HDON == "VC");
                                            if (hdon != null && hdon.Count() > 0)
                                            {
                                                hcosfi.ID_HDON = hdon.First().ID_HDON;
                                            }
                                        }
                                    }
                                }
                                inpTHD.LST_HDCF.AddRange(lstHDonCosfiGC_Temp);
                                //obj_HDN_HDONCOSFI_Controller.lstCosfi = lstHDonCosfi_Temp;
                                //strTemp = strTemp + obj_HDN_HDONCOSFI_Controller.InsertHDN_HDONCOSFI();
                                lstHDonCosfiGC = lstHDonCosfiGC.Where(c => c.MA_KHANG != x).ToList();
                            }
                            lstHDonGC = lstHDonGC.Where(c => c.MA_KHANG != x).ToList();
                            lstHDonCTietGC = lstHDonCTietGC.Where(c => c.MA_KHANG != x).ToList();
                        }
                    }

                }
                if (lstChiSo != null && lstChiSo.Count > 0)
                {
                    //obj_GCS_CHISO_Controller.LstInfo = lstChiSo;
                    //obj_GCS_CHISO_Controller.UpdateList();
                    inpTHD.LST_CSO.AddRange(lstChiSo);
                }
                //Insert luong so ghi chi so
                string strInput = JsonConvert.SerializeObject(inpTHD);
                //Gọi hàm insert
                string strIP = ConfigurationManager.AppSettings["URI"];
                var baseAddress = "http://" + strIP + "/ServiceHDonPSinh-HDonPSinh-context-root/resources/serviceHDonPSinh/insertTHD_KH";
                var http = (HttpWebRequest)WebRequest.Create(new Uri(baseAddress));
                http.Accept = "application/json";
                http.ContentType = "application/json";
                http.Method = "POST";

                //string parsedContent = "{'MA_DVIQLY':'PC06AA','TEN_DANH_MUC':'D_SOGCS'}";
                string parsedContent = strInput;
                //ASCIIEncoding encoding = new ASCIIEncoding();
                Byte[] bytes = System.Text.Encoding.UTF8.GetBytes(parsedContent);

                Stream newStream = http.GetRequestStream();
                newStream.Write(bytes, 0, bytes.Length);
                newStream.Close();

                var response = http.GetResponse();

                var stream = response.GetResponseStream();
                var sr = new StreamReader(stream);
                var content = sr.ReadToEnd();
                ReturnObject obj = JsonConvert.DeserializeObject<ReturnObject>(content);
                if (obj.TYPE == "ERROR") return obj.MESSAGE;
                else return "";

            }
            catch (Exception ex)
            {
                return "Lỗi khi Insert dữ liệu hóa đơn với mã sổ= " + strMa_SoGCS + ": " + ex.Message;
            }
            finally
            {
                //db.ReleaseConnection();
            }
        }
        protected string CreateObject_GTRU(DataSet dsCustomerData, List<HDN_HDON_PLUS> lstHDon, List<HDN_HDONCTIET_PLUS> lstHDonCTiet, List<HDN_HDONCOSFI_PLUS> lstHDonCosfi, string strTenDNhap, string strMaCNang)
        {
            //String strddo = "";
            //HDN_HDON
            if (dsCustomerData == null) return "NoDataFound!- Không tìm thấy dữ liệu!";
            if (dsCustomerData.Tables["HDN_HDON_GTRU"] == null)
            {
                return "NoInvoiceDataFound!- Không tìm thấy dữ liệu hóa đơn giảm trừ!";
            }
            if (dsCustomerData.Tables["HDN_HDONCTIET_GTRU"] == null)
            {
                return "NoInvoiceDataFound!- Không tìm thấy dữ liệu hóa đơn chi tiết giảm trừ!";
            }

            try
            {

                foreach (DataRow row in dsCustomerData.Tables["HDN_HDON_GTRU"].Rows)
                {
                    #region HDN_HDON
                    HDN_HDON_PLUS info = new HDN_HDON_PLUS();

                    if (row["CHI_TIET"].ToString().Trim().Length > 0)
                        info.CHI_TIET = Convert.ToInt16(row["CHI_TIET"].ToString());
                    if (row["COSFI"].ToString().Trim().Length > 0)
                        info.COSFI = Convert.ToDecimal(row["COSFI"].ToString());
                    info.DCHI_KHANG = row["DCHI_KHANG"].ToString();
                    info.DCHI_KHANGTT = row["DCHI_KHANGTT"].ToString();
                    if (row["DIEN_TTHU"].ToString().Trim().Length > 0)
                        info.DIEN_TTHU = Convert.ToDecimal(row["DIEN_TTHU"].ToString());
                    else
                        return "Lỗi dữ liệu HDN_HDON: DIEN_TTHU = null";
                    if (row["ID_BBANPHANH"].ToString().Trim().Length > 0)
                        info.ID_BBANPHANH = Convert.ToInt64(row["ID_BBANPHANH"].ToString());
                    //if (row["ID_HDON"].ToString().Trim().Length > 0 || row["ID_HDON"].ToString() != "0")
                    //    info.ID_HDON = Convert.ToInt64(row["ID_HDON"].ToString());
                    //else
                    //obj_HDN_HDON_Controller.CMIS2 = db;
                    info.ID_HDON = Convert.ToInt32(row["ID_HDON"].ToString());

                    if (row["KCOSFI"].ToString().Trim().Length > 0)
                        info.KCOSFI = Convert.ToDecimal(row["KCOSFI"].ToString());
                    info.KIHIEU_SERY = row["KIHIEU_SERY"].ToString();
                    if (row["KY"].ToString().Trim().Length > 0)
                        info.KY = Convert.ToInt16(row["KY"].ToString());
                    else
                        return "Lỗi dữ liệu HDN_HDON: KY = null";
                    if (row["LOAI_KHANG"].ToString().Trim().Length > 0)
                        info.LOAI_KHANG = Convert.ToInt16(row["LOAI_KHANG"]);
                    else
                        return "Lỗi dữ liệu HDN_HDON: LOAI_KHANG = null";
                    //info.DCHI_TTOAN = row["DCHI_TTOAN"].ToString();
                    info.MANHOM_KHANG = row["MANHOM_KHANG"].ToString();
                    //info.MA_LOAIDN = row["MA_LOAIDN"].ToString();
                    info.MA_PTTT = row["MA_PTTT"].ToString();
                    info.LOAI_HDON = row["LOAI_HDON"].ToString();
                    info.MA_CNANG = strMaCNang;
                    info.MA_DVIQLY = row["MA_DVIQLY"].ToString();
                    info.MA_HTTT = row["MA_HTTT"].ToString();
                    info.MA_KHANG = row["MA_KHANG"].ToString();
                    info.MA_KHANGTT = row["MA_KHANGTT"].ToString();
                    info.MA_KVUC = row["MA_KVUC"].ToString();
                    info.MA_NHANG = row["MA_NHANG"].ToString();
                    //info.MA_NVIN = row["MA_NVIN"].ToString();
                    info.MA_NVPHANH = row["MA_NVPHANH"].ToString();
                    info.MA_SOGCS = row["MA_SOGCS"].ToString();
                    info.MA_TO = row["MA_TO"].ToString();
                    info.MASO_THUE = row["MASO_THUE"].ToString();
                    if (row["NAM"].ToString().Trim().Length > 0)
                        info.NAM = Convert.ToInt16(row["NAM"].ToString());
                    else
                        return "Lỗi dữ liệu HDN_HDON: NAM = null";
                    if (row["NGAY_CKY"].ToString().Trim().Length > 0)
                        info.NGAY_CKY = Convert.ToDateTime(row["NGAY_CKY"].ToString()).ToString("dd/MM/yyyy");
                    else
                        return "Lỗi dữ liệu HDN_HDON: NGAY_CKY = null";
                    if (row["NGAY_DKY"].ToString().Trim().Length > 0)
                        info.NGAY_DKY = Convert.ToDateTime(row["NGAY_DKY"].ToString()).ToString("dd/MM/yyyy");
                    else
                        return "Lỗi dữ liệu HDN_HDON: NGAY_DKY = null";
                    //if (row["NGAY_IN"].ToString().Trim().Length > 0)
                    //    info.NGAY_IN = row["NGAY_IN"].ToString();
                    //if (row["NGAY_PHANH"].ToString().Trim().Length > 0)
                    //    info.NGAY_PHANH = row["NGAY_PHANH"].ToString();
                    //if (row["NGAY_SUA"].ToString().Trim().Length > 0)
                    //    info.NGAY_SUA = Convert.ToDateTime(row["NGAY_SUA"].ToString());
                    //else
                    //    return "Lỗi dữ liệu HDN_HDON: NGAY_SUA = null";
                    //if (row["NGAY_TAO"].ToString().Trim().Length > 0)
                    //    info.NGAY_TAO = Convert.ToDateTime(row["NGAY_TAO"].ToString());
                    //else
                    //    return "Lỗi dữ liệu HDN_HDON: NGAY_TAO = null";
                    info.NGAY_TAO = DateTime.Now;
                    info.NGAY_SUA = DateTime.Now;
                    info.NGUOI_SUA = strTenDNhap;
                    info.NGUOI_TAO = strTenDNhap;
                    info.SO_CTO = row["SO_CTO"].ToString();
                    if (row["SO_HO"].ToString().Trim().Length > 0)
                        info.SO_HO = Convert.ToDecimal(row["SO_HO"].ToString());
                    else
                        return "Lỗi dữ liệu HDN_HDON: SO_HO = null";
                    //if (row["SO_LANIN"].ToString().Trim().Length > 0)
                    //    info.SO_LANIN = Convert.ToInt16(row["SO_LANIN"].ToString());
                    info.SO_SERY = row["SO_SERY"].ToString();
                    if (row["SO_TIEN"].ToString().Trim().Length > 0)
                        info.SO_TIEN = Convert.ToDecimal(row["SO_TIEN"].ToString());
                    else
                        return "Lỗi dữ liệu HDN_HDON: SO_TIEN = null";
                    info.STT = row["STT"].ToString();
                    //if (row["STT_IN"].ToString().Trim().Length > 0)
                    //    info.STT_IN = Convert.ToInt32(row["STT_IN"].ToString());
                    info.TEN_KHANG = row["TEN_KHANG"].ToString();
                    info.TEN_KHANGTT = row["TEN_KHANGTT"].ToString();
                    if (row["THANG"].ToString().Trim().Length > 0)
                        info.THANG = Convert.ToInt16(row["THANG"].ToString());
                    else
                        return "Lỗi dữ liệu HDN_HDON: THANG = null";
                    if (row["TIEN_GTGT"].ToString().Trim().Length > 0)
                        info.TIEN_GTGT = Convert.ToDecimal(row["TIEN_GTGT"].ToString());
                    else
                        return "Lỗi dữ liệu HDN_HDON: TIEN_GTGT = null";
                    info.TKHOAN_KHANG = row["TKHOAN_KHANG"].ToString();
                    if (row["TONG_TIEN"].ToString().Trim().Length > 0)
                        info.TONG_TIEN = Convert.ToDecimal(row["TONG_TIEN"].ToString());
                    else
                        return "Lỗi dữ liệu HDN_HDON: TONG_TIEN = null";
                    if (row["TYLE_THUE"].ToString().Trim().Length > 0)
                        info.TYLE_THUE = Convert.ToDecimal(row["TYLE_THUE"].ToString());
                    else
                        return "Lỗi dữ liệu HDN_HDON: TYLE_THUE = null";
                    //long lngIDHDong = Convert.ToInt64(dsCustomerData.Tables["HDG_KHACH_HANG"].Select("MA_KHANG='" + info.MA_KHANG + "'")[0]["ID_HDONG"]);
                    //string strDChiTToan = (new cls_HDG_HOP_DONG_Controller()).getDCHI_TTOAN(info.MA_DVIQLY, lngIDHDong, info.NGAY_CKY.Date);
                    //if (strDChiTToan != null && strDChiTToan.Trim().Length > 0)
                    //    info.DCHI_TTOAN = strDChiTToan;
                    //else
                    //    return "Không tìm thấy hợp đồng tương ứng với mã khách hàng " + info.MA_KHANG;
                    //if (info.DCHI_TTOAN == null || info.DCHI_TTOAN.Trim().Length == 0)
                    //    return "Không tìm thấy hợp đồng tương ứng với mã khách hàng " + info.MA_KHANG;
                    info.TIEN_TD = Utility.DecimalDbnull(row["TIEN_TD"]);
                    info.THUE_TD = Utility.DecimalDbnull(row["THUE_TD"]);
                    info.TIEN_VC = Utility.DecimalDbnull(row["TIEN_VC"]);
                    info.THUE_VC = Utility.DecimalDbnull(row["THUE_VC"]);
                    info.DTHOAI = row["DTHOAI"].ToString();


                    info.TIEN_GTRU = Utility.DecimalDbnull(row["TIEN_GTRU"]);

                    if (row["TIEN_GOC"].ToString().Trim().Length > 0)
                        info.TIEN_GOC = Convert.ToDecimal(row["TIEN_GOC"].ToString());
                    else
                        info.TIEN_GOC = Convert.ToDecimal(row["SO_TIEN"].ToString());
                    info.TIEN_TD_GTRU = Utility.DecimalDbnull(row["TIEN_TD_GTRU"]);

                    if (row["TIEN_TD_GOC"].ToString().Trim().Length > 0)
                        info.TIEN_TD_GOC = Convert.ToDecimal(row["TIEN_TD_GOC"].ToString());
                    else
                        info.TIEN_TD_GOC = Utility.DecimalDbnull(row["TIEN_TD"]);

                    info.TIEN_VC_GTRU = Utility.DecimalDbnull(row["TIEN_VC_GTRU"]);

                    if (row["TIEN_VC_GOC"].ToString().Trim().Length > 0)
                        info.TIEN_VC_GOC = Convert.ToDecimal(row["TIEN_VC_GOC"].ToString());
                    else
                        info.TIEN_VC_GOC = Utility.DecimalDbnull(row["TIEN_VC"]);
                    lstHDon.Add(info);

                    #endregion
                }

                #region HDN_HDONCTIET

                //obj_HDN_HDONCTIET_Controller.CMIS2 = db;
                foreach (DataRow dr in dsCustomerData.Tables["HDN_HDONCTIET_GTRU"].Rows)
                {

                    HDN_HDONCTIET_PLUS infoHDCT = new HDN_HDONCTIET_PLUS();
                    infoHDCT.BCS = dr["BCS"].ToString();
                    if (dr["DIEN_TTHU"].ToString().Trim().Length > 0)
                        infoHDCT.DIEN_TTHU = Convert.ToDecimal(dr["DIEN_TTHU"]);
                    else
                        return "Lỗi dữ liệu HDN_HDONCTIET: DIEN_TTHU = null";
                    infoHDCT.DINH_MUC = dr["DINH_MUC"].ToString();
                    if (dr["DON_GIA"].ToString().Trim().Length > 0)
                        infoHDCT.DON_GIA = Convert.ToDecimal(dr["DON_GIA"]);
                    if (dr["ID_CHISO"].ToString().Trim().Length > 0)
                        infoHDCT.ID_CHISO = Convert.ToInt64(dr["ID_CHISO"]);
                    else
                        return "Lỗi dữ liệu HDN_HDONCTIET: ID_CHISO = null";
                    infoHDCT.ID_HDON = Convert.ToInt64(dr["ID_HDON"]);
                    //if (dr["ID_HDONCTIET"].ToString().Trim().Length > 0)
                    //    infoHDCT.ID_HDONCTIET = Convert.ToInt64(dr["ID_HDONCTIET"]);
                    //else
                    //    return "Lỗi dữ liệu HDN_HDONCTIET: ID_HDONCTIET = null";
                    infoHDCT.ID_HDONCTIET = Convert.ToInt64(dr["ID_HDONCTIET"]);
                    if (dr["KY"].ToString().Trim().Length > 0)
                        infoHDCT.KY = Convert.ToInt16(dr["KY"]);
                    else
                        return "Lỗi dữ liệu HDN_HDONCTIET: KY = null";
                    //if (dr["LOAI_KHANG"].ToString().Trim().Length > 0)
                    //    infoHDCT.LOAI_KHANG = Convert.ToInt16(dr["LOAI_KHANG"].ToString());
                    if (dr["LOAI_DDO"].ToString().Trim().Length > 0)
                        infoHDCT.LOAI_DDO = Convert.ToInt16(dr["LOAI_DDO"].ToString());
                    if (dr["SO_PHA"].ToString().Trim().Length > 0)
                        infoHDCT.SO_PHA = Convert.ToInt16(dr["SO_PHA"].ToString());
                    infoHDCT.LOAI_DMUC = dr["LOAI_DMUC"].ToString();
                    infoHDCT.MA_CAPDA = dr["MA_CAPDA"].ToString();
                    infoHDCT.MA_CNANG = strMaCNang;
                    infoHDCT.MA_DDO = dr["MA_DDO"].ToString();
                    infoHDCT.MA_DVIQLY = dr["MA_DVIQLY"].ToString();
                    infoHDCT.MA_KHANG = dr["MA_KHANG"].ToString();
                    infoHDCT.MA_KVUC = dr["MA_KVUC"].ToString();
                    infoHDCT.MA_LO = dr["MA_LO"].ToString();
                    infoHDCT.MA_NGIA = dr["MA_NGIA"].ToString();
                    infoHDCT.MA_NHOMNN = dr["MA_NHOMNN"].ToString();
                    infoHDCT.MA_NN = dr["MA_NN"].ToString();
                    infoHDCT.MA_SOGCS = dr["MA_SOGCS"].ToString();
                    infoHDCT.MA_TO = dr["MA_TO"].ToString();
                    infoHDCT.MA_TRAM = dr["MA_TRAM"].ToString();
                    if (dr["NAM"].ToString().Trim().Length > 0)
                        infoHDCT.NAM = Convert.ToInt16(dr["NAM"]);
                    else
                        return "Lỗi dữ liệu HDN_HDONCTIET: NAM = null";
                    if (dr["NGAY_APDUNG"].ToString().Trim().Length > 0)
                        infoHDCT.NGAY_APDUNG = Convert.ToDateTime(dr["NGAY_APDUNG"]).ToString("dd/MM/yyyy");
                    //if (dr["NGAY_SUA"].ToString().Trim().Length > 0)
                    //    infoHDCT.NGAY_SUA = Convert.ToDateTime(dr["NGAY_SUA"]);
                    //else
                    //    return "Lỗi dữ liệu HDN_HDONCTIET: NGAY_SUA = null";
                    //if (dr["NGAY_TAO"].ToString().Trim().Length > 0)
                    //    infoHDCT.NGAY_TAO = Convert.ToDateTime(dr["NGAY_TAO"]);
                    //else
                    //    return "Lỗi dữ liệu HDN_HDONCTIET: NGAY_TAO = null";
                    infoHDCT.NGAY_TAO = DateTime.Now;
                    infoHDCT.NGAY_SUA = DateTime.Now;
                    infoHDCT.NGUOI_SUA = strTenDNhap;
                    infoHDCT.NGUOI_TAO = strTenDNhap;
                    infoHDCT.SO_CTO = dr["SO_CTO"].ToString();
                    if (dr["SO_TIEN"].ToString().Trim().Length > 0)
                        infoHDCT.SO_TIEN = Convert.ToDecimal(dr["SO_TIEN"]);
                    else
                        return "Lỗi dữ liệu HDN_HDONCTIET: SO_TIEN = null";
                    infoHDCT.SO_TIEN = Convert.ToDecimal(dr["SO_TIEN"]);
                    infoHDCT.STT = dr["STT"].ToString();
                    infoHDCT.TGIAN_BDIEN = dr["TGIAN_BDIEN"].ToString();
                    if (dr["THANG"].ToString().Trim().Length > 0)
                        infoHDCT.THANG = Convert.ToInt16(dr["THANG"]);
                    else
                        return "Lỗi dữ liệu HDN_HDONCTIET: THANG = null";

                    infoHDCT.TY_LE = Utility.DecimalDbnull(dr["TY_LE"]);
                    infoHDCT.TIEN_GTRU = Utility.DecimalDbnull(dr["TIEN_GTRU"]);
                    if (dr["TIEN_GOC"].ToString().Trim().Length > 0)
                        infoHDCT.TIEN_GOC = Convert.ToDecimal(dr["TIEN_GOC"].ToString());
                    else
                        infoHDCT.TIEN_GOC = Utility.DecimalDbnull(dr["SO_TIEN"]);
                    infoHDCT.NOI_DUNG = infoHDCT.KY + "-" + infoHDCT.THANG + "-" + infoHDCT.NAM;
                    lstHDonCTiet.Add(infoHDCT);
                }

                #endregion

                #region HDN_HDONCOSFI

                foreach (DataRow drCF in dsCustomerData.Tables["HDN_HDONCOSFI_GTRU"].Rows)
                {

                    HDN_HDONCOSFI_PLUS infoHDCF = new HDN_HDONCOSFI_PLUS();
                    if (drCF["COSFI"].ToString().Trim().Length > 0)
                        infoHDCF.COSFI = Convert.ToDecimal(drCF["COSFI"].ToString());
                    else
                        return "Lỗi dữ liệu HDN_HDONCOSFI: COSFI = null";
                    //MessageBox.Show("1");
                    if (drCF["HUU_CONG"].ToString().Trim().Length > 0)
                        infoHDCF.HUU_CONG = Convert.ToDecimal(drCF["HUU_CONG"].ToString());
                    else
                        return "Lỗi dữ liệu HDN_HDONCOSFI: HUU_CONG = null";
                    //MessageBox.Show("2");
                    infoHDCF.ID_HDON = Convert.ToInt64(drCF["ID_HDON"].ToString());
                    //if (drCF["ID_HDONCOSFI"].ToString().Trim().Length > 0)
                    //    infoHDCF.ID_HDONCOSFI = Convert.ToInt64(drCF["ID_HDONCOSFI"].ToString());
                    //else
                    //    return "Lỗi dữ liệu HDN_HDONCOSFI: ID_HDONCOSFI = null";
                    infoHDCF.ID_HDONCOSFI = Convert.ToInt64(drCF["ID_HDONCOSFI"].ToString());
                    if (drCF["KCOSFI"].ToString().Trim().Length > 0)
                        infoHDCF.KCOSFI = Convert.ToDecimal(drCF["KCOSFI"].ToString());
                    else
                        return "Lỗi dữ liệu HDN_HDONCOSFI: KCOSFI = null";
                    //MessageBox.Show("3");
                    if (drCF["KIMUA_CSPK"].ToString().Trim().Length > 0)
                        infoHDCF.KIMUA_CSPK = Convert.ToInt16(drCF["KIMUA_CSPK"].ToString());
                    else
                        return "Lỗi dữ liệu HDN_HDONCOSFI: KIMUA_CSPK = null";
                    if (drCF["KY"].ToString().Trim().Length > 0)
                        infoHDCF.KY = Convert.ToInt16(drCF["KY"].ToString());
                    else
                        return "Lỗi dữ liệu HDN_HDONCOSFI: KY = null";
                    //MessageBox.Show("4");
                    infoHDCF.MA_CNANG = strMaCNang;
                    infoHDCF.MA_DDO = drCF["MA_DDO"].ToString();
                    infoHDCF.MA_DVIQLY = drCF["MA_DVIQLY"].ToString();
                    infoHDCF.MA_KHANG = drCF["MA_KHANG"].ToString();
                    infoHDCF.MA_KVUC = drCF["MA_KVUC"].ToString();
                    infoHDCF.MA_LO = drCF["MA_LO"].ToString();
                    infoHDCF.MA_SOGCS = drCF["MA_SOGCS"].ToString();
                    infoHDCF.MA_TO = drCF["MA_TO"].ToString();
                    infoHDCF.MA_TRAM = drCF["MA_TRAM"].ToString();
                    //MessageBox.Show("5");
                    if (drCF["NAM"].ToString().Trim().Length > 0)
                        infoHDCF.NAM = Convert.ToInt16(drCF["NAM"].ToString());
                    else
                        return "Lỗi dữ liệu HDN_HDONCOSFI: NAM = null";
                    if (drCF["NGAY_SUA"].ToString().Trim().Length > 0)
                        infoHDCF.NGAY_SUA = Convert.ToDateTime(drCF["NGAY_SUA"].ToString());
                    else
                        return "Lỗi dữ liệu HDN_HDONCOSFI: NGAY_SUA = null";
                    if (drCF["NGAY_TAO"].ToString().Trim().Length > 0)
                        infoHDCF.NGAY_TAO = Convert.ToDateTime(drCF["NGAY_TAO"].ToString());
                    else
                        return "Lỗi dữ liệu HDN_HDONCOSFI: NGAY_TAO = null";
                    //MessageBox.Show("6");

                    infoHDCF.NGAY_TAO = DateTime.Now;
                    infoHDCF.NGAY_SUA = DateTime.Now;
                    infoHDCF.NGUOI_SUA = strTenDNhap;
                    infoHDCF.NGUOI_TAO = strTenDNhap;
                    infoHDCF.STT = drCF["STT"].ToString();
                    if (drCF["ID_CHISO"].ToString().Trim().Length > 0)
                        infoHDCF.ID_CHISO = Convert.ToInt64(drCF["ID_CHISO"]);
                    //MessageBox.Show("7");
                    if (drCF["THANG"].ToString().Trim().Length > 0)
                        infoHDCF.THANG = Convert.ToInt16(drCF["THANG"].ToString());
                    else
                        return "Lỗi dữ liệu HDN_HDONCOSFI: THANG = null";
                    if (drCF["TIEN_HUUCONG"].ToString().Trim().Length > 0)
                        infoHDCF.TIEN_HUUCONG = Convert.ToDecimal(drCF["TIEN_HUUCONG"].ToString());
                    else
                        return "Lỗi dữ liệu HDN_HDONCOSFI: TIEN_HUUCONG = null";
                    if (drCF["TIEN_VOCONG"].ToString().Trim().Length > 0)
                        infoHDCF.TIEN_VOCONG = Convert.ToDecimal(drCF["TIEN_VOCONG"].ToString());
                    else
                        return "Lỗi dữ liệu HDN_HDONCOSFI: TIEN_VOCONG = null";
                    if (drCF["VO_CONG"].ToString().Trim().Length > 0)
                        infoHDCF.VO_CONG = Convert.ToDecimal(drCF["VO_CONG"].ToString());
                    else
                        return "Lỗi dữ liệu HDN_HDONCOSFI: VO_CONG = null";

                    infoHDCF.TIEN_HC_GTRU = Utility.DecimalDbnull(drCF["TIEN_HC_GTRU"]);
                    if (drCF["TIEN_HC_GOC"].ToString().Trim().Length > 0)
                        infoHDCF.TIEN_HC_GOC = Convert.ToDecimal(drCF["TIEN_HC_GOC"].ToString());
                    else
                        infoHDCF.TIEN_HC_GOC = Utility.DecimalDbnull(drCF["TIEN_HUUCONG"]);
                    infoHDCF.TIEN_VC_GTRU = Utility.DecimalDbnull(drCF["TIEN_VC_GTRU"]);
                    if (drCF["TIEN_VC_GOC"].ToString().Trim().Length > 0)
                        infoHDCF.TIEN_VC_GOC = Convert.ToDecimal(drCF["TIEN_VC_GOC"].ToString());
                    else
                        infoHDCF.TIEN_VC_GOC = Utility.DecimalDbnull(drCF["TIEN_VOCONG"]);
                    //MessageBox.Show("8");
                    infoHDCF.NOI_DUNG = infoHDCF.KY + "-" + infoHDCF.THANG + "-" + infoHDCF.NAM;

                    lstHDonCosfi.Add(infoHDCF);
                    //MessageBox.Show("9");
                }

                #endregion
                //MessageBox.Show("End", "Thông báo", MessageBoxButtons.OK, MessageBoxIcon.Information);


                return "";

            }
            catch (Exception e)
            {
                //MessageBox.Show(strddo, "Thông báo", MessageBoxButtons.OK, MessageBoxIcon.Information);
                return "Error In CreateObject_GTRU Method: " + e.Message;
            }
        }

        #endregion

        #region Tính hóa đơn hiệu năng 2022
        public string InsertInvoiceDataPlus_2022(DataSet dsInvoiceData, DataSet dsCustomerData, string strMa_DViQLy, string strMa_SoGCS, string strTenDNhap, long lngCurrentLibID, long lngWorkflowID, short ky, short thang, short nam)
        {
            //CMIS2 db = new CMIS2();
            try
            {
                //Debug.WriteLine(DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff") + " InsertInvoiceDataPlus Start");
                List<GCS_CHISO_PLUS> lstChiSo = new List<GCS_CHISO_PLUS>();
                List<HDN_HDON_PLUS> lstHDon = new List<HDN_HDON_PLUS>();
                List<HDN_HDONCOSFI_PLUS> lstHDonCosfi = new List<HDN_HDONCOSFI_PLUS>();
                List<HDN_HDONCTIET_PLUS> lstHDonCTiet = new List<HDN_HDONCTIET_PLUS>();
                string strResult = SetPropertiesForObjectPlus(dsInvoiceData, dsCustomerData, lstChiSo, lstHDon, lstHDonCTiet, lstHDonCosfi, strTenDNhap, lngCurrentLibID.ToString());
                //Debug.WriteLine(DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff") + " InsertInvoiceDataPlus SetPropertiesForObjectPlusComplete");
                if (strResult != "") return strResult;
                if (lstHDon == null || lstHDon.Count <= 0) return "Danh sach hoa don khong co";
                if (lstHDonCTiet == null || lstHDonCTiet.Count <= 0) return "Danh sach hoa don chi tiet khong co";
                //Viet lai doan code nay de submitchange 1 lan
                //var arrIDHDon = lstHDon.Select(c => c.ID_HDON).Distinct();
                //obj_HDN_HDON_Controller.CMIS2 = db;
                //obj_HDN_HDONCTIET_Controller.CMIS2 = db;
                //obj_HDN_HDONCOSFI_Controller.CMIS2 = db;
                //obj_GCS_CHISO_Controller.CMIS2 = db;
                inpDLieuHDonPerformance inpTHD = new inpDLieuHDonPerformance();
                inpTHD.LST_CSO = lstChiSo;
                inpTHD.LST_HDON = new List<HDN_HDON_PLUS>();
                //Tach ra theo tung ma khach hang
                foreach (HDN_HDON_PLUS objHDon in lstHDon)
                {
                    if (objHDon.LST_HDCT == null) objHDon.LST_HDCT = new List<HDN_HDONCTIET_PLUS>();
                    if (objHDon.LST_HDCF == null) objHDon.LST_HDCF = new List<HDN_HDONCOSFI_PLUS>();
                    if (objHDon.LOAI_HDON == "TD")
                    {
                        List<HDN_HDON_PLUS> lstHDonVC = lstHDon.Where(c => c.MA_KHANG == objHDon.MA_KHANG && c.LOAI_HDON == "VC").ToList();
                        List<HDN_HDONCTIET_PLUS> lstHDonCTiet_Temp = new List<HDN_HDONCTIET_PLUS>();
                        if (lstHDonVC.Count > 0) lstHDonCTiet_Temp = lstHDonCTiet.Where(c => c.ID_HDON == objHDon.ID_HDON).ToList();
                        else lstHDonCTiet_Temp = lstHDonCTiet.Where(c => c.MA_KHANG == objHDon.MA_KHANG).ToList();
                        //inpTHD.LST_HDON.AddRange(lstHDon_Temp);
                        objHDon.LST_HDCT.AddRange(lstHDonCTiet_Temp);

                        if (lstHDonCosfi != null && lstHDonCosfi.Count > 0)
                        {
                            List<HDN_HDONCOSFI_PLUS> lstHDonCosfi_Temp = lstHDonCosfi.Where(c => c.MA_KHANG == objHDon.MA_KHANG).ToList();
                            if (lstHDonCosfi_Temp != null && lstHDonCosfi_Temp.Count > 0)
                            {
                                foreach (HDN_HDONCOSFI_PLUS hcosfi in lstHDonCosfi_Temp)
                                {
                                    if (lstHDonVC.Count > 0)
                                        //Là hóa đơn TD, có hóa đơn VC khác cùng mã KH, gán và ko thêm vào danh sách của HD này nữa
                                        hcosfi.ID_HDON = lstHDonVC[0].ID_HDON;
                                    else objHDon.LST_HDCF.Add(hcosfi);
                                }

                            }
                        }
                    }
                    else
                    {
                        List<HDN_HDONCTIET_PLUS> lstHDonCTiet_Temp = lstHDonCTiet.Where(c => c.ID_HDON == objHDon.ID_HDON).ToList();

                        //inpTHD.LST_HDON.AddRange(lstHDon_Temp);
                        objHDon.LST_HDCT.AddRange(lstHDonCTiet_Temp);

                        if (lstHDonCosfi != null && lstHDonCosfi.Count > 0)
                        {
                            List<HDN_HDONCOSFI_PLUS> lstHDonCosfi_Temp = lstHDonCosfi.Where(c => c.MA_KHANG == objHDon.MA_KHANG).ToList();
                            if (lstHDonCosfi_Temp != null && lstHDonCosfi_Temp.Count > 0)
                            {
                                foreach (HDN_HDONCOSFI_PLUS hcosfi in lstHDonCosfi_Temp)
                                {
                                    hcosfi.ID_HDON = objHDon.ID_HDON;
                                    objHDon.LST_HDCF.Add(hcosfi);
                                }

                            }
                        }
                    }





                }
                inpTHD.LST_HDON = lstHDon;
                //Debug.WriteLine(DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff") + " InsertInvoiceDataPlus TachKH");

                //Insert luong so ghi chi so
                string strInput = JsonConvert.SerializeObject(inpTHD);
                //Debug.WriteLine(DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff") + " InsertInvoiceDataPlus SerializeObject");
                //Gọi hàm insert
                string strIP = ConfigurationManager.AppSettings["URI"];
                string strIP_Per = ConfigurationManager.AppSettings["URI_PERFORMANCE"];
                //var baseAddress = "http://10.1.117.177:7001/ServiceHDonPSinh/resources/serviceHDonPSinh/insertTHDPerformance";
                //var baseAddress = "http://" + strIP + "/ServiceHDonPSinh-HDonPSinh-context-root/resources/serviceHDonPSinh/insertTHDPerformance";
                var baseAddress = "http://" + strIP_Per + "/api/hdon/insertTHDPerformance";
                var http = (HttpWebRequest)WebRequest.Create(new Uri(baseAddress));
                http.Accept = "application/json";
                http.ContentType = "application/json";
                http.Method = "POST";

                //string parsedContent = "{'MA_DVIQLY':'PC06AA','TEN_DANH_MUC':'D_SOGCS'}";
                string parsedContent = strInput;
                //ASCIIEncoding encoding = new ASCIIEncoding();
                Byte[] bytes = System.Text.Encoding.UTF8.GetBytes(parsedContent);

                Stream newStream = http.GetRequestStream();
                newStream.Write(bytes, 0, bytes.Length);
                newStream.Close();

                var response = http.GetResponse();
                //Debug.WriteLine(DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff") + " InsertInvoiceDataPlus GetResponse");
                var stream = response.GetResponseStream();
                var sr = new StreamReader(stream);
                var content = sr.ReadToEnd();
                ReturnObject obj = JsonConvert.DeserializeObject<ReturnObject>(content);
                //Debug.WriteLine(DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff") + " InsertInvoiceDataPlus DeserializeObject");
                if (obj.TYPE == "ERROR") return obj.MESSAGE;
                else return "";

            }
            catch (Exception ex)
            {
                return "Lỗi khi Insert dữ liệu hóa đơn với mã sổ= " + strMa_SoGCS + ": " + ex.Message;
            }
            finally
            {
                //db.ReleaseConnection();
            }
        }
        public string InsertInvoiceDataPlus_DCN(DataSet dsInvoiceData, DataSet dsInvoiceDataDCN, DataSet dsCustomerData, string strMa_DViQLy, string strMa_SoGCS, string strTenDNhap, long lngCurrentLibID, long lngWorkflowID, short ky, short thang, short nam)
        {
            //CMIS2 db = new CMIS2();
            try
            {
                //Debug.WriteLine(DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff") + " InsertInvoiceDataPlus Start");
                List<GCS_CHISO_PLUS> lstChiSo = new List<GCS_CHISO_PLUS>();
                List<HDN_HDON_PLUS> lstHDon = new List<HDN_HDON_PLUS>();
                List<HDN_HDONCOSFI_PLUS> lstHDonCosfi = new List<HDN_HDONCOSFI_PLUS>();
                List<HDN_HDONCTIET_PLUS> lstHDonCTiet = new List<HDN_HDONCTIET_PLUS>();

                List<GCS_CHISO_PLUS> lstChiSoDCN = new List<GCS_CHISO_PLUS>();
                List<HDN_HDON_PLUS> lstHDonBT = new List<HDN_HDON_PLUS>();
                List<HDN_HDONCOSFI_PLUS> lstHDonCosfiBT = new List<HDN_HDONCOSFI_PLUS>();
                List<HDN_HDONCTIET_PLUS> lstHDonCTietBT = new List<HDN_HDONCTIET_PLUS>();
                string strResult = SetPropertiesForObjectPlus(dsInvoiceDataDCN, dsCustomerData, lstChiSoDCN, lstHDonBT, lstHDonCTietBT, lstHDonCosfiBT, strTenDNhap, lngCurrentLibID.ToString());
                //Debug.WriteLine(DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff") + " InsertInvoiceDataPlus SetPropertiesForObjectPlusComplete");
                if (strResult != "") return strResult;
                //if (lstHDonBT == null || lstHDonBT.Count <= 0) return "Danh sach hoa don DCN khong co";
                //if (lstHDonCTietBT == null || lstHDonCTietBT.Count <= 0) return "Danh sach hoa don chi tiet DCN khong co";
                //Đổi lại tên bảng về GCS_CHISO để update
                #region Đổi lại tên bảng về GCS_CHISO để update
                dsCustomerData.Tables["GCS_CHISO"].TableName = "GCS_CSO_DCN";
                dsCustomerData.Tables["GCS_CHISO_GT"].TableName = "GCS_CSO_DCN_GT";
                dsCustomerData.Tables["GCS_CHISO_TP"].TableName = "GCS_CSO_DCN_TP"; ;
                dsCustomerData.Tables["GCS_CHISO_BQ"].TableName = "GCS_CSO_DCN_BQ" ;
                dsCustomerData.Tables["GCS_CHISO_DDK"].TableName = "GCS_CHISO";
                dsCustomerData.Tables["GCS_CHISO_DDK_GT"].TableName = "GCS_CHISO_GT";
                dsCustomerData.Tables["GCS_CHISO_DDK_TP"].TableName = "GCS_CHISO_TP";
                dsCustomerData.Tables["GCS_CHISO_DDK_BQ"].TableName = "GCS_CHISO_BQ";
                #endregion
                strResult = SetPropertiesForObjectPlus(dsInvoiceData, dsCustomerData, lstChiSo, lstHDon, lstHDonCTiet, lstHDonCosfi, strTenDNhap, lngCurrentLibID.ToString());
                //Debug.WriteLine(DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff") + " InsertInvoiceDataPlus SetPropertiesForObjectPlusComplete");
                if (strResult != "") return strResult;
                if (lstHDon == null || lstHDon.Count <= 0) return "Danh sach hoa don khong co";
                if (lstHDonCTiet == null || lstHDonCTiet.Count <= 0) return "Danh sach hoa don chi tiet khong co";
                



                inpDLieuHDonPerformance inpTHD = new inpDLieuHDonPerformance();
                inpTHD.LST_CSO = lstChiSo;
                inpTHD.LST_HDON = new List<HDN_HDON_PLUS>();
                //Tach ra theo tung ma khach hang
                foreach (HDN_HDON_PLUS objHDon in lstHDon)
                {
                    if (objHDon.LST_HDCT == null) objHDon.LST_HDCT = new List<HDN_HDONCTIET_PLUS>();
                    if (objHDon.LST_HDCF == null) objHDon.LST_HDCF = new List<HDN_HDONCOSFI_PLUS>();
                    if (objHDon.LOAI_HDON == "TD")
                    {
                        List<HDN_HDON_PLUS> lstHDonVC = lstHDon.Where(c => c.MA_KHANG == objHDon.MA_KHANG && c.LOAI_HDON == "VC").ToList();
                        List<HDN_HDONCTIET_PLUS> lstHDonCTiet_Temp = new List<HDN_HDONCTIET_PLUS>();
                        if (lstHDonVC.Count > 0) lstHDonCTiet_Temp = lstHDonCTiet.Where(c => c.ID_HDON == objHDon.ID_HDON).ToList();
                        else lstHDonCTiet_Temp = lstHDonCTiet.Where(c => c.MA_KHANG == objHDon.MA_KHANG).ToList();
                        //inpTHD.LST_HDON.AddRange(lstHDon_Temp);
                        objHDon.LST_HDCT.AddRange(lstHDonCTiet_Temp);

                        if (lstHDonCosfi != null && lstHDonCosfi.Count > 0)
                        {
                            List<HDN_HDONCOSFI_PLUS> lstHDonCosfi_Temp = lstHDonCosfi.Where(c => c.MA_KHANG == objHDon.MA_KHANG).ToList();
                            if (lstHDonCosfi_Temp != null && lstHDonCosfi_Temp.Count > 0)
                            {
                                foreach (HDN_HDONCOSFI_PLUS hcosfi in lstHDonCosfi_Temp)
                                {
                                    if (lstHDonVC.Count > 0)
                                        //Là hóa đơn TD, có hóa đơn VC khác cùng mã KH, gán và ko thêm vào danh sách của HD này nữa
                                        hcosfi.ID_HDON = lstHDonVC[0].ID_HDON;
                                    else objHDon.LST_HDCF.Add(hcosfi);
                                }
                            }
                        }
                    }
                    else
                    {
                        List<HDN_HDONCTIET_PLUS> lstHDonCTiet_Temp = lstHDonCTiet.Where(c => c.ID_HDON == objHDon.ID_HDON).ToList();

                        //inpTHD.LST_HDON.AddRange(lstHDon_Temp);
                        objHDon.LST_HDCT.AddRange(lstHDonCTiet_Temp);

                        if (lstHDonCosfi != null && lstHDonCosfi.Count > 0)
                        {
                            List<HDN_HDONCOSFI_PLUS> lstHDonCosfi_Temp = lstHDonCosfi.Where(c => c.MA_KHANG == objHDon.MA_KHANG).ToList();
                            if (lstHDonCosfi_Temp != null && lstHDonCosfi_Temp.Count > 0)
                            {
                                foreach (HDN_HDONCOSFI_PLUS hcosfi in lstHDonCosfi_Temp)
                                {
                                    hcosfi.ID_HDON = objHDon.ID_HDON;
                                    objHDon.LST_HDCF.Add(hcosfi);
                                }
                            }
                        }
                    }





                }
                inpTHD.LST_HDON = lstHDon;

                #region DCN
                inpTHD.LST_HDON_BT = new List<HDN_HDON_PLUS>();
                if(lstHDonBT!=null&& lstHDonBT.Count>0)
                {
                    foreach (HDN_HDON_PLUS objHDon in lstHDonBT)
                    {
                        if (objHDon.LST_HDCT == null) objHDon.LST_HDCT = new List<HDN_HDONCTIET_PLUS>();
                        if (objHDon.LST_HDCF == null) objHDon.LST_HDCF = new List<HDN_HDONCOSFI_PLUS>();
                        if (objHDon.LOAI_HDON == "TD")
                        {
                            List<HDN_HDON_PLUS> lstHDonVC = lstHDonBT.Where(c => c.MA_KHANG == objHDon.MA_KHANG && c.LOAI_HDON == "VC").ToList();
                            List<HDN_HDONCTIET_PLUS> lstHDonCTiet_Temp = new List<HDN_HDONCTIET_PLUS>();
                            if (lstHDonVC.Count > 0) lstHDonCTiet_Temp = lstHDonCTietBT.Where(c => c.ID_HDON == objHDon.ID_HDON).ToList();
                            else lstHDonCTiet_Temp = lstHDonCTietBT.Where(c => c.MA_KHANG == objHDon.MA_KHANG).ToList();
                            //inpTHD.LST_HDON.AddRange(lstHDon_Temp);
                            objHDon.LST_HDCT.AddRange(lstHDonCTiet_Temp);

                            if (lstHDonCosfiBT != null && lstHDonCosfiBT.Count > 0)
                            {
                                List<HDN_HDONCOSFI_PLUS> lstHDonCosfi_Temp = lstHDonCosfiBT.Where(c => c.MA_KHANG == objHDon.MA_KHANG).ToList();
                                if (lstHDonCosfi_Temp != null && lstHDonCosfi_Temp.Count > 0)
                                {
                                    foreach (HDN_HDONCOSFI_PLUS hcosfi in lstHDonCosfi_Temp)
                                    {
                                        if (lstHDonVC.Count > 0)
                                            //Là hóa đơn TD, có hóa đơn VC khác cùng mã KH, gán và ko thêm vào danh sách của HD này nữa
                                            hcosfi.ID_HDON = lstHDonVC[0].ID_HDON;
                                        else objHDon.LST_HDCF.Add(hcosfi);
                                    }

                                }
                            }
                        }
                        else
                        {
                            List<HDN_HDONCTIET_PLUS> lstHDonCTiet_Temp = lstHDonCTietBT.Where(c => c.ID_HDON == objHDon.ID_HDON).ToList();

                            //inpTHD.LST_HDON.AddRange(lstHDon_Temp);
                            objHDon.LST_HDCT.AddRange(lstHDonCTiet_Temp);

                            if (lstHDonCosfiBT != null && lstHDonCosfiBT.Count > 0)
                            {
                                List<HDN_HDONCOSFI_PLUS> lstHDonCosfi_Temp = lstHDonCosfiBT.Where(c => c.MA_KHANG == objHDon.MA_KHANG).ToList();
                                if (lstHDonCosfi_Temp != null && lstHDonCosfi_Temp.Count > 0)
                                {
                                    foreach (HDN_HDONCOSFI_PLUS hcosfi in lstHDonCosfi_Temp)
                                    {
                                        hcosfi.ID_HDON = objHDon.ID_HDON;
                                        objHDon.LST_HDCF.Add(hcosfi);
                                    }

                                }
                            }
                        }





                    }
                    inpTHD.LST_HDON_BT = lstHDonBT;
                }    
                //Tach ra theo tung ma khach hang
                
                #endregion

                //Debug.WriteLine(DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff") + " InsertInvoiceDataPlus TachKH");

                //Insert luong so ghi chi so
                string strInput = JsonConvert.SerializeObject(inpTHD);
                //Debug.WriteLine(DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff") + " InsertInvoiceDataPlus SerializeObject");
                //Gọi hàm insert
                string strIP = ConfigurationManager.AppSettings["URI"];
                string strIP_Per = ConfigurationManager.AppSettings["URI_PERFORMANCE"];
                //var baseAddress = "http://10.1.117.177:7001/ServiceHDonPSinh/resources/serviceHDonPSinh/insertTHDPerformance";
                //var baseAddress = "http://" + strIP + "/ServiceHDonPSinh-HDonPSinh-context-root/resources/serviceHDonPSinh/insertTHDPerformance";
                var baseAddress = "http://" + strIP_Per + "/api/hdon/insertTHDPerformance";
                var http = (HttpWebRequest)WebRequest.Create(new Uri(baseAddress));
                http.Accept = "application/json";
                http.ContentType = "application/json";
                http.Method = "POST";

                //string parsedContent = "{'MA_DVIQLY':'PC06AA','TEN_DANH_MUC':'D_SOGCS'}";
                string parsedContent = strInput;
                //ASCIIEncoding encoding = new ASCIIEncoding();
                Byte[] bytes = System.Text.Encoding.UTF8.GetBytes(parsedContent);

                Stream newStream = http.GetRequestStream();
                newStream.Write(bytes, 0, bytes.Length);
                newStream.Close();

                var response = http.GetResponse();
                //Debug.WriteLine(DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff") + " InsertInvoiceDataPlus GetResponse");
                var stream = response.GetResponseStream();
                var sr = new StreamReader(stream);
                var content = sr.ReadToEnd();
                ReturnObject obj = JsonConvert.DeserializeObject<ReturnObject>(content);
                //Debug.WriteLine(DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff") + " InsertInvoiceDataPlus DeserializeObject");
                if (obj.TYPE == "ERROR") return obj.MESSAGE;
                else return "";

            }
            catch (Exception ex)
            {
                return "Lỗi khi Insert dữ liệu hóa đơn với mã sổ= " + strMa_SoGCS + ": " + ex.Message;
            }
            finally
            {
                //db.ReleaseConnection();
            }
        }

        #endregion
    }
}
