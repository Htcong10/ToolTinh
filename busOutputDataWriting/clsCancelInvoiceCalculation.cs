using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using OutputDataObject;
using System.Data;
using busBillingWF;
using DbConnect.DB;

namespace busOutputDataWriting
{
    public class clsCancelInvoiceCalculation
    {
        #region Attributes
        cls_HDN_HDON_Controller obj_HDN_HDON_Controller;
        cls_HDN_HDONCOSFI_Controller obj_HDN_HDONCOSFI_Controller;
        cls_HDN_HDONCTIET_Controller obj_HDN_HDONCTIET_Controller;
        cls_GCS_CHISO_Controller obj_GCS_CHISO_Controller;
        cls_Workflows_GCS objWF;

        #endregion

        #region Constructor
        public clsCancelInvoiceCalculation()
        {
            obj_HDN_HDON_Controller = new cls_HDN_HDON_Controller();
            obj_HDN_HDONCOSFI_Controller = new cls_HDN_HDONCOSFI_Controller();
            obj_HDN_HDONCTIET_Controller = new cls_HDN_HDONCTIET_Controller();
            obj_GCS_CHISO_Controller = new cls_GCS_CHISO_Controller();
        }
        #endregion

        #region Method DũngNT
        public string CancelInvoiceCalculation(string strMaDViQLy, string[] strMaSoGCs, string strTenDNhap, long lngCurrentLibID, long lngWorkflowID, Int16 i16Ky, Int16 i16Thang, Int16 i16Nam)
        {
            CMIS2 db = new CMIS2();
            if (strMaSoGCs.Length == 0)
            {
                return "";
            }

            try
            {
                DataSet ds = new GCS_SOGCS_XULY_Entity();
                DataRow dr = ds.Tables["GCS_SOGCS_XULY"].NewRow();
                dr["CURRENTLIBID"] = lngCurrentLibID;
                dr["KY"] = i16Ky;
                dr["MA_CNANG"] = lngCurrentLibID.ToString();
                dr["MA_DVIQLY"] = strMaDViQLy;
                dr["MA_SOGCS"] = "";
                dr["NAM"] = i16Nam;
                dr["NGAY_SUA"] = DateTime.Now;
                dr["NGAY_TAO"] = DateTime.Now;
                dr["NGUOI_SUA"] = strTenDNhap;
                dr["NGUOI_TAO"] = strTenDNhap;
                dr["NGUOI_THIEN"] = strTenDNhap;
                dr["THANG"] = i16Thang;
                dr["WORKFLOWID"] = lngWorkflowID;
                ds.Tables["GCS_SOGCS_XULY"].Rows.Add(dr);
                bool ok = true;
                string[] strListMaSo = new string[1];
                string strResult = "", strListError = "";
                obj_HDN_HDON_Controller.CMIS2 = db;
                obj_HDN_HDONCTIET_Controller.CMIS2 = db;
                obj_HDN_HDONCOSFI_Controller.CMIS2 = db;
                obj_GCS_CHISO_Controller.CMIS2 = db;

                foreach (string strMaSo in strMaSoGCs)
                {
                    if (strResult != "")
                    {
                        strListError = strListError + strResult + "\r\n";
                        strResult = "";
                    }

                    ds.Tables["GCS_SOGCS_XULY"].Rows[0]["MA_SOGCS"] = strMaSo;
                    objWF = new cls_Workflows_GCS(ds);
                    objWF.CMIS2 = db;
                    objWF.getCurrentState();
                    objWF.getConfigState();
                    ok = objWF.deleteOnUndo();

                    if (ok == false)
                    {
                        strResult = "Sổ " + strMaSo + ": lỗi khi huỷ trạng thái tính hoá đơn";
                        continue; //Loi, chuyen den so sau
                    }
                    
                    strListMaSo[0] = strMaSo;
                    var hdon = obj_HDN_HDON_Controller.getHDonByMaSoGCS(strMaDViQLy, strMaSo, i16Ky, i16Thang, i16Nam);

                    if (hdon != null && hdon.Count() != 0)
                    {
                        List<HDN_HDON> HDN_HDON_delete = new List<HDN_HDON>();
                        List<HDN_HDONCTIET> HDN_HDONCTIET_delete = new List<HDN_HDONCTIET>();
                        List<HDN_HDONCOSFI> HDN_HDONCOSFI_delete = new List<HDN_HDONCOSFI>();
                        List<GCS_CHISO> GCS_CHISO_delete = new List<GCS_CHISO>();
                        
                        
                        long[] lngID_HDon = (from a in hdon select a.ID_HDON).ToArray();
                        //foreach (var a in hdon)
                        //{

                        //    //lấy ra chi tiet, trả vê mã điểm đo
                        //    //Xóa cosfi theo mã điêm đo
                        //    //Xóa chi tiết theo mã điểm đo
                        //    //Cập nhật GCS_CHISO
                        //}

                        List<string> arrMaDDo = obj_HDN_HDONCTIET_Controller.getMA_DDO(strMaDViQLy, lngID_HDon, i16Ky, i16Thang, i16Nam);
                        if (arrMaDDo != null && arrMaDDo.Count != 0)
                        {                          

                            
                            strResult = obj_HDN_HDONCOSFI_Controller.DeleteHDN_HDONCOSFI(strMaDViQLy, arrMaDDo.ToArray(), i16Ky, i16Thang, i16Nam);
                            if (strResult != "")
                            {
                                strResult = "Sổ " + strMaSo + ": " + strResult;
                                continue;
                            }

                            strResult = obj_HDN_HDONCTIET_Controller.DeleteHDN_HDONCTIET(strMaDViQLy, arrMaDDo.ToArray(), i16Ky, i16Thang, i16Nam);
                            if (strResult != "")
                            {
                                strResult = "Sổ " + strMaSo + ": " + strResult;
                                continue;
                            }
                            //Lấy danh sách điểm đo chuẩn
                            //Gồm các điểm đo thuộc sổ
                            var arrDDoChinh = (from a in db.DB.HDG_QHE_DDO
                                                 where a.MA_DVIQLY == strMaDViQLy
                                                 && a.MA_SOGCS_CHINH == strMaSo
                                                 && a.NAM == i16Nam
                                                 && a.THANG == i16Thang
                                                 && a.KY == i16Ky
                                                 && a.LOAI_QHE != "40"
                                                    select a.MA_DDO_CHINH).Distinct().ToList();
                            //Và các điểm đo phụ ghép tổng
                            var arrDDoPhuGT = (from a in db.DB.HDG_QHE_DDO
                                                    where a.MA_DVIQLY == strMaDViQLy
                                                    && a.MA_SOGCS_CHINH == strMaSo
                                                    && a.NAM == i16Nam
                                                    && a.THANG == i16Thang
                                                    && a.KY == i16Ky
                                                    && a.LOAI_QHE == "40"
                                                    select a.MA_DDO_PHU).Distinct().ToList();
                            List<string> lstDDoUpdateSLTP = new List<string>();
                            if (arrDDoChinh == null || arrDDoChinh.Count <= 0)
                                lstDDoUpdateSLTP = arrMaDDo;
                            else
                                lstDDoUpdateSLTP = arrMaDDo.Union(arrDDoChinh).Union(arrDDoPhuGT).ToList();
                            strResult = obj_GCS_CHISO_Controller.UpdateSLTP(strMaDViQLy, lstDDoUpdateSLTP.ToArray(), i16Ky, i16Thang, i16Nam);
                            if (strResult != "")
                            {
                                strResult = "Sổ " + strMaSo + ": " + strResult;
                                continue;
                            }
                        }
                    }
                    
                    strResult = obj_HDN_HDON_Controller.DeleteHDN_HDON(hdon);
                    if (strResult != "")
                    {
                        strResult = "Sổ " + strMaSo + ": " + strResult;
                        continue;
                    }

                    try
                    {
                        //obj_HDN_HDON_Controller.InsertNhatKy(strMaDViQLy, "PSINH_HDON", "MA_SOGCS", strMaSo, i16Ky, i16Thang, i16Nam, "Hủy tính hóa đơn cho sổ", strTenDNhap, lngCurrentLibID.ToString());
                        db.DB.SubmitChanges(1); //Cap nhat du lieu khi thanh cong theo tung so
                    }
                    catch
                    {
                        strResult = "Sổ " + strMaSo + ": Lỗi khi đẩy dữ liệu vào DB";
                    }
                }

                //Gan cho item cuoi cung trong danh sach so
                if (strResult != "")
                {
                    strListError = strListError + strResult + "\r\n";
                    strResult = "";
                }                

                if (strListError != "")
                {
                    return strListError;
                }
                else
                {
                    return "";
                }                
            }
            catch (Exception ex)
            {
                return "Lỗi khi thực hiện huỷ tính hoá đơn: " + ex.Message;
            }
            finally
            {
                db.ReleaseConnection();
            }
        }
        #endregion

        #region Huy hoa don le theo khach hang
        public string DeleteCustomerInvoice(string strMaDViQLy, string strMa_KHang, short ky, short thang, short nam)
        {
            CMIS2 db = new CMIS2();
            
            try
            {
                string strResult = "";
                obj_HDN_HDON_Controller.CMIS2 = db;
                obj_HDN_HDONCTIET_Controller.CMIS2 = db;
                obj_HDN_HDONCOSFI_Controller.CMIS2 = db;
                obj_GCS_CHISO_Controller.CMIS2 = db;

                var hdon = obj_HDN_HDON_Controller.GetHDon_MaKHang(strMaDViQLy, strMa_KHang, ky, thang, nam);
                if (hdon != null && hdon.Count() != 0)
                {
                    long[] lngID_HDon = (from a in hdon select a.ID_HDON).ToArray();
                    List<string> arrMaDDo = obj_HDN_HDONCTIET_Controller.getMA_DDO(strMaDViQLy, lngID_HDon, ky, thang, nam);
                    if (arrMaDDo != null && arrMaDDo.Count != 0)
                    {
                        strResult = obj_HDN_HDONCOSFI_Controller.DeleteHDN_HDONCOSFI(strMaDViQLy, arrMaDDo.ToArray(), ky, thang, nam);
                        if (strResult != "")
                        {
                            strResult = "Lỗi huỷ hoá đơn của khách hàng " + strMa_KHang + ": " + strResult;
                            return strResult;
                        }

                        strResult = obj_HDN_HDONCTIET_Controller.DeleteHDN_HDONCTIET(strMaDViQLy, arrMaDDo.ToArray(), ky, thang, nam);
                        if (strResult != "")
                        {
                            strResult = "Lỗi huỷ hoá đơn của khách hàng " + strMa_KHang + ": " + strResult;
                            return strResult;
                        }

                        strResult = obj_GCS_CHISO_Controller.UpdateSLTP(strMaDViQLy, arrMaDDo.ToArray(), ky, thang, nam);
                        if (strResult != "")
                        {
                            strResult = "Lỗi huỷ hoá đơn của khách hàng " + strMa_KHang + ": " + strResult;
                            return strResult;
                        }
                    }
                }

                strResult = obj_HDN_HDON_Controller.DeleteHDN_HDON(hdon);
                if (strResult != "")
                {
                    strResult = "Lỗi huỷ hoá đơn của khách hàng " + strMa_KHang + ": " + strResult;
                    return strResult;
                }

                db.DB.SubmitChanges(); //Cap nhat du lieu khi thanh cong theo tung khach hang
                
                return "";
            }
            catch (Exception ex)
            {
                return "Lỗi huỷ hoá đơn của khách hàng: " + ex.Message;
            }
            finally
            {
                db.ReleaseConnection();
            }
        }
        #endregion
    }
}
