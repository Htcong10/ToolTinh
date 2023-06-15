using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using InputDataObject;
using System.Windows.Forms;
using System.Collections;
using System.Data.OracleClient;
using CMISLibrary;
using QTObject;
using DbConnect.DB;
using System.Security.Cryptography;
using System.Net;
using System.IO;
using System.Configuration;
using Newtonsoft.Json;
using System.Reflection;
using BillingLibrary;
using System.Diagnostics;
using System.Threading.Tasks;
using System.Net.Http;
using System.Net.Http.Headers;

namespace busInputDataReading
{
    public class cls_InputDataReading 
    {
        #region Attributes
        cls_D_BAC_THANG_Controller obj_D_BAC_THANG_Controller;
        cls_D_CAP_DAP_Controller obj_D_CAP_DAP_Controller;
        cls_D_THAMCHIEU_CAPDA_Controller obj_D_THAMCHIEU_CAPDA_Controller;
        cls_D_COSFI_Controller obj_D_COSFI_Controller;
        cls_D_GIA_NHOMNN_Controller obj_D_GIA_NHOMNN_Controller;
        cls_D_NGANH_NGHE_Controller obj_D_NGANH_NGHE_Controller;
        cls_D_NHOM_NN_Controller obj_D_NHOM_NN_Controller;
        cls_D_SOGCS_Controller obj_D_SOGCS_Controller;
        cls_HDG_DIEM_DO_Controller obj_HDG_DIEM_DO_Controller;
        cls_HDG_DDO_SOGCS_Controller obj_HDG_DDO_SOGCS_Controller;
        cls_HDG_QHE_DDO_Controller obj_HDG_QHE_DDO_Controller;
        cls_HDG_BBAN_APGIA_Controller obj_HDG_BBAN_APGIA_Controller;
        cls_HDG_KHACH_HANG_Controller obj_HDG_KHACH_HANG_Controller;
        cls_HDG_VITRI_DDO_Controller obj_HDG_VITRI_DDO_Controller;
        cls_GCS_LICHGCS_Controller obj_GCS_LICHGCS_Controller;
        cls_GCS_SOGCS_XULY_Controller obj_GCS_SOGCS_XULY_Controller;
        cls_GCS_CHISO_Controller obj_GCS_CHISO_Controller;
        cls_HDG_PTHUC_TTOAN_Controller obj_HDG_PTHUC_TTOAN_Controller;
        cls_D_TY_GIA_Controller obj_D_TY_GIA_Controller;
        cls_S_PARAMETER_Controller obj_S_PARAMETERS_Controller;
        const string key = "VI27NA10MILK2010";//Đang uống sữa
        #endregion
        private bool IsDisposed = false;
        #region Constructor
        /// <summary>
        /// Hàm khởi tạo
        /// </summary>
        /// <param name="dsSoGCS">DataSet chứa thông tin sổ cần tính, lấy từ trên Client</param>        
        public cls_InputDataReading()
        {
            //Phần khởi tạo này đang suy nghĩ xem có nên chuyển xuống lúc gọi hàm mới khởi tạo hay không 
            obj_D_BAC_THANG_Controller = new cls_D_BAC_THANG_Controller();
            obj_D_CAP_DAP_Controller = new cls_D_CAP_DAP_Controller();
            obj_D_THAMCHIEU_CAPDA_Controller = new cls_D_THAMCHIEU_CAPDA_Controller();
            obj_D_COSFI_Controller = new cls_D_COSFI_Controller();
            obj_D_GIA_NHOMNN_Controller = new cls_D_GIA_NHOMNN_Controller();
            obj_D_NGANH_NGHE_Controller = new cls_D_NGANH_NGHE_Controller();
            obj_D_NHOM_NN_Controller = new cls_D_NHOM_NN_Controller();
            obj_D_SOGCS_Controller = new cls_D_SOGCS_Controller();
            obj_HDG_DIEM_DO_Controller = new cls_HDG_DIEM_DO_Controller();
            obj_HDG_DDO_SOGCS_Controller = new cls_HDG_DDO_SOGCS_Controller();
            obj_HDG_QHE_DDO_Controller = new cls_HDG_QHE_DDO_Controller();
            obj_HDG_BBAN_APGIA_Controller = new cls_HDG_BBAN_APGIA_Controller();
            obj_HDG_KHACH_HANG_Controller = new cls_HDG_KHACH_HANG_Controller();
            obj_HDG_VITRI_DDO_Controller = new cls_HDG_VITRI_DDO_Controller();
            obj_GCS_LICHGCS_Controller = new cls_GCS_LICHGCS_Controller();
            obj_GCS_SOGCS_XULY_Controller = new cls_GCS_SOGCS_XULY_Controller();
            obj_GCS_CHISO_Controller = new cls_GCS_CHISO_Controller();
            obj_HDG_PTHUC_TTOAN_Controller = new cls_HDG_PTHUC_TTOAN_Controller();
            obj_D_TY_GIA_Controller = new cls_D_TY_GIA_Controller();
            obj_S_PARAMETERS_Controller = new cls_S_PARAMETER_Controller();
        }
        #endregion
/*        ~cls_InputDataReading()
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
        }*/
        #region Method DũngNT
        public string checkPhuGhepTong(string strMaDViQLy, string strMaSoGCS, Int16 i16Ky, Int16 i16Thang, Int16 i16Nam)
        {
            CMIS2 db = new CMIS2();
            try
            {
                string strResult = "";
                db.DB.Sp_phugheptong(strMaDViQLy, strMaSoGCS, i16Ky, i16Thang, i16Nam, out strResult);
                return strResult;
            }
            catch (Exception ex)
            {
                return "Lỗi khi lấy dữ liệu: " + ex.Message;
            }
            finally
            {
                db.ReleaseConnection();
            }
        }

        public string getCustomerDataReading(ref DataSet dsCustomerData, string strMaDViQLy, string strMaSoGCS, Int16 i16Ky, Int16 i16Thang, Int16 i16Nam)
        {
            CMIS2 db = new CMIS2();
            dsCustomerData.Clear();
            dsCustomerData = new DataSet();
            try
            {
                db.DB.Sp_getcustomerdata(strMaDViQLy, strMaSoGCS, i16Ky, i16Thang, i16Nam,
                                         out dsCustomerData, out dsCustomerData, out dsCustomerData, out dsCustomerData, out dsCustomerData, out dsCustomerData, out dsCustomerData, out dsCustomerData, out dsCustomerData, out dsCustomerData,
                                         out dsCustomerData, out dsCustomerData, out dsCustomerData, out dsCustomerData, out dsCustomerData, out dsCustomerData, out dsCustomerData, out dsCustomerData, out dsCustomerData, out dsCustomerData,
                                         out dsCustomerData, out dsCustomerData, out dsCustomerData);

                for (Int16 iDs = 0; iDs < 23; iDs++)
                {
                    switch (iDs)
                    {
                        case 0:
                            if (dsCustomerData.Tables[iDs] != null && dsCustomerData.Tables[iDs].Rows.Count != 0)
                            {
                                dsCustomerData.Tables[iDs].TableName = "HDG_DDO_SOGCS";
                            }
                            break;
                        case 1:
                            if (dsCustomerData.Tables[iDs] != null && dsCustomerData.Tables[iDs].Rows.Count != 0)
                            {
                                dsCustomerData.Tables[iDs].TableName = "HDG_QHE_DDO";
                            }
                            break;
                        case 2:
                            if (dsCustomerData.Tables[iDs] != null && dsCustomerData.Tables[iDs].Rows.Count != 0)
                            {
                                dsCustomerData.Tables[iDs].TableName = "HDG_DIEM_DO";
                            }
                            break;
                        case 3:
                            if (dsCustomerData.Tables[iDs] != null && dsCustomerData.Tables[iDs].Rows.Count != 0)
                            {
                                dsCustomerData.Tables[iDs].TableName = "HDG_BBAN_APGIA";
                            }
                            break;
                        case 4:
                            if (dsCustomerData.Tables[iDs] != null && dsCustomerData.Tables[iDs].Rows.Count != 0)
                            {
                                dsCustomerData.Tables[iDs].TableName = "HDG_KHACH_HANG";
                            }
                            break;
                        case 5:
                            if (dsCustomerData.Tables[iDs] != null && dsCustomerData.Tables[iDs].Rows.Count != 0)
                            {
                                dsCustomerData.Tables[iDs].TableName = "HDG_PTHUC_TTOAN";
                            }
                            break;
                        case 6:
                            if (dsCustomerData.Tables[iDs] != null && dsCustomerData.Tables[iDs].Rows.Count != 0)
                            {
                                dsCustomerData.Tables[iDs].TableName = "HDG_VITRI_DDO";
                            }
                            break;
                        case 7:
                            if (dsCustomerData.Tables[iDs] != null && dsCustomerData.Tables[iDs].Rows.Count != 0)
                            {
                                dsCustomerData.Tables[iDs].TableName = "D_SOGCS";
                            }
                            break;
                        case 8:
                            if (dsCustomerData.Tables[iDs] != null && dsCustomerData.Tables[iDs].Rows.Count != 0)
                            {
                                dsCustomerData.Tables[iDs].TableName = "GCS_LICHGCS";
                            }
                            break;
                        case 9:
                            if (dsCustomerData.Tables[iDs] != null && dsCustomerData.Tables[iDs].Rows.Count != 0)
                            {
                                dsCustomerData.Tables[iDs].TableName = "GCS_CHISO";
                            }
                            break;
                        case 10:
                            if (dsCustomerData.Tables[iDs] != null && dsCustomerData.Tables[iDs].Rows.Count != 0)
                            {
                                dsCustomerData.Tables[iDs].TableName = "HDG_DIEM_DO_GT";
                            }
                            break;
                        case 11:
                            if (dsCustomerData.Tables[iDs] != null && dsCustomerData.Tables[iDs].Rows.Count != 0)
                            {
                                dsCustomerData.Tables[iDs].TableName = "HDG_DDO_SOGCS_GT";
                            }
                            break;
                        case 12:
                            if (dsCustomerData.Tables[iDs] != null && dsCustomerData.Tables[iDs].Rows.Count != 0)
                            {
                                dsCustomerData.Tables[iDs].TableName = "HDG_BBAN_APGIA_GT";
                            }
                            break;
                        case 13:
                            if (dsCustomerData.Tables[iDs] != null && dsCustomerData.Tables[iDs].Rows.Count != 0)
                            {
                                dsCustomerData.Tables[iDs].TableName = "D_SOGCS_GT";
                            }
                            break;
                        case 14:
                            if (dsCustomerData.Tables[iDs] != null && dsCustomerData.Tables[iDs].Rows.Count != 0)
                            {
                                dsCustomerData.Tables[iDs].TableName = "GCS_LICHGCS_GT";
                            }
                            break;
                        case 15:
                            if (dsCustomerData.Tables[iDs] != null && dsCustomerData.Tables[iDs].Rows.Count != 0)
                            {
                                dsCustomerData.Tables[iDs].TableName = "GCS_CHISO_TP";
                            }
                            break;
                        case 16:
                            if (dsCustomerData.Tables[iDs] != null && dsCustomerData.Tables[iDs].Rows.Count != 0)
                            {
                                dsCustomerData.Tables[iDs].TableName = "GCS_CHISO_BQ";
                            }
                            break;
                        case 17:
                            if (dsCustomerData.Tables[iDs] != null && dsCustomerData.Tables[iDs].Rows.Count != 0)
                            {
                                dsCustomerData.Tables[iDs].TableName = "GCS_CHISO_GT";
                            }
                            break;
                        case 18:
                            if (dsCustomerData.Tables[iDs] != null && dsCustomerData.Tables[iDs].Rows.Count != 0)
                            {
                                dsCustomerData.Tables[iDs].TableName = "GCS_SLMDK_SHBB";
                            }
                            break;
                        case 19:
                            if (dsCustomerData.Tables[iDs] != null && dsCustomerData.Tables[iDs].Rows.Count != 0)
                            {
                                dsCustomerData.Tables[iDs].TableName = "GCS_SLMDK_SHBB_GT";
                            }
                            break;
                        case 20:
                            if (dsCustomerData.Tables[iDs] != null && dsCustomerData.Tables[iDs].Rows.Count != 0)
                            {
                                dsCustomerData.Tables[iDs].TableName = "HDG_VITRI_DDO_GT";
                            }
                            break;
                        case 21:
                            if (dsCustomerData.Tables[iDs] != null && dsCustomerData.Tables[iDs].Rows.Count != 0)
                            {
                                dsCustomerData.Tables[iDs].TableName = "GCS_CHISO_PHUGT";
                            }
                            break;
                        case 22:
                            if (dsCustomerData.Tables[iDs] != null && dsCustomerData.Tables[iDs].Rows.Count != 0)
                            {
                                dsCustomerData.Tables[iDs].TableName = "HDG_KHACH_HANG_TT";
                            }
                            break;
                        default:
                            break;
                    }
                }

                for (Int16 iDs = 22; iDs >= 0; iDs--)
                {
                    if (dsCustomerData.Tables[iDs] != null && dsCustomerData.Tables[iDs].Rows.Count == 0)
                    {
                        dsCustomerData.Tables.RemoveAt(iDs);
                    }
                }

                dsCustomerData.AcceptChanges();

                //Xu ly du lieu 
                dsCustomerData.Tables["GCS_CHISO"].Columns.Add("ID_CHISO1", typeof(System.Int64));
                dsCustomerData.Tables["GCS_CHISO"].Columns.Add("ID_BCS1", typeof(System.Int64));
                dsCustomerData.Tables["GCS_CHISO"].Columns.Add("KY1", typeof(System.Int16));
                dsCustomerData.Tables["GCS_CHISO"].Columns.Add("THANG1", typeof(System.Int16));
                dsCustomerData.Tables["GCS_CHISO"].Columns.Add("NAM1", typeof(System.Int16));
                foreach (DataRow dr in dsCustomerData.Tables["GCS_CHISO"].Rows)
                {
                    dr["ID_CHISO1"] = Convert.ToInt64(dr["ID_CHISO"]);
                    dr["ID_BCS1"] = Convert.ToInt64(dr["ID_BCS"]);
                    dr["KY1"] = Convert.ToInt16(dr["KY"]);
                    dr["THANG1"] = Convert.ToInt16(dr["THANG"]);
                    dr["NAM1"] = Convert.ToInt16(dr["NAM"]);
                }
                dsCustomerData.Tables["GCS_CHISO"].Columns.Remove("ID_CHISO");
                dsCustomerData.Tables["GCS_CHISO"].Columns.Remove("ID_BCS");
                dsCustomerData.Tables["GCS_CHISO"].Columns.Remove("KY");
                dsCustomerData.Tables["GCS_CHISO"].Columns.Remove("THANG");
                dsCustomerData.Tables["GCS_CHISO"].Columns.Remove("NAM");

                dsCustomerData.Tables["GCS_CHISO"].Columns.Add("ID_CHISO", typeof(System.Int64));
                dsCustomerData.Tables["GCS_CHISO"].Columns.Add("ID_BCS", typeof(System.Int64));
                dsCustomerData.Tables["GCS_CHISO"].Columns.Add("KY", typeof(System.Int16));
                dsCustomerData.Tables["GCS_CHISO"].Columns.Add("THANG", typeof(System.Int16));
                dsCustomerData.Tables["GCS_CHISO"].Columns.Add("NAM", typeof(System.Int16));
                foreach (DataRow dr in dsCustomerData.Tables["GCS_CHISO"].Rows)
                {
                    dr["ID_CHISO"] = Convert.ToInt64(dr["ID_CHISO1"]);
                    dr["ID_BCS"] = Convert.ToInt64(dr["ID_BCS1"]);
                    dr["KY"] = Convert.ToInt16(dr["KY1"]);
                    dr["THANG"] = Convert.ToInt16(dr["THANG1"]);
                    dr["NAM"] = Convert.ToInt16(dr["NAM1"]);
                }
                dsCustomerData.Tables["GCS_CHISO"].Columns.Remove("ID_CHISO1");
                dsCustomerData.Tables["GCS_CHISO"].Columns.Remove("ID_BCS1");
                dsCustomerData.Tables["GCS_CHISO"].Columns.Remove("KY1");
                dsCustomerData.Tables["GCS_CHISO"].Columns.Remove("THANG1");
                dsCustomerData.Tables["GCS_CHISO"].Columns.Remove("NAM1");

                if (dsCustomerData.Tables.Contains("GCS_CHISO_GT") == true)
                {
                    if (dsCustomerData.Tables["GCS_CHISO_GT"].Rows.Count > 0)
                    {
                        dsCustomerData.Tables["GCS_CHISO_GT"].Columns.Add("ID_CHISO1", typeof(System.Int64));
                        dsCustomerData.Tables["GCS_CHISO_GT"].Columns.Add("ID_BCS1", typeof(System.Int64));
                        dsCustomerData.Tables["GCS_CHISO_GT"].Columns.Add("KY1", typeof(System.Int16));
                        dsCustomerData.Tables["GCS_CHISO_GT"].Columns.Add("THANG1", typeof(System.Int16));
                        dsCustomerData.Tables["GCS_CHISO_GT"].Columns.Add("NAM1", typeof(System.Int16));
                        foreach (DataRow dr in dsCustomerData.Tables["GCS_CHISO_GT"].Rows)
                        {
                            dr["ID_CHISO1"] = Convert.ToInt64(dr["ID_CHISO"]);
                            dr["ID_BCS1"] = Convert.ToInt64(dr["ID_BCS"]);
                            dr["KY1"] = Convert.ToInt16(dr["KY"]);
                            dr["THANG1"] = Convert.ToInt16(dr["THANG"]);
                            dr["NAM1"] = Convert.ToInt16(dr["NAM"]);
                        }
                        dsCustomerData.Tables["GCS_CHISO_GT"].Columns.Remove("ID_CHISO");
                        dsCustomerData.Tables["GCS_CHISO_GT"].Columns.Remove("ID_BCS");
                        dsCustomerData.Tables["GCS_CHISO_GT"].Columns.Remove("KY");
                        dsCustomerData.Tables["GCS_CHISO_GT"].Columns.Remove("THANG");
                        dsCustomerData.Tables["GCS_CHISO_GT"].Columns.Remove("NAM");

                        dsCustomerData.Tables["GCS_CHISO_GT"].Columns.Add("ID_CHISO", typeof(System.Int64));
                        dsCustomerData.Tables["GCS_CHISO_GT"].Columns.Add("ID_BCS", typeof(System.Int64));
                        dsCustomerData.Tables["GCS_CHISO_GT"].Columns.Add("KY", typeof(System.Int16));
                        dsCustomerData.Tables["GCS_CHISO_GT"].Columns.Add("THANG", typeof(System.Int16));
                        dsCustomerData.Tables["GCS_CHISO_GT"].Columns.Add("NAM", typeof(System.Int16));
                        foreach (DataRow dr in dsCustomerData.Tables["GCS_CHISO_GT"].Rows)
                        {
                            dr["ID_CHISO"] = Convert.ToInt64(dr["ID_CHISO1"]);
                            dr["ID_BCS"] = Convert.ToInt64(dr["ID_BCS1"]);
                            dr["KY"] = Convert.ToInt16(dr["KY1"]);
                            dr["THANG"] = Convert.ToInt16(dr["THANG1"]);
                            dr["NAM"] = Convert.ToInt16(dr["NAM1"]);
                        }
                        dsCustomerData.Tables["GCS_CHISO_GT"].Columns.Remove("ID_CHISO1");
                        dsCustomerData.Tables["GCS_CHISO_GT"].Columns.Remove("ID_BCS1");
                        dsCustomerData.Tables["GCS_CHISO_GT"].Columns.Remove("KY1");
                        dsCustomerData.Tables["GCS_CHISO_GT"].Columns.Remove("THANG1");
                        dsCustomerData.Tables["GCS_CHISO_GT"].Columns.Remove("NAM1");
                    }
                }
                dsCustomerData.AcceptChanges();
                List<string> strNotNull = new List<string>()
            {
                "HDG_DDO_SOGCS",
                "HDG_DIEM_DO",
                "HDG_BBAN_APGIA",
                "HDG_KHACH_HANG",
                "HDG_PTHUC_TTOAN",
                "HDG_VITRI_DDO",
                "D_SOGCS",
                "GCS_LICHGCS",
                "GCS_CHISO"
            };
                int i = dsCustomerData.Tables.Count - 1;
                while (i >= 0)
                {
                    DataTable dt = dsCustomerData.Tables[i];
                    if (dt.Rows.Count == 0)
                    {
                        if (strNotNull.Contains(dt.TableName))
                        {
                            dsCustomerData = null;
                            return dt.TableName;
                        }
                        else
                        {
                            dsCustomerData.Tables.Remove(dt);
                        }
                    }
                    i--;
                }
                return "";
            }
            catch (Exception ex)
            {
                return "Lỗi khi lấy dữ liệu: " + ex.Message;
            }
            finally
            {
                db.ReleaseConnection();
            }
        }
        public string getCustomerDataReading_1(ref DataSet dsCustomerData)
        {
            CMIS2 db = new CMIS2();
            try
            {
                if (!dsCustomerData.Tables.Contains("HDN_CHISO_DC") || dsCustomerData.Tables["HDN_CHISO_DC"].Rows.Count == 0)
                {
                    return "Lỗi phương thức getCustomerDataReading_1 - Không tìm thấy dữ liệu trong bảng HDN_CHISO_DC";
                }
                cls_HDN_CHISO_DC_Controller obj_HDN_CHISO_DC_Controller = new cls_HDN_CHISO_DC_Controller();
                obj_HDN_CHISO_DC_Controller.CMIS2 = db;
                foreach (DataRow dr in dsCustomerData.Tables["HDN_CHISO_DC"].Rows)
                {
                    long id = obj_HDN_CHISO_DC_Controller.getMaxID();
                    if (id == -1) return "Lỗi trong phương thức getCustomerDataReading_1 khi lấy SEQ_GCS_CHISO";
                    dr["ID_CHISO"] = id;
                }
                return "";
            }
            catch (Exception ex)
            {
                return "Lỗi phương thức getCustomerDataReading_1:" + ex.Message;
            }
            finally
            {
                db.ReleaseConnection();
            }
        }
        public string getStaticCatalogDataReading(ref DataSet dsStaticCatalog)
        {
            CMIS2 db = new CMIS2();
            try
            {
                dsStaticCatalog.Clear();
                dsStaticCatalog = new DataSet();

                obj_D_BAC_THANG_Controller.CMIS2 = db;
                DataTable dtD_BAC_THANG = obj_D_BAC_THANG_Controller.getD_BAC_THANG();
                if (dtD_BAC_THANG == null)
                    return "D_BAC_THANG";
                dsStaticCatalog.Tables.Add(dtD_BAC_THANG);

                obj_D_CAP_DAP_Controller.CMIS2 = db;
                DataTable dtD_CAP_DAP = obj_D_CAP_DAP_Controller.getD_CAP_DAP();
                if (dtD_CAP_DAP == null)
                    return "D_CAP_DAP";
                dsStaticCatalog.Tables.Add(dtD_CAP_DAP);

                obj_D_COSFI_Controller.CMIS2 = db;
                DataTable dtD_COSFI = obj_D_COSFI_Controller.getD_COSFI();
                if (dtD_COSFI == null)
                    return "D_COSFI";
                dsStaticCatalog.Tables.Add(dtD_COSFI);

                obj_D_GIA_NHOMNN_Controller.CMIS2 = db;
                DataTable dtD_GIA_NHOMNN = obj_D_GIA_NHOMNN_Controller.getD_GIA_NHOMNN();
                if (dtD_GIA_NHOMNN == null)
                    return "D_GIA_NHOMNN";
                dsStaticCatalog.Tables.Add(dtD_GIA_NHOMNN);

                obj_D_NGANH_NGHE_Controller.CMIS2 = db;
                DataTable dtD_NGANH_NGHE = obj_D_NGANH_NGHE_Controller.getD_NGANH_NGHE();
                if (dtD_NGANH_NGHE == null)
                    return "D_NGANH_NGHE";
                dsStaticCatalog.Tables.Add(dtD_NGANH_NGHE);

                obj_D_NHOM_NN_Controller.CMIS2 = db;
                DataTable dtD_NHOM_NN = obj_D_NHOM_NN_Controller.getD_NHOM_NN();
                if (dtD_NHOM_NN == null)
                    return "D_NHOM_NN";
                dsStaticCatalog.Tables.Add(dtD_NHOM_NN);

                obj_D_THAMCHIEU_CAPDA_Controller.CMIS2 = db;
                DataTable dtD_THAMCHIEU_CAPDA = obj_D_THAMCHIEU_CAPDA_Controller.getD_THAMCHIEU_CAPDA();
                if (dtD_THAMCHIEU_CAPDA == null)
                    return "D_THAMCHIEU_CAPDA";
                dsStaticCatalog.Tables.Add(dtD_THAMCHIEU_CAPDA);

                //Lay them du lieu trong bang D_TY_GIA, khong can phai bao loi
                obj_D_TY_GIA_Controller.CMIS2 = db;
                DataTable dtD_TY_GIA = obj_D_TY_GIA_Controller.getD_TY_GIA();
                if (dtD_TY_GIA != null)
                {
                    dsStaticCatalog.Tables.Add(dtD_TY_GIA);
                }

                obj_S_PARAMETERS_Controller.CMIS2 = db;
                //Lay them du lieu trong bang S_PARAMETERS, khong can phai bao loi
                var q = from p in obj_S_PARAMETERS_Controller.select_S_PARAMETER().ToList()
                        select new
                        {
                            p.CREATEDBY,
                            p.CREATEDDATE,
                            p.DATATYPE,
                            p.DESCRIPTION,
                            p.NAME,
                            p.PARAMETERID,
                            p.PARAMETERTYPE,
                            p.PRAVALUE,
                            p.STARTDATE,
                            p.STATE,
                            p.SUBDIVISIONID,
                        };
                if (q != null && q.Take(1).Count() != 0)
                {
                    DataTable dtS_PARAMETERS = Utility.LINQToDataTable(q);
                    dtS_PARAMETERS.TableName = "S_PARAMETER";
                    if (dtS_PARAMETERS != null)
                    {
                        dtS_PARAMETERS.AcceptChanges();
                        dsStaticCatalog.Tables.Add(dtS_PARAMETERS);
                    }
                }
                return "";
            }
            catch (Exception ex)
            {
                return ex.Message;
            }
            finally
            {
                dsStaticCatalog.AcceptChanges();
                db.ReleaseConnection();
            }
        }
        #endregion

        #region Kiem tra so lieu truoc khi tinh hoa don
        #region Mã hóa

        protected string ENCODE(string strIn)
        {
            string temp = Encrypt(strIn, true);
            string s = "";
            char[] arrChar = temp.ToCharArray();
            //if (arrChar.Length >= 16)
            //{
            //    for (int i = 0; i < 16; i++)
            //    {
            //        arrChar[i] = arrChar[i].ToString() == arrChar[i].ToString().ToUpper() ? arrChar[i].ToString().ToLower()[0] : arrChar[i].ToString().ToUpper()[0];
            //        //arrChar[i] = Convert.ToChar(Convert.ToInt32(arrChar[i]) + Convert.ToInt32(key[i]));
            //        s += arrChar[i].ToString();
            //    }
            //    for (int i = 16; i < arrChar.Length; i++)
            //    {
            //        s += arrChar[i].ToString();
            //    }
            //}
            //else
            //{
            for (int i = 0; i < arrChar.Length; i++)
            {
                arrChar[i] = arrChar[i].ToString() == arrChar[i].ToString().ToUpper() ? arrChar[i].ToString().ToLower()[0] : arrChar[i].ToString().ToUpper()[0];
                //arrChar[i] = Convert.ToChar(Convert.ToInt32(arrChar[i]) + Convert.ToInt32(key[i]));
                s += arrChar[i].ToString();
            }
            //}
            return s;
        }
        public string Encrypt(string toEncrypt, bool useHashing)
        {
            byte[] keyArray;
            byte[] toEncryptArray = UTF8Encoding.UTF8.GetBytes(toEncrypt);

            if (useHashing)
            {
                MD5CryptoServiceProvider hashmd5 = new MD5CryptoServiceProvider();
                keyArray = hashmd5.ComputeHash(UTF8Encoding.UTF8.GetBytes(key));
            }
            else
                keyArray = UTF8Encoding.UTF8.GetBytes(key);

            TripleDESCryptoServiceProvider tdes = new TripleDESCryptoServiceProvider();
            tdes.Key = keyArray;
            tdes.Mode = CipherMode.ECB;
            tdes.Padding = PaddingMode.PKCS7;

            ICryptoTransform cTransform = tdes.CreateEncryptor();
            byte[] resultArray = cTransform.TransformFinalBlock(toEncryptArray, 0, toEncryptArray.Length);
            return Convert.ToBase64String(resultArray, 0, resultArray.Length);
        }

        #endregion
        public string CheckValidData(string strMa_DViQLy, DataSet dsCustomerData, DataSet dsStaticCatalog)
        {
            string strErrTmp = "";
            DataView dwCSBQ = null;
            DataRow[] arrCheck = null;
            try
            {
                //Debug.WriteLine(DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff") + " CheckValidData begin");
                strErrTmp = "B1";
                //Kiem tra su ton tai cua cac bang du lieu can thiet
                if (dsCustomerData == null)
                    return "dsCustomerDataIsNull"; //du lieu khach hang bi null

                if (dsCustomerData.Tables.Contains("HDG_DDO_SOGCS") == false)
                    return "NotExistsHDG_DDO_SOGCS"; //khong ton tai bang HDG_DDO_SOGCS
                else
                    if (dsCustomerData.Tables["HDG_DDO_SOGCS"].Rows.Count == 0)
                    return "NoDataFoundInHDG_DDO_SOGCS"; //khong co du lieu trong bang HDG_DDO_SOGCS

                if (dsCustomerData.Tables.Contains("HDG_DIEM_DO") == false)
                    return "NotExistsHDG_DIEM_DO"; //khong ton tai bang HDG_DIEM_DO
                else
                    if (dsCustomerData.Tables["HDG_DIEM_DO"].Rows.Count == 0)
                    return "NoDataFoundInHDG_DIEM_DO"; //khong co du lieu trong bang HDG_DIEM_DO

                if (dsCustomerData.Tables.Contains("HDG_BBAN_APGIA") == false)
                    return "NotExistsHDG_BBAN_APGIA"; //khong ton tai bang HDG_BBAN_APGIA
                else
                    if (dsCustomerData.Tables["HDG_BBAN_APGIA"].Rows.Count == 0)
                    return "NoDataFoundInHDG_BBAN_APGIA"; //khong co du lieu trong bang HDG_BBAN_APGIA                

                if (dsCustomerData.Tables.Contains("HDG_KHACH_HANG") == false)
                    return "NotExistsHDG_KHACH_HANG"; //khong ton tai bang HDG_KHACH_HANG
                else
                    if (dsCustomerData.Tables["HDG_KHACH_HANG"].Rows.Count == 0)
                    return "NoDataFoundInHDG_KHACH_HANG"; //khong co du lieu trong bang HDG_KHACH_HANG

                if (dsCustomerData.Tables.Contains("HDG_PTHUC_TTOAN") == false)
                    return "NotExistsHDG_PTHUC_TTOAN"; //khong ton tai bang HDG_PTHUC_TTOAN
                else
                    if (dsCustomerData.Tables["HDG_PTHUC_TTOAN"].Rows.Count == 0)
                    return "NoDataFoundInHDG_PTHUC_TTOAN"; //khong co du lieu trong bang HDG_PTHUC_TTOAN

                if (dsCustomerData.Tables.Contains("HDG_VITRI_DDO") == false)
                    return "NotExistsHDG_VITRI_DDO"; //khong ton tai bang HDG_VITRI_DDO
                else
                    if (dsCustomerData.Tables["HDG_VITRI_DDO"].Rows.Count == 0)
                    return "NoDataFoundInHDG_VITRI_DDO"; //khong co du lieu trong bang HDG_VITRI_DDO

                //if (dsCustomerData.Tables.Contains("D_SOGCS") == false)
                //    return "NotExistsD_SOGCS"; //khong ton tai bang D_SOGCS
                //else
                //    if (dsCustomerData.Tables["D_SOGCS"].Rows.Count == 0)
                //        return "NoDataFoundInD_SOGCS"; //khong co du lieu trong bang D_SOGCS

                //if (dsCustomerData.Tables.Contains("GCS_LICHGCS") == false)
                //    return "NotExistsGCS_LICHGCS"; //khong ton tai bang GCS_LICHGCS
                //else
                //    if (dsCustomerData.Tables["GCS_LICHGCS"].Rows.Count == 0)
                //        return "NoDataFoundInGCS_LICHGCS"; //khong co du lieu trong bang GCS_LICHGCS

                if (dsCustomerData.Tables.Contains("GCS_CHISO") == false)
                    return "NotExistsGCS_CHISO"; //khong ton tai bang GCS_CHISO
                else
                    if (dsCustomerData.Tables["GCS_CHISO"].Rows.Count == 0)
                    return "NoDataFoundInGCS_CHISO"; //khong co du lieu trong bang GCS_CHISO

                if (dsStaticCatalog == null)
                    return "dsStaticCatalogIsNull"; //du lieu danh muc bi null

                if (dsStaticCatalog.Tables.Contains("D_BAC_THANG") == false)
                    return "NotExistsD_BAC_THANG"; //khong ton tai bang D_BAC_THANG
                else
                    if (dsStaticCatalog.Tables["D_BAC_THANG"].Rows.Count == 0)
                    return "NoDataFoundInD_BAC_THANG"; //khong co du lieu trong bang D_BAC_THANG

                if (dsStaticCatalog.Tables.Contains("D_CAP_DAP") == false)
                    return "NotExistsD_CAP_DAP"; //khong ton tai bang D_CAP_DAP
                else
                    if (dsStaticCatalog.Tables["D_CAP_DAP"].Rows.Count == 0)
                    return "NoDataFoundInD_CAP_DAP"; //khong co du lieu trong bang D_CAP_DAP

                if (dsStaticCatalog.Tables.Contains("D_COSFI") == false)
                    return "NotExistsD_COSFI"; //khong ton tai bang D_COSFI
                else
                    if (dsStaticCatalog.Tables["D_COSFI"].Rows.Count == 0)
                    return "NoDataFoundInD_COSFI"; //khong co du lieu trong bang D_COSFI

                if (dsStaticCatalog.Tables.Contains("D_GIA_NHOMNN") == false)
                    return "NotExistsD_GIA_NHOMNN"; //khong ton tai bang D_GIA_NHOMNN
                else
                    if (dsStaticCatalog.Tables["D_GIA_NHOMNN"].Rows.Count == 0)
                    return "NoDataFoundInD_GIA_NHOMNN"; //khong co du lieu trong bang D_GIA_NHOMNN

                if (dsStaticCatalog.Tables.Contains("D_NGANH_NGHE") == false)
                    return "NotExistsD_NGANH_NGHE"; //khong ton tai bang D_NGANH_NGHE
                else
                    if (dsStaticCatalog.Tables["D_NGANH_NGHE"].Rows.Count == 0)
                    return "NoDataFoundInD_NGANH_NGHE"; //khong co du lieu trong bang D_NGANH_NGHE

                if (dsStaticCatalog.Tables.Contains("D_NHOM_NN") == false)
                    return "NotExistsD_NHOM_NN"; //khong ton tai bang D_NHOM_NN
                else
                    if (dsStaticCatalog.Tables["D_NHOM_NN"].Rows.Count == 0)
                    return "NoDataFoundInD_NHOM_NN"; //khong co du lieu trong bang D_NHOM_NN

                if (dsStaticCatalog.Tables.Contains("D_THAMCHIEU_CAPDA") == false)
                    return "NotExistsD_THAMCHIEU_CAPDA"; //khong ton tai bang D_THAMCHIEU_CAPDA   
                else
                    if (dsStaticCatalog.Tables["D_THAMCHIEU_CAPDA"].Rows.Count == 0)
                    return "NoDataFoundInD_THAMCHIEU_CAPDA"; //khong co du lieu trong bang D_THAMCHIEU_CAPDA
                //Debug.WriteLine(DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff") + " CheckValidData CheckExists");
                DataView dwCS, dwBBanAGia, dwGiaNhomNN, dwTChieuCapDA, dwTyGia, dwBThang, dvMDK, dvCS_GT, dwDoiGia = null;
                DataRow[] rows;
                string strMa_DDo = "";
                DateTime ngay_dkymin, ngay_ckymax, ngay_hlucgia1, ngayhlucmin, ngay_hluc, ngay_hluc1, ngay_hhluc, ngay_doigia, dt1900;
                string strMa_NhomNN = "", strTGian_BDien = "", strMa_CapDA = "", strKhoang_DA = "", strMa_NGia = "";
                Decimal Don_Gia = -1, IsExists = 0, So_Ho = -1, ICountGiaBB = 0;
                string strLoai_Tien = "";
                Int16 IsExistsPGT = 0, IsChanged = 0, IsCKyTrungDGia = 0;
                if (dsCustomerData.Tables.Contains("GCS_CHISO_PHUGT") == true)
                {
                    IsExistsPGT = 1; //Ton tai bang GCS_CHISO_PHUGT
                }
                else
                {
                    IsExistsPGT = 0;
                }
                dt1900 = new DateTime(1900, 1, 1);
                //Khoi tao cac dataview de lay index cua DataView
                dwTChieuCapDA = new DataView(dsStaticCatalog.Tables["D_THAMCHIEU_CAPDA"]);
                dwTChieuCapDA.RowFilter = "MA_NHOMNN = 'SHBT' AND MA_CAPDA = '1'";
                dwTChieuCapDA.Sort = "NGAY_ADUNG DESC";

                dwGiaNhomNN = new DataView(dsStaticCatalog.Tables["D_GIA_NHOMNN"]);
                dwGiaNhomNN.RowFilter = "MA_NHOMNN = 'SHBT' AND KHOANG_DA = '2' AND THOIGIAN_BDIEN = 'KT' AND MA_NGIA = 'A'";
                dwGiaNhomNN.Sort = "NGAY_ADUNG DESC";

                dwTyGia = new DataView(dsStaticCatalog.Tables["D_TY_GIA"]);
                dwTyGia.RowFilter = "MA_DVIQLY = '" + strMa_DViQLy + "'";

                dwBBanAGia = new DataView(dsCustomerData.Tables["HDG_BBAN_APGIA"]);
                dwBBanAGia.RowFilter = "MA_DVIQLY = '" + strMa_DViQLy + "' AND LOAI_BCS IN ('BT','CD','TD','KT')";
                dwBBanAGia.Sort = "MA_DDO, NGAY_HLUC";

                dwDoiGia = new DataView(dsCustomerData.Tables["HDG_BBAN_APGIA"]);

                dwBThang = new DataView(dsStaticCatalog.Tables["D_BAC_THANG"]);
                dwBThang.RowFilter = "MA_NHOMNN = 'SHBT' AND MA_NGIA = 'A'";
                dwBThang.Sort = "NGAY_HLUC DESC";

                //DataTable dt = dwBThang.ToTable();

                //dwBThang.RowFilter = "MA_NHOMNN = 'SHBT' AND MA_NGIA <> 'A'";
                //dt = dwBThang.ToTable();

                dwCS = new DataView(dsCustomerData.Tables["GCS_CHISO"]);
                //dwCS.RowFilter = "BCS IN ('BT','CD','TD','KT')";
                dwCS.Sort = "MA_DVIQLY, MA_DDO";
                //Debug.WriteLine(DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff") + " CheckValidData SortAndRowFilter");
                //Tính luôn ngày đổi giá
                ngay_doigia = DateTime.Parse(Convert.ToDateTime(dsStaticCatalog.Tables["D_GIA_NHOMNN"].Compute("MAX(NGAY_ADUNG)", "1=1")).ToString("dd/MM/yyyy", new System.Globalization.CultureInfo("vi-VN")), new System.Globalization.CultureInfo("vi-VN"));
                //Debug.WriteLine(DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff") + " CheckValidData MAXD_GIA_NHOMNN");
                //Nếu là tính HD phát sinh thì kiểm tra vì HDDC không có NGUOI_TAO, phải thêm NGUOI_TAO trong PKG
                //Bỏ đi, sang CMIS3 không cần kiểm soát cái này nữa, nếu cần sau này mở ra sau
                //if (!dsCustomerData.Tables.Contains("HDN_HDON") && strMa_DViQLy.Substring(0, 2) == "PA")
                //{
                //    Random random = new Random();
                //    int randomNumber = random.Next(0, 100);
                //    if (randomNumber > 10)
                //    {
                //        //kiểm tra
                //        DataView dvCS_Valid = new DataView(dsCustomerData.Tables["GCS_CHISO"]);
                //        dvCS_Valid.RowFilter = "LOAI_CHISO='DDK'";
                //        int i32For = dvCS_Valid.Count > 10 ? 10 : dvCS_Valid.Count;
                //        List<int> lstRand = new List<int>();
                //        while (lstRand.Count < i32For)
                //        {
                //            Random rad = new Random();
                //            int i32Index = rad.Next(0, dvCS_Valid.Count);
                //            if (!lstRand.Contains(i32Index))
                //                lstRand.Add(i32Index);
                //        }
                //        foreach (int i32Index in lstRand)
                //        {
                //            DataRowView info = dvCS_Valid[i32Index];
                //            string strEncrypt = ENCODE(Convert.ToDateTime(info["NGAY_DKY"]).ToString("dd/MM/yyyy") + "_" + Convert.ToDateTime(info["NGAY_CKY"]).ToString("dd/MM/yyyy"));
                //            if (strEncrypt.Length < 50)
                //            {
                //                if (!info["NGUOI_TAO"].ToString().Contains(strEncrypt))
                //                {
                //                    //false
                //                    return "Data is invalid";
                //                }
                //            }
                //        }
                //    }
                //}

                foreach (DataRowView drw in dwCS)
                {
                    //Kiem tra xem co phai diem do phu ghep tong khong
                    //Loai bo du lieu thua cua cac diem do phu ghep tong
                    if (IsExistsPGT == 1)
                    {
                        if (dsCustomerData.Tables["GCS_CHISO_PHUGT"].Select("MA_DVIQLY = '" + Convert.ToString(drw["MA_DVIQLY"]) + "' AND ID_CHISO = " + Convert.ToString(drw["ID_CHISO"])).Length == 0)
                        {
                            drw.Row.Delete();
                            IsChanged = 1;
                            continue;
                        }
                    }

                    if (drw["BCS"].ToString().Trim() == "SG" || drw["BCS"].ToString().Trim() == "VC") continue;

                    //Kiem tra so sanh ngay dau ky va ngay cuoi ky
                    //if (DateTime.Parse(Convert.ToDateTime(drw["NGAY_DKY"]).ToString("dd/MM/yyyy", new System.Globalization.CultureInfo("vi-VN")), new System.Globalization.CultureInfo("vi-VN")) > DateTime.Parse(Convert.ToDateTime(drw["NGAY_CKY"]).ToString("dd/MM/yyyy", new System.Globalization.CultureInfo("vi-VN")), new System.Globalization.CultureInfo("vi-VN")))
                    if (Convert.ToDateTime(drw["NGAY_DKY"]) > Convert.ToDateTime(drw["NGAY_CKY"]))
                        return "Ngay_DKyIsLargeThanNgay_CKy - " + Convert.ToString(drw["MA_DDO"]).Trim();

                    if (strMa_DDo != Convert.ToString(drw["MA_DDO"]).Trim())
                    {
                        strMa_DDo = Convert.ToString(drw["MA_DDO"]).Trim();
                        //ngay_dkymin = DateTime.Parse("01/01/3000", new System.Globalization.CultureInfo("vi-VN"));
                        //ngay_ckymax = DateTime.Parse("01/01/1900", new System.Globalization.CultureInfo("vi-VN"));
                        //ngayhlucmin = DateTime.Parse("01/01/3000", new System.Globalization.CultureInfo("vi-VN"));
                        ngay_dkymin = new DateTime(3000, 1, 1);
                        ngay_ckymax = new DateTime(1900, 1, 1);
                        ngayhlucmin = new DateTime(3000, 1, 1); ;
                        IsCKyTrungDGia = 0;
                        DataRow[] arrCS = dsCustomerData.Tables["GCS_CHISO"].Select("MA_DVIQLY = '" + strMa_DViQLy + "' AND MA_DDO = '" + strMa_DDo + "' AND BCS IN ('KT','BT','CD','TD')");
                        //ngay_dkymin = DateTime.Parse(Convert.ToDateTime(dsCustomerData.Tables["GCS_CHISO"].Compute("MIN(NGAY_DKY)", "MA_DVIQLY = '" + strMa_DViQLy + "' AND MA_DDO = '" + strMa_DDo + "' AND BCS IN ('KT','BT','CD','TD')")).ToString("dd/MM/yyyy", new System.Globalization.CultureInfo("vi-VN")), new System.Globalization.CultureInfo("vi-VN"));
                        //ngay_ckymax = DateTime.Parse(Convert.ToDateTime(dsCustomerData.Tables["GCS_CHISO"].Compute("MAX(NGAY_CKY)", "MA_DVIQLY = '" + strMa_DViQLy + "' AND MA_DDO = '" + strMa_DDo + "' AND BCS IN ('KT','BT','CD','TD')")).ToString("dd/MM/yyyy", new System.Globalization.CultureInfo("vi-VN")), new System.Globalization.CultureInfo("vi-VN"));
                        foreach (DataRow dr in arrCS)
                        {
                            DateTime dtNgayDKy = Convert.ToDateTime(dr["NGAY_DKY"]);
                            DateTime dtNgayCKy = Convert.ToDateTime(dr["NGAY_CKY"]);
                            if (ngay_dkymin > dtNgayDKy) ngay_dkymin = dtNgayDKy;
                            if (ngay_ckymax < dtNgayCKy) ngay_ckymax = dtNgayCKy;
                        }
                        if (ngay_ckymax == ngay_doigia)
                            IsCKyTrungDGia = 1;
                        else
                            IsCKyTrungDGia = 0;
                        strErrTmp = "B2:" + strMa_DDo;

                        //Kiem tra chi tiet thong tin bien ban gia 
                        rows = dsCustomerData.Tables["HDG_BBAN_APGIA"].Select("MA_DVIQLY = '" + strMa_DViQLy + "' AND MA_DDO = '" + strMa_DDo + "' AND LOAI_BCS IN ('BT','CD','TD','KT')");
                        if (rows.Length > 0)
                        {
                            strErrTmp = "B3:" + strMa_DDo;
                            //Kiem tra du lieu bien ban gia
                            foreach (DataRow dr in rows)
                            {
                                DateTime dtNgayHLuc = Convert.ToDateTime(dr["NGAY_HLUC"]);
                                if (ngayhlucmin > dtNgayHLuc) ngayhlucmin = dtNgayHLuc;
                            }
                            //ngayhlucmin = DateTime.Parse(Convert.ToDateTime(dsCustomerData.Tables["HDG_BBAN_APGIA"].Compute("MIN(NGAY_HLUC)", "MA_DVIQLY = '" + strMa_DViQLy + "' AND MA_DDO = '" + strMa_DDo + "' AND LOAI_BCS IN ('KT','BT','CD','TD')")).ToString("dd/MM/yyyy", new System.Globalization.CultureInfo("vi-VN")), new System.Globalization.CultureInfo("vi-VN"));
                            if (ngay_dkymin < ngayhlucmin)
                                return "NotEnoughDataInHDG_BBAN_APGIA - " + strMa_DDo;

                            //if (strMa_DDo == "PK02000052544001")
                            //{
                            //    strMa_DDo = "PK02000052544001";
                            //}

                            foreach (DataRow dr in rows)
                            {
                                strMa_NhomNN = Convert.ToString(dr["MA_NHOMNN"]).Trim();
                                strTGian_BDien = Convert.ToString(dr["TGIAN_BDIEN"]).Trim();
                                strMa_NGia = Convert.ToString(dr["MA_NGIA"]).Trim();
                                strMa_CapDA = Convert.ToString(dr["MA_CAPDAP"]).Trim();

                                strErrTmp = "B4:" + strMa_DDo;
                                //Kiem tra trong D_THAMCHIEU_CAPDA
                                //dwGia = new DataView(dsStaticCatalog.Tables["D_THAMCHIEU_CAPDA"]);
                                //dwGia.RowFilter = "MA_NHOMNN = '" + strMa_NhomNN + "' AND MA_CAPDA = '" + strMa_CapDA + "'";
                                //dwGia.Sort = "NGAY_ADUNG DESC";

                                dwTChieuCapDA.RowFilter = "MA_NHOMNN = '" + strMa_NhomNN + "' AND MA_CAPDA = '" + strMa_CapDA + "'";
                                strKhoang_DA = "";
                                //foreach (DataRowView drCapDA in dwGia)
                                foreach (DataRowView drCapDA in dwTChieuCapDA)
                                {
                                    //ngay_hluc = DateTime.Parse(Convert.ToDateTime(drCapDA["NGAY_ADUNG"]).ToString("dd/MM/yyyy", new System.Globalization.CultureInfo("vi-VN")), new System.Globalization.CultureInfo("vi-VN"));
                                    ngay_hluc = Convert.ToDateTime(drCapDA["NGAY_ADUNG"]);
                                    if (ngay_dkymin >= ngay_hluc)
                                    {
                                        strKhoang_DA = Convert.ToString(drCapDA["KHOANG_DA"]);
                                        break;
                                    }
                                    else if (ngay_ckymax >= ngay_hluc)
                                    {
                                        strKhoang_DA = Convert.ToString(drCapDA["KHOANG_DA"]);
                                        break;
                                    }

                                }
                                if (strKhoang_DA == "")
                                    return "NotExistsCapDAInD_THAMCHIEU_CAPDA - " + strMa_DDo; //Khong ton tai trong bang D_THAMCHIEU_CAPDA

                                strErrTmp = "B5:" + strMa_DDo;
                                //Kiem tra trong D_GIA_NHOMNN
                                //dwGia = new DataView(dsStaticCatalog.Tables["D_GIA_NHOMNN"]);
                                //dwGia.RowFilter = "MA_NHOMNN = '" + strMa_NhomNN + "' AND KHOANG_DA = '" + strKhoang_DA + "' AND THOIGIAN_BDIEN = '" + strTGian_BDien + "' AND MA_NGIA = '" + strMa_NGia + "'";
                                //dwGia.Sort = "NGAY_ADUNG DESC";

                                dwGiaNhomNN.RowFilter = "MA_NHOMNN = '" + strMa_NhomNN + "' AND KHOANG_DA = '" + strKhoang_DA + "' AND THOIGIAN_BDIEN = '" + strTGian_BDien + "' AND MA_NGIA = '" + strMa_NGia + "'";
                                Don_Gia = -1;
                                strLoai_Tien = "";
                                //foreach (DataRowView drGia in dwGia)
                                foreach (DataRowView drGia in dwGiaNhomNN)
                                {
                                    //ngay_hluc = DateTime.Parse(Convert.ToDateTime(drGia["NGAY_ADUNG"]).ToString("dd/MM/yyyy", new System.Globalization.CultureInfo("vi-VN")), new System.Globalization.CultureInfo("vi-VN"));
                                    ngay_hluc = Convert.ToDateTime(drGia["NGAY_ADUNG"]);
                                    if (ngay_dkymin >= ngay_hluc)
                                    {
                                        Don_Gia = Convert.ToDecimal(drGia["DON_GIA"]);
                                        strLoai_Tien = Convert.ToString(drGia["LOAI_TIEN"]);
                                        //Doi gia 01/06/2014 Dũng NT
                                        //Kiểm tra nếu giá nhóm NN hết hiệu lực thì phải có giá mới tương ứng trong bảng HDG_BBAN_APGIA
                                        if (IsCKyTrungDGia == 0)
                                        {
                                            //Nếu sổ ghi không trùng ngày đổi giá, thực hiện kiểm tra
                                            if (drGia["NGAY_HETHLUC"].ToString().Trim().Length > 0)
                                            {
                                                //dwDoiGia.RowFilter = "";
                                                //ngay_hhluc = DateTime.Parse(Convert.ToDateTime(drGia["NGAY_HETHLUC"]).ToString("dd/MM/yyyy", new System.Globalization.CultureInfo("vi-VN")), new System.Globalization.CultureInfo("vi-VN"));
                                                ngay_hhluc = Convert.ToDateTime(drGia["NGAY_HETHLUC"]);
                                                if (ngay_ckymax > ngay_hhluc)
                                                {
                                                    //dwDoiGia.RowFilter = "MA_DVIQLY = '" + strMa_DViQLy + "' and MA_DDO ='" + strMa_DDo + "' AND LOAI_BCS IN ('BT','CD','TD','KT')";
                                                    bool isExistsDoiGiaKH = false;
                                                    foreach (DataRow drvKTraDGia in rows)
                                                    {
                                                        //DateTime ngayhluc_temp = DateTime.Parse(Convert.ToDateTime(drvKTraDGia["NGAY_HLUC"]).ToString("dd/MM/yyyy", new System.Globalization.CultureInfo("vi-VN")), new System.Globalization.CultureInfo("vi-VN"));
                                                        DateTime ngayhluc_temp = Convert.ToDateTime(drvKTraDGia["NGAY_HLUC"]);
                                                        if (ngayhluc_temp <= ngay_hhluc.AddDays(1))
                                                        {
                                                            isExistsDoiGiaKH = true;
                                                            //Chỉ kiểm tra tồn tại ít nhất 1 biên bản giá ngày đổi giá là đủ
                                                            //Việc kiểm soát đủ bộ chỉ số giành cho chức năng áp giá
                                                            break;
                                                        }
                                                    }
                                                    if (!isExistsDoiGiaKH)
                                                        return "NotEnoughDataInHDG_BBAN_APGIA - " + strMa_DDo + " - " + strMa_NhomNN + " nhom " + strMa_NGia + " het hieu luc";
                                                    //Khong ton tai bien ban ap gia moi trong khi ap gia cu da het hieu luc
                                                }
                                                else
                                                {
                                                    break;
                                                }
                                            }
                                            else if (Convert.ToDecimal(drGia["SOTHUTU"]) < 0 && (strMa_NGia == "D" || strMa_NGia == "F") && strMa_NhomNN != "SHBT")
                                            {
                                                //Kiểm tra nếu áp giá bán buôn bị phạt đã áp lại giá bán buôn nhóm E chưa
                                                dwDoiGia.RowFilter = "";
                                                dwDoiGia.RowFilter = "MA_DVIQLY = '" + strMa_DViQLy + "' and MA_DDO ='" + strMa_DDo + "' AND LOAI_BCS IN ('BT','CD','TD','KT') and MA_NGIA ='E'";

                                                if (dwDoiGia.Count <= 0)
                                                    return "NotEnoughDataInHDG_BBAN_APGIA - " + strMa_DDo + " - " + strMa_NhomNN + " nhom " + strMa_NGia + " ap lai gia phat";
                                                //Khong ton tai bien ban ap gia phat moi E (gia phat cu la nhom D)

                                            }
                                        }
                                        //End doi gia 01/06/2014 Dũng NT
                                        break;
                                    }
                                    else
                                    {
                                        if (ngay_ckymax >= ngay_hluc)
                                        {
                                            Don_Gia = Convert.ToDecimal(drGia["DON_GIA"]);
                                            strLoai_Tien = Convert.ToString(drGia["LOAI_TIEN"]);
                                            //Doi gia 01/06/2014 Dũng NT
                                            //Kiểm tra nếu giá nhóm NN hết hiệu lực thì phải có giá mới tương ứng trong bảng HDG_BBAN_APGIA
                                            if (IsCKyTrungDGia == 0)
                                            {
                                                //Nếu sổ ghi không trùng ngày đổi giá, thực hiện kiểm tra
                                                if (drGia["NGAY_HETHLUC"].ToString().Trim().Length > 0)
                                                {
                                                    //dwDoiGia.RowFilter = "";
                                                    //ngay_hhluc = DateTime.Parse(Convert.ToDateTime(drGia["NGAY_HETHLUC"]).ToString("dd/MM/yyyy", new System.Globalization.CultureInfo("vi-VN")), new System.Globalization.CultureInfo("vi-VN"));
                                                    ngay_hhluc = Convert.ToDateTime(drGia["NGAY_HETHLUC"]);
                                                    if (ngay_ckymax > ngay_hhluc)
                                                    {
                                                        //dwDoiGia.RowFilter = "MA_DVIQLY = '" + strMa_DViQLy + "' and MA_DDO ='" + strMa_DDo + "' AND LOAI_BCS IN ('BT','CD','TD','KT')";
                                                        bool isExistsDoiGiaKH = false;
                                                        foreach (DataRow drvKTraDGia in rows)
                                                        {
                                                            //DateTime ngayhluc_temp = DateTime.Parse(Convert.ToDateTime(drvKTraDGia["NGAY_HLUC"]).ToString("dd/MM/yyyy", new System.Globalization.CultureInfo("vi-VN")), new System.Globalization.CultureInfo("vi-VN"));
                                                            DateTime ngayhluc_temp = Convert.ToDateTime(drvKTraDGia["NGAY_HLUC"]);
                                                            if (ngayhluc_temp <= ngay_hhluc.AddDays(1))
                                                            {
                                                                isExistsDoiGiaKH = true;
                                                                //Chỉ kiểm tra tồn tại ít nhất 1 biên bản giá ngày đổi giá là đủ
                                                                //Việc kiểm soát đủ bộ chỉ số giành cho chức năng áp giá
                                                                break;
                                                            }
                                                        }
                                                        if (!isExistsDoiGiaKH)
                                                            return "NotEnoughDataInHDG_BBAN_APGIA - " + strMa_DDo + " - " + strMa_NhomNN + " nhom " + strMa_NGia + " het hieu luc";
                                                    }
                                                    else
                                                    {
                                                        break;
                                                    }
                                                    //Khong ton tai bien ban ap gia moi trong khi ap gia cu da het hieu luc
                                                }
                                                else if (Convert.ToDecimal(drGia["SOTHUTU"]) < 0 && (strMa_NGia == "D" || strMa_NGia == "F") && strMa_NhomNN != "SHBT")
                                                {
                                                    //Kiểm tra nếu áp giá bán buôn bị phạt đã áp lại giá bán buôn nhóm E chưa
                                                    dwDoiGia.RowFilter = "";
                                                    dwDoiGia.RowFilter = "MA_DVIQLY = '" + strMa_DViQLy + "' and MA_DDO ='" + strMa_DDo + "' AND LOAI_BCS IN ('BT','CD','TD','KT') and (MA_NHOMNN in ('SHBC','SHBD','SHBH','SHBL','SHBB') and MA_NGIA ='E') or (MA_NHOMNN='SHTM' and MA_NGIA = 'K')";

                                                    if (dwDoiGia.Count <= 0)
                                                        return "NotEnoughDataInHDG_BBAN_APGIA - " + strMa_DDo + " - " + strMa_NhomNN + " nhom " + strMa_NGia + " ap lai gia phat";
                                                    foreach (DataRowView drvGiaNhomNN in dwDoiGia)
                                                    {
                                                        //DateTime ngayhluc_temp = DateTime.Parse(Convert.ToDateTime(drvGiaNhomNN["NGAY_HLUC"]).ToString("dd/MM/yyyy", new System.Globalization.CultureInfo("vi-VN")), new System.Globalization.CultureInfo("vi-VN"));
                                                        DateTime ngayhluc_temp = Convert.ToDateTime(drvGiaNhomNN["NGAY_HLUC"]);
                                                        if (ngayhluc_temp > ngay_hluc)
                                                        {
                                                            return "NotEnoughDataInHDG_BBAN_APGIA - " + strMa_DDo + " - " + strMa_NhomNN + " nhom " + strMa_NGia + " ap lai gia phat";
                                                            //Chỉ kiểm tra tồn tại ít nhất 1 biên bản giá ngày đổi giá là đủ
                                                            //Việc kiểm soát đủ bộ chỉ số giành cho chức năng áp giá
                                                            break;
                                                        }
                                                    }
                                                    //Khong ton tai bien ban ap gia phat moi E (gia phat cu la nhom D)

                                                }
                                            }
                                            //End doi gia 01/06/2014 Dũng NT
                                            break;
                                        }
                                    }

                                }
                                if (Don_Gia == -1)
                                    return "NotExistsDonGiaInD_GIA_NHOMNN - " + strMa_DDo; //Khong ton tai don gia trong bang D_GIA_NHOMNN

                                strErrTmp = "B6:" + strMa_DDo;
                                //Kiem tra su ton tai cua ty gia ngoai te (neu la gia cua nuoc ngoai)
                                if (strLoai_Tien.ToUpper() != "VND")
                                {
                                    if (dsStaticCatalog.Tables.Contains("D_TY_GIA") == false)
                                        return "NotExistsD_TY_GIA"; //khong ton tai bang D_TY_GIA
                                    else
                                        if (dsStaticCatalog.Tables["D_TY_GIA"].Rows.Count == 0)
                                        return "NoDataFoundInD_TY_GIA"; //khong co du lieu trong bang D_TY_GIA
                                    else
                                    {
                                        //Kiem tra theo ma don vi quan ly
                                        //dwGia = new DataView(dsStaticCatalog.Tables["D_TY_GIA"]);
                                        //dwGia.RowFilter = "MA_DVIQLY = '" + strMa_DViQLy + "'";
                                        if (dwTyGia.Count == 0)
                                            return "NoDataFoundInD_TY_GIA_DONVI"; //Khong ton tai ty gia ngoai te cua don vi
                                    }
                                }

                                strErrTmp = "B7:" + strMa_DDo;
                                //dwGia = new DataView(dsCustomerData.Tables["HDG_BBAN_APGIA"]);
                                //dwGia.RowFilter = "MA_DVIQLY = '" + strMa_DViQLy + "' AND MA_DDO = '" + strMa_DDo + "' AND LOAI_BCS IN ('BT','CD','TD','KT')";
                                //dwGia.Sort = "NGAY_HLUC";

                                //dwBBanAGia.RowFilter = "MA_DVIQLY = '" + strMa_DViQLy + "' AND MA_DDO = '" + strMa_DDo + "' AND LOAI_BCS IN ('BT','CD','TD','KT')";
                                //Tim bien ban ap gia co max ngay hieu luc lon nhat nho hon min ngay dau ky
                                ngay_hlucgia1 = DateTime.Parse("01/01/1900", new System.Globalization.CultureInfo("vi-VN"));
                                foreach (DataRow drGia in rows)
                                {
                                    if (Convert.ToDateTime(drGia["NGAY_HLUC"]) <= ngay_dkymin)
                                    {
                                        ngay_hlucgia1 = Convert.ToDateTime(drGia["NGAY_HLUC"]);
                                    }
                                    else
                                    {
                                        break;
                                    }
                                }

                                //Kiem tra doi voi nhom gia bac thang, so ho phai lon hon 0                                
                                //dwGia = new DataView(dsStaticCatalog.Tables["D_BAC_THANG"]);
                                //dwGia.RowFilter = "MA_NHOMNN = '" + strMa_NhomNN + "' AND MA_NGIA = '" + strMa_NGia + "'";
                                //dwGia.Sort = "NGAY_HLUC DESC";
                                dwBThang.RowFilter = "MA_NHOMNN = '" + strMa_NhomNN + "' AND MA_NGIA = '" + strMa_NGia + "'";

                                IsExists = 0;
                                foreach (DataRowView drGia in dwBThang)
                                {
                                    //ngay_hluc = DateTime.Parse(Convert.ToDateTime(drGia["NGAY_HLUC"]).ToString("dd/MM/yyyy", new System.Globalization.CultureInfo("vi-VN")), new System.Globalization.CultureInfo("vi-VN"));
                                    ngay_hluc = Convert.ToDateTime(drGia["NGAY_HLUC"]);
                                    if (ngay_dkymin >= ngay_hluc)
                                    {
                                        IsExists = 1;
                                        break;
                                    }
                                    else
                                    {
                                        if (ngay_ckymax >= ngay_hluc)
                                        {
                                            IsExists = 1;
                                            break;
                                        }
                                    }
                                }

                                if (IsExists == 1)
                                {
                                    //La gia bac thang                                    
                                    try
                                    {
                                        //if (ngay_hlucgia1 == DateTime.Parse("01/01/1900", new System.Globalization.CultureInfo("vi-VN")))
                                        if (ngay_hlucgia1 == dt1900)
                                        {
                                            So_Ho = Convert.ToDecimal(dr["SO_HO"]);
                                            if (So_Ho <= 0)
                                                return "NegativeNumberInHDG_BBAN_APGIA - " + strMa_DDo; //Du lieu so ho <= 0
                                        }
                                        else
                                        {
                                            if (Convert.ToDateTime(dr["NGAY_HLUC"]) >= ngay_hlucgia1)
                                            {
                                                So_Ho = Convert.ToDecimal(dr["SO_HO"]);
                                                if (So_Ho <= 0)
                                                    return "NegativeNumberInHDG_BBAN_APGIA - " + strMa_DDo; //Du lieu so ho <= 0
                                            }
                                        }
                                    }
                                    catch
                                    {
                                        return "NotANumberInHDG_BBAN_APGIA - " + strMa_DDo; //Du lieu so ho khong chinh xac
                                    }
                                }
                            }
                        }
                        else
                            return "NoDataFoundInHDG_BBAN_APGIA - " + strMa_DDo;

                        strErrTmp = "B8:" + strMa_DDo;
                        //Kiem tra gia ban buon (neu co)                        
                        //dwGia = new DataView(dsCustomerData.Tables["HDG_BBAN_APGIA"]);
                        //dwGia.RowFilter = "MA_DVIQLY = '" + strMa_DViQLy + "' AND MA_DDO = '" + strMa_DDo + "' AND LOAI_BCS IN ('BT','CD','TD','KT') AND MA_NHOMNN IN ('SHBB','SHBC','SHBD','SHBH','SHBL','SHBM','SHBN')";
                        //dwGia.Sort = "NGAY_HLUC";
                        if (strMa_DDo == "PD02000010470001")
                        {
                        }


                        dwBBanAGia.RowFilter = "MA_DVIQLY = '" + strMa_DViQLy + "' AND MA_DDO = '" + strMa_DDo + "' AND LOAI_BCS IN ('BT','CD','TD','KT') AND MA_NHOMNN IN ('SHBB','SHBC','SHBD','SHBH','SHBL','SHBM','SHTM')";//,'SHBN'
                        if (dwBBanAGia.Count > 0)
                        {
                            //Tim bien ban ap gia co max ngay hieu luc lon nhat nho hon min ngay dau ky
                            //ngay_hlucgia1 = DateTime.Parse("01/01/1900", new System.Globalization.CultureInfo("vi-VN"));
                            ngay_hlucgia1 = new DateTime(1900, 1, 1);
                            foreach (DataRowView drGia in dwBBanAGia)
                            {
                                if (Convert.ToDateTime(drGia["NGAY_HLUC"]) <= ngay_dkymin)
                                {
                                    ngay_hlucgia1 = Convert.ToDateTime(drGia["NGAY_HLUC"]);
                                    strMa_NhomNN = Convert.ToString(drGia["MA_NHOMNN"]);
                                }
                                else
                                {
                                    break;
                                }
                            }

                            //if (ngay_hlucgia1 != DateTime.Parse("01/01/1900", new System.Globalization.CultureInfo("vi-VN")))
                            if (ngay_hlucgia1 != dt1900)
                            {
                                //Co ton tai bien ban ap gia ban buon co max ngay hieu luc nho hon min ngay dau ky
                                ICountGiaBB = 0;
                                foreach (DataRowView drGia in dwBBanAGia)
                                {
                                    if (Convert.ToDateTime(drGia["NGAY_HLUC"]) == ngay_hlucgia1)
                                    {
                                        if (strMa_NhomNN != Convert.ToString(drGia["MA_NHOMNN"]))
                                        {
                                            //nhiều thành phần giá bán buôn khác mã nhóm NN thì bị chặn
                                            ICountGiaBB = 1;
                                            break;
                                        }
                                    }
                                    else if (Convert.ToDateTime(drGia["NGAY_HLUC"]) > ngay_hlucgia1 && Convert.ToDateTime(drGia["NGAY_HLUC"]) <= ngay_ckymax)
                                    {
                                        //Tính lại ngày đổi giá theo mã nhóm ngành nghề
                                        DateTime ngay_doigia_temp = DateTime.Parse(Convert.ToDateTime(dsStaticCatalog.Tables["D_GIA_NHOMNN"].Compute("MAX(NGAY_ADUNG)", "MA_NHOMNN='" + Convert.ToString(drGia["MA_NHOMNN"]) + "'")).ToString("dd/MM/yyyy", new System.Globalization.CultureInfo("vi-VN")), new System.Globalization.CultureInfo("vi-VN"));
                                        if (Convert.ToDateTime(drGia["NGAY_HLUC"]) != ngay_doigia_temp)
                                        {
                                            //nếu ngày đổi giá KH khác ngày đổi giá nhà nước, chặn lại
                                            if (strMa_NhomNN != Convert.ToString(drGia["MA_NHOMNN"]))
                                            {
                                                ICountGiaBB = 1;
                                                break;
                                            }
                                        }
                                        else
                                        {
                                            //Ngày hiệu lực = ngày đổi giá, kiểm tra nếu là SHBB,C,D,H,L,M sang SHBB,C,D,H,L thì cũng chặn lại
                                            //nếu ngày đổi giá KH khác ngày đổi giá nhà nước, chặn lại
                                            if (strMa_NhomNN != Convert.ToString(drGia["MA_NHOMNN"]) && Convert.ToString(drGia["MA_NHOMNN"]) != "SHTM")
                                            {
                                                ICountGiaBB = 1;
                                                break;
                                            }
                                        }
                                    }
                                }
                                if (ICountGiaBB == 1)
                                    return "InvalidGiaBanBuon - " + strMa_DDo; //Co nhieu hon 2 thanh phan ban buon khac nhau, khong cho phep tinh
                            }
                            else
                            {
                                //Khong ton tai bien ban ap gia ban buon co max ngay hieu luc nho hon min ngay dau ky
                                ICountGiaBB = 0;
                                foreach (DataRowView drGia in dwBBanAGia)
                                {
                                    ////Tính lại ngày đổi giá theo mã nhóm ngành nghề của dòng đầu tiên
                                    //DateTime ngay_doigia_temp = DateTime.Parse(Convert.ToDateTime(dsStaticCatalog.Tables["D_GIA_NHOMNN"].Compute("MAX(NGAY_ADUNG)", "MA_NHOMNN='" + Convert.ToString(drGia["MA_NHOMNN"]) + "'")).ToString("dd/MM/yyyy", new System.Globalization.CultureInfo("vi-VN")), new System.Globalization.CultureInfo("vi-VN"));
                                    //if (Convert.ToDateTime(drGia["NGAY_HLUC"]) != ngay_doigia_temp)
                                    //{
                                    //    //nếu ngày đổi giá KH khác ngày đổi giá nhà nước, chặn lại
                                    if (Convert.ToString(dwBBanAGia[0]["MA_NHOMNN"]) != Convert.ToString(drGia["MA_NHOMNN"]))
                                    {
                                        if (Convert.ToDateTime(dwBBanAGia[0]["NGAY_HLUC"]) != Convert.ToDateTime(drGia["NGAY_HLUC"]))
                                        {
                                            //Tính lại ngày đổi giá theo mã nhóm ngành nghề của dòng giá đang xét
                                            DateTime ngay_doigia_temp = DateTime.Parse(Convert.ToDateTime(dsStaticCatalog.Tables["D_GIA_NHOMNN"].Compute("MAX(NGAY_ADUNG)", "MA_NHOMNN='" + Convert.ToString(drGia["MA_NHOMNN"]) + "'")).ToString("dd/MM/yyyy", new System.Globalization.CultureInfo("vi-VN")), new System.Globalization.CultureInfo("vi-VN"));
                                            if (Convert.ToDateTime(drGia["NGAY_HLUC"]) != ngay_doigia_temp)
                                            {
                                                //Không phải thay đổi giá bán buôn vào ngày đổi giá nhà nước
                                                ICountGiaBB = 1;
                                                break;
                                            }
                                            else
                                            {
                                                //Kiểm tra nếu nhóm giá mới khác SHTM thì chặn
                                                if (Convert.ToString(dwBBanAGia[0]["MA_NHOMNN"]) != "SHTM")
                                                {
                                                    ICountGiaBB = 1;
                                                    break;
                                                }

                                            }
                                        }
                                        else
                                        {
                                            //Nhiều thành phần giá bán buôn
                                            ICountGiaBB = 1;
                                            break;
                                        }
                                    }
                                    //}  
                                }
                                if (ICountGiaBB == 1)
                                    return "InvalidGiaBanBuon - " + strMa_DDo; //Co nhieu hon 2 thanh phan ban buon khac nhau, khong cho phep tinh
                            }
                        }

                        strErrTmp = "B9:" + strMa_DDo;
                        //Kiem tra dup chi so dinh ki
                        rows = dsCustomerData.Tables["GCS_CHISO"].Select("MA_DVIQLY = '" + strMa_DViQLy + "' AND MA_DDO = '" + strMa_DDo + "' AND BCS = 'BT' AND LOAI_CHISO = 'DDK'");
                        if (rows.Length > 1)
                            return "DuplicateDataInGCS_CHISO - " + strMa_DDo; //Dup chi so dinh ki bo BT

                        rows = dsCustomerData.Tables["GCS_CHISO"].Select("MA_DVIQLY = '" + strMa_DViQLy + "' AND MA_DDO = '" + strMa_DDo + "' AND BCS = 'CD' AND LOAI_CHISO = 'DDK'");
                        if (rows.Length > 1)
                            return "DuplicateDataInGCS_CHISO - " + strMa_DDo; //Dup chi so dinh ki bo CD

                        rows = dsCustomerData.Tables["GCS_CHISO"].Select("MA_DVIQLY = '" + strMa_DViQLy + "' AND MA_DDO = '" + strMa_DDo + "' AND BCS = 'TD' AND LOAI_CHISO = 'DDK'");
                        if (rows.Length > 1)
                            return "DuplicateDataInGCS_CHISO - " + strMa_DDo; //Dup chi so dinh ki bo TD

                        rows = dsCustomerData.Tables["GCS_CHISO"].Select("MA_DVIQLY = '" + strMa_DViQLy + "' AND MA_DDO = '" + strMa_DDo + "' AND BCS = 'KT' AND LOAI_CHISO = 'DDK'");
                        if (rows.Length > 1)
                            return "DuplicateDataInGCS_CHISO - " + strMa_DDo; //Dup chi so dinh ki bo KT

                        rows = dsCustomerData.Tables["GCS_CHISO"].Select("MA_DVIQLY = '" + strMa_DViQLy + "' AND MA_DDO = '" + strMa_DDo + "' AND BCS = 'VC' AND LOAI_CHISO = 'DDK'");
                        if (rows.Length > 1)
                            return "DuplicateDataInGCS_CHISO - " + strMa_DDo; //Dup chi so dinh ki bo VC
                        //Kiểm tra có dòng VC mà không có HC thì cảnh báo
                        else if (rows.Length == 1)
                        {
                            arrCheck = dsCustomerData.Tables["GCS_CHISO"].Select("MA_DVIQLY = '" + strMa_DViQLy + "' AND MA_DDO = '" + strMa_DDo + "' AND BCS in ('BT','CD','TD','KT') AND LOAI_CHISO = 'DDK'");
                            if (arrCheck == null || arrCheck.Length <= 0)
                                return "NotExitsData(HC)InGCS_CHISO - " + strMa_DDo;
                        }
                        //Kiem tra dup chi so chot doi gia
                        rows = dsCustomerData.Tables["GCS_CHISO"].Select("MA_DVIQLY = '" + strMa_DViQLy + "' AND MA_DDO = '" + strMa_DDo + "' AND BCS = 'BT' AND LOAI_CHISO = 'CCS'");
                        if (rows.Length > 1)
                            return "DuplicateDataCCS_InGCS_CHISO - " + strMa_DDo; //Dup chi so dinh ki bo BT

                        rows = dsCustomerData.Tables["GCS_CHISO"].Select("MA_DVIQLY = '" + strMa_DViQLy + "' AND MA_DDO = '" + strMa_DDo + "' AND BCS = 'CD' AND LOAI_CHISO = 'CCS'");
                        if (rows.Length > 1)
                            return "DuplicateDataCCS_InGCS_CHISO - " + strMa_DDo; //Dup chi so dinh ki bo CD

                        rows = dsCustomerData.Tables["GCS_CHISO"].Select("MA_DVIQLY = '" + strMa_DViQLy + "' AND MA_DDO = '" + strMa_DDo + "' AND BCS = 'TD' AND LOAI_CHISO = 'CCS'");
                        if (rows.Length > 1)
                            return "DuplicateDataCCS_InGCS_CHISO - " + strMa_DDo; //Dup chi so dinh ki bo TD

                        rows = dsCustomerData.Tables["GCS_CHISO"].Select("MA_DVIQLY = '" + strMa_DViQLy + "' AND MA_DDO = '" + strMa_DDo + "' AND BCS = 'KT' AND LOAI_CHISO = 'CCS'");
                        if (rows.Length > 1)
                            return "DuplicateDataCCS_InGCS_CHISO - " + strMa_DDo; //Dup chi so dinh ki bo KT

                        rows = dsCustomerData.Tables["GCS_CHISO"].Select("MA_DVIQLY = '" + strMa_DViQLy + "' AND MA_DDO = '" + strMa_DDo + "' AND BCS = 'VC' AND LOAI_CHISO = 'CCS'");
                        if (rows.Length > 1)
                            return "DuplicateDataCCS_InGCS_CHISO - " + strMa_DDo; //Dup chi so dinh ki bo VC
                        else if (rows.Length == 1)
                        {
                            arrCheck = dsCustomerData.Tables["GCS_CHISO"].Select("MA_DVIQLY = '" + strMa_DViQLy + "' AND MA_DDO = '" + strMa_DDo + "' AND BCS in ('BT','CD','TD','KT') AND LOAI_CHISO = 'CCS'");
                            if (arrCheck == null || arrCheck.Length <= 0)
                                return "NotExitsData(HC)InGCS_CHISO - " + strMa_DDo;
                        }

                        string strDiemDo_Phu = "", strDiemDo_Chinh = "";
                        decimal sl_chinh, sl_phu;
                        DataRow[] rowsChinh;
                        strErrTmp = "B10:" + strMa_DDo;
                        //Kiem tra san luong diem do chinh > san luong diem do phu
                        if (dsCustomerData.Tables.Contains("HDG_QHE_DDO") == true && dsCustomerData.Tables.Contains("GCS_CHISO_TP"))
                        {
                            rows = dsCustomerData.Tables["HDG_QHE_DDO"].Select("MA_DDO_CHINH = '" + strMa_DDo + "' AND LOAI_QHE IN ('10','11','21','20')");
                            if (rows.Length > 0)
                            {
                                //Co ton tai quan he tru phu
                                foreach (DataRow dr in rows)
                                {
                                    strDiemDo_Chinh = "";
                                    strDiemDo_Phu = Convert.ToString(dr["MA_DDO_PHU"]).Trim();
                                    rowsChinh = dsCustomerData.Tables["HDG_QHE_DDO"].Select("MA_DDO_PHU = '" + strDiemDo_Phu + "' AND LOAI_QHE IN ('10','11','21','20')");
                                    foreach (DataRow drChinh in rowsChinh)
                                    {
                                        strDiemDo_Chinh = strDiemDo_Chinh + "'" + Convert.ToString(drChinh["MA_DDO_CHINH"]) + "',";
                                    }
                                    strDiemDo_Chinh = strDiemDo_Chinh.Substring(0, strDiemDo_Chinh.Length - 1);

                                    sl_chinh = 0;
                                    sl_phu = 0;
                                    DataRow[] arrCS_Chinh = dsCustomerData.Tables["GCS_CHISO"].Select("MA_DVIQLY = '" + strMa_DViQLy + "' AND MA_DDO IN (" + strDiemDo_Chinh + ") AND BCS IN ('KT','BT','CD','TD')");
                                    DataRow[] arrCS_Phu = dsCustomerData.Tables["GCS_CHISO"].Select("MA_DVIQLY = '" + strMa_DViQLy + "' AND MA_DDO = '" + strDiemDo_Phu + "' AND BCS IN ('KT','BT','CD','TD')");
                                    if (arrCS_Chinh.Length > 0)
                                        foreach (DataRow drCSChinh in arrCS_Chinh)
                                        {
                                            sl_chinh += Utility.DecimalDbnull(drCSChinh["SAN_LUONG"]) + Utility.DecimalDbnull(drCSChinh["SLUONG_TTIEP"]);
                                        }
                                    if (arrCS_Phu.Length > 0)
                                        foreach (DataRow drCSPhu in arrCS_Phu)
                                        {
                                            sl_phu += Utility.DecimalDbnull(drCSPhu["SAN_LUONG"]) + Utility.DecimalDbnull(drCSPhu["SLUONG_TTIEP"]);
                                        }
                                    //sl_chinh = Utility.DecimalDbnull(dsCustomerData.Tables["GCS_CHISO"].Compute("SUM(SAN_LUONG)", "MA_DVIQLY = '" + strMa_DViQLy + "' AND MA_DDO IN (" + strDiemDo_Chinh + ") AND BCS IN ('KT','BT','CD','TD')")) + Utility.DecimalDbnull(dsCustomerData.Tables["GCS_CHISO"].Compute("SUM(SLUONG_TTIEP)", "MA_DVIQLY = '" + strMa_DViQLy + "' AND MA_DDO IN (" + strDiemDo_Chinh + ") AND BCS IN ('KT','BT','CD','TD')"));
                                    //sl_phu = Utility.DecimalDbnull(dsCustomerData.Tables["GCS_CHISO_TP"].Compute("SUM(SAN_LUONG)", "MA_DVIQLY = '" + strMa_DViQLy + "' AND MA_DDO = '" + strDiemDo_Phu + "' AND BCS IN ('KT','BT','CD','TD')")) + Utility.DecimalDbnull(dsCustomerData.Tables["GCS_CHISO_TP"].Compute("SUM(SLUONG_TTIEP)", "MA_DVIQLY = '" + strMa_DViQLy + "' AND MA_DDO = '" + strDiemDo_Phu + "' AND BCS IN ('KT','BT','CD','TD')"));



                                    if (sl_chinh < sl_phu)
                                        return "SLCHC_LessThan_SLPHC - " + strMa_DDo + " & " + strDiemDo_Phu; //San luong diem do chinh huu cong nho hon diem do phu

                                    //sl_chinh = Utility.DecimalDbnull(dsCustomerData.Tables["GCS_CHISO"].Compute("SUM(SAN_LUONG)", "MA_DVIQLY = '" + strMa_DViQLy + "' AND MA_DDO = '" + strMa_DDo + "' AND BCS = 'VC'")) + Utility.DecimalDbnull(dsCustomerData.Tables["GCS_CHISO"].Compute("SUM(SLUONG_TTIEP)", "MA_DVIQLY = '" + strMa_DViQLy + "' AND MA_DDO = '" + strMa_DDo + "' AND BCS = 'VC'"));
                                    //sl_phu = Utility.DecimalDbnull(dsCustomerData.Tables["GCS_CHISO_TP"].Compute("SUM(SAN_LUONG)", "MA_DVIQLY = '" + strMa_DViQLy + "' AND MA_DDO = '" + strDiemDo_Phu + "' AND BCS = 'VC'")) + Utility.DecimalDbnull(dsCustomerData.Tables["GCS_CHISO_TP"].Compute("SUM(SLUONG_TTIEP)", "MA_DVIQLY = '" + strMa_DViQLy + "' AND MA_DDO = '" + strDiemDo_Phu + "' AND BCS = 'VC'"));
                                    //if (sl_chinh < sl_phu)
                                    //    return "SLCVC_LessThan_SLPVC - " + strMa_DDo + " & " + strDiemDo_Phu; //San luong diem do chinh vo cong nho hon diem do phu

                                }
                            }
                        }
                        strErrTmp = "B11:" + strMa_DDo;
                    }
                }
                //Debug.WriteLine(DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff") + " CheckValidData CheckCSAndAG");
                //Dũng NT bổ sung kiểm tra trường hợp SLMDK chốt chỉ số, CTT ko chốt
                string strDDo = "";
                if (dsCustomerData.Tables.Contains("GCS_SLMDK_SHBB") == true && dsCustomerData.Tables["GCS_SLMDK_SHBB"].Rows.Count > 0)
                {
                    dvMDK = dsCustomerData.Tables["GCS_SLMDK_SHBB"].DefaultView;
                    dvMDK.RowFilter = "MDK_CCS is not null or MDK_BT_CCS is not null or MDK_CD_CCS is not null or MDK_TD_CCS is not null";
                    if (dvMDK.Count > 0)
                    {
                        dwCS = new DataView(dsCustomerData.Tables["GCS_CHISO"]);
                        foreach (DataRowView drv in dvMDK)
                        {
                            strMa_DDo = drv["MA_DDO"].ToString();
                            dwCS.RowFilter = "MA_DDO = '" + strMa_DDo + "' and LOAI_CHISO = 'CCS'";
                            if (dwCS == null || dwCS.Count <= 0)
                            {
                                strDDo += strDDo.Trim().Length > 0 ? (";" + strMa_DDo) : strMa_DDo;
                            }
                            dwCS.RowFilter = "";
                        }
                    }
                }
                if (dsCustomerData.Tables.Contains("GCS_SLMDK_SHBB_GT") == true && dsCustomerData.Tables["GCS_SLMDK_SHBB_GT"].Rows.Count > 0)
                {
                    dvMDK = dsCustomerData.Tables["GCS_SLMDK_SHBB_GT"].DefaultView;
                    dvMDK.RowFilter = "MDK_CCS is not null or MDK_BT_CCS is not null or MDK_CD_CCS is not null or MDK_TD_CCS is not null";
                    if (dvMDK.Count > 0)
                    {
                        if (dsCustomerData.Tables["GCS_CHISO_GT"] == null || dsCustomerData.Tables["GCS_CHISO_GT"].Rows.Count <= 0)
                        {
                            foreach (DataRowView drv in dvMDK)
                            {
                                strMa_DDo = drv["MA_DDO"].ToString();
                                strDDo += strDDo.Trim().Length > 0 ? (";" + strMa_DDo) : strMa_DDo;
                                continue;
                            }
                        }
                        else
                        {
                            dvCS_GT = new DataView(dsCustomerData.Tables["GCS_CHISO_GT"]);
                            foreach (DataRowView drv in dvMDK)
                            {
                                strMa_DDo = drv["MA_DDO"].ToString();
                                dvCS_GT.RowFilter = "MA_DDO = '" + strMa_DDo + "' and LOAI_CHISO = 'CCS'";
                                if (dvCS_GT == null || dvCS_GT.Count <= 0)
                                {
                                    strDDo += strDDo.Trim().Length > 0 ? (";" + strMa_DDo) : strMa_DDo;
                                }
                            }
                        }
                    }
                }
                if (strDDo.Trim().Length > 0)
                {
                    return "CheckValidData: Các điểm đo " + strDDo + " có chốt MDK nhưng không chốt chỉ số CTT. Cần nhập chỉ số chốt CTT";
                }
                //Debug.WriteLine(DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff") + " CheckValidData GCS_SLMDK_SHBB");
                //Dũng NT bổ sung thêm trường hợp kiểm tra điểm đo cosfi bình quân phải ký mua CSPK
                if (dsCustomerData.Tables.Contains("HDG_QHE_DDO_BQ") && dsCustomerData.Tables["HDG_QHE_DDO_BQ"].Rows.Count > 0)
                {
                    if (!dsCustomerData.Tables["HDG_QHE_DDO_BQ"].Columns.Contains("KIMUA_CSPK"))
                    {
                        return "CheckValidData: Package chưa cập nhật chính xác, thiếu cột KIMUA_CSPK (HDG_QHE_DDO_BQ)";
                    }
                    dwCSBQ = new DataView(dsCustomerData.Tables["HDG_QHE_DDO_BQ"]);
                    dwCSBQ.RowFilter = "KIMUA_CSPK=0";
                    if (dwCSBQ != null && dwCSBQ.Count > 0)
                    {
                        foreach (DataRowView drv in dwCSBQ)
                        {
                            strDDo += strDDo.Trim().Length > 0 ? (";" + drv["MA_DDO"].ToString().Trim()) : drv["MA_DDO"].ToString().Trim();

                        }
                    }
                    if (strDDo.Trim().Length > 0)
                    {
                        return "CheckValidData: Các điểm đo " + strDDo + " có quan hệ cosfi bình quân nhưng chưa ký mua CSPK.";
                    }
                }
                else if (dsCustomerData.Tables.Contains("GCS_CHISO_BQ") && dsCustomerData.Tables["GCS_CHISO_BQ"].Rows.Count > 0)
                {
                    if (!dsCustomerData.Tables["GCS_CHISO_BQ"].Columns.Contains("KIMUA_CSPK"))
                    {
                        return "CheckValidData: Package chưa cập nhật chính xác, thiếu cột KIMUA_CSPK";
                    }
                    dwCSBQ = new DataView(dsCustomerData.Tables["GCS_CHISO_BQ"]);
                    dwCSBQ.RowFilter = "KIMUA_CSPK=0";
                    if (dwCSBQ != null && dwCSBQ.Count > 0)
                    {
                        foreach (DataRowView drv in dwCSBQ)
                        {
                            strDDo += strDDo.Trim().Length > 0 ? (";" + drv["MA_DDO"].ToString().Trim()) : drv["MA_DDO"].ToString().Trim();

                        }
                    }
                    if (strDDo.Trim().Length > 0)
                    {
                        return "CheckValidData: Các điểm đo " + strDDo + " có quan hệ cosfi bình quân nhưng chưa ký mua CSPK.";
                    }
                }
                //Debug.WriteLine(DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff") + " CheckValidData KIMUA_CSPK");
                if (IsChanged == 1)
                {
                    dsCustomerData.Tables["GCS_CHISO"].AcceptChanges();
                }
                //Debug.WriteLine(DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff") + " CheckValidData AcceptChanges");
                cls_Config config = new cls_Config();
                DateTime dtNow = DateTime.Now.Date;
                string strListKH_8 = "";
                string strListKH_10 = "";
                if (dtNow>config.dtEnd_8 || dtNow < config.dtBegin_8)
                {
                    foreach ( DataRow drKH in dsCustomerData.Tables["HDG_KHACH_HANG"].Rows)
                    {
                        int i32TLThue = Convert.ToInt32(drKH["TLE_THUE"]);
                        if (i32TLThue == 8) strListKH_8 += drKH["MA_KHANG"].ToString().Trim() + "\n";
                    }
                    if (strListKH_8.Trim().Length > 0)
                        return "CheckValidData: Tồn tại KH có tỷ lệ thuế 8%, cần đổi tỷ lệ thuế thành 10%: " + "\n" + strListKH_8;
                }    
                else 
                {
                    foreach (DataRow drKH in dsCustomerData.Tables["HDG_KHACH_HANG"].Rows)
                    {
                        int i32TLThue = Convert.ToInt32(drKH["TLE_THUE"]);
                        if (i32TLThue == 10) strListKH_10 += drKH["MA_KHANG"].ToString().Trim() + "\n";
                    }
                    if (strListKH_8.Trim().Length > 0)
                        return "CheckValidData: Tồn tại KH có tỷ lệ thuế 10%, cần đổi tỷ lệ thuế thành 8%: " + "\n" + strListKH_8;
                }    



                return "";
            }
            catch (Exception ex)
            {
                return "CheckValidData: " + strErrTmp + " - " + ex.Message;
            }
        }
        #endregion

        #region Method Tinh hoa don le cho khach hang
        public string getCustomerDataReading_2(ref DataSet dsCustomerData, string strMaDViQLy, string strMa_KHang, Int16 i16Ky, Int16 i16Thang, Int16 i16Nam)
        {
            CMIS2 db = new CMIS2();
            dsCustomerData.Clear();
            dsCustomerData = new DataSet();
            try
            {
                db.DB.Sp_getcustomerdata_1(strMaDViQLy, strMa_KHang, i16Ky, i16Thang, i16Nam,
                                         out dsCustomerData, out dsCustomerData, out dsCustomerData, out dsCustomerData, out dsCustomerData, out dsCustomerData, out dsCustomerData, out dsCustomerData, out dsCustomerData, out dsCustomerData,
                                         out dsCustomerData, out dsCustomerData, out dsCustomerData, out dsCustomerData, out dsCustomerData, out dsCustomerData, out dsCustomerData, out dsCustomerData, out dsCustomerData, out dsCustomerData,
                                         out dsCustomerData, out dsCustomerData);

                for (Int16 iDs = 0; iDs < 22; iDs++)
                {
                    switch (iDs)
                    {
                        case 0:
                            if (dsCustomerData.Tables[iDs] != null && dsCustomerData.Tables[iDs].Rows.Count != 0)
                            {
                                dsCustomerData.Tables[iDs].TableName = "HDG_DDO_SOGCS";
                            }
                            break;
                        case 1:
                            if (dsCustomerData.Tables[iDs] != null && dsCustomerData.Tables[iDs].Rows.Count != 0)
                            {
                                dsCustomerData.Tables[iDs].TableName = "HDG_QHE_DDO";
                            }
                            break;
                        case 2:
                            if (dsCustomerData.Tables[iDs] != null && dsCustomerData.Tables[iDs].Rows.Count != 0)
                            {
                                dsCustomerData.Tables[iDs].TableName = "HDG_DIEM_DO";
                            }
                            break;
                        case 3:
                            if (dsCustomerData.Tables[iDs] != null && dsCustomerData.Tables[iDs].Rows.Count != 0)
                            {
                                dsCustomerData.Tables[iDs].TableName = "HDG_BBAN_APGIA";
                            }
                            break;
                        case 4:
                            if (dsCustomerData.Tables[iDs] != null && dsCustomerData.Tables[iDs].Rows.Count != 0)
                            {
                                dsCustomerData.Tables[iDs].TableName = "HDG_KHACH_HANG";
                            }
                            break;
                        case 5:
                            if (dsCustomerData.Tables[iDs] != null && dsCustomerData.Tables[iDs].Rows.Count != 0)
                            {
                                dsCustomerData.Tables[iDs].TableName = "HDG_PTHUC_TTOAN";
                            }
                            break;
                        case 6:
                            if (dsCustomerData.Tables[iDs] != null && dsCustomerData.Tables[iDs].Rows.Count != 0)
                            {
                                dsCustomerData.Tables[iDs].TableName = "HDG_VITRI_DDO";
                            }
                            break;
                        case 7:
                            if (dsCustomerData.Tables[iDs] != null && dsCustomerData.Tables[iDs].Rows.Count != 0)
                            {
                                dsCustomerData.Tables[iDs].TableName = "D_SOGCS";
                            }
                            break;
                        case 8:
                            if (dsCustomerData.Tables[iDs] != null && dsCustomerData.Tables[iDs].Rows.Count != 0)
                            {
                                dsCustomerData.Tables[iDs].TableName = "GCS_LICHGCS";
                            }
                            break;
                        case 9:
                            if (dsCustomerData.Tables[iDs] != null && dsCustomerData.Tables[iDs].Rows.Count != 0)
                            {
                                dsCustomerData.Tables[iDs].TableName = "GCS_CHISO";
                            }
                            break;
                        case 10:
                            if (dsCustomerData.Tables[iDs] != null && dsCustomerData.Tables[iDs].Rows.Count != 0)
                            {
                                dsCustomerData.Tables[iDs].TableName = "HDG_DIEM_DO_GT";
                            }
                            break;
                        case 11:
                            if (dsCustomerData.Tables[iDs] != null && dsCustomerData.Tables[iDs].Rows.Count != 0)
                            {
                                dsCustomerData.Tables[iDs].TableName = "HDG_DDO_SOGCS_GT";
                            }
                            break;
                        case 12:
                            if (dsCustomerData.Tables[iDs] != null && dsCustomerData.Tables[iDs].Rows.Count != 0)
                            {
                                dsCustomerData.Tables[iDs].TableName = "HDG_BBAN_APGIA_GT";
                            }
                            break;
                        case 13:
                            if (dsCustomerData.Tables[iDs] != null && dsCustomerData.Tables[iDs].Rows.Count != 0)
                            {
                                dsCustomerData.Tables[iDs].TableName = "D_SOGCS_GT";
                            }
                            break;
                        case 14:
                            if (dsCustomerData.Tables[iDs] != null && dsCustomerData.Tables[iDs].Rows.Count != 0)
                            {
                                dsCustomerData.Tables[iDs].TableName = "GCS_LICHGCS_GT";
                            }
                            break;
                        case 15:
                            if (dsCustomerData.Tables[iDs] != null && dsCustomerData.Tables[iDs].Rows.Count != 0)
                            {
                                dsCustomerData.Tables[iDs].TableName = "GCS_CHISO_TP";
                            }
                            break;
                        case 16:
                            if (dsCustomerData.Tables[iDs] != null && dsCustomerData.Tables[iDs].Rows.Count != 0)
                            {
                                dsCustomerData.Tables[iDs].TableName = "GCS_CHISO_BQ";
                            }
                            break;
                        case 17:
                            if (dsCustomerData.Tables[iDs] != null && dsCustomerData.Tables[iDs].Rows.Count != 0)
                            {
                                dsCustomerData.Tables[iDs].TableName = "GCS_CHISO_GT";
                            }
                            break;
                        case 18:
                            if (dsCustomerData.Tables[iDs] != null && dsCustomerData.Tables[iDs].Rows.Count != 0)
                            {
                                dsCustomerData.Tables[iDs].TableName = "GCS_SLMDK_SHBB";
                            }
                            break;
                        case 19:
                            if (dsCustomerData.Tables[iDs] != null && dsCustomerData.Tables[iDs].Rows.Count != 0)
                            {
                                dsCustomerData.Tables[iDs].TableName = "GCS_SLMDK_SHBB_GT";
                            }
                            break;
                        case 20:
                            if (dsCustomerData.Tables[iDs] != null && dsCustomerData.Tables[iDs].Rows.Count != 0)
                            {
                                dsCustomerData.Tables[iDs].TableName = "HDG_VITRI_DDO_GT";
                            }
                            break;
                        case 21:
                            if (dsCustomerData.Tables[iDs] != null && dsCustomerData.Tables[iDs].Rows.Count != 0)
                            {
                                dsCustomerData.Tables[iDs].TableName = "GCS_CHISO_PHUGT";
                            }
                            break;
                        default:
                            break;
                    }
                }

                for (Int16 iDs = 21; iDs >= 0; iDs--)
                {
                    if (dsCustomerData.Tables[iDs] != null && dsCustomerData.Tables[iDs].Rows.Count == 0)
                    {
                        dsCustomerData.Tables.RemoveAt(iDs);
                    }
                }

                dsCustomerData.AcceptChanges();

                //Xu ly du lieu 
                dsCustomerData.Tables["GCS_CHISO"].Columns.Add("ID_CHISO1", typeof(System.Int64));
                dsCustomerData.Tables["GCS_CHISO"].Columns.Add("ID_BCS1", typeof(System.Int64));
                dsCustomerData.Tables["GCS_CHISO"].Columns.Add("KY1", typeof(System.Int16));
                dsCustomerData.Tables["GCS_CHISO"].Columns.Add("THANG1", typeof(System.Int16));
                dsCustomerData.Tables["GCS_CHISO"].Columns.Add("NAM1", typeof(System.Int16));
                foreach (DataRow dr in dsCustomerData.Tables["GCS_CHISO"].Rows)
                {
                    dr["ID_CHISO1"] = Convert.ToInt64(dr["ID_CHISO"]);
                    dr["ID_BCS1"] = Convert.ToInt64(dr["ID_BCS"]);
                    dr["KY1"] = Convert.ToInt16(dr["KY"]);
                    dr["THANG1"] = Convert.ToInt16(dr["THANG"]);
                    dr["NAM1"] = Convert.ToInt16(dr["NAM"]);
                }
                dsCustomerData.Tables["GCS_CHISO"].Columns.Remove("ID_CHISO");
                dsCustomerData.Tables["GCS_CHISO"].Columns.Remove("ID_BCS");
                dsCustomerData.Tables["GCS_CHISO"].Columns.Remove("KY");
                dsCustomerData.Tables["GCS_CHISO"].Columns.Remove("THANG");
                dsCustomerData.Tables["GCS_CHISO"].Columns.Remove("NAM");

                dsCustomerData.Tables["GCS_CHISO"].Columns.Add("ID_CHISO", typeof(System.Int64));
                dsCustomerData.Tables["GCS_CHISO"].Columns.Add("ID_BCS", typeof(System.Int64));
                dsCustomerData.Tables["GCS_CHISO"].Columns.Add("KY", typeof(System.Int16));
                dsCustomerData.Tables["GCS_CHISO"].Columns.Add("THANG", typeof(System.Int16));
                dsCustomerData.Tables["GCS_CHISO"].Columns.Add("NAM", typeof(System.Int16));
                foreach (DataRow dr in dsCustomerData.Tables["GCS_CHISO"].Rows)
                {
                    dr["ID_CHISO"] = Convert.ToInt64(dr["ID_CHISO1"]);
                    dr["ID_BCS"] = Convert.ToInt64(dr["ID_BCS1"]);
                    dr["KY"] = Convert.ToInt16(dr["KY1"]);
                    dr["THANG"] = Convert.ToInt16(dr["THANG1"]);
                    dr["NAM"] = Convert.ToInt16(dr["NAM1"]);
                }
                dsCustomerData.Tables["GCS_CHISO"].Columns.Remove("ID_CHISO1");
                dsCustomerData.Tables["GCS_CHISO"].Columns.Remove("ID_BCS1");
                dsCustomerData.Tables["GCS_CHISO"].Columns.Remove("KY1");
                dsCustomerData.Tables["GCS_CHISO"].Columns.Remove("THANG1");
                dsCustomerData.Tables["GCS_CHISO"].Columns.Remove("NAM1");

                if (dsCustomerData.Tables.Contains("GCS_CHISO_GT") == true)
                {
                    if (dsCustomerData.Tables["GCS_CHISO_GT"].Rows.Count > 0)
                    {
                        dsCustomerData.Tables["GCS_CHISO_GT"].Columns.Add("ID_CHISO1", typeof(System.Int64));
                        dsCustomerData.Tables["GCS_CHISO_GT"].Columns.Add("ID_BCS1", typeof(System.Int64));
                        dsCustomerData.Tables["GCS_CHISO_GT"].Columns.Add("KY1", typeof(System.Int16));
                        dsCustomerData.Tables["GCS_CHISO_GT"].Columns.Add("THANG1", typeof(System.Int16));
                        dsCustomerData.Tables["GCS_CHISO_GT"].Columns.Add("NAM1", typeof(System.Int16));
                        foreach (DataRow dr in dsCustomerData.Tables["GCS_CHISO_GT"].Rows)
                        {
                            dr["ID_CHISO1"] = Convert.ToInt64(dr["ID_CHISO"]);
                            dr["ID_BCS1"] = Convert.ToInt64(dr["ID_BCS"]);
                            dr["KY1"] = Convert.ToInt16(dr["KY"]);
                            dr["THANG1"] = Convert.ToInt16(dr["THANG"]);
                            dr["NAM1"] = Convert.ToInt16(dr["NAM"]);
                        }
                        dsCustomerData.Tables["GCS_CHISO_GT"].Columns.Remove("ID_CHISO");
                        dsCustomerData.Tables["GCS_CHISO_GT"].Columns.Remove("ID_BCS");
                        dsCustomerData.Tables["GCS_CHISO_GT"].Columns.Remove("KY");
                        dsCustomerData.Tables["GCS_CHISO_GT"].Columns.Remove("THANG");
                        dsCustomerData.Tables["GCS_CHISO_GT"].Columns.Remove("NAM");

                        dsCustomerData.Tables["GCS_CHISO_GT"].Columns.Add("ID_CHISO", typeof(System.Int64));
                        dsCustomerData.Tables["GCS_CHISO_GT"].Columns.Add("ID_BCS", typeof(System.Int64));
                        dsCustomerData.Tables["GCS_CHISO_GT"].Columns.Add("KY", typeof(System.Int16));
                        dsCustomerData.Tables["GCS_CHISO_GT"].Columns.Add("THANG", typeof(System.Int16));
                        dsCustomerData.Tables["GCS_CHISO_GT"].Columns.Add("NAM", typeof(System.Int16));
                        foreach (DataRow dr in dsCustomerData.Tables["GCS_CHISO_GT"].Rows)
                        {
                            dr["ID_CHISO"] = Convert.ToInt64(dr["ID_CHISO1"]);
                            dr["ID_BCS"] = Convert.ToInt64(dr["ID_BCS1"]);
                            dr["KY"] = Convert.ToInt16(dr["KY1"]);
                            dr["THANG"] = Convert.ToInt16(dr["THANG1"]);
                            dr["NAM"] = Convert.ToInt16(dr["NAM1"]);
                        }
                        dsCustomerData.Tables["GCS_CHISO_GT"].Columns.Remove("ID_CHISO1");
                        dsCustomerData.Tables["GCS_CHISO_GT"].Columns.Remove("ID_BCS1");
                        dsCustomerData.Tables["GCS_CHISO_GT"].Columns.Remove("KY1");
                        dsCustomerData.Tables["GCS_CHISO_GT"].Columns.Remove("THANG1");
                        dsCustomerData.Tables["GCS_CHISO_GT"].Columns.Remove("NAM1");
                    }
                }
                dsCustomerData.AcceptChanges();
                List<string> strNotNull = new List<string>()
            {
                "HDG_DDO_SOGCS",
                "HDG_DIEM_DO",
                "HDG_BBAN_APGIA",
                "HDG_KHACH_HANG",
                "HDG_PTHUC_TTOAN",
                "HDG_VITRI_DDO",
                "D_SOGCS",
                "GCS_LICHGCS",
                "GCS_CHISO"
            };
                int i = dsCustomerData.Tables.Count - 1;
                while (i >= 0)
                {
                    DataTable dt = dsCustomerData.Tables[i];
                    if (dt.Rows.Count == 0)
                    {
                        if (strNotNull.Contains(dt.TableName))
                        {
                            dsCustomerData = null;
                            return dt.TableName;
                        }
                        else
                        {
                            dsCustomerData.Tables.Remove(dt);
                        }
                    }
                    i--;
                }
                return "";
            }
            catch (Exception ex)
            {
                return "Lỗi khi lấy dữ liệu: " + ex.Message;
            }
            finally
            {
                db.ReleaseConnection();
            }
        }
        #endregion
        #region Tinh hoa don cho ung dung hien truong 2016
        //public string getCustomerData_For_App(ref DataSet dsCustomerData, string strMaDViQLy, string strMaSoGCS, string strListMaKHang, string strListMaDDo, Int16 i16Ky, Int16 i16Thang, Int16 i16Nam)
        //{
        //    CMIS2 db = new CMIS2();
        //    dsCustomerData.Clear();
        //    dsCustomerData = new DataSet();
        //    try
        //    {
        //        db.DB.Sp_getcustomerdata_for_app(strMaDViQLy, strMaSoGCS, i16Ky, i16Thang, i16Nam, strListMaKHang, strListMaDDo,
        //                                 out dsCustomerData, out dsCustomerData, out dsCustomerData, out dsCustomerData, out dsCustomerData, out dsCustomerData, out dsCustomerData, out dsCustomerData, out dsCustomerData, out dsCustomerData,
        //                                 out dsCustomerData);

        //        for (Int16 iDs = 0; iDs < 11; iDs++)
        //        {
        //            switch (iDs)
        //            {
        //                case 0:
        //                    if (dsCustomerData.Tables[iDs] != null && dsCustomerData.Tables[iDs].Rows.Count != 0)
        //                    {
        //                        dsCustomerData.Tables[iDs].TableName = "HDG_DDO_SOGCS";
        //                    }
        //                    break;
        //                case 1:
        //                    if (dsCustomerData.Tables[iDs] != null && dsCustomerData.Tables[iDs].Rows.Count != 0)
        //                    {
        //                        dsCustomerData.Tables[iDs].TableName = "HDG_QHE_DDO";
        //                    }
        //                    break;
        //                case 2:
        //                    if (dsCustomerData.Tables[iDs] != null && dsCustomerData.Tables[iDs].Rows.Count != 0)
        //                    {
        //                        dsCustomerData.Tables[iDs].TableName = "HDG_DIEM_DO";
        //                    }
        //                    break;
        //                case 3:
        //                    if (dsCustomerData.Tables[iDs] != null && dsCustomerData.Tables[iDs].Rows.Count != 0)
        //                    {
        //                        dsCustomerData.Tables[iDs].TableName = "HDG_BBAN_APGIA";
        //                    }
        //                    break;
        //                case 4:
        //                    if (dsCustomerData.Tables[iDs] != null && dsCustomerData.Tables[iDs].Rows.Count != 0)
        //                    {
        //                        dsCustomerData.Tables[iDs].TableName = "HDG_KHACH_HANG";
        //                    }
        //                    break;
        //                case 5:
        //                    if (dsCustomerData.Tables[iDs] != null && dsCustomerData.Tables[iDs].Rows.Count != 0)
        //                    {
        //                        dsCustomerData.Tables[iDs].TableName = "HDG_PTHUC_TTOAN";
        //                    }
        //                    break;
        //                case 6:
        //                    if (dsCustomerData.Tables[iDs] != null && dsCustomerData.Tables[iDs].Rows.Count != 0)
        //                    {
        //                        dsCustomerData.Tables[iDs].TableName = "HDG_VITRI_DDO";
        //                    }
        //                    break;
        //                case 7:
        //                    if (dsCustomerData.Tables[iDs] != null && dsCustomerData.Tables[iDs].Rows.Count != 0)
        //                    {
        //                        dsCustomerData.Tables[iDs].TableName = "D_SOGCS";
        //                    }
        //                    break;
        //                case 8:
        //                    if (dsCustomerData.Tables[iDs] != null && dsCustomerData.Tables[iDs].Rows.Count != 0)
        //                    {
        //                        dsCustomerData.Tables[iDs].TableName = "GCS_LICHGCS";
        //                    }
        //                    break;
        //                case 9:
        //                    if (dsCustomerData.Tables[iDs] != null && dsCustomerData.Tables[iDs].Rows.Count != 0)
        //                    {
        //                        dsCustomerData.Tables[iDs].TableName = "GCS_SLMDK_SHBB";
        //                    }
        //                    break;
        //                case 10:
        //                    if (dsCustomerData.Tables[iDs] != null && dsCustomerData.Tables[iDs].Rows.Count != 0)
        //                    {
        //                        dsCustomerData.Tables[iDs].TableName = "HDG_KHACH_HANG_TT";
        //                    }
        //                    break;
        //                default:
        //                    break;
        //            }
        //        }

        //        //for (Int16 iDs = 21; iDs >= 0; iDs--)
        //        //{
        //        //    if (dsCustomerData.Tables[iDs] != null && dsCustomerData.Tables[iDs].Rows.Count == 0)
        //        //    {
        //        //        dsCustomerData.Tables.RemoveAt(iDs);
        //        //    }
        //        //}

        //        dsCustomerData.AcceptChanges();

        //        //Xu ly du lieu 

        //        List<string> strNotNull = new List<string>() 
        //    { 
        //        "HDG_DDO_SOGCS", 
        //        "HDG_DIEM_DO", 
        //        "HDG_BBAN_APGIA", 
        //        "HDG_KHACH_HANG", 
        //        "HDG_PTHUC_TTOAN",
        //        "HDG_VITRI_DDO", 
        //        "D_SOGCS", 
        //        "GCS_LICHGCS"                
        //    };
        //        int i = dsCustomerData.Tables.Count - 1;
        //        while (i >= 0)
        //        {
        //            DataTable dt = dsCustomerData.Tables[i];
        //            if (dt.Rows.Count == 0)
        //            {
        //                if (strNotNull.Contains(dt.TableName))
        //                {
        //                    dsCustomerData = null;
        //                    return dt.TableName;
        //                }
        //                else
        //                {
        //                    dsCustomerData.Tables.Remove(dt);
        //                }
        //            }
        //            i--;
        //        }
        //        return "";
        //    }
        //    catch (Exception ex)
        //    {
        //        return "Lỗi khi lấy dữ liệu: " + ex.Message;
        //    }
        //    finally
        //    {
        //        db.ReleaseConnection();
        //    }
        //}

        #endregion
        #region Tính hóa đơn cho CMIS 3
        public string getCustomerDataReadingPlus(ref DataSet dsCustomerData, string strMaDViQLy, string strMaSoGCS, Int16 i16Ky, Int16 i16Thang, Int16 i16Nam, string strNgayGhi)
        {
            cls_Config config = new cls_Config();
            dsCustomerData = new DataSet();
            try
            {
                //Debug.WriteLine(DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff") + " begin.");
                List<string> lstKHang = new List<string>();
                List<string> lstDDo = new List<string>();
                List<string> lstDDoGT = new List<string>();
                List<string> lstDDoTP = new List<string>();
                List<string> lstDDoBQ = new List<string>();
                List<short> lstKyP = new List<short>();
                String strKyPhu = ";";
                #region Hop Dong
                string strIP = ConfigurationManager.AppSettings["URI"];
                string strIP_Per = ConfigurationManager.AppSettings["URI_PERFORMANCE"];
                dsCustomerData.ReadXml("HopDong.xml");
                //var baseAddress = "http://localhost:7001/ServiceHopDong/resources/serviceHopDong/getCustomerData";
                //var baseAddress = "http://10.1.117.39:5525/api/hdong/customerData";
                var baseAddress = "http://" + strIP_Per + "/api/hdong/customerData";
                //var baseAddress = "http://" + strIP + "/ServiceHopDong-HopDong-context-root/resources/serviceHopDong/getCustomerData";

                var http = (HttpWebRequest)WebRequest.Create(new Uri(baseAddress));
                http.Accept = "application/json";
                http.ContentType = "application/json";
                http.Method = "POST";

                //string parsedContent = "{'MA_DVIQLY':'PC06AA','TEN_DANH_MUC':'D_SOGCS'}";
                string parsedContent = "{\"MA_DVIQLY\":\"" + strMaDViQLy + "\",\"MA_SOGCS\":\"" + strMaSoGCS + "\",\"KY\":" + i16Ky + ",\"THANG\": " + i16Thang + ",\"NAM\": " + i16Nam + ",\"NGAY_GHI\":\"" + strNgayGhi + "\"}";
                ASCIIEncoding encoding = new ASCIIEncoding();
                Byte[] bytes = encoding.GetBytes(parsedContent);

                Stream newStream = http.GetRequestStream();
                newStream.Write(bytes, 0, bytes.Length);
                newStream.Close();

                var response = http.GetResponse();
                //Debug.WriteLine(DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff") + " HOPDONG GetResponse.");
                var stream = response.GetResponseStream();
                //Debug.WriteLine(DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff") + " HOPDONG GetResponseStream.");
                var sr = new StreamReader(stream);
                var content = sr.ReadToEnd();
                //Debug.WriteLine(DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff") + " HOPDONG ReadToEnd.");
                outCustomerData deserializedProduct = JsonConvert.DeserializeObject<outCustomerData>(content);
                //Debug.WriteLine(DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff") + " HOPDONG DeserializeObject.");
                Type myType = deserializedProduct.GetType();
                IList<PropertyInfo> props = new List<PropertyInfo>(myType.GetProperties());

                foreach (DataTable dt in dsCustomerData.Tables)
                {

                    foreach (PropertyInfo prop in props)
                    {
                        if (prop.Name == dt.TableName)
                        {
                            object propValue1 = prop.GetValue(deserializedProduct, null);
                            List<List<string>> propValue = (List<List<string>>)propValue1;

                            foreach (object _lstObj in propValue)
                            {
                                List<string> lstObj = (List<string>)_lstObj;
                                DataRow dr = dt.NewRow();
                                int i = 0;
                                foreach (DataColumn col in dt.Columns)
                                {
                                    try
                                    {
                                        if (col.ColumnName == "MA_KHANG")
                                        {
                                            if (dt.TableName == "HDG_KHACH_HANG")
                                            {
                                                lstKHang.Add(lstObj[i]);
                                            }
                                        }
                                        if (col.ColumnName == "MA_DDO")
                                        {
                                            if (dt.TableName == "HDG_DDO_SOGCS")
                                            {
                                                lstDDo.Add(lstObj[i]);
                                            }
                                            if (dt.TableName == "HDG_QHE_DDO_GT")
                                            {
                                                lstDDoGT.Add(lstObj[i]);
                                            }
                                            if (dt.TableName == "HDG_QHE_DDO_BQ")
                                            {
                                                lstDDoBQ.Add(lstObj[i]);
                                            }
                                            if (dt.TableName == "HDG_QHE_DDO_TP")
                                            {
                                                lstDDoTP.Add(lstObj[i]);
                                            }
                                        }
                                        if (col.ColumnName == "MA_DDO_PHU")
                                        {
                                            if (dt.TableName == "HDG_QHE_DDO_GT")
                                            {
                                                lstDDoGT.Add(lstObj[i]);
                                            }
                                            if (dt.TableName == "HDG_QHE_DDO_BQ")
                                            {
                                                lstDDoBQ.Add(lstObj[i]);
                                            }
                                            if (dt.TableName == "HDG_QHE_DDO_TP")
                                            {
                                                lstDDoTP.Add(lstObj[i]);
                                            }
                                        }
                                        if (col.ColumnName == "KY_P")
                                        {
                                            if (dt.TableName == "HDG_QHE_DDO_TP")
                                            {
                                                if (!lstKyP.Contains(Convert.ToInt16(lstObj[i])))
                                                    lstKyP.Add(Convert.ToInt16(lstObj[i]));
                                            }
                                        }
                                        if (col.DataType == typeof(DateTime) && lstObj[i].Trim().Length > 0)
                                            dr[col.ColumnName] = DateTime.ParseExact(lstObj[i], "dd/MM/yyyy",
                                               System.Globalization.CultureInfo.InvariantCulture);
                                        if (col.DataType == typeof(Decimal) && lstObj[i].Trim().Length > 0)
                                            dr[col.ColumnName] = Convert.ToDecimal(lstObj[i]);
                                        if (col.DataType == typeof(Int64) && lstObj[i].Trim().Length > 0)
                                            dr[col.ColumnName] = Convert.ToInt64(lstObj[i]);
                                        if (col.DataType == typeof(Int16) && lstObj[i].Trim().Length > 0)
                                            dr[col.ColumnName] = Convert.ToInt16(lstObj[i]);
                                        if (col.DataType == typeof(String))
                                            dr[col.ColumnName] = lstObj[i];
                                    }
                                    catch
                                    {
                                    }
                                    i++;
                                }

                                dt.Rows.Add(dr);
                            }

                            break;
                        }
                        // Do something with propValue
                    }
                }
                //Debug.WriteLine(DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff") + " HOPDONG DataSet.");
                dsCustomerData.AcceptChanges();
                //Debug.WriteLine(DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff") + " HOPDONG DataSet AcceptChanges.");
                #endregion
                #region Chi So
                //Lấy thông tin chỉ số
                intIndexData inpGetChiSo = new intIndexData();
                inpGetChiSo.MA_DVIQLY = strMaDViQLy;
                inpGetChiSo.MA_SOGCS = strMaSoGCS;
                inpGetChiSo.KY = i16Ky;
                inpGetChiSo.THANG = i16Thang;
                inpGetChiSo.NAM = i16Nam;
                inpGetChiSo.LST_KYP = lstKyP;
                inpGetChiSo.LST_DDO = lstDDo;
                inpGetChiSo.LST_DDO_BQ = lstDDoBQ;
                inpGetChiSo.LST_DDO_GT = lstDDoGT;
                inpGetChiSo.LST_DDO_TP = lstDDoTP;
                string strInput = JsonConvert.SerializeObject(inpGetChiSo);
                //Debug.WriteLine(DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff") + " ChiSoKHang SerializeObject.");
                //baseAddress = "http://localhost:7001/ServiceChiSoKHang/resources/serviceChiSoKHang/getCustomerData";
                //baseAddress = "http://" + strIP + "/ServiceChiSoKHang-ChiSoKHang-context-root/resources/serviceChiSoKHang/getCustomerData";
                //baseAddress = "http://10.1.117.39:5525/api/chiso/customerData";
                baseAddress = "http://" + strIP_Per + "/api/chiso/customerData";
                http = (HttpWebRequest)WebRequest.Create(new Uri(baseAddress));
                http.Accept = "application/json";
                http.ContentType = "application/json";
                http.Method = "POST";

                //string parsedContent = "{'MA_DVIQLY':'PC06AA','TEN_DANH_MUC':'D_SOGCS'}";
                parsedContent = strInput;
                encoding = new ASCIIEncoding();
                bytes = encoding.GetBytes(parsedContent);

                newStream = http.GetRequestStream();
                newStream.Write(bytes, 0, bytes.Length);
                newStream.Close();

                response = http.GetResponse();
                //Debug.WriteLine(DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff") + " ChiSoKHang GetResponse.");
                stream = response.GetResponseStream();
                //Debug.WriteLine(DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff") + " ChiSoKHang GetResponseStream.");
                sr = new StreamReader(stream);
                content = sr.ReadToEnd();
                //Debug.WriteLine(DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff") + " ChiSoKHang ReadToEnd.");
                outIndexData csData = JsonConvert.DeserializeObject<outIndexData>(content);
                //Debug.WriteLine(DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff") + " ChiSoKHang DeserializeObject.");
                myType = csData.GetType();
                props = new List<PropertyInfo>(myType.GetProperties());

                foreach (DataTable dt in dsCustomerData.Tables)
                {

                    foreach (PropertyInfo prop in props)
                    {
                        if (prop.Name == dt.TableName)
                        {
                            object propValue1 = prop.GetValue(csData, null);
                            List<List<string>> propValue = (List<List<string>>)propValue1;

                            foreach (object _lstObj in propValue)
                            {
                                List<string> lstObj = (List<string>)_lstObj;
                                DataRow dr = dt.NewRow();
                                int i = 0;
                                foreach (DataColumn col in dt.Columns)
                                {
                                    try
                                    {
                                        if (col.DataType == typeof(DateTime) && lstObj[i] != null && lstObj[i].Trim().Length > 0)
                                            dr[col.ColumnName] = DateTime.ParseExact(lstObj[i], "dd/MM/yyyy",
                                               System.Globalization.CultureInfo.InvariantCulture);
                                        if (col.DataType == typeof(Decimal) && lstObj[i] != null && lstObj[i].Trim().Length > 0)
                                            dr[col.ColumnName] = Convert.ToDecimal(lstObj[i]);
                                        if (col.DataType == typeof(Int64) && lstObj[i] != null && lstObj[i].Trim().Length > 0)
                                            dr[col.ColumnName] = Convert.ToInt64(lstObj[i]);
                                        if (col.DataType == typeof(Int16) && lstObj[i] != null && lstObj[i].Trim().Length > 0)
                                            dr[col.ColumnName] = Convert.ToInt16(lstObj[i]);
                                        if (col.DataType == typeof(String))
                                            dr[col.ColumnName] = lstObj[i];
                                    }
                                    catch
                                    {

                                    }
                                    i++;
                                }
                                if (dt.TableName == "GCS_CHISO_TP" && deserializedProduct.HDG_QHE_DDO_TP.Count > 0)
                                {
                                    //Bổ sung kiểm tra bản ghi có kỳ đúng = kỳ phụ không
                                    var exists = deserializedProduct.HDG_QHE_DDO_TP.Where(c => c[0] == dr["MA_DDO"].ToString().Trim() && c[1] == dr["KY"].ToString().Trim());
                                    if (exists != null && exists.Count() > 0)
                                        dt.Rows.Add(dr);
                                }
                                else
                                    dt.Rows.Add(dr);
                            }

                            break;
                        }
                        // Do something with propValue
                    }
                }
                #endregion
                //Debug.WriteLine(DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff") + " ChiSoKHang DataSet.");
                //lấy thông tin HDN_GTRU
                #region Cac bang GTRU
                //inpReductionData inpGetReduction = new inpReductionData();
                //inpGetReduction.MA_DVIQLY = strMaDViQLy;
                //inpGetReduction.LST_THANGNAM = config.lstTNamHTro;
                //inpGetReduction.LST_KHANG = lstKHang;

                //string strInputReduction = JsonConvert.SerializeObject(inpGetReduction);
                ////baseAddress = "http://" + strIP + "/ServiceBCaoThang-BCaoThang-context-root/resources/serviceBCaoThang/getCustomerData";
                //baseAddress = "http://" + strIP + "/ServiceHDonPSinh-HDonPSinh-context-root/resources/serviceHDonPSinh/getCustomerData";
                //http = (HttpWebRequest)WebRequest.Create(new Uri(baseAddress));
                //http.Accept = "application/json";
                //http.ContentType = "application/json";
                //http.Method = "POST";

                ////string parsedContent = "{'MA_DVIQLY':'PC06AA','TEN_DANH_MUC':'D_SOGCS'}";
                //parsedContent = strInputReduction;
                //encoding = new ASCIIEncoding();
                //bytes = encoding.GetBytes(parsedContent);

                //newStream = http.GetRequestStream();
                //newStream.Write(bytes, 0, bytes.Length);
                //newStream.Close();

                //response = http.GetResponse();

                //stream = response.GetResponseStream();
                //sr = new StreamReader(stream);
                //content = sr.ReadToEnd();
                //outReductionData csReductionData = JsonConvert.DeserializeObject<outReductionData>(content);
                //myType = csReductionData.GetType();
                //props = new List<PropertyInfo>(myType.GetProperties());

                //foreach (DataTable dt in dsCustomerData.Tables)
                //{
                //    foreach (PropertyInfo prop in props)
                //    {
                //        if (prop.Name == dt.TableName)
                //        {
                //            object propValue1 = prop.GetValue(csReductionData, null);
                //            List<List<string>> propValue = (List<List<string>>)propValue1;

                //            foreach (object _lstObj in propValue)
                //            {
                //                List<string> lstObj = (List<string>)_lstObj;
                //                DataRow dr = dt.NewRow();
                //                int i = 0;
                //                foreach (DataColumn col in dt.Columns)
                //                {
                //                    try
                //                    {
                //                        if (col.DataType == typeof(DateTime) && lstObj[i] != null && lstObj[i].Trim().Length > 0)
                //                            dr[col.ColumnName] = DateTime.ParseExact(lstObj[i], "dd/MM/yyyy",
                //                               System.Globalization.CultureInfo.InvariantCulture);
                //                        if (col.DataType == typeof(Decimal) && lstObj[i] != null && lstObj[i].Trim().Length > 0)
                //                            dr[col.ColumnName] = Convert.ToDecimal(lstObj[i]);
                //                        if (col.DataType == typeof(Int64) && lstObj[i] != null && lstObj[i].Trim().Length > 0)
                //                            dr[col.ColumnName] = Convert.ToInt64(lstObj[i]);
                //                        if (col.DataType == typeof(Int16) && lstObj[i] != null && lstObj[i].Trim().Length > 0)
                //                            dr[col.ColumnName] = Convert.ToInt16(lstObj[i]);
                //                        if (col.DataType == typeof(String))
                //                            dr[col.ColumnName] = lstObj[i];
                //                    }
                //                    catch (Exception ex)
                //                    {

                //                    }
                //                    i++;
                //                }

                //                dt.Rows.Add(dr);
                //            }

                //            break;
                //        }
                //        // Do something with propValue
                //    }
                //}
                #endregion
                #region Bổ sung các dòng chỉ số trước khi chốt sang các bảng _DCN (GCS_CSO_DCN)
                if (dsCustomerData != null && dsCustomerData.Tables.Count > 0 && dsCustomerData.Tables.Contains("GCS_CSO_DCN"))
                {
                    DataView dvCS = new DataView(dsCustomerData.Tables["GCS_CHISO"]);
                    List<string> lstMaDDoDCN = new List<string>();
                    for (int i = dsCustomerData.Tables["GCS_CSO_DCN"].Rows.Count - 1; i >= 0; i--)
                    {
                        DataRow drDCN = dsCustomerData.Tables["GCS_CSO_DCN"].Rows[i];
                        string strMaDDo = drDCN["MA_DDO"].ToString().Trim();
                        if (lstMaDDoDCN.Contains(strMaDDo)) continue;
                        lstMaDDoDCN.Add(strMaDDo);
                        //Lấy các dòng trong GCS_CHISO cùng mã điểm đo, cùng ID_BCS, ngày cuối kỳ <= ngày đầu kỳ của CSC, đẩy vào DCN
                        dvCS.RowFilter = "MA_DDO='" + drDCN["MA_DDO"].ToString().Trim() + "'";
                        if (dvCS.Count > 0)
                        {
                            foreach (DataRowView drvCS in dvCS)
                            {
                                if (Convert.ToDateTime(drvCS["NGAY_CKY"]) <= Convert.ToDateTime(drDCN["NGAY_DKY"]))
                                {
                                    //Import vào DCN
                                    dsCustomerData.Tables["GCS_CSO_DCN"].ImportRow(drvCS.Row);
                                }
                            }
                        }

                    }
                }
                #endregion
                #region GCS_CSO_DCN_GT
                if (dsCustomerData != null && dsCustomerData.Tables.Count > 0 && dsCustomerData.Tables.Contains("GCS_CSO_DCN_GT"))
                {
                    DataView dvCS = new DataView(dsCustomerData.Tables["GCS_CHISO_GT"]);
                    List<string> lstMaDDoDCN = new List<string>();
                    for (int i = dsCustomerData.Tables["GCS_CSO_DCN_GT"].Rows.Count - 1; i >= 0; i--)
                    {
                        DataRow drDCN = dsCustomerData.Tables["GCS_CSO_DCN_GT"].Rows[i];
                        string strMaDDo = drDCN["MA_DDO"].ToString().Trim();
                        if (lstMaDDoDCN.Contains(strMaDDo)) continue;
                        lstMaDDoDCN.Add(strMaDDo);
                        //Lấy các dòng trong GCS_CHISO cùng mã điểm đo, cùng ID_BCS, ngày cuối kỳ <= ngày đầu kỳ của CSC, đẩy vào DCN
                        dvCS.RowFilter = "MA_DDO='" + drDCN["MA_DDO"].ToString().Trim() + "'";
                        if (dvCS.Count > 0)
                        {
                            foreach (DataRowView drvCS in dvCS)
                            {
                                if (Convert.ToDateTime(drvCS["NGAY_CKY"]) <= Convert.ToDateTime(drDCN["NGAY_DKY"]))
                                {
                                    //Import vào DCN
                                    dsCustomerData.Tables["GCS_CSO_DCN_GT"].ImportRow(drvCS.Row);
                                }
                            }
                        }

                    }
                }
                #endregion
                #region GCS_CSO_DCN_TP
                if (dsCustomerData != null && dsCustomerData.Tables.Count > 0 && dsCustomerData.Tables.Contains("GCS_CSO_DCN_TP"))
                {
                    DataView dvCS = new DataView(dsCustomerData.Tables["GCS_CHISO_TP"]);
                    List<string> lstMaDDoDCN = new List<string>();
                    for (int i = dsCustomerData.Tables["GCS_CSO_DCN_TP"].Rows.Count - 1; i >= 0; i--)
                    {
                        DataRow drDCN = dsCustomerData.Tables["GCS_CSO_DCN_TP"].Rows[i];
                        string strMaDDo = drDCN["MA_DDO"].ToString().Trim();
                        if (lstMaDDoDCN.Contains(strMaDDo)) continue;
                        lstMaDDoDCN.Add(strMaDDo);
                        //Lấy các dòng trong GCS_CHISO cùng mã điểm đo, cùng ID_BCS, ngày cuối kỳ <= ngày đầu kỳ của CSC, đẩy vào DCN
                        dvCS.RowFilter = "MA_DDO='" + drDCN["MA_DDO"].ToString().Trim() + "'";
                        if (dvCS.Count > 0)
                        {
                            foreach (DataRowView drvCS in dvCS)
                            {
                                if (Convert.ToDateTime(drvCS["NGAY_CKY"]) <= Convert.ToDateTime(drDCN["NGAY_DKY"]))
                                {
                                    //Import vào DCN
                                    dsCustomerData.Tables["GCS_CSO_DCN_TP"].ImportRow(drvCS.Row);
                                }
                            }
                        }

                    }
                }
                #endregion
                #region GCS_CSO_DCN_BQ
                if (dsCustomerData != null && dsCustomerData.Tables.Count > 0 && dsCustomerData.Tables.Contains("GCS_CSO_DCN_BQ"))
                {
                    DataView dvCS = new DataView(dsCustomerData.Tables["GCS_CHISO_BQ"]);
                    List<string> lstMaDDoDCN = new List<string>();
                    for (int i = dsCustomerData.Tables["GCS_CSO_DCN_BQ"].Rows.Count - 1; i >= 0; i--)
                    {
                        DataRow drDCN = dsCustomerData.Tables["GCS_CSO_DCN_BQ"].Rows[i];
                        string strMaDDo = drDCN["MA_DDO"].ToString().Trim();
                        if (lstMaDDoDCN.Contains(strMaDDo)) continue;
                        lstMaDDoDCN.Add(strMaDDo);
                        //Lấy các dòng trong GCS_CHISO cùng mã điểm đo, cùng ID_BCS, ngày cuối kỳ <= ngày đầu kỳ của CSC, đẩy vào DCN
                        dvCS.RowFilter = "MA_DDO='" + drDCN["MA_DDO"].ToString().Trim() + "'";
                        if (dvCS.Count > 0)
                        {
                            foreach (DataRowView drvCS in dvCS)
                            {
                                if (Convert.ToDateTime(drvCS["NGAY_CKY"]) <= Convert.ToDateTime(drDCN["NGAY_DKY"]))
                                {
                                    //Import vào DCN
                                    dsCustomerData.Tables["GCS_CSO_DCN_BQ"].ImportRow(drvCS.Row);
                                }
                            }
                        }

                    }
                }
                #endregion
                dsCustomerData.AcceptChanges();
                //Debug.WriteLine(DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff") + " ChiSoKHang DataSetAcceptChanges.");
                return "";
            }
            catch (Exception ex)
            {
                return "Lỗi khi lấy dữ liệu: " + ex.Message;
            }
            finally
            {
                //db.ReleaseConnection();
            }
        }
        public async Task<T> PostDataJsonAsync<T>(string url, string body, string strMaSoGCS)
        {
            using (var httpClient = new HttpClient())
            {
                var content = new StringContent(body, Encoding.ASCII, "application/json");
                httpClient.DefaultRequestHeaders.Accept.Clear();
                httpClient.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
                using (HttpResponseMessage response = await httpClient.PostAsync(url, content))
                {
                    string serialized = string.Empty;
                    try
                    {
                        serialized = await response.Content.ReadAsStringAsync();
                        if (typeof(T) == typeof(string))
                        {
                            return (T)(object)serialized;
                        }
                        else
                        {                           
                            return JsonConvert.DeserializeObject<T>(serialized);
                        }
                    }
                    catch (Exception ex)
                    {
                        throw ex;
                    }
                }
            }
        }
        public async Task<TinhHDonModel> getCustomerDataReadingPlusAsync( string strMaDViQLy, string strMaSoGCS, Int16 i16Ky, Int16 i16Thang, Int16 i16Nam, string strNgayGhi)
        { 
            cls_Config config = new cls_Config();
            DataSet dsCustomerData = new DataSet();
            try
            {
                //Debug.WriteLine(DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff") + " begin.");
                List<string> lstKHang = new List<string>();
                List<string> lstDDo = new List<string>();
                List<string> lstDDoGT = new List<string>();
                List<string> lstDDoTP = new List<string>();
                List<string> lstDDoBQ = new List<string>();
                List<short> lstKyP = new List<short>();
                String strKyPhu = ";";
                #region Hop Dong
                string strIP = ConfigurationManager.AppSettings["URI"];
                string strIP_Per = ConfigurationManager.AppSettings["URI_PERFORMANCE"];
                dsCustomerData.ReadXml("HopDong.xml");
                var baseAddress = "http://" + strIP_Per + "/api/hdong/customerData";
                //Console.WriteLine("Tạo request cho luồng mã sổ " + strMaSoGCS);
                string parsedContent = "{\"MA_DVIQLY\":\"" + strMaDViQLy + "\",\"MA_SOGCS\":\"" + strMaSoGCS + "\",\"KY\":" + i16Ky + ",\"THANG\": " + i16Thang + ",\"NAM\": " + i16Nam + ",\"NGAY_GHI\":\"" + strNgayGhi + "\"}";
                //var httpClient = new HttpClient();
                outCustomerData deserializedProduct = await this.PostDataJsonAsync<outCustomerData>(baseAddress, parsedContent, strMaSoGCS);
                //Console.WriteLine("Lấy xong dữ liệu cho HDONG: " + strMaSoGCS + "lúc  " + DateTime.Now.ToString("dd/MM/yyyy HH:mm:ss:ff"));
                Type myType = deserializedProduct.GetType();    
                IList<PropertyInfo> props = new List<PropertyInfo>(myType.GetProperties());

                foreach (DataTable dt in dsCustomerData.Tables)
                {

                    foreach (PropertyInfo prop in props)
                    {
                        if (prop.Name == dt.TableName)
                        {
                            object propValue1 = prop.GetValue(deserializedProduct, null);
                            List<List<string>> propValue = (List<List<string>>)propValue1;

                            foreach (object _lstObj in propValue)
                            {
                                List<string> lstObj = (List<string>)_lstObj;
                                DataRow dr = dt.NewRow();
                                int i = 0;
                                foreach (DataColumn col in dt.Columns)
                                {
                                    try
                                    {
                                        if (col.ColumnName == "MA_KHANG")
                                        {
                                            if (dt.TableName == "HDG_KHACH_HANG")
                                            {
                                                lstKHang.Add(lstObj[i]);
                                            }
                                        }
                                        if (col.ColumnName == "MA_DDO")
                                        {
                                            if (dt.TableName == "HDG_DDO_SOGCS")
                                            {
                                                lstDDo.Add(lstObj[i]);
                                            }
                                            if (dt.TableName == "HDG_QHE_DDO_GT")
                                            {
                                                lstDDoGT.Add(lstObj[i]);
                                            }
                                            if (dt.TableName == "HDG_QHE_DDO_BQ")
                                            {
                                                lstDDoBQ.Add(lstObj[i]);
                                            }
                                            if (dt.TableName == "HDG_QHE_DDO_TP")
                                            {
                                                lstDDoTP.Add(lstObj[i]);
                                            }
                                        }
                                        if (col.ColumnName == "MA_DDO_PHU")
                                        {
                                            if (dt.TableName == "HDG_QHE_DDO_GT")
                                            {
                                                lstDDoGT.Add(lstObj[i]);
                                            }
                                            if (dt.TableName == "HDG_QHE_DDO_BQ")
                                            {
                                                lstDDoBQ.Add(lstObj[i]);
                                            }
                                            if (dt.TableName == "HDG_QHE_DDO_TP")
                                            {
                                                lstDDoTP.Add(lstObj[i]);
                                            }
                                        }
                                        if (col.ColumnName == "KY_P")
                                        {
                                            if (dt.TableName == "HDG_QHE_DDO_TP")
                                            {
                                                if (!lstKyP.Contains(Convert.ToInt16(lstObj[i])))
                                                    lstKyP.Add(Convert.ToInt16(lstObj[i]));
                                            }
                                        }
                                        if (col.DataType == typeof(DateTime) && lstObj[i].Trim().Length > 0)
                                            dr[col.ColumnName] = DateTime.ParseExact(lstObj[i], "dd/MM/yyyy",
                                               System.Globalization.CultureInfo.InvariantCulture);
                                        if (col.DataType == typeof(Decimal) && lstObj[i].Trim().Length > 0)
                                            dr[col.ColumnName] = Convert.ToDecimal(lstObj[i]);
                                        if (col.DataType == typeof(Int64) && lstObj[i].Trim().Length > 0)
                                            dr[col.ColumnName] = Convert.ToInt64(lstObj[i]);
                                        if (col.DataType == typeof(Int16) && lstObj[i].Trim().Length > 0)
                                            dr[col.ColumnName] = Convert.ToInt16(lstObj[i]);
                                        if (col.DataType == typeof(String))
                                            dr[col.ColumnName] = lstObj[i];
                                    }
                                    catch
                                    {
                                    }
                                    i++;
                                }

                                dt.Rows.Add(dr);
                            }

                            break;
                        }
                        // Do something with propValue
                    }
                }



                //dsCustomerData.AcceptChanges();

                #endregion
                #region Chi So
                intIndexData inpGetChiSo = new intIndexData();
                inpGetChiSo.MA_DVIQLY = strMaDViQLy;
                inpGetChiSo.MA_SOGCS = strMaSoGCS;
                inpGetChiSo.KY = i16Ky;
                inpGetChiSo.THANG = i16Thang;
                inpGetChiSo.NAM = i16Nam;
                inpGetChiSo.LST_KYP = lstKyP;
                inpGetChiSo.LST_DDO = lstDDo;
                inpGetChiSo.LST_DDO_BQ = lstDDoBQ;
                inpGetChiSo.LST_DDO_GT = lstDDoGT;
                inpGetChiSo.LST_DDO_TP = lstDDoTP;
                string strInput = JsonConvert.SerializeObject(inpGetChiSo);
                baseAddress = "http://" + strIP_Per + "/api/chiso/customerData";          
                parsedContent = strInput;
                outIndexData csData = await this.PostDataJsonAsync<outIndexData>(baseAddress, parsedContent, strMaSoGCS);
                //Console.WriteLine("Lấy xong dữ liệu cho CHISO: " + strMaSoGCS + "lúc  " + DateTime.Now.ToString("dd/MM/yyyy HH:mm:ss"));
                myType = csData.GetType();
                props = new List<PropertyInfo>(myType.GetProperties());

                foreach (DataTable dt in dsCustomerData.Tables)
                {

                    foreach (PropertyInfo prop in props)
                    {
                        if (prop.Name == dt.TableName)
                        {
                            object propValue1 = prop.GetValue(csData, null);
                            List<List<string>> propValue = (List<List<string>>)propValue1;

                            foreach (object _lstObj in propValue)
                            {
                                List<string> lstObj = (List<string>)_lstObj;
                                DataRow dr = dt.NewRow();
                                int i = 0;
                                foreach (DataColumn col in dt.Columns)
                                {
                                    try
                                    {
                                        if (col.DataType == typeof(DateTime) && lstObj[i] != null && lstObj[i].Trim().Length > 0)
                                            dr[col.ColumnName] = DateTime.ParseExact(lstObj[i], "dd/MM/yyyy",
                                               System.Globalization.CultureInfo.InvariantCulture);
                                        if (col.DataType == typeof(Decimal) && lstObj[i] != null && lstObj[i].Trim().Length > 0)
                                            dr[col.ColumnName] = Convert.ToDecimal(lstObj[i]);
                                        if (col.DataType == typeof(Int64) && lstObj[i] != null && lstObj[i].Trim().Length > 0)
                                            dr[col.ColumnName] = Convert.ToInt64(lstObj[i]);
                                        if (col.DataType == typeof(Int16) && lstObj[i] != null && lstObj[i].Trim().Length > 0)
                                            dr[col.ColumnName] = Convert.ToInt16(lstObj[i]);
                                        if (col.DataType == typeof(String))
                                            dr[col.ColumnName] = lstObj[i];
                                    }
                                    catch
                                    {

                                    }
                                    i++;
                                }
                                if (dt.TableName == "GCS_CHISO_TP" && deserializedProduct.HDG_QHE_DDO_TP.Count > 0)
                                {
                                    //Bổ sung kiểm tra bản ghi có kỳ đúng = kỳ phụ không
                                    var exists = deserializedProduct.HDG_QHE_DDO_TP.Where(c => c[0] == dr["MA_DDO"].ToString().Trim() && c[1] == dr["KY"].ToString().Trim());
                                    if (exists != null && exists.Count() > 0)
                                        dt.Rows.Add(dr);
                                }
                                else
                                    dt.Rows.Add(dr);
                            }

                            break;
                        }
                        // Do something with propValue
                    }
                }
                #endregion
                #region Bổ sung các dòng chỉ số trước khi chốt sang các bảng _DCN (GCS_CSO_DCN)
                if (dsCustomerData != null && dsCustomerData.Tables.Count > 0 && dsCustomerData.Tables.Contains("GCS_CSO_DCN"))
                {
                    DataView dvCS = new DataView(dsCustomerData.Tables["GCS_CHISO"]);
                    List<string> lstMaDDoDCN = new List<string>();
                    for (int i = dsCustomerData.Tables["GCS_CSO_DCN"].Rows.Count - 1; i >= 0; i--)
                    {
                        DataRow drDCN = dsCustomerData.Tables["GCS_CSO_DCN"].Rows[i];
                        string strMaDDo = drDCN["MA_DDO"].ToString().Trim();
                        if (lstMaDDoDCN.Contains(strMaDDo)) continue;
                        lstMaDDoDCN.Add(strMaDDo);
                        //Lấy các dòng trong GCS_CHISO cùng mã điểm đo, cùng ID_BCS, ngày cuối kỳ <= ngày đầu kỳ của CSC, đẩy vào DCN
                        dvCS.RowFilter = "MA_DDO='" + drDCN["MA_DDO"].ToString().Trim() + "'";
                        if (dvCS.Count > 0)
                        {
                            foreach (DataRowView drvCS in dvCS)
                            {
                                if (Convert.ToDateTime(drvCS["NGAY_CKY"]) <= Convert.ToDateTime(drDCN["NGAY_DKY"]))
                                {
                                    //Import vào DCN
                                    dsCustomerData.Tables["GCS_CSO_DCN"].ImportRow(drvCS.Row);
                                }
                            }
                        }

                    }
                }
                #endregion
                #region GCS_CSO_DCN_GT
                if (dsCustomerData != null && dsCustomerData.Tables.Count > 0 && dsCustomerData.Tables.Contains("GCS_CSO_DCN_GT"))
                {
                    DataView dvCS = new DataView(dsCustomerData.Tables["GCS_CHISO_GT"]);
                    List<string> lstMaDDoDCN = new List<string>();
                    for (int i = dsCustomerData.Tables["GCS_CSO_DCN_GT"].Rows.Count - 1; i >= 0; i--)
                    {
                        DataRow drDCN = dsCustomerData.Tables["GCS_CSO_DCN_GT"].Rows[i];
                        string strMaDDo = drDCN["MA_DDO"].ToString().Trim();
                        if (lstMaDDoDCN.Contains(strMaDDo)) continue;
                        lstMaDDoDCN.Add(strMaDDo);
                        //Lấy các dòng trong GCS_CHISO cùng mã điểm đo, cùng ID_BCS, ngày cuối kỳ <= ngày đầu kỳ của CSC, đẩy vào DCN
                        dvCS.RowFilter = "MA_DDO='" + drDCN["MA_DDO"].ToString().Trim() + "'";
                        if (dvCS.Count > 0)
                        {
                            foreach (DataRowView drvCS in dvCS)
                            {
                                if (Convert.ToDateTime(drvCS["NGAY_CKY"]) <= Convert.ToDateTime(drDCN["NGAY_DKY"]))
                                {
                                    //Import vào DCN
                                    dsCustomerData.Tables["GCS_CSO_DCN_GT"].ImportRow(drvCS.Row);
                                }
                            }
                        }

                    }
                }
                #endregion
                #region GCS_CSO_DCN_TP
                if (dsCustomerData != null && dsCustomerData.Tables.Count > 0 && dsCustomerData.Tables.Contains("GCS_CSO_DCN_TP"))
                {
                    DataView dvCS = new DataView(dsCustomerData.Tables["GCS_CHISO_TP"]);
                    List<string> lstMaDDoDCN = new List<string>();
                    for (int i = dsCustomerData.Tables["GCS_CSO_DCN_TP"].Rows.Count - 1; i >= 0; i--)
                    {
                        DataRow drDCN = dsCustomerData.Tables["GCS_CSO_DCN_TP"].Rows[i];
                        string strMaDDo = drDCN["MA_DDO"].ToString().Trim();
                        if (lstMaDDoDCN.Contains(strMaDDo)) continue;
                        lstMaDDoDCN.Add(strMaDDo);
                        //Lấy các dòng trong GCS_CHISO cùng mã điểm đo, cùng ID_BCS, ngày cuối kỳ <= ngày đầu kỳ của CSC, đẩy vào DCN
                        dvCS.RowFilter = "MA_DDO='" + drDCN["MA_DDO"].ToString().Trim() + "'";
                        if (dvCS.Count > 0)
                        {
                            foreach (DataRowView drvCS in dvCS)
                            {
                                if (Convert.ToDateTime(drvCS["NGAY_CKY"]) <= Convert.ToDateTime(drDCN["NGAY_DKY"]))
                                {
                                    //Import vào DCN
                                    dsCustomerData.Tables["GCS_CSO_DCN_TP"].ImportRow(drvCS.Row);
                                }
                            }
                        }

                    }
                }
                #endregion
                #region GCS_CSO_DCN_BQ
                if (dsCustomerData != null && dsCustomerData.Tables.Count > 0 && dsCustomerData.Tables.Contains("GCS_CSO_DCN_BQ"))
                {
                    DataView dvCS = new DataView(dsCustomerData.Tables["GCS_CHISO_BQ"]);
                    List<string> lstMaDDoDCN = new List<string>();
                    for (int i = dsCustomerData.Tables["GCS_CSO_DCN_BQ"].Rows.Count - 1; i >= 0; i--)
                    {
                        DataRow drDCN = dsCustomerData.Tables["GCS_CSO_DCN_BQ"].Rows[i];
                        string strMaDDo = drDCN["MA_DDO"].ToString().Trim();
                        if (lstMaDDoDCN.Contains(strMaDDo)) continue;
                        lstMaDDoDCN.Add(strMaDDo);
                        //Lấy các dòng trong GCS_CHISO cùng mã điểm đo, cùng ID_BCS, ngày cuối kỳ <= ngày đầu kỳ của CSC, đẩy vào DCN
                        dvCS.RowFilter = "MA_DDO='" + drDCN["MA_DDO"].ToString().Trim() + "'";
                        if (dvCS.Count > 0)
                        {
                            foreach (DataRowView drvCS in dvCS)
                            {
                                if (Convert.ToDateTime(drvCS["NGAY_CKY"]) <= Convert.ToDateTime(drDCN["NGAY_DKY"]))
                                {
                                    //Import vào DCN
                                    dsCustomerData.Tables["GCS_CSO_DCN_BQ"].ImportRow(drvCS.Row);
                                }
                            }
                        }

                    }
                }
                #endregion
                //dsCustomerData.AcceptChanges();
                //Debug.WriteLine(DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff") + " ChiSoKHang DataSetAcceptChanges.");
                //Console.WriteLine("Kết thúc request cho luồng mã sổ: " + strMaSoGCS + "lúc  " + DateTime.Now.ToString("dd/MM/yyyy HH:mm:ss:ffff"));
                return new TinhHDonModel("", dsCustomerData);
            }
            catch (Exception ex)
            {
                return new TinhHDonModel("Lỗi khi lấy dữ liệu: " + ex.Message, null);
            }
            finally
            {
                //db.ReleaseConnection();
            }
        }

        public async Task<string> checkPhuGhepTongPlusAsync(string strMaDViQLy, string strMaSoGCS, Int16 i16Ky, Int16 i16Thang, Int16 i16Nam, string strNgayGhi)
        {
            //dsCustomerData = new DataSet();
            try
            {
                string strIP = ConfigurationManager.AppSettings["URI"];
                //dsCustomerData.ReadXml("HopDong.xml");
                string parsedContent = "{'MA_DVIQLY':'" + strMaDViQLy + "','MA_SOGCS':'" + strMaSoGCS + "','KY':" + i16Ky + ",'THANG': " + i16Thang + ",'NAM': " + i16Nam + ",'NGAY_GHI':'" + strNgayGhi + "'}";
                var baseAddress = "http://" + strIP + "/ServiceHopDong-HopDong-context-root/resources/serviceHopDong/checkPhuGT";
               /* var http = (HttpWebRequest)WebRequest.Create(new Uri(baseAddress));
                http.Accept = "application/json";
                http.ContentType = "application/json";
                http.Method = "POST";

                //string parsedContent = "{'MA_DVIQLY':'PC06AA','TEN_DANH_MUC':'D_SOGCS'}";
                ASCIIEncoding encoding = new ASCIIEncoding();
                Byte[] bytes = encoding.GetBytes(parsedContent);

                Stream newStream = http.GetRequestStream();
                newStream.Write(bytes, 0, bytes.Length);
                newStream.Close();

                var response = http.GetResponse();

                var stream = response.GetResponseStream();
                var sr = new StreamReader(stream);
                var content = sr.ReadToEnd();*/
                string content = await this.PostDataJsonAsync<string>(baseAddress, parsedContent, strMaSoGCS);
                return content.ToString();
            }
            catch (Exception ex)
            {
                return "Lỗi khi lấy dữ liệu: " + ex.Message;
            }
            finally
            {
                //db.ReleaseConnection();
            }
        }
        public string getStaticCatalogDataReadingPlus(ref DataSet dsStaticCatalog)
        {
            try
            {
                DateTime dtRemove = new DateTime(2017, 12, 31);
                dsStaticCatalog.Clear();
                dsStaticCatalog = new DataSet();
                dsStaticCatalog.ReadXml("DanhMuc.xml");
                string strIP = ConfigurationManager.AppSettings["URI"];
                //dsCustomerData.ReadXml("HopDong.xml");
                var baseAddress = "http://" + strIP + "/ServiceDanhMuc-DanhMuc-context-root/resources/serviceDanhMuc/getDMucTHD";
                //var baseAddress = "http://" + strIP + "/ServiceDanhMuc-DanhMuc-context-root/resources/serviceDanhMuc/getDMucTHD";
                var http = (HttpWebRequest)WebRequest.Create(new Uri(baseAddress));
                http.Accept = "application/json";
                http.ContentType = "application/json";
                http.Method = "GET";
                var response = http.GetResponse();

                var stream = response.GetResponseStream();
                var sr = new StreamReader(stream);
                var content = sr.ReadToEnd();
                StaticData deserializedProduct = JsonConvert.DeserializeObject<StaticData>(content);
                Type myType = deserializedProduct.GetType();
                IList<PropertyInfo> props = new List<PropertyInfo>(myType.GetProperties());

                foreach (DataTable dt in dsStaticCatalog.Tables)
                {

                    foreach (PropertyInfo prop in props)
                    {
                        if (prop.Name == dt.TableName)
                        {
                            object propValue1 = prop.GetValue(deserializedProduct, null);
                            List<List<string>> propValue = (List<List<string>>)propValue1;

                            foreach (object _lstObj in propValue)
                            {
                                List<string> lstObj = (List<string>)_lstObj;
                                DataRow dr = dt.NewRow();
                                int i = 0;
                                foreach (DataColumn col in dt.Columns)
                                {

                                    if (col.DataType == typeof(DateTime) && lstObj[i] != null && lstObj[i].Trim().Length > 0)
                                        dr[col.ColumnName] = DateTime.ParseExact(lstObj[i], "dd/MM/yyyy",
                                           System.Globalization.CultureInfo.InvariantCulture);
                                    if (col.DataType == typeof(Decimal) && lstObj[i] != null && lstObj[i].Trim().Length > 0)
                                        dr[col.ColumnName] = Convert.ToDecimal(lstObj[i]);
                                    if (col.DataType == typeof(Int64) && lstObj[i] != null && lstObj[i].Trim().Length > 0)
                                        dr[col.ColumnName] = Convert.ToInt64(lstObj[i]);
                                    if (col.DataType == typeof(Int16) && lstObj[i] != null && lstObj[i].Trim().Length > 0)
                                        dr[col.ColumnName] = Convert.ToInt16(lstObj[i]);
                                    if (col.DataType == typeof(String))
                                        dr[col.ColumnName] = lstObj[i];
                                    i++;
                                }

                                //Bổ sung đoạn remove các bản ghi hết hiệu lực từ 2017 trở về trước của 3 bảng giá, bậc thang, tham chiếu cấp DA
                                if (dt.TableName == "D_GIA_NHOMNN" || dt.TableName == "D_THAMCHIEU_CAPDA")
                                {
                                    DateTime dtNgayADung = Convert.ToDateTime(dr["NGAY_ADUNG"]);
                                    if (!(dtNgayADung < dtRemove && dr["NGAY_HETHLUC"].ToString().Trim().Length > 0))
                                        dt.Rows.Add(dr);
                                }
                                else if (dt.TableName == "D_BAC_THANG")
                                {
                                    DateTime dtNgayADung = Convert.ToDateTime(dr["NGAY_HLUC"]);
                                    if (!(dtNgayADung < dtRemove && dr["NGAY_HHLUC"].ToString().Trim().Length > 0))
                                        dt.Rows.Add(dr);
                                }
                                else
                                    dt.Rows.Add(dr);
                            }

                            break;
                        }
                        // Do something with propValue
                    }
                }
                baseAddress = "http://" + strIP + "/ServiceQTriHThong-QTriHThong-context-root/resources/serviceQTriHThong/getDMucTHD";
                //var baseAddress = "http://" + strIP + "/ServiceDanhMuc-DanhMuc-context-root/resources/serviceDanhMuc/getDMucTHD";
                http = (HttpWebRequest)WebRequest.Create(new Uri(baseAddress));
                http.Accept = "application/json";
                http.ContentType = "application/json";
                http.Method = "GET";
                response = http.GetResponse();

                stream = response.GetResponseStream();
                sr = new StreamReader(stream);
                content = sr.ReadToEnd();
                S_PARAMETER_PLUS sparam = JsonConvert.DeserializeObject<S_PARAMETER_PLUS>(content);
                myType = sparam.GetType();
                props = new List<PropertyInfo>(myType.GetProperties());

                foreach (DataTable dt in dsStaticCatalog.Tables)
                {

                    foreach (PropertyInfo prop in props)
                    {
                        if (prop.Name == dt.TableName)
                        {
                            object propValue1 = prop.GetValue(sparam, null);
                            List<List<string>> propValue = (List<List<string>>)propValue1;

                            foreach (object _lstObj in propValue)
                            {
                                List<string> lstObj = (List<string>)_lstObj;
                                DataRow dr = dt.NewRow();
                                int i = 0;
                                foreach (DataColumn col in dt.Columns)
                                {

                                    if (col.DataType == typeof(DateTime) && lstObj[i] != null && lstObj[i].Trim().Length > 0)
                                        dr[col.ColumnName] = DateTime.ParseExact(lstObj[i], "dd/MM/yyyy",
                                           System.Globalization.CultureInfo.InvariantCulture);
                                    if (col.DataType == typeof(Decimal) && lstObj[i] != null && lstObj[i].Trim().Length > 0)
                                        dr[col.ColumnName] = Convert.ToDecimal(lstObj[i]);
                                    if (col.DataType == typeof(Int64) && lstObj[i] != null && lstObj[i].Trim().Length > 0)
                                        dr[col.ColumnName] = Convert.ToInt64(lstObj[i]);
                                    if (col.DataType == typeof(Int16) && lstObj[i] != null && lstObj[i].Trim().Length > 0)
                                        dr[col.ColumnName] = Convert.ToInt16(lstObj[i]);
                                    if (col.DataType == typeof(String))
                                        dr[col.ColumnName] = lstObj[i];
                                    i++;
                                }
                                dt.Rows.Add(dr);
                            }

                            break;
                        }
                        // Do something with propValue
                    }
                }
                dsStaticCatalog.AcceptChanges();
                //dsStaticCatalog.WriteXml("D:/DMucWebDG.xml", XmlWriteMode.WriteSchema);
                //DataSet dsDanhMuc = new DataSet();
                //dsDanhMuc.ReadXml("D:/DMucWeb.xml", XmlReadMode.ReadSchema);
                //dsDanhMuc.Tables.Remove("D_GIA_NHOMNN");
                //dsDanhMuc.Tables.Remove("D_BAC_THANG");
                //DataTable dtGia = dsStaticCatalog.Tables["D_GIA_NHOMNN"].Copy();
                //dtGia.TableName = "D_GIA_NHOMNN";
                //dsDanhMuc.Tables.Add(dtGia);
                //DataTable dtBT = dsStaticCatalog.Tables["D_BAC_THANG"].Copy();
                //dtBT.TableName = "D_BAC_THANG";
                //dsDanhMuc.Tables.Add(dtBT);
                //dsDanhMuc.WriteXml("D:/DMucWebFinal.xml", XmlWriteMode.WriteSchema);
                //Thieu S_PARAMETER
                return "";



            }
            catch (Exception ex)
            {
                return ex.Message;
            }
            finally
            {
                dsStaticCatalog.AcceptChanges();
            }
        }
        public DataSet GetDataDChinhPlus(string strMaDViQLy, string strMaSoGCS, Int16 i16Ky, Int16 i16Thang, Int16 i16Nam, string strIDHDon, ref string strError)
        {
            //dsCustomerData = new DataSet();
            try
            {
                string strIP = ConfigurationManager.AppSettings["URI"];
                //dsCustomerData.ReadXml("HopDong.xml");

                //Target URL -- http://127.0.0.1:7101
                var baseAddress = "http://" + strIP + "/ServiceHDonDChinh-HDonDChinh-context-root/resources/serviceHDonDChinh/getDataDChinh";
                var http = (HttpWebRequest)WebRequest.Create(new Uri(baseAddress));
                http.Accept = "application/json";
                http.ContentType = "application/json";
                http.Method = "POST";

                //string parsedContent = "{'MA_DVIQLY':'PC06AA','TEN_DANH_MUC':'D_SOGCS'}";
                string parsedContent = "{'MA_DVIQLY':'" + strMaDViQLy + "','ID_HDON':'" + strIDHDon + "','KY':" + i16Ky + ",'THANG': " + i16Thang + ",'NAM': " + i16Nam + "}";
                ASCIIEncoding encoding = new ASCIIEncoding();
                Byte[] bytes = encoding.GetBytes(parsedContent);

                Stream newStream = http.GetRequestStream();
                newStream.Write(bytes, 0, bytes.Length);
                newStream.Close();

                var response = http.GetResponse();

                var stream = response.GetResponseStream();
                var sr = new StreamReader(stream);
                var content = sr.ReadToEnd();
                List<outDataDChinhDC> deserializedProduct = JsonConvert.DeserializeObject<List<outDataDChinhDC>>(content);
                String strData = deserializedProduct[0].DATA_DCHINH;
                //customersDataDC dataJson = JsonConvert.DeserializeObject<customersDataDC>(strData);
                //string strJSon = JsonConvert.SerializeObject(dataJson);
                DataSet dsJSon = JsonConvert.DeserializeObject<DataSet>(strData);
                //dsJSon.WriteXml("D://HDDC.xml", XmlWriteMode.WriteSchema);
                //Tạo lại cấu trúc bảng
                DataSet dsResult = new DataSet();
                dsResult.ReadXml("HDDC.xml");

                foreach (DataTable dt in dsResult.Tables)
                {
                    //DataColumn[] cols = ;
                    if (dt.TableName == "GCS_CHISO")
                    {
                    }
                    if (dsJSon.Tables.Contains(dt.TableName))
                    {
                        foreach (DataRow dr in dsJSon.Tables[dt.TableName].Rows)
                        {
                            DataRow drResult = dt.NewRow();
                            foreach (DataColumn col in dt.Columns)
                            {
                                if (dsJSon.Tables[dt.TableName].Columns.Contains(col.ColumnName))
                                {
                                    if (col.DataType == typeof(DateTime) && dr[col.ColumnName].ToString().Trim().Length > 0 && !dr[col.ColumnName].ToString().ToUpper().Contains("NAN/"))
                                    {
                                        //try
                                        //{
                                        //    drResult[col.ColumnName] = Convert.ToDateTime(dr[col.ColumnName]);
                                        //}
                                        //catch
                                        //{
                                        try
                                        {
                                            drResult[col.ColumnName] = DateTime.ParseExact(dr[col.ColumnName].ToString(), "d/M/yyyy",
                                                                                      System.Globalization.CultureInfo.InvariantCulture);
                                        }
                                        catch
                                        {
                                            //if (dt.TableName == "GCS_CHISO_GT")
                                            //{
                                            //drResult[col.ColumnName] = Convert.ToDateTime(dr[col.ColumnName]);
                                            //}
                                        }
                                        //}
                                    }
                                    if (col.DataType == typeof(Decimal) && dr[col.ColumnName].ToString().Trim().Length > 0)
                                        drResult[col.ColumnName] = Convert.ToDecimal(dr[col.ColumnName]);
                                    if (col.DataType == typeof(Int64) && dr[col.ColumnName].ToString().Trim().Length > 0)
                                        drResult[col.ColumnName] = Convert.ToInt64(dr[col.ColumnName]);
                                    if (col.DataType == typeof(Int16) && dr[col.ColumnName].ToString().Trim().Length > 0)
                                        drResult[col.ColumnName] = Convert.ToInt16(dr[col.ColumnName]);
                                    if (col.DataType == typeof(String))
                                        drResult[col.ColumnName] = dr[col.ColumnName];
                                }
                            }
                            dt.Rows.Add(drResult);
                        }

                    }
                }
                dsResult.AcceptChanges();
                if (dsJSon.Tables.Contains("LST_TIEN_TRINH"))
                {
                    DataTable dtTienTrinh = dsJSon.Tables["LST_TIEN_TRINH"].Copy();
                    dtTienTrinh.TableName = "LST_TIEN_TRINH";
                    dsResult.Tables.Add(dtTienTrinh);
                }
                if (dsJSon.Tables.Contains("LST_PAN_PHAT"))
                {
                    DataTable dtPAPhat = dsJSon.Tables["LST_PAN_PHAT"].Copy();
                    dtPAPhat.TableName = "LST_PAN_PHAT";
                    dsResult.Tables.Add(dtPAPhat);
                }
                if (dsJSon.Tables.Contains("LST_KHANG_DDO"))
                {
                    DataTable dtKHDDo = dsJSon.Tables["LST_KHANG_DDO"].Copy();
                    dtKHDDo.TableName = "LST_KHANG_DDO";
                    dsResult.Tables.Add(dtKHDDo);
                }
                if (dsJSon.Tables.Contains("LST_BBAN_PLUC"))
                {
                    DataTable dtKHDDo = dsJSon.Tables["LST_BBAN_PLUC"].Copy();
                    dtKHDDo.TableName = "LST_BBAN_PLUC";
                    dsResult.Tables.Add(dtKHDDo);
                }
                //int iLength = dsResult.Tables["HDN_DIEMDO_DC"].Rows.Count-1;
                //while(iLength>=0) 
                //{
                //    DataRow dr = dsResult.Tables["HDN_DIEMDO_DC"].Rows[iLength];
                //    DataRow[] arrDDo = dsResult.Tables["HDN_DIEMDO_DC"].Select("MA_DDO='" + dr["MA_DDO"].ToString().Trim() + "'");
                //    if (arrDDo.Length > 1)
                //    {
                //        dsResult.Tables["HDN_DIEMDO_DC"].Rows.RemoveAt(iLength);
                //    }
                //    iLength--;
                //}
                //iLength = dsResult.Tables["HDG_DIEM_DO"].Rows.Count - 1;
                //while (iLength >= 0)
                //{
                //    DataRow dr = dsResult.Tables["HDG_DIEM_DO"].Rows[iLength];
                //    DataRow[] arrDDo = dsResult.Tables["HDG_DIEM_DO_GT"].Select("MA_DDO='" + dr["MA_DDO"].ToString().Trim() + "'");
                //    if (arrDDo.Length > 0)
                //    {
                //        dsResult.Tables["HDG_DIEM_DO"].Rows.RemoveAt(iLength);
                //    }
                //    iLength--;
                //}
                //iLength = dsResult.Tables["GCS_CHISO_GT"].Rows.Count - 1;
                //while (iLength >= 0)
                //{
                //    DataRow dr = dsResult.Tables["GCS_CHISO_GT"].Rows[iLength];
                //    DataRow[] arrDDo = dsResult.Tables["GCS_CHISO"].Select("ID_CHISO='" + dr["ID_CHISO"].ToString().Trim() + "'");
                //    if (arrDDo.Length > 0)
                //    {
                //        dsResult.Tables["GCS_CHISO_GT"].Rows.RemoveAt(iLength);
                //    }
                //    iLength--;
                //}
                //dsResult.Tables.Remove("HDG_BBAN_APGIA_GT");
                //dsResult.Tables.Remove("HDG_DIEM_DO_GT");
                //dsResult.Tables.Remove("GCS_CHISO_GT");
                return dsResult;
                //return content.ToString();
            }
            catch (Exception ex)
            {
                string str = "Lỗi khi lấy dữ liệu: " + ex.Message;
                return null;
            }
            finally
            {
                //db.ReleaseConnection();
            }
        }
        public string getCustomerDataReadingKH(ref DataSet dsCustomerData, string strMaDViQLy, string strMaSoGCS, string strMaKHang, Int16 i16Ky, Int16 i16Thang, Int16 i16Nam, string strNgayGhi)
        {

            dsCustomerData = new DataSet();
            try
            {
                List<string> lstDDo = new List<string>();
                List<string> lstDDoGT = new List<string>();
                List<string> lstDDoTP = new List<string>();
                List<string> lstDDoBQ = new List<string>();
                List<short> lstKyP = new List<short>();
                String strKyPhu = ";";
                string strIP = ConfigurationManager.AppSettings["URI"];
                string strIP_Per = ConfigurationManager.AppSettings["URI_PERFORMANCE"];
                dsCustomerData.ReadXml("HopDong.xml");
                //var baseAddress = "http://" + strIP + "/ServiceHopDong-HopDong-context-root/resources/serviceHopDong/getCustomerDataKH";

                var baseAddress = "http://" + strIP_Per + "/api/hdong/getCustomerDataKH";

                var http = (HttpWebRequest)WebRequest.Create(new Uri(baseAddress));
                http.Accept = "application/json";
                http.ContentType = "application/json";
                http.Method = "POST";

                //string parsedContent = "{'MA_DVIQLY':'PC06AA','TEN_DANH_MUC':'D_SOGCS'}";
                string parsedContent = "{\"MA_DVIQLY\":\"" + strMaDViQLy + "\",\"MA_SOGCS\":\"" + strMaSoGCS + "\",\"MA_KHANG\":\"" + strMaKHang + "\",\"KY\":" + i16Ky + ",\"THANG\": " + i16Thang + ",\"NAM\": " + i16Nam + ",\"NGAY_GHI\":\"" + strNgayGhi + "\"}";
                ASCIIEncoding encoding = new ASCIIEncoding();
                Byte[] bytes = encoding.GetBytes(parsedContent);

                Stream newStream = http.GetRequestStream();
                newStream.Write(bytes, 0, bytes.Length);
                newStream.Close();

                var response = http.GetResponse();

                var stream = response.GetResponseStream();
                var sr = new StreamReader(stream);
                var content = sr.ReadToEnd();
                outCustomerData deserializedProduct = JsonConvert.DeserializeObject<outCustomerData>(content);
                Type myType = deserializedProduct.GetType();
                IList<PropertyInfo> props = new List<PropertyInfo>(myType.GetProperties());

                foreach (DataTable dt in dsCustomerData.Tables)
                {

                    foreach (PropertyInfo prop in props)
                    {
                        if (prop.Name == dt.TableName)
                        {
                            object propValue1 = prop.GetValue(deserializedProduct, null);
                            List<List<string>> propValue = (List<List<string>>)propValue1;

                            foreach (object _lstObj in propValue)
                            {
                                List<string> lstObj = (List<string>)_lstObj;
                                DataRow dr = dt.NewRow();
                                int i = 0;
                                foreach (DataColumn col in dt.Columns)
                                {
                                    try
                                    {
                                        if (col.ColumnName == "MA_DDO")
                                        {
                                            if (dt.TableName == "HDG_DDO_SOGCS")
                                            {
                                                lstDDo.Add(lstObj[i]);
                                            }
                                            if (dt.TableName == "HDG_QHE_DDO_GT")
                                            {
                                                lstDDoGT.Add(lstObj[i]);
                                            }
                                            if (dt.TableName == "HDG_QHE_DDO_BQ")
                                            {
                                                lstDDoBQ.Add(lstObj[i]);
                                            }
                                            if (dt.TableName == "HDG_QHE_DDO_TP")
                                            {
                                                lstDDoTP.Add(lstObj[i]);
                                            }
                                        }
                                        if (col.ColumnName == "MA_DDO_PHU")
                                        {
                                            if (dt.TableName == "HDG_QHE_DDO_GT")
                                            {
                                                lstDDoGT.Add(lstObj[i]);
                                            }
                                            if (dt.TableName == "HDG_QHE_DDO_BQ")
                                            {
                                                lstDDoBQ.Add(lstObj[i]);
                                            }
                                            if (dt.TableName == "HDG_QHE_DDO_TP")
                                            {
                                                lstDDoTP.Add(lstObj[i]);
                                            }
                                        }
                                        if (col.ColumnName == "KY_P")
                                        {
                                            if (dt.TableName == "HDG_QHE_DDO_TP")
                                            {
                                                if (!lstKyP.Contains(Convert.ToInt16(lstObj[i])))
                                                    lstKyP.Add(Convert.ToInt16(lstObj[i]));
                                            }
                                        }
                                        if (col.DataType == typeof(DateTime) && lstObj[i].Trim().Length > 0)
                                            dr[col.ColumnName] = DateTime.ParseExact(lstObj[i], "dd/MM/yyyy",
                                               System.Globalization.CultureInfo.InvariantCulture);
                                        if (col.DataType == typeof(Decimal) && lstObj[i].Trim().Length > 0)
                                            dr[col.ColumnName] = Convert.ToDecimal(lstObj[i]);
                                        if (col.DataType == typeof(Int64) && lstObj[i].Trim().Length > 0)
                                            dr[col.ColumnName] = Convert.ToInt64(lstObj[i]);
                                        if (col.DataType == typeof(Int16) && lstObj[i].Trim().Length > 0)
                                            dr[col.ColumnName] = Convert.ToInt16(lstObj[i]);
                                        if (col.DataType == typeof(String))
                                            dr[col.ColumnName] = lstObj[i];
                                    }
                                    catch
                                    {
                                    }
                                    i++;
                                }
                                dt.Rows.Add(dr);
                            }

                            break;
                        }
                        // Do something with propValue
                    }
                }
                dsCustomerData.AcceptChanges();
                //Lấy thông tin chỉ số
                intIndexData inpGetChiSo = new intIndexData();
                inpGetChiSo.MA_DVIQLY = strMaDViQLy;
                inpGetChiSo.MA_SOGCS = strMaSoGCS;
                inpGetChiSo.KY = i16Ky;
                inpGetChiSo.THANG = i16Thang;
                inpGetChiSo.NAM = i16Nam;
                inpGetChiSo.LST_KYP = lstKyP;
                inpGetChiSo.LST_DDO = lstDDo;
                inpGetChiSo.LST_DDO_BQ = lstDDoBQ;
                inpGetChiSo.LST_DDO_GT = lstDDoGT;
                inpGetChiSo.LST_DDO_TP = lstDDoTP;
                string strInput = JsonConvert.SerializeObject(inpGetChiSo);
                //baseAddress = "http://" + strIP + "/ServiceChiSoKHang-ChiSoKHang-context-root/resources/serviceChiSoKHang/getCustomerDataTHDLe";
                baseAddress = "http://" + strIP_Per + "/api/chiso/getCustomerDataTHDLe";
                http = (HttpWebRequest)WebRequest.Create(new Uri(baseAddress));
                http.Accept = "application/json";
                http.ContentType = "application/json";
                http.Method = "POST";

                //string parsedContent = "{'MA_DVIQLY':'PC06AA','TEN_DANH_MUC':'D_SOGCS'}";
                parsedContent = strInput;
                encoding = new ASCIIEncoding();
                bytes = encoding.GetBytes(parsedContent);

                newStream = http.GetRequestStream();
                newStream.Write(bytes, 0, bytes.Length);
                newStream.Close();

                response = http.GetResponse();

                stream = response.GetResponseStream();
                sr = new StreamReader(stream);
                content = sr.ReadToEnd();
                outIndexData csData = JsonConvert.DeserializeObject<outIndexData>(content);
                myType = csData.GetType();
                props = new List<PropertyInfo>(myType.GetProperties());

                foreach (DataTable dt in dsCustomerData.Tables)
                {

                    foreach (PropertyInfo prop in props)
                    {
                        if (prop.Name == dt.TableName)
                        {
                            object propValue1 = prop.GetValue(csData, null);
                            if (propValue1 == null) continue;
                            List<List<string>> propValue = (List<List<string>>)propValue1;

                            foreach (object _lstObj in propValue)
                            {
                                List<string> lstObj = (List<string>)_lstObj;
                                DataRow dr = dt.NewRow();
                                int i = 0;
                                foreach (DataColumn col in dt.Columns)
                                {
                                    try
                                    {
                                        if (col.DataType == typeof(DateTime) && lstObj[i] != null && lstObj[i].Trim().Length > 0)
                                            dr[col.ColumnName] = DateTime.ParseExact(lstObj[i], "dd/MM/yyyy",
                                               System.Globalization.CultureInfo.InvariantCulture);
                                        if (col.DataType == typeof(Decimal) && lstObj[i] != null && lstObj[i].Trim().Length > 0)
                                            dr[col.ColumnName] = Convert.ToDecimal(lstObj[i]);
                                        if (col.DataType == typeof(Int64) && lstObj[i] != null && lstObj[i].Trim().Length > 0)
                                            dr[col.ColumnName] = Convert.ToInt64(lstObj[i]);
                                        if (col.DataType == typeof(Int16) && lstObj[i] != null && lstObj[i].Trim().Length > 0)
                                            dr[col.ColumnName] = Convert.ToInt16(lstObj[i]);
                                        if (col.DataType == typeof(String))
                                            dr[col.ColumnName] = lstObj[i];
                                    }
                                    catch (Exception ex)
                                    {

                                    }
                                    i++;
                                }
                                if (dt.TableName == "GCS_CHISO_TP" && deserializedProduct.HDG_QHE_DDO_TP.Count > 0)
                                {
                                    //Bổ sung kiểm tra bản ghi có kỳ đúng = kỳ phụ không
                                    var exists = deserializedProduct.HDG_QHE_DDO_TP.Where(c => c[0] == dr["MA_DDO"].ToString().Trim() && c[1] == dr["KY"].ToString().Trim());
                                    if (exists != null && exists.Count() > 0)
                                        dt.Rows.Add(dr);
                                }
                                else
                                    dt.Rows.Add(dr);
                            }

                            break;
                        }
                        // Do something with propValue
                    }
                }
                //String strCSo = JsonConvert.SerializeObject(dsCustomerData.Tables["GCS_CHISO"]);
                dsCustomerData.AcceptChanges();
                return "";
            }
            catch (Exception ex)
            {
                return "Lỗi khi lấy dữ liệu: " + ex.Message;
            }
            finally
            {
                //db.ReleaseConnection();
            }
        }

        #endregion
        #region Tính hóa đơn cho CMIS 4
        public string getCustomerDataReadingPlus_kodung(ref DataSet dsCustomerData, string strMaDViQLy, string strMaSoGCS, Int16 i16Ky, Int16 i16Thang, Int16 i16Nam, string strNgayGhi)
        {
            //Không dùng, đã thư nghiệm nhưng tốc độ chậm hơn hẳn
            cls_Config config = new cls_Config();
            cls_Connection getConn = new cls_Connection(cls_Connection.Schema.HOPDONG);
            dsCustomerData = new DataSet();
            try
            {
                //Debug.WriteLine(DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff") + " begin.");
                List<string> lstKHang = new List<string>();
                List<string> lstDDo = new List<string>();
                List<string> lstDDoGT = new List<string>();
                List<string> lstDDoTP = new List<string>();
                List<string> lstDDoBQ = new List<string>();
                List<short> lstKyP = new List<short>();
                String strKyPhu = ";";
                string strIP = ConfigurationManager.AppSettings["URI"];
                #region Hop Dong



                OracleConnection conn = getConn.OraConn;
                DataSet ds = new DataSet();
                OracleCommand cmd = new OracleCommand();
                cmd.Parameters.Clear();
                cmd.Connection = conn;
                cmd.CommandType = CommandType.StoredProcedure;
                cmd.CommandText = "PKG_TINHHOADON.SP_GETCUSTOMERDATA";
                cmd.Parameters.AddWithValue("p_MA_DVIQLY", strMaDViQLy).Direction = ParameterDirection.Input;
                cmd.Parameters.AddWithValue("p_MA_SO_GCS", strMaSoGCS).Direction = ParameterDirection.Input;
                cmd.Parameters.AddWithValue("p_KY", i16Ky).Direction = ParameterDirection.Input;
                cmd.Parameters.AddWithValue("p_THANG", i16Thang).Direction = ParameterDirection.Input;
                cmd.Parameters.AddWithValue("p_NAM", i16Nam).Direction = ParameterDirection.Input;
                cmd.Parameters.AddWithValue("p_NGAY_GHI", strNgayGhi).Direction = ParameterDirection.Input;

                cmd.Parameters.Add(new OracleParameter("p_HDG_DDO_SOGCS", System.Data.OracleClient.OracleType.Cursor)).Direction = ParameterDirection.Output;
                cmd.Parameters.Add(new OracleParameter("p_HDG_QHE_DDO", System.Data.OracleClient.OracleType.Cursor)).Direction = ParameterDirection.Output;
                cmd.Parameters.Add(new OracleParameter("p_HDG_DIEM_DO", System.Data.OracleClient.OracleType.Cursor)).Direction = ParameterDirection.Output;
                cmd.Parameters.Add(new OracleParameter("p_HDG_BBAN_APGIA", System.Data.OracleClient.OracleType.Cursor)).Direction = ParameterDirection.Output;
                cmd.Parameters.Add(new OracleParameter("p_HDG_KHACH_HANG", System.Data.OracleClient.OracleType.Cursor)).Direction = ParameterDirection.Output;
                cmd.Parameters.Add(new OracleParameter("p_HDG_PTHUC_TTOAN", System.Data.OracleClient.OracleType.Cursor)).Direction = ParameterDirection.Output;
                cmd.Parameters.Add(new OracleParameter("p_HDG_VITRI_DDO", System.Data.OracleClient.OracleType.Cursor)).Direction = ParameterDirection.Output;
                cmd.Parameters.Add(new OracleParameter("p_HDG_DIEM_DO_GT", System.Data.OracleClient.OracleType.Cursor)).Direction = ParameterDirection.Output;
                cmd.Parameters.Add(new OracleParameter("p_HDG_DDO_SOGCS_GT", System.Data.OracleClient.OracleType.Cursor)).Direction = ParameterDirection.Output;
                cmd.Parameters.Add(new OracleParameter("p_HDG_BBAN_APGIA_GT", System.Data.OracleClient.OracleType.Cursor)).Direction = ParameterDirection.Output;
                cmd.Parameters.Add(new OracleParameter("p_HDG_VITRI_DDO_GT", System.Data.OracleClient.OracleType.Cursor)).Direction = ParameterDirection.Output;
                cmd.Parameters.Add(new OracleParameter("p_HDG_KHACH_HANG_TT", System.Data.OracleClient.OracleType.Cursor)).Direction = ParameterDirection.Output;
                cmd.Parameters.Add(new OracleParameter("p_HDG_QHE_DDO_TP", System.Data.OracleClient.OracleType.Cursor)).Direction = ParameterDirection.Output;
                cmd.Parameters.Add(new OracleParameter("p_HDG_QHE_DDO_BQ", System.Data.OracleClient.OracleType.Cursor)).Direction = ParameterDirection.Output;
                cmd.Parameters.Add(new OracleParameter("p_HDG_QHE_DDO_GT", System.Data.OracleClient.OracleType.Cursor)).Direction = ParameterDirection.Output;
                cmd.Parameters.Add(new OracleParameter("p_HDG_DDO_GTRU", System.Data.OracleClient.OracleType.Cursor)).Direction = ParameterDirection.Output;

                OracleDataAdapter da = new OracleDataAdapter(cmd);
                da.Fill(ds);


                ////Debug.WriteLine(DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff") + " HOPDONG GetResponse.");
                ////var stream = response.GetResponseStream();
                ////Debug.WriteLine(DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff") + " HOPDONG GetResponseStream.");
                //var sr = new StreamReader(stream);
                //var content = sr.ReadToEnd();
                ////Debug.WriteLine(DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff") + " HOPDONG ReadToEnd.");
                //outCustomerData deserializedProduct = JsonConvert.DeserializeObject<outCustomerData>(content);
                ////Debug.WriteLine(DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff") + " HOPDONG DeserializeObject.");
                //Type myType = deserializedProduct.GetType();
                //IList<PropertyInfo> props = new List<PropertyInfo>(myType.GetProperties());



                //Debug.WriteLine(DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff") + " HOPDONG DataSet.");
                dsCustomerData.AcceptChanges();
                //Debug.WriteLine(DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff") + " HOPDONG DataSet AcceptChanges.");
                #endregion
                #region Chi So
                //Lấy thông tin chỉ số
                intIndexData inpGetChiSo = new intIndexData();
                inpGetChiSo.MA_DVIQLY = strMaDViQLy;
                inpGetChiSo.MA_SOGCS = strMaSoGCS;
                inpGetChiSo.KY = i16Ky;
                inpGetChiSo.THANG = i16Thang;
                inpGetChiSo.NAM = i16Nam;
                inpGetChiSo.LST_KYP = lstKyP;
                inpGetChiSo.LST_DDO = lstDDo;
                inpGetChiSo.LST_DDO_BQ = lstDDoBQ;
                inpGetChiSo.LST_DDO_GT = lstDDoGT;
                inpGetChiSo.LST_DDO_TP = lstDDoTP;
                string strInput = JsonConvert.SerializeObject(inpGetChiSo);
                //Debug.WriteLine(DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff") + " ChiSoKHang SerializeObject.");
                //baseAddress = "http://localhost:7001/ServiceChiSoKHang/resources/serviceChiSoKHang/getCustomerData";
                string baseAddress = "http://" + strIP + "/ServiceChiSoKHang-ChiSoKHang-context-root/resources/serviceChiSoKHang/getCustomerData";
                HttpWebRequest http = (HttpWebRequest)WebRequest.Create(new Uri(baseAddress));
                http.Accept = "application/json";
                http.ContentType = "application/json";
                http.Method = "POST";

                //string parsedContent = "{'MA_DVIQLY':'PC06AA','TEN_DANH_MUC':'D_SOGCS'}";
                string parsedContent = strInput;
                ASCIIEncoding encoding = new ASCIIEncoding();
                byte[] bytes = encoding.GetBytes(parsedContent);

                Stream newStream = http.GetRequestStream();
                newStream.Write(bytes, 0, bytes.Length);
                newStream.Close();

                var response = http.GetResponse();
                //Debug.WriteLine(DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff") + " ChiSoKHang GetResponse.");
                Stream stream = response.GetResponseStream();
                //Debug.WriteLine(DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff") + " ChiSoKHang GetResponseStream.");
                var sr = new StreamReader(stream);
                string content = sr.ReadToEnd();
                //Debug.WriteLine(DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff") + " ChiSoKHang ReadToEnd.");
                outIndexData csData = JsonConvert.DeserializeObject<outIndexData>(content);
                //Debug.WriteLine(DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff") + " ChiSoKHang DeserializeObject.");
                Type myType = csData.GetType();
                IList<PropertyInfo> props = new List<PropertyInfo>(myType.GetProperties());

                foreach (DataTable dt in dsCustomerData.Tables)
                {

                    foreach (PropertyInfo prop in props)
                    {
                        if (prop.Name == dt.TableName)
                        {
                            object propValue1 = prop.GetValue(csData, null);
                            List<List<string>> propValue = (List<List<string>>)propValue1;

                            foreach (object _lstObj in propValue)
                            {
                                List<string> lstObj = (List<string>)_lstObj;
                                DataRow dr = dt.NewRow();
                                int i = 0;
                                foreach (DataColumn col in dt.Columns)
                                {
                                    try
                                    {
                                        if (col.DataType == typeof(DateTime) && lstObj[i] != null && lstObj[i].Trim().Length > 0)
                                            dr[col.ColumnName] = DateTime.ParseExact(lstObj[i], "dd/MM/yyyy",
                                               System.Globalization.CultureInfo.InvariantCulture);
                                        if (col.DataType == typeof(Decimal) && lstObj[i] != null && lstObj[i].Trim().Length > 0)
                                            dr[col.ColumnName] = Convert.ToDecimal(lstObj[i]);
                                        if (col.DataType == typeof(Int64) && lstObj[i] != null && lstObj[i].Trim().Length > 0)
                                            dr[col.ColumnName] = Convert.ToInt64(lstObj[i]);
                                        if (col.DataType == typeof(Int16) && lstObj[i] != null && lstObj[i].Trim().Length > 0)
                                            dr[col.ColumnName] = Convert.ToInt16(lstObj[i]);
                                        if (col.DataType == typeof(String))
                                            dr[col.ColumnName] = lstObj[i];
                                    }
                                    catch
                                    {

                                    }
                                    i++;
                                }
                                //if (dt.TableName == "GCS_CHISO_TP" && deserializedProduct.HDG_QHE_DDO_TP.Count > 0)
                                //{
                                //    //Bổ sung kiểm tra bản ghi có kỳ đúng = kỳ phụ không
                                //    var exists = deserializedProduct.HDG_QHE_DDO_TP.Where(c => c[0] == dr["MA_DDO"].ToString().Trim() && c[1] == dr["KY"].ToString().Trim());
                                //    if (exists != null && exists.Count() > 0)
                                //        dt.Rows.Add(dr);
                                //}
                                //else
                                //    dt.Rows.Add(dr);
                            }

                            break;
                        }
                        // Do something with propValue
                    }
                }
                #endregion
                //Debug.WriteLine(DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff") + " ChiSoKHang DataSet.");



                dsCustomerData.AcceptChanges();
                //Debug.WriteLine(DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff") + " ChiSoKHang DataSetAcceptChanges.");
                return "";
            }
            catch (Exception ex)
            {
                return "Lỗi khi lấy dữ liệu: " + ex.Message;
            }
            finally
            {
                //db.ReleaseConnection();
            }
        }

        #endregion
    }
}
