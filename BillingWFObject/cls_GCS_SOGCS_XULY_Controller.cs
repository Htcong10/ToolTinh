using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using CMISLibrary.Entities;
using System.Data.OracleClient;
using DbConnect.DB;

namespace BillingWFObject
{
    public class cls_GCS_SOGCS_XULY_Controller
    {
        #region   Atributes

        private DataSet CMIS_Header = new CMISOutputParameter();
        private DataSet ds = new DataSet();
        private DataTable dt = new DataTable();
        List<GCS_SOGCS_XULY> lstGCSXL = new List<GCS_SOGCS_XULY>();
        private GCS_SOGCS_XULY info;
        public DataSet dsCurrentState, dsConfigState;
        private DataTable dtCurrentState, dtConfigState;
        public string strError = "";

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

        public cls_GCS_SOGCS_XULY_Controller()
        {
            info = new GCS_SOGCS_XULY();
        }
        public cls_GCS_SOGCS_XULY_Controller(GCS_SOGCS_XULY Info)
        {
            this.info = Info;
        }

        #endregion

        #region   Properties

        public GCS_SOGCS_XULY pInfor
        {
            get { return info; }
            set { info = value; }
        }

        #endregion

        #region Method DungNT
        protected long getIDBuocXLy()
        {
            //string strError = "";
            try
            {
                //if (DB.g_conn.State == ConnectionState.Closed) DB.g_conn.Open();
                //OracleCommand cmd = new OracleCommand();
                //cmd.CommandType = CommandType.StoredProcedure;
                //cmd.CommandText = "PKG_TINHHOADON.SP_GETSEQUENCE";
                //cmd.Connection = DB.g_conn;
                //cmd.Parameters.Clear();
                //cmd.Parameters.AddWithValue("p_SEQUENCE_NAME", "SEQ_GCS_SOGCS_XULY").Direction = ParameterDirection.Input;
                //cmd.Parameters.Add("p_RESULT", OracleType.Number).Direction = ParameterDirection.Output;
                //cmd.ExecuteNonQuery();
                //long lngSeqLog = Convert.ToInt64(cmd.Parameters["p_RESULT"].Value);
                //return lngSeqLog;
                decimal result = 0;
                this.CMIS2.DB.Sp_getsequence("SEQ_GCS_SOGCS_XULY", out result);
                return Convert.ToInt64(result);
            }
            catch
            {
                return -1;
            }           
        }
        public DataSet getConfigState()
        {
            try
            {
                var a = from p in this.CMIS2.DB.S_WFCONFIGURATION
                        where p.SUBDIVISIONID == info.MA_DVIQLY
                        && p.PARENTLIBID == info.CURRENTLIBID
                        && p.WORKFLOWID == info.WORKFLOWID
                        select new
                        {
                            p.CHILDLIBID,
                            p.PATH,
                            p.STATE,
                            p.PRESTATE,
                            p.PARENTLIBID
                        };
                if (a != null)
                {
                    dtConfigState = new DataTable();
                    dsConfigState = new DataSet();
                    dtConfigState = CMISLibrary.Utility.LINQToDataTable(a);
                    dtConfigState.AcceptChanges();
                    dsConfigState.Tables.Add(dtConfigState);
                    return dsConfigState;
                }
                else
                {
                    return null;
                }
            }
            catch (Exception ex)
            {                
                strError += ex.ToString();
                return null;
            }
        }
        
        public DataSet getCurrentState()
        {
            try
            {
                var a = (from p in this.CMIS2.DB.GCS_SOGCS_XULY
                         where p.MA_DVIQLY == info.MA_DVIQLY
                         && p.MA_SOGCS == info.MA_SOGCS
                         && p.NAM == info.NAM
                         && p.THANG == info.THANG
                         && p.KY == info.KY
                         && p.WORKFLOWID == info.WORKFLOWID
                         select p).ToList();
                if (a != null && a.Take(1).Count() > 0)
                {
                    var q = a.Where(c => c.ID_BUOCXLY == a.Max(b => b.ID_BUOCXLY));
                    if (q != null && q.Take(1).Count() > 0)
                    {
                        dtCurrentState = new DataTable();
                        dsCurrentState = new DataSet();
                        dtCurrentState = CMISLibrary.Utility.LINQToDataTable(q);
                        dtCurrentState.AcceptChanges();
                        dsCurrentState.Tables.Clear();
                        dsCurrentState.Tables.Add(dtCurrentState);
                        return dsCurrentState;
                    }
                    else
                    {
                        dsCurrentState = null;
                        return null;
                    }
                }
                else
                {
                    dsCurrentState = null;
                    return null;
                }
            }
            catch(Exception ex)
            {
                dsCurrentState = null;                
                strError += ex.ToString();
                return null;
            }
        }       
        public string GetState()
        {
            try
            {
                if (dsConfigState == null || dsConfigState.Tables.Count == 0 || dsConfigState.Tables[0].Rows.Count == 0)
                {
                    getConfigState();
                    if (dsConfigState == null || dsConfigState.Tables.Count == 0 || dsConfigState.Tables[0].Rows.Count == 0)
                    {
                        return "OUT";
                    }
                }
                if (dsCurrentState == null || dsCurrentState.Tables.Count == 0 || dsCurrentState.Tables[0].Rows.Count == 0)
                {
                    //Chỉ trả về IN đối với chức năng khởi đầu luồng
                    var q = from a in dsConfigState.Tables[0].AsEnumerable()
                            where a.Field<long>("PARENTLIBID") == info.CURRENTLIBID
                               && a.Field<string>("PRESTATE") == "BEG"
                            select a;
                    if (q != null && q.Count() > 0)
                        return "IN";
                    else return "OUT";
                }
                else
                {
                    string strCurrentState = dsCurrentState.Tables[0].Rows[0]["PATH"].ToString().Trim();
                    if (strCurrentState.Contains("-0")) return "OFF"; //-0 là trạng thái chỉ có khi hủy/tạm dừng
                    var q = from a in dsConfigState.Tables[0].AsEnumerable()
                            where a.Field<long>("PARENTLIBID") == info.CURRENTLIBID
                               && a.Field<string>("PRESTATE") == strCurrentState
                            select a;
                    if (q != null && q.Count() > 0)
                        return "IN";
                    else
                    {
                        string strTemp = strCurrentState.Replace("-" + info.CURRENTLIBID.ToString(), "");
                        var p = from a in dsConfigState.Tables[0].AsEnumerable()
                                where a.Field<long>("PARENTLIBID") == info.CURRENTLIBID
                                   && a.Field<string>("PRESTATE") == strTemp
                                select a;
                        if (p != null && p.Count() > 0)
                        {
                            //Bổ sung thêm kiểm tra đã tồn tại xử lý nào sau đó chưa
                            long IDMax = Convert.ToInt64(dsCurrentState.Tables[0].Rows[0]["ID_BUOCXLY"]);
                            long[] arrNextLib = (from a in dsConfigState.Tables[0].AsEnumerable()
                                                 select a.Field<long>("CHILDLIBID")).ToArray();
                            var exists = from a in this.CMIS2.DB.GCS_SOGCS_XULY
                                         where a.MA_DVIQLY == info.MA_DVIQLY
                                         && a.WORKFLOWID == info.WORKFLOWID
                                         && arrNextLib.Contains(a.CURRENTLIBID)
                                         && a.MA_SOGCS == info.MA_SOGCS
                                         && a.NAM == info.NAM
                                         && a.THANG == info.THANG
                                         && a.KY == info.KY
                                         && a.ID_BUOCXLY > IDMax
                                         select a;
                            if (exists != null && exists.Take(1).Count() > 0)
                                return "OUT";
                            //End
                            return "RETURN";
                        }
                        else
                            return "OUT";
                    }
                }
            }
            catch (Exception ex)
            {
                return "Lỗi trong busWF: " + ex.ToString();
            }
        }
        public bool insertOnSuccessOrDestroy(string strState)
        {
            try
            {
                string strCurrentState = "";
                if (dsCurrentState == null || dsCurrentState.Tables.Count == 0 || dsCurrentState.Tables[0].Rows.Count == 0)
                    strCurrentState = "BEG";
                else
                    strCurrentState = dsCurrentState.Tables[0].Rows[0]["PATH"].ToString().Trim();
                if (dsConfigState == null || dsConfigState.Tables.Count == 0 || dsConfigState.Tables[0].Rows.Count == 0)
                {
                    getConfigState();
                    if (dsConfigState == null || dsConfigState.Tables.Count == 0 || dsConfigState.Tables[0].Rows.Count == 0)
                    {
                        strError += "Lỗi tại busWF: Không tìm thấy dữ liệu config trong S_WFCONFIGURATION";
                        return false;
                    }
                }
                DataRow drRowConfig = dsConfigState.Tables[0].Select("PARENTLIBID='" + info.CURRENTLIBID.ToString().Trim() + "' and PRESTATE='" + strCurrentState + "'")[0];
                string strPath = strCurrentState + "-" + info.CURRENTLIBID.ToString().Trim();
                info.ID_BUOCXLY = getIDBuocXLy();
                info.NEXTLIBID = Convert.ToInt64(drRowConfig["CHILDLIBID"]);
                info.PATH = strPath;
                info.RESULTSTATE = drRowConfig["STATE"].ToString().Trim();
                var exists = from a in this.CMIS2.DB.GCS_SOGCS_XULY
                             where a.MA_DVIQLY == info.MA_DVIQLY
                             && a.MA_SOGCS == info.MA_SOGCS
                             && a.NAM == info.NAM
                             && a.THANG == info.THANG
                             && a.KY == info.KY
                             && a.CURRENTLIBID == info.CURRENTLIBID
                             && a.WORKFLOWID == info.WORKFLOWID
                             && a.RESULTSTATE == info.RESULTSTATE
                             && a.NEXTLIBID == info.NEXTLIBID
                             && a.PATH == info.PATH
                             select a;
                if (exists != null && exists.Take(1).Count() > 0)
                {
                    //return true;
                }
                else
                {
                    this.CMIS2.DB.GCS_SOGCS_XULY.InsertOnSubmit(info);
                }
                GCS_LICHGCS objGCS_LICHGCS = this.CMIS2.DB.GCS_LICHGCS.Single(c => c.MA_DVIQLY == info.MA_DVIQLY && c.MA_SOGCS == info.MA_SOGCS && c.NAM == info.NAM && c.THANG == info.THANG && c.KY == info.KY);
                if (objGCS_LICHGCS != null)
                {
                    objGCS_LICHGCS.TRANG_THAI = strState;
                    objGCS_LICHGCS.NGAY_SUA = info.NGAY_SUA;
                    objGCS_LICHGCS.NGUOI_SUA = info.NGUOI_SUA;
                    this.CMIS2.DB.GCS_LICHGCS.UpdateOnSubmit(objGCS_LICHGCS);
                }
                
                return true;
            }
            catch (Exception ex)
            {
                strError += "Lỗi tại busWF: " + ex.ToString();
                return false;
            }
        }
        public bool deleteOnUndo()
        {
            try
            {
                bool success = true;
                if (GetState() != "RETURN") return false;
                else
                {
                    var kq = from a in this.CMIS2.DB.GCS_SOGCS_XULY
                             where a.MA_DVIQLY == info.MA_DVIQLY
                             && a.MA_SOGCS == info.MA_SOGCS
                             && a.CURRENTLIBID == info.CURRENTLIBID
                             && a.WORKFLOWID == info.WORKFLOWID
                             && a.NAM == info.NAM
                             && a.THANG == info.THANG
                             && a.KY == info.KY
                             select a;
                    List<GCS_SOGCS_XULY> lstDelete = new List<GCS_SOGCS_XULY>();
                    foreach (var x in kq)
                    {
                        GCS_SOGCS_XULY obj = new GCS_SOGCS_XULY();
                        obj.CURRENTLIBID = x.CURRENTLIBID;
                        obj.ID_BUOCXLY = x.ID_BUOCXLY;
                        obj.KY = x.KY;
                        obj.MA_CNANG = x.CURRENTLIBID.ToString();
                        obj.MA_DVIQLY = x.MA_DVIQLY;
                        obj.MA_SOGCS = x.MA_SOGCS;
                        obj.NAM = x.NAM;
                        obj.NEXTLIBID = x.NEXTLIBID;
                        obj.NGAY_BDAU = x.NGAY_BDAU;
                        obj.NGAY_KTHUC = x.NGAY_KTHUC;
                        obj.NGAY_SUA = x.NGAY_SUA;
                        obj.NGAY_TAO = x.NGAY_TAO;
                        obj.NGUOI_SUA = x.NGUOI_SUA;
                        obj.NGUOI_TAO = x.NGUOI_TAO;
                        obj.NGUOI_THIEN = x.NGUOI_THIEN;
                        obj.PATH = x.PATH;
                        obj.RESULTSTATE = x.RESULTSTATE;
                        obj.THANG = x.THANG;
                        obj.WORKFLOWID = x.WORKFLOWID;
                        lstDelete.Add(obj);
                    }
                    if (lstDelete.Count > 0)
                        this.CMIS2.DB.GCS_SOGCS_XULY.DeleteAllOnSubmit(lstDelete);
                    

                    //GCS_SOGCS_XULY sogcsxly = new GCS_SOGCS_XULY();
                    //sogcsxly.MA_DVIQLY = info.MA_DVIQLY;
                    //sogcsxly.MA_SOGCS = info.MA_SOGCS;
                    //sogcsxly.CURRENTLIBID = info.CURRENTLIBID;
                    //sogcsxly.WORKFLOWID = info.WORKFLOWID;
                    //sogcsxly.NAM = info.NAM;
                    //sogcsxly.THANG = info.THANG;
                    //sogcsxly.KY = info.KY;
                    //List<GCS_SOGCS_XULY> lstSoGCSXLy = new List<GCS_SOGCS_XULY>();
                    //lstSoGCSXLy.Add(sogcsxly);
                    //this.CMIS2.DB.GCS_SOGCS_XULY.DeleteAllOnSubmit(lstSoGCSXLy);

                    string strLibID = info.CURRENTLIBID.ToString().Trim();
                    var kqNext = from a in this.CMIS2.DB.GCS_SOGCS_XULY
                                 where a.MA_DVIQLY == info.MA_DVIQLY
                                 && a.MA_SOGCS == info.MA_SOGCS
                                 && a.WORKFLOWID == info.WORKFLOWID
                                 && a.NAM == info.NAM
                                 && a.THANG == info.THANG
                                 && a.KY == info.KY
                                 && a.PATH.Contains(strLibID)
                                 orderby a.ID_BUOCXLY descending
                                 select a;
                    if (kqNext != null && kqNext.Take(1).Count() > 0)
                    {
                        foreach (var x in kqNext)
                        {
                            x.PATH = x.PATH.Replace("-" + info.CURRENTLIBID.ToString().Trim(), "");
                        }
                        this.CMIS2.DB.GCS_SOGCS_XULY.UpdateAllOnSubmit(kqNext);
                    }

                    var kqAll = (from c in this.CMIS2.DB.GCS_SOGCS_XULY
                                 where c.MA_DVIQLY == info.MA_DVIQLY
                                 && c.MA_SOGCS == info.MA_SOGCS
                                 && c.WORKFLOWID == info.WORKFLOWID
                                 && c.NAM == info.NAM
                                 && c.THANG == info.THANG
                                 && c.KY == info.KY
                                 && c.CURRENTLIBID != info.CURRENTLIBID
                                 orderby c.ID_BUOCXLY descending
                                 select c).ToList();
                    if (kqAll != null && kqAll.Count > 0)
                    {
                        GCS_SOGCS_XULY objXuLy = kqAll.Where(c => c.ID_BUOCXLY == kqAll.Max(b => b.ID_BUOCXLY)).First();
                        GCS_LICHGCS objGCS_LICHGCS = this.CMIS2.DB.GCS_LICHGCS.Single(c => c.MA_DVIQLY == info.MA_DVIQLY && c.MA_SOGCS == info.MA_SOGCS && c.NAM == info.NAM && c.THANG == info.THANG && c.KY == info.KY);
                        if (objGCS_LICHGCS != null)
                        {
                            objGCS_LICHGCS.TRANG_THAI = objXuLy != null ? objXuLy.RESULTSTATE : "";
                            objGCS_LICHGCS.NGAY_SUA = info.NGAY_SUA;
                            objGCS_LICHGCS.NGUOI_SUA = info.NGUOI_SUA;
                            this.CMIS2.DB.GCS_LICHGCS.UpdateOnSubmit(objGCS_LICHGCS);
                        }
                    }                    
                    return true;
                }
            }
            catch (Exception ex)
            {
                strError += "Lỗi tại busWF: " + ex.ToString();
                return false;
            }
        }
        #endregion
    }

}
