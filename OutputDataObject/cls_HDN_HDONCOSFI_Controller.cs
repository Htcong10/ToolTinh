using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using DbConnect.DB;
using System.Data.OracleClient;

namespace OutputDataObject
{
    public class cls_HDN_HDONCOSFI_Controller
    {
        #region   Atributes       
        private DataSet ds = new DataSet();
        private DataTable dt = new DataTable();
        private HDN_HDONCOSFI info;
        public List<HDN_HDONCOSFI> lstCosfi;
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

        public cls_HDN_HDONCOSFI_Controller()
        {
            info = new HDN_HDONCOSFI();
        }
        public cls_HDN_HDONCOSFI_Controller(HDN_HDONCOSFI Info)
        {
            this.info = Info;
        }

        #endregion

        #region   Properties

        public HDN_HDONCOSFI pInfor
        {
            get { return info; }
            set { info = value; }
        }

        #endregion

        #region Method DungNT
        public string InsertHDN_HDONCOSFI()
        {
            try
            {
                this.CMIS2.DB.HDN_HDONCOSFI.InsertAllOnSubmit(lstCosfi);
                return "";
            }
            catch(Exception e)
            {
                return e.Message;
            }
        }
        public long getID_HDON()
        {
            try
            {
                //if (CMIS2.g_conn.State == ConnectionState.Closed) CMIS2.g_conn.Open();
                //OracleCommand cmd = new OracleCommand();
                //cmd.CommandType = CommandType.StoredProcedure;
                //cmd.CommandText = "PKG_TINHHOADON.SP_GETSEQUENCE";
                //cmd.Connection = CMIS2.g_conn;
                //cmd.Parameters.Clear();
                //cmd.Parameters.AddWithValue("p_SEQUENCE_NAME", "SEQ_HDN_HDONCOSFI").Direction = ParameterDirection.Input;
                //cmd.Parameters.Add("p_RESULT", OracleType.Number).Direction = ParameterDirection.Output;
                //cmd.ExecuteNonQuery();
                //long lngSeqLog = Convert.ToInt64(cmd.Parameters["p_RESULT"].Value);                
                //return lngSeqLog;
                string strError = "";
                long lngID = this.CMIS2.DB.ExecuteCommand("select SEQ_HDN_HDONCOSFI.nextval from dual", ref strError);                
                return lngID;
                //decimal result = 0;
                //this.CMIS2.DB.Sp_getsequence("SEQ_HDN_HDONCOSFI", out result);
                //return Convert.ToInt64(result);
            }
            catch
            {
                return -1;
            }
        }
        public string DeleteHDN_HDONCOSFI(string strmaDViQLy, string[] arrMaDDo, Int16 i16Ky, Int16 i16Thang, Int16 i16Nam)
        {
            try
            {
                lstCosfi = new List<HDN_HDONCOSFI>();
                foreach (string strMaDDo in arrMaDDo)
                {
                    HDN_HDONCOSFI ddo = new HDN_HDONCOSFI();
                    ddo.MA_DVIQLY = strmaDViQLy;
                    ddo.MA_DDO = strMaDDo;
                    ddo.NAM = i16Nam;
                    ddo.THANG = i16Thang;
                    ddo.KY = i16Ky;
                    lstCosfi.Add(ddo);
                }
                this.CMIS2.DB.HDN_HDONCOSFI.DeleteAllOnSubmit(lstCosfi);
                //while (arrMaDDo != null && arrMaDDo.Length > 0)
                //{
                //    string[] arrTemp = arrMaDDo.Take(1000).ToArray();
                //    var q = from a in this.CMIS2.DB.HDN_HDONCOSFI
                //            where arrTemp.Contains(a.MA_DDO) && a.MA_DVIQLY == strmaDViQLy && a.NAM == i16Nam && a.THANG == i16Thang && a.KY == i16Ky
                //            select a;
                //    if (q != null && q.Take(1).Count() > 0)
                //    {
                //        lstCosfi.AddRange(q.ToList());
                //    }
                //    arrMaDDo = arrMaDDo.Skip(1000).ToArray();
                //}
                //this.CMIS2.DB.HDN_HDONCOSFI.DeleteAllOnSubmit(lstCosfi);
                //delete các điểm đo phụ ghép tổng
                return "";
            }
            catch (Exception ex)
            {
                lstCosfi = new List<HDN_HDONCOSFI>();
                return "Lỗi khi xóa  HDN_HDONCOSFI: " + ex.Message;
            }
        }        
        #endregion
    }
}
