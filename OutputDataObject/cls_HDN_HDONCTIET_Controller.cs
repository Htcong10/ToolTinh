using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using DbConnect.DB;
using System.Data.OracleClient;

namespace OutputDataObject
{
    public class cls_HDN_HDONCTIET_Controller
    {
        #region   Atributes
        private DataSet ds = new DataSet();
        private DataTable dt = new DataTable();
        private HDN_HDONCTIET info;
        public List<HDN_HDONCTIET> lstCTiet;
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

        public cls_HDN_HDONCTIET_Controller()
        {
            info = new HDN_HDONCTIET();
        }
        public cls_HDN_HDONCTIET_Controller(HDN_HDONCTIET Info)
        {
            this.info = Info;
        }

        #endregion

        #region   Properties

        public HDN_HDONCTIET pInfor
        {
            get { return info; }
            set { info = value; }
        }

        #endregion

        #region Method DungNT
        public string InsertHDN_HDONCTIET()
        {
            try
            {
                this.CMIS2.DB.HDN_HDONCTIET.InsertAllOnSubmit(lstCTiet);
                return "";
            }
            catch (Exception e)
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
                //cmd.Parameters.AddWithValue("p_SEQUENCE_NAME", "SEQ_HDN_HDONCTIET").Direction = ParameterDirection.Input;
                //cmd.Parameters.Add("p_RESULT", OracleType.Number).Direction = ParameterDirection.Output;
                //cmd.ExecuteNonQuery();
                //long lngSeqLog = Convert.ToInt64(cmd.Parameters["p_RESULT"].Value);
                //return lngSeqLog;
                string strError = "";
                long lngID = this.CMIS2.DB.ExecuteCommand("select SEQ_HDN_HDONCTIET.nextval from dual", ref strError);                
                return lngID;
                //decimal result = 0;
                //this.CMIS2.DB.Sp_getsequence("SEQ_HDN_HDONCTIET", out result);
                //return Convert.ToInt64(result);
            }
            catch
            {
                return -1;
            }
        }
        public string DeleteHDN_HDONCTIET(string strmaDViQLy, string[] arrMaDDo, Int16 i16Ky, Int16 i16Thang, Int16 i16Nam)
        {
            try
            {
                lstCTiet = new List<HDN_HDONCTIET>();
                foreach (string strMaDDo in arrMaDDo)
                {
                    HDN_HDONCTIET ddo = new HDN_HDONCTIET();
                    ddo.MA_DVIQLY = strmaDViQLy;
                    ddo.KY = i16Ky;
                    ddo.THANG = i16Thang;
                    ddo.NAM = i16Nam;
                    ddo.MA_DDO = strMaDDo;
                    lstCTiet.Add(ddo);
                }
                this.CMIS2.DB.HDN_HDONCTIET.DeleteAllOnSubmit(lstCTiet);
                //while (arrMaDDo != null && arrMaDDo.Length > 0)
                //{
                //    string[] arrTemp = arrMaDDo.Take(1000).ToArray();
                //    var q = from a in this.CMIS2.DB.HDN_HDONCTIET
                //            where arrTemp.Contains(a.MA_DDO) && a.MA_DVIQLY == strmaDViQLy && a.NAM == i16Nam && a.THANG == i16Thang && a.KY == i16Ky
                //            select a;
                //    if (q != null && q.Take(1).Count() > 0)
                //    {
                //        lstCTiet.AddRange(q.ToList());
                //    }
                //    arrMaDDo = arrMaDDo.Skip(1000).ToArray();
                //}
                ////Xóa các dữ liệu chi tiết ghép tổng khác sổ             
                //this.CMIS2.DB.HDN_HDONCTIET.DeleteAllOnSubmit(lstCTiet);
                return "";
            }
            catch (Exception ex)
            {
                return "Lỗi khi xóa HDN_HDONCTIET: " + ex.Message;
            }
        }
        public List<string> getMA_DDO(string strMaDViQLy, long[] arrIDHDon, Int16 i16Ky, Int16 i16Thang, Int16 i16Nam)
        {
            try
            {
                List<string> lstMaDDo = new List<string>();
                while (arrIDHDon != null && arrIDHDon.Length > 0)
                {
                    long[] arrTemp = arrIDHDon.Take(1000).ToArray();
                    var q = from a in this.CMIS2.DB.HDN_HDONCTIET
                            where arrTemp.Contains(a.ID_HDON) && a.MA_DVIQLY == strMaDViQLy && a.NAM == i16Nam && a.THANG == i16Thang && a.KY == i16Ky
                            select a.MA_DDO;
                    if (q != null && q.Take(1).Count() > 0)
                    {
                        lstMaDDo.AddRange(q.Distinct().ToList());
                    }
                    arrIDHDon = arrIDHDon.Skip(1000).ToArray();
                }
                //Xóa các dữ liệu chi tiết ghép tổng khác sổ
                return lstMaDDo;
            }
            catch (Exception ex)
            {
                return null;
            }
        }

        public List<HDN_HDONCTIET> getHDonCTiet(string strMa_DViQLy, long[] arrIDHDon)
        {
            try
            {
                List<HDN_HDONCTIET> lstHDonCTiet = new List<HDN_HDONCTIET>();
                while (arrIDHDon != null && arrIDHDon.Length > 0)
                {
                    long[] arrTemp = arrIDHDon.Take(1000).ToArray();
                    var q = from a in this.CMIS2.DB.HDN_HDONCTIET
                            where a.MA_DVIQLY == strMa_DViQLy && arrTemp.Contains(a.ID_HDON)
                            select a;
                    if (q != null && q.Take(1).Count() > 0)
                    {
                        lstHDonCTiet.AddRange(q);
                    }
                    arrIDHDon = arrIDHDon.Skip(1000).ToArray();
                }
                
                return lstHDonCTiet;
            }
            catch
            {
                return null;
            }
        }
        #endregion
    }
}
