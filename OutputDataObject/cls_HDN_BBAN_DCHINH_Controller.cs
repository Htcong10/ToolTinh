using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using DbConnect.DB;

namespace OutputDataObject
{
    public class cls_HDN_BBAN_DCHINH_Controller
    {
        #region   Atributes

        private HDN_BBAN_DCHINH info;
        private List<HDN_BBAN_DCHINH> lstInfo;

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

        public cls_HDN_BBAN_DCHINH_Controller()
        {
            lstInfo = new List<HDN_BBAN_DCHINH>();
            info = new HDN_BBAN_DCHINH();
        }
        public cls_HDN_BBAN_DCHINH_Controller(HDN_BBAN_DCHINH Info)
        {
            this.info = Info;
        }

        #endregion

        #region   Properties
        public List<HDN_BBAN_DCHINH> LstInfo
        {
            get { return lstInfo; }
            set { lstInfo = value; }
        }
        public HDN_BBAN_DCHINH pInfor
        {
            get { return info; }
            set { info = value; }
        }

        #endregion

        #region Method DungNT
        public List<HDN_BBAN_DCHINH> get_HDN_BBAN_DCHINH(string strMaDViQLy, long lngIDHDon)
        {
            try
            {
                List<HDN_BBAN_DCHINH> q = (from p in this.CMIS2.DB.HDN_BBAN_DCHINH
                                           where p.MA_DVIQLY == strMaDViQLy && p.ID_HDON == lngIDHDon
                                           select p).ToList();
                return q;
            }
            catch
            {
                
                return null;
            }
        }
        public string Insert()
        {
            try
            {
                //this.info.ID_BBAN = getMaxID();
                this.CMIS2.DB.HDN_BBAN_DCHINH.InsertAllOnSubmit(this.lstInfo);
                return "";
            }
            catch (Exception ex)
            {
                
                return "Lỗi khi Insert HDN_BBAN_DCHINH: " + ex.Message;
            }

        }
        public long getMaxID()
        {
            try
            {
                string str = "";
                long id = this.CMIS2.DB.ExecuteCommand("SELECT SEQ_HDN_BBAN_DCHINH.NEXTVAL FROM DUAL", ref str);
                return id;
            }
            catch
            {
                
                return -1;
            }
        }
        public bool Delete(string strMaDViQLy, long lngIDHDonDC)
        {
            try
            {

                this.CMIS2.DB.HDN_BBAN_DCHINH.DeleteAllOnSubmit(from a in this.CMIS2.DB.HDN_BBAN_DCHINH where a.MA_DVIQLY == strMaDViQLy && a.ID_HDON_DC == lngIDHDonDC select a);
                return true;
            }
            catch
            {
                
                return false;
            }
        }
        //public string SubmitChanges()
        //{
        //    try
        //    {
                
                
        //        return "";
        //    }
        //    catch (Exception ex)
        //    {
                
        //        return "Lỗi khi Submit: " + ex.Message;
        //    }
        //}
        #endregion

    }
}
