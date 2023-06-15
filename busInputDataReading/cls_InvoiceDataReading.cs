using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using InputDataObject;
using System.Windows.Forms;
using DbConnect.DB;

namespace busInputDataReading
{
    public class cls_InvoiceDataReading
    {
        #region Attributes
        cls_GCS_LICHGCS_Controller obj_GCS_LICHGCS_Controller;
        #endregion

        #region Constructor
        public cls_InvoiceDataReading()
        {
            obj_GCS_LICHGCS_Controller = new cls_GCS_LICHGCS_Controller();
        }
        #endregion

        #region Method DungNT
        public DataSet getInvoiceData_ForCalculation(string strMaDViQLy, short ky, short thang, short nam)
        {
            CMIS2 db = new CMIS2();
            try
            {
                obj_GCS_LICHGCS_Controller.CMIS2 = db;
                DataSet ds = obj_GCS_LICHGCS_Controller.getGCS_LICHGCS_ForCalculation(strMaDViQLy, ky, thang, nam);
                return ds;
            }
            catch (Exception ex)
            {
                MessageBox.Show(ex.Message);
                return null;
            }
            finally
            {
                db.ReleaseConnection();
            }
        }
        public DataSet getInvoiceData_ForCancel(string strMaDViQLy, short ky, short thang, short nam)
        {
            CMIS2 db = new CMIS2();
            try
            {
                obj_GCS_LICHGCS_Controller.CMIS2 = db;
                DataSet ds = obj_GCS_LICHGCS_Controller.getGCS_LICHGCS_ForCancel(strMaDViQLy, ky, thang, nam);
                return ds;
            }

            catch (Exception ex)
            {
                MessageBox.Show(ex.Message);
                return null;
            }
            finally
            {
                db.ReleaseConnection();
            }
        }
        #endregion

    }
}
