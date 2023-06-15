using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.ServiceModel;
using System.Text;
using System.Data;
using busLogManagement;
using busInputDataReading;
using busOutputDataWriting;

namespace WCFApplicationServer
{
    // NOTE: If you change the class name "ServiceTinhHDon" here, you must also update the reference to "ServiceTinhHDon" in Web.config.
    public class ServiceTinhHDon : IServiceTinhHDon
    {
        public string GetData(int value)
        {
            return string.Format("You entered: {0}", value);
        }

        public CompositeType GetDataUsingDataContract(CompositeType composite)
        {
            if (composite.BoolValue)
            {
                composite.StringValue += "Suffix";
            }
            return composite;
        }
        #region DũngNT
        cls_LogManagement objLogManagement;
        cls_InvoiceDataReading objInvoiceData;
        clsCancelInvoiceCalculation objCancelCalculation;
        public DataSet getMaSoGCS()
        {
            try
            {
                DataSet ds = HDONLibrary.THDInfomation.dsDSachSo;
                if (ds != null && ds.Tables.Count > 0 && ds.Tables[0].Rows.Count > 0)
                {
                    DataSet dsResult = new DataSet();
                    DataTable dt = ds.Tables[0].Copy();
                    dsResult.Tables.Add(dt);
                    HDONLibrary.THDInfomation.dsDSachSo.Tables[0].Rows.RemoveAt(0);
                    return dsResult;
                }
            }
            catch
            {

            }
            return null;
        }
        public long CountMaSoGCS()
        {
            try
            {
                DataSet ds = HDONLibrary.THDInfomation.dsDSachSo;
                if (ds != null && ds.Tables.Count > 0)
                {
                    long lngCount = ds.Tables[0].Rows.Count;
                    return lngCount;
                }
            }
            catch
            {

            }
            return 0;
        }
        public bool InsertDSachSo(DataSet dsDSachMoi)
        {
            try
            {
                DataSet ds = HDONLibrary.THDInfomation.dsDSachSo;
                if (dsDSachMoi.Tables.Count == 0) return true;
                if (HDONLibrary.THDInfomation.dsDSachSo == null || HDONLibrary.THDInfomation.dsDSachSo.Tables.Count == 0)
                {
                    HDONLibrary.THDInfomation.dsDSachSo = dsDSachMoi;
                    return true;
                }
                HDONLibrary.THDInfomation.dsDSachSo.Tables[0].Merge(dsDSachMoi.Tables[0]);
                return true;
            }
            catch
            {
                return false;
            }
        }
        public bool DeleteMaSoGCS()
        {
            DataSet ds = HDONLibrary.THDInfomation.dsDSachSo;
            if (ds != null && ds.Tables.Count > 0 && ds.Tables[0].Rows.Count > 0)
            {
                try
                {
                    ds.Tables[0].Rows.RemoveAt(0);
                    return true;
                }
                catch
                {
                    return false;
                }
            }
            return false;
        }
        public DataSet getLogByNumOfRecord(int intNumRecord)
        {
            objLogManagement = new cls_LogManagement();
            DataSet ds = objLogManagement.getLogByParameter(intNumRecord);
            return ds;
        }
        public DataSet getLogBySubdivisionID(string SubdivisionID, int intNumRecord)
        {
            objLogManagement = new cls_LogManagement();
            DataSet ds = objLogManagement.getLogByMaDViQLy(SubdivisionID, intNumRecord);
            return ds;
        }
        public DataSet getLogByBookID(string SubdivisionID, string strMaSoGCS, int intNumRecord)
        {
            objLogManagement = new cls_LogManagement();
            DataSet ds = objLogManagement.getLogByMaSoGCS(SubdivisionID, strMaSoGCS, intNumRecord);
            return ds;
        }
        public DataSet getInvoiceData_ForCalculation(string strMaDViQLy, short ky, short thang, short nam)
        {
            objInvoiceData = new cls_InvoiceDataReading();
            DataSet ds = objInvoiceData.getInvoiceData_ForCalculation(strMaDViQLy, ky, thang, nam);
            return ds;
        }
        public DataSet getInvoiceData_ForCancel(string strMaDViQLy, short ky, short thang, short nam)
        {
            objInvoiceData = new cls_InvoiceDataReading();
            DataSet ds = objInvoiceData.getInvoiceData_ForCancel(strMaDViQLy, ky, thang, nam);
            return ds;           
        }
        public string CancelInvoiceCalculation(string strMaDViQLy, string[] strMaSoGCs, string strTenDNhap, long lngCurrentLibID, long lngWorkflowID, Int16 i16Ky, Int16 i16Thang, Int16 i16Nam)
        {
            objCancelCalculation = new clsCancelInvoiceCalculation();
            string strResult = objCancelCalculation.CancelInvoiceCalculation(strMaDViQLy, strMaSoGCs,strTenDNhap,lngCurrentLibID,lngWorkflowID, i16Ky, i16Thang, i16Nam);
            return strResult;
        }
        #endregion

    }
}
