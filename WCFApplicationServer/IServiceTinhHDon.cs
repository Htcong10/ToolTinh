using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.ServiceModel;
using System.Text;
using System.Data;

namespace WCFApplicationServer
{
    // NOTE: If you change the interface name "IServiceTinhHDon" here, you must also update the reference to "IServiceTinhHDon" in Web.config.
    [ServiceContract]
    public interface IServiceTinhHDon
    {
        [OperationContract]
        [XmlSerializerFormat]
        DataSet getMaSoGCS();

        [OperationContract]
        [XmlSerializerFormat]
        bool InsertDSachSo(DataSet dsDSachMoi);

        [OperationContract]
        bool DeleteMaSoGCS();

        [OperationContract]
        long CountMaSoGCS();

        [OperationContract]
        [XmlSerializerFormat]
        DataSet getLogByNumOfRecord(int intNumRecord);

        [OperationContract]
        [XmlSerializerFormat]
        DataSet getLogBySubdivisionID(string SubdivisionID, int intNumRecord);

        [OperationContract]
        [XmlSerializerFormat]
        DataSet getLogByBookID(string SubdivisionID, string strMaSoGCS, int intNumRecord);

        [OperationContract]
        [XmlSerializerFormat]
        DataSet getInvoiceData_ForCancel(string strMaDViQLy, short ky, short thang, short nam);

        [OperationContract]
        [XmlSerializerFormat]
        DataSet getInvoiceData_ForCalculation(string strMaDViQLy, short ky, short thang, short nam);

        [OperationContract]
        [XmlSerializerFormat]
        string CancelInvoiceCalculation(string strMaDViQLy, string[] strMaSoGCs, string strTenDNhap, long lngCurrentLibID, long lngWorkflowID, Int16 i16Ky, Int16 i16Thang, Int16 i16Nam);

        [OperationContract]
        CompositeType GetDataUsingDataContract(CompositeType composite);
    }
    [DataContract]
    public class CompositeType
    {
        bool boolValue = true;
        string stringValue = "Hello ";

        [DataMember]
        public bool BoolValue
        {
            get { return boolValue; }
            set { boolValue = value; }
        }

        [DataMember]
        public string StringValue
        {
            get { return stringValue; }
            set { stringValue = value; }
        }
    }
}
