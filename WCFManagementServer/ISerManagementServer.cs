using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.ServiceModel;
using System.Text;
using System.Data;

namespace WCFManagementServer
{
    // NOTE: If you change the interface name "ISerManagementServer" here, you must also update the reference to "ISerManagementServer" in Web.config.
    [ServiceContract]
    public interface ISerManagementServer
    {
        [OperationContract]
        [XmlSerializerFormat]
        string getConnectString();

        [OperationContract]
        [XmlSerializerFormat]
        string insertServer(string strIP, string strConnectAddress);

        [OperationContract]
        [XmlSerializerFormat]
         string updateServer(string strIP, string strConnectAddress);

        [OperationContract]
        [XmlSerializerFormat]
         string deleteServer(string strIP);

        [OperationContract]
        [XmlSerializerFormat]
         string deleteAllServer();

        [OperationContract]
        [XmlSerializerFormat]
         DataSet getListServer();

        [OperationContract]
        [XmlSerializerFormat]
         DataSet getServer(string strIP);
    }
}
