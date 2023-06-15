using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.ServiceModel;
using System.Text;
using busManagementServer;
using System.Data;


namespace WCFManagementServer
{
    // NOTE: If you change the class name "SerManagementServer" here, you must also update the reference to "SerManagementServer" in Web.config.
    public class SerManagementServer : ISerManagementServer
    {
        #region DũngNT
       
        public string getConnectString()
        {
            string strResult = clsRequestAdministration.getConnectString();
            return strResult;
        }
        public string insertServer(string strIP, string strConnectAddress)
        {
            clsAppServerAdministration clsApp = new clsAppServerAdministration();
            string strResult = clsApp.insertServer(strIP, strConnectAddress);
            return strResult;
        }
        public string updateServer(string strIP, string strConnectAddress)
        {
            clsAppServerAdministration clsApp = new clsAppServerAdministration();
            string strResult = clsApp.updateServer(strIP, strConnectAddress);
            return strResult;
        }
        public string deleteServer(string strIP)
        {
            clsAppServerAdministration clsApp = new clsAppServerAdministration();
            string strResult = clsApp.deleteServer(strIP);
            return strResult;
        }
        public string deleteAllServer()
        {
            clsAppServerAdministration clsApp = new clsAppServerAdministration();
            string strResult = clsApp.deleteAllServer();
            return strResult;
        }
        public DataSet getListServer()
        {
            clsAppServerAdministration clsApp = new clsAppServerAdministration();
            DataSet dsResult = clsApp.getListServer();
            return dsResult;
        }
        public DataSet getServer(string strIP)
        {
            clsAppServerAdministration clsApp = new clsAppServerAdministration();
            DataSet dsResult = clsApp.getServer(strIP);
            return dsResult;
        }
        #endregion

    }
}
