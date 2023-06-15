using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data.OracleClient;
using System.Linq;
using System.Text;

namespace BillingLibrary
{
    public class cls_Connection
    {
        //private string strPath = @"C:\CMIS2Config\ServersHDDT.xml";
        //private string strCMISPath = @"C:\CMIS2Config\Servers.xml";
        public static class Schema
        {
            public static string DMUC = "DMUC";
            public static string HDONPSINH = "HDONPSINH";
            public static string CHISO = "CHISO";
            public static string HOPDONG = "HOPDONG";
        };
        private string strConnectionString = "";
        private OracleConnection conn;
        public cls_Connection()
        {

        }
        public cls_Connection(string strSchema)
        {
            strConnectionString = ConfigurationManager.AppSettings[strSchema];
        }
        public OracleConnection OraConn
        {
            get
            {
                if (conn == null)
                {
                    conn = new OracleConnection();

                    conn.ConnectionString = strConnectionString;
                }
                if (conn.State != System.Data.ConnectionState.Open)
                    conn.Open();
                return conn;
            }
        }

        public bool testConnection(ref string strMess)
        {
            bool ok = true;
            OracleConnection _conn = null;
            strConnectionString = ConfigurationManager.AppSettings[Schema.DMUC];
            try
            {
                _conn = new OracleConnection(strConnectionString);
                _conn.Open();
                _conn.Close();
                strMess += "DMUC: Successful \n";
            }
            catch
            {
                strMess += "DMUC: Don't successful \n";
                ok &= false;
            }


            strConnectionString = ConfigurationManager.AppSettings[Schema.HDONPSINH];
            try
            {
                _conn = new OracleConnection(strConnectionString);
                _conn.Open();
                _conn.Close();
                strMess += "HDONPSINH: Successful \n";
            }
            catch
            {
                strMess += "HDONPSINH: Don't successful \n";
                ok &= false;
            }

            strConnectionString = ConfigurationManager.AppSettings[Schema.HOPDONG];
            try
            {
                _conn = new OracleConnection(strConnectionString);
                _conn.Open();
                _conn.Close();
                strMess += "HOPDONG: Successful \n";
            }
            catch
            {
                strMess += "HOPDONG: Don't successful \n";
                ok &= false;
            }
            strConnectionString = ConfigurationManager.AppSettings[Schema.CHISO];
            try
            {
                _conn = new OracleConnection(strConnectionString);
                _conn.Open();
                _conn.Close();
                strMess += "CHISO: Successful \n";
            }
            catch
            {
                strMess += "CHISO: Don't successful \n";
                ok &= false;
            }
            return ok;

        }
        public void dispose()
        {
            if(conn!=null)
            {
                conn.Dispose();
            }    
        }
    }
}
