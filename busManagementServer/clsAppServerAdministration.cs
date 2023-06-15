using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.IO;
using System.Windows.Forms;

namespace busManagementServer
{
    public class clsAppServerAdministration
    {
        public string insertServer(string strIP, string strConnectAddress)
        {
            try
            {
                string strPath = Application.StartupPath + @"\ApplicationServerList.xml";
                if (ApplicationServerList.dsApplicationServerList == null || ApplicationServerList.dsApplicationServerList.Tables["APP_SERVER_LIST"] == null)
                {
                    ApplicationServerList.dsApplicationServerList = new APP_SERVER_LIST_Entity();
                }
                if (ApplicationServerList.dsApplicationServerList.Tables["APP_SERVER_LIST"].Rows.Find(strIP) == null)
                {
                    //Kiểm tra nếu tồn tại địa chỉ 
                    DataRow row = ApplicationServerList.dsApplicationServerList.Tables["APP_SERVER_LIST"].NewRow();
                    row["IP"] = strIP;
                    row["CONNECTION_ADDRESS"] = strConnectAddress;
                    ApplicationServerList.dsApplicationServerList.Tables["APP_SERVER_LIST"].Rows.Add(row);
                    ApplicationServerList.dsApplicationServerList.AcceptChanges();
                    //Kiểm tra trong Database để ghi thông tin danh sách ApplicationServer - Hiện tại là trong file XML                    
                    if (File.Exists(strPath)) File.Delete(strPath);
                    System.IO.FileStream myFileStream = new System.IO.FileStream
                           (strPath, System.IO.FileMode.Create);
                    try
                    {
                        // create an XmlTextWriter with the fileStream.
                        System.Xml.XmlTextWriter myXmlWriter =
                           new System.Xml.XmlTextWriter(myFileStream, System.Text.Encoding.Unicode);
                        // Write to the file with the WriteXml method.
                        ApplicationServerList.dsApplicationServerList.WriteXml(myXmlWriter);
                        myXmlWriter.Close();
                    }
                    catch (Exception ex)
                    {
                        return "Lỗi khi ghi file ApplicationServerList.xml: " + ex.Message;
                    }
                    //End
                }
                return "";
            }
            catch (Exception ex)
            {
                return "Lỗi khi Insert Application Server: " + ex.Message;
            }
        }
        public string updateServer(string strIP, string strConnectAddress)
        {
            return "";
        }
        public string deleteServer(string strIP)
        {
            return "";
        }
        public string deleteAllServer()
        {
            return "";
        }
        public DataSet getListServer()
        {
            return null;
        }
        public DataSet getServer(string strIP)
        {
            return null;
        }

    }
}
