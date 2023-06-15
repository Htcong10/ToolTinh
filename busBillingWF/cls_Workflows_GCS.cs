using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using BillingWFObject;
using System.Data;
using CMISLibrary;
using DbConnect.DB;

namespace busBillingWF
{
    public class cls_Workflows_GCS
    {
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

        public string strError = "";
        
        private cls_GCS_SOGCS_XULY_Controller obj_GCS_SOGCS_XULY_Controller;

        public cls_GCS_SOGCS_XULY_Controller Obj_GCS_SOGCS_XULY_Controller
        {
            get { return obj_GCS_SOGCS_XULY_Controller; }
            set { obj_GCS_SOGCS_XULY_Controller = value; }
        }

        #region   Constructor
        public cls_Workflows_GCS()
        {
            obj_GCS_SOGCS_XULY_Controller = new cls_GCS_SOGCS_XULY_Controller();            
        }
        public cls_Workflows_GCS(DataSet Entity)
        {
            GCS_SOGCS_XULY info = new GCS_SOGCS_XULY();
            if (Entity.Tables.Count >= 0)
            {
                if (Entity.Tables["GCS_SOGCS_XULY"].Rows.Count > 0)
                {
                    DataRow dr = Entity.Tables["GCS_SOGCS_XULY"].Rows[0];
                    info = Utility.MapDatarowToObject<GCS_SOGCS_XULY>(dr, ref strError);
                }
            }
            obj_GCS_SOGCS_XULY_Controller = new cls_GCS_SOGCS_XULY_Controller(info);
            obj_GCS_SOGCS_XULY_Controller.CMIS2 = cmis2;
            //obj_GCS_SOGCS_XULY_Controller.getCurrentState();
            //obj_GCS_SOGCS_XULY_Controller.getConfigState();
        }

        #endregion

        #region   Methods DũngNT
        /// <summary>
        /// Dùng khi kết thúc chức năng và chuyển trạng thái của đối tượng sang trạng thái khác 
        /// </summary>
        /// <param name="strState">Trạng thái trả về của đối tượng khi kết thúc chức năng</param>
        /// <returns></returns>
        public bool insertOnSuccessOrDestroy(string strState)
        {
            obj_GCS_SOGCS_XULY_Controller.CMIS2 = cmis2;
            bool ok = obj_GCS_SOGCS_XULY_Controller.insertOnSuccessOrDestroy(strState);
            strError = obj_GCS_SOGCS_XULY_Controller.strError;
            return ok;
        }
        /// <summary>
        /// Xóa dữ liệu trong bảng GCS_SOGCS_XULY trong trường hợp hủy thao tác
        /// Trả về true trong trường hợp xóa thành công
        /// Trả về false trong trường hợp chức năng tiếp theo đã thao tác lên đối tượng rồi
        /// </summary>
        /// <returns></returns>
        public bool deleteOnUndo()
        {
            obj_GCS_SOGCS_XULY_Controller.CMIS2 = cmis2;
            bool ok = obj_GCS_SOGCS_XULY_Controller.deleteOnUndo();
            strError = obj_GCS_SOGCS_XULY_Controller.strError;
            return ok;
        }

        public DataSet getCurrentState()
        {
            obj_GCS_SOGCS_XULY_Controller.CMIS2 = cmis2;
            return obj_GCS_SOGCS_XULY_Controller.getCurrentState();
        }
        public DataSet getConfigState()
        {
            obj_GCS_SOGCS_XULY_Controller.CMIS2 = cmis2;
            return obj_GCS_SOGCS_XULY_Controller.getConfigState();
        }
        /// <summary>
        /// Dùng cho việc kiểm tra 1 đối tượng.Trả về 4 trạng thái: IN, RETURN, OFF(Hủy/Tạm dừng), OUT
        /// </summary>
        /// <returns></returns> 
        public string GetState()
        {
            obj_GCS_SOGCS_XULY_Controller.CMIS2 = cmis2;
            return obj_GCS_SOGCS_XULY_Controller.GetState();
        }
        public DataSet GetStateMultiObject(DataSet Entity, string strGetState, ref string strError)
        {
            try
            {
                obj_GCS_SOGCS_XULY_Controller.CMIS2 = cmis2;
                if (Entity == null || Entity.Tables.Count == 0 || Entity.Tables["GCS_SOGCS_XULY"] == null || Entity.Tables["GCS_SOGCS_XULY"].Rows.Count == 0)
                    return null;
                else
                {
                    int i = Entity.Tables["GCS_SOGCS_XULY"].Rows.Count - 1;
                    while (i >= 0)
                    {
                        DataRow dr = Entity.Tables["GCS_SOGCS_XULY"].Rows[i];
                        GCS_SOGCS_XULY info = new GCS_SOGCS_XULY();
                        info = Utility.MapDatarowToObject<GCS_SOGCS_XULY>(dr, ref strError);
                        if (strError.Trim().Length > 0)
                        {
                            return null;
                        }
                        obj_GCS_SOGCS_XULY_Controller.pInfor = info;                        
                        obj_GCS_SOGCS_XULY_Controller.getCurrentState();
                        if (obj_GCS_SOGCS_XULY_Controller.dsConfigState == null || obj_GCS_SOGCS_XULY_Controller.dsConfigState.Tables.Count == 0)
                            obj_GCS_SOGCS_XULY_Controller.getConfigState();
                        if (obj_GCS_SOGCS_XULY_Controller.dsConfigState == null || obj_GCS_SOGCS_XULY_Controller.dsConfigState.Tables.Count == 0)
                            return null;
                        string strResult = obj_GCS_SOGCS_XULY_Controller.GetState();
                        if (strResult != strGetState)
                            Entity.Tables["GCS_SOGCS_XULY"].Rows.Remove(dr);
                        i--;
                    }
                    Entity.AcceptChanges();
                    return Entity;
                }
            }
            catch (Exception ex)
            {
                strError = "Lỗi tại busWF: " + ex.ToString();
                return null;
            }
        }
        public DataSet InsertList(DataSet Entity, string strState, ref string strError)
        {
            try
            {
                obj_GCS_SOGCS_XULY_Controller.CMIS2 = cmis2;
                if (Entity == null || Entity.Tables.Count == 0 || Entity.Tables["GCS_SOGCS_XULY"] == null || Entity.Tables["GCS_SOGCS_XULY"].Rows.Count == 0)
                    return null;
                else
                {
                    int i = Entity.Tables["GCS_SOGCS_XULY"].Rows.Count - 1;
                    while (i >= 0)
                    {
                        DataRow dr = Entity.Tables["GCS_SOGCS_XULY"].Rows[i];
                        GCS_SOGCS_XULY info = new GCS_SOGCS_XULY();
                        info = Utility.MapDatarowToObject<GCS_SOGCS_XULY>(dr, ref strError);
                        if (strError.Trim().Length > 0)
                        {
                            return Entity;
                        }
                        obj_GCS_SOGCS_XULY_Controller.pInfor = info;
                        obj_GCS_SOGCS_XULY_Controller.getCurrentState();
                        if (obj_GCS_SOGCS_XULY_Controller.dsConfigState == null || obj_GCS_SOGCS_XULY_Controller.dsConfigState.Tables.Count == 0)
                            obj_GCS_SOGCS_XULY_Controller.getConfigState();
                        if (obj_GCS_SOGCS_XULY_Controller.dsConfigState == null || obj_GCS_SOGCS_XULY_Controller.dsConfigState.Tables.Count == 0)
                            return Entity;
                        if (obj_GCS_SOGCS_XULY_Controller.insertOnSuccessOrDestroy(strState))
                            Entity.Tables["GCS_SOGCS_XULY"].Rows.Remove(dr);
                        i--;
                    }
                    Entity.AcceptChanges();
                    return Entity;
                }
            }
            catch (Exception ex)
            {
                strError = "Lỗi tại busWF: " + ex.ToString();
                return Entity;
            }
        }
        public DataSet DeleteList(DataSet Entity, string strState, ref string strError)
        {
            try
            {
                obj_GCS_SOGCS_XULY_Controller.CMIS2 = cmis2;
                if (Entity == null || Entity.Tables.Count == 0 || Entity.Tables["GCS_SOGCS_XULY"] == null || Entity.Tables["GCS_SOGCS_XULY"].Rows.Count == 0)
                    return null;
                else
                {
                    int i = Entity.Tables["GCS_SOGCS_XULY"].Rows.Count - 1;
                    while (i >= 0)
                    {
                        DataRow dr = Entity.Tables["GCS_SOGCS_XULY"].Rows[i];
                        GCS_SOGCS_XULY info = new GCS_SOGCS_XULY();
                        info = Utility.MapDatarowToObject<GCS_SOGCS_XULY>(dr, ref strError);
                        if (strError.Trim().Length > 0)
                        {
                            return Entity;
                        }
                        obj_GCS_SOGCS_XULY_Controller.pInfor = info;
                        obj_GCS_SOGCS_XULY_Controller.getCurrentState();
                        if (obj_GCS_SOGCS_XULY_Controller.dsConfigState == null || obj_GCS_SOGCS_XULY_Controller.dsConfigState.Tables.Count == 0)
                            obj_GCS_SOGCS_XULY_Controller.getConfigState();
                        if (obj_GCS_SOGCS_XULY_Controller.dsConfigState == null || obj_GCS_SOGCS_XULY_Controller.dsConfigState.Tables.Count == 0)
                            return Entity;
                        if (obj_GCS_SOGCS_XULY_Controller.deleteOnUndo())
                            Entity.Tables["GCS_SOGCS_XULY"].Rows.Remove(dr);
                        i--;
                    }
                    Entity.AcceptChanges();
                    return Entity;
                }
            }
            catch (Exception ex)
            {
                strError = "Lỗi tại busWF: " + ex.ToString();
                return Entity;
            }
        }
        #endregion
    }
}
