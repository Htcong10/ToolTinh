using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using QTObject.DB;
using CMISLibrary.Entities;
using CMISLibrary;

namespace QTObject
{
    public class cls_S_PARAMETER_Controller
    {
        #region   Atributes

        private DataSet CMIS_Header = new CMISOutputParameter();
        private DataSet ds = new DataSet();
        private DataTable dt = new DataTable();
        private cls_S_PARAMETER_Info info;

        #endregion

        #region   Constructor

        public cls_S_PARAMETER_Controller()
        {
            info = new cls_S_PARAMETER_Info();
        }
        public cls_S_PARAMETER_Controller(cls_S_PARAMETER_Info Info)
        {
            this.info = Info;
        }

        #endregion

        #region   Properties

        public cls_S_PARAMETER_Info pInfor
        {
            get { return info; }
            set { info = value; }
        }

        #endregion

        #region   Methods

        public IEnumerable<S_PARAMETER> select_S_PARAMETER()
        {
            try
            {
                var q = from p in CMIS2.DB.S_PARAMETER select p;
                return q;
            }
            catch
            {
                CMIS2.ResetDB(); return null;
            }
        }
        public IEnumerable<S_PARAMETER> select_S_PARAMETER(string strMadviqly)
        {
            try
            {
                var q = from p in CMIS2.DB.S_PARAMETER where p.SUBDIVISIONID == strMadviqly select p;
                return q;
            }
            catch
            {
                CMIS2.ResetDB(); return null;
            }
        }
        public int GetMaxSequenceByBranch()
        {
            try
            {
                var query = from p in CMIS2.DB.S_PARAMETER where p.SUBDIVISIONID == info.SUBDIVISIONID select p.PARAMETERID;
                if (query.ToList().Count > 0) return Convert.ToInt32(query.Max()) + 1;
                else return 1;
            }
            catch
            {
                CMIS2.ResetDB(); return -1;
            }
        }
        public bool insert_S_PARAMETER()
        {
            int _ID = GetMaxSequenceByBranch();
            if (_ID == -1) return false;
            try
            {
                S_PARAMETER objS_PARAMETER = new S_PARAMETER()
                {
                    CREATEDBY = info.CREATEDBY,
                    CREATEDDATE = info.CREATEDDATE,
                    DATATYPE = info.DATATYPE,
                    DESCRIPTION = info.DESCRIPTION,
                    NAME = info.NAME,
                    PARAMETERID = _ID,
                    PARAMETERTYPE = info.PARAMETERTYPE,
                    STARTDATE = info.STARTDATE,
                    STATE = info.STATE,
                    SUBDIVISIONID = info.SUBDIVISIONID,
                    PRAVALUE = info.PRAVALUE
                };
                CMIS2.DB.S_PARAMETER.InsertOnSubmit(objS_PARAMETER);
                CMIS2.DB.SubmitChanges();
                CMIS2.ResetDB();
                return true;
            }
            catch (Exception ex)
            {
                CMIS2.ResetDB(); return false;
                Utility.ShowMsg(ex.ToString());
            }
        }

        public IEnumerable<S_PARAMETER> selectWithPK_S_PARAMETER()
        {
            try
            {
                var q = from p in CMIS2.DB.S_PARAMETER where p.SUBDIVISIONID == info.SUBDIVISIONID select p;
                return q;
            }
            catch
            {
                CMIS2.ResetDB(); return null;
            }
        }
        public IEnumerable<S_PARAMETER> select_S_PARAMETER_ByPK(System.String pr_SUBDIVISIONID)
        {
            try
            {
                var q = from p in CMIS2.DB.S_PARAMETER where p.SUBDIVISIONID == pr_SUBDIVISIONID select p;
                return q;
            }
            catch
            {
                CMIS2.ResetDB(); return null;
            }
        }
        public bool update_S_PARAMETER()
        {
            try
            {
                S_PARAMETER objS_PARAMETER = CMIS2.DB.S_PARAMETER.Single(c => c.SUBDIVISIONID == info.SUBDIVISIONID && c.PARAMETERID == info.PARAMETERID);
                objS_PARAMETER.CREATEDBY = info.CREATEDBY;
                objS_PARAMETER.CREATEDDATE = info.CREATEDDATE;
                objS_PARAMETER.DATATYPE = info.DATATYPE;
                objS_PARAMETER.DESCRIPTION = info.DESCRIPTION;
                objS_PARAMETER.NAME = info.NAME;
                objS_PARAMETER.PARAMETERTYPE = info.PARAMETERTYPE;
                objS_PARAMETER.STARTDATE = info.STARTDATE;
                objS_PARAMETER.STATE = info.STATE;
                objS_PARAMETER.PRAVALUE = info.PRAVALUE;
                CMIS2.DB.SubmitChanges();
                return true;
            }
            catch
            {
                CMIS2.ResetDB(); return false;
            }
        }
        public bool delete_S_PARAMETER()
        {
            try
            {
                var query = from p in CMIS2.DB.S_PARAMETER where p.SUBDIVISIONID == info.SUBDIVISIONID && p.PARAMETERID == info.PARAMETERID select p;
                if (query != null && query.Count() > 0)
                {
                    CMIS2.DB.S_PARAMETER.DeleteAllOnSubmit(query.ToList());
                    CMIS2.DB.SubmitChanges();
                    return true;
                }
                return true;
            }
            catch
            {
                CMIS2.ResetDB(); return false;
            }
        }
        public bool delete_S_PARAMETER_ByPK(System.String pr_SUBDIVISIONID)
        {
            try
            {
                var query = from p in CMIS2.DB.S_PARAMETER where p.SUBDIVISIONID == pr_SUBDIVISIONID select p;
                if (query != null && query.Count() > 0)
                {
                    CMIS2.DB.S_PARAMETER.DeleteAllOnSubmit(query.ToList());
                    CMIS2.DB.SubmitChanges();
                    return true;
                }
                return true;
            }
            catch
            {
                CMIS2.ResetDB(); return false;
            }
        }
        public DataSet getAllParameter()
        {
            DataSet ds = new DataSet();
            try
            {
                var q = from p in CMIS2.DB.S_PARAMETER
                        where p.SUBDIVISIONID == info.SUBDIVISIONID
                        select new
                        {
                            p.SUBDIVISIONID,
                            p.STARTDATE,
                            p.NAME,
                            p.PRAVALUE,
                            p.STATE,
                            p.PARAMETERID,
                            p.PARAMETERTYPE,
                            p.DATATYPE,
                            p.DESCRIPTION,
                        };
                if (q == null || q.Count() == 0) return null;
                //var kq = q.ToList();
                ////var w = kq.Where(c => c.STARTDATE == (kq.Max(b => b.STARTDATE)));
                ////var w = kq.GroupBy(p => new { p.SUBDIVISIONID, p.NAME }).Select(k => new { NAME = k.Key.NAME, 
                ////                                                                            PARAMETERID = k.Key., 
                ////                                                                            STARTDATE = k.Max(m => m.STARTDATE) });
                //var v = kq.GroupBy(p => new { p.SUBDIVISIONID, p.NAME }).Select(k => new
                //{
                //    SUBDIVISIONID = k.Key.SUBDIVISIONID,
                //    NAME = k.Key.NAME,
                //    STARTDATE = k.Max(m => m.STARTDATE),
                //});
                //var w = (from p in kq
                //         join m in v on new { p.SUBDIVISIONID, p.NAME, p.STARTDATE } equals new { m.SUBDIVISIONID, m.NAME, m.STARTDATE }
                //         select new
                //         {
                //             p.SUBDIVISIONID,
                //             STARTDATE=p.STARTDATE.ToShortDateString(),
                //             p.NAME,
                //             p.PRAVALUE,
                //             p.STATE,
                //             p.PARAMETERID,
                //             p.PARAMETERTYPE,
                //             p.DATATYPE,
                //             p.DESCRIPTION,
                //         }
                //              ).ToList();

                DataTable dt = Utility.LINQToDataTable(q);
                dt.TableName = "S_PARAMETER";
                if (dt != null)
                {
                    dt.AcceptChanges();
                    ds.Tables.Add(dt);
                    return ds;
                }
                else
                    return null;

            }
            catch (Exception ex)
            {
                CMIS2.ResetDB();
                Utility.ShowMsg(ex.ToString());
                return null;
            }
        }
        #endregion
        public DataSet GetParaByName()
        {
            DataSet ds = new DataSet();
            try
            {
                var q = from p in CMIS2.DB.S_PARAMETER
                        where p.SUBDIVISIONID == info.SUBDIVISIONID && p.NAME.Contains(info.NAME)
                        select new
                        {
                            p.SUBDIVISIONID,
                            p.STARTDATE,
                            p.NAME,
                            p.PRAVALUE,
                            p.STATE,
                            p.PARAMETERID,
                            p.PARAMETERTYPE,
                            p.DATATYPE,
                            p.DESCRIPTION,
                        };

                if (q == null || q.Count() == 0) return null;
                var kq = q.ToList();
                //var w = kq.Where(c => c.STARTDATE == (kq.Max(b => b.STARTDATE)));
                var v = kq.GroupBy(p => new { p.SUBDIVISIONID, p.NAME }).Select(k => new
                {
                    SUBDIVISIONID = k.Key.SUBDIVISIONID,
                    NAME = k.Key.NAME,
                    STARTDATE = k.Max(m => m.STARTDATE),
                });
                var w = (from p in kq
                         join m in v on new { p.SUBDIVISIONID, p.NAME, p.STARTDATE } equals new { m.SUBDIVISIONID, m.NAME, m.STARTDATE }
                         select new
                         {
                             p.SUBDIVISIONID,
                             STARTDATE = p.STARTDATE.ToShortDateString(),
                             p.NAME,
                             p.PRAVALUE,
                             p.STATE,
                             p.PARAMETERID,
                             p.PARAMETERTYPE,
                             p.DATATYPE,
                             p.DESCRIPTION,
                         }
                              ).ToList();

                DataTable dt = Utility.LINQToDataTable(w);
                dt.TableName = "S_PARAMETER";
                if (dt != null)
                {
                    dt.AcceptChanges();
                    ds.Tables.Add(dt);
                    return ds;
                }
                else
                    return null;
            }
            catch (Exception ex)
            {
                CMIS2.ResetDB();
                Utility.ShowMsg(ex.ToString());
                return null;
            }
        }
        public DataSet GetParaByTenPHe()
        {
            DataSet ds = new DataSet();
            try
            {
                var q = from p in CMIS2.DB.S_PARAMETER
                        where p.SUBDIVISIONID == info.SUBDIVISIONID && p.PARAMETERTYPE.Contains(info.PARAMETERTYPE)
                        select new
                        {
                            p.SUBDIVISIONID,
                            p.STARTDATE,
                            p.NAME,
                            p.PRAVALUE,
                            p.STATE,
                            p.PARAMETERID,
                            p.PARAMETERTYPE,
                            p.DATATYPE,
                            p.DESCRIPTION,
                        };

                if (q == null || q.Count() == 0) return null;
                var kq = q.ToList();
                //var w = kq.Where(c => c.STARTDATE == (kq.Max(b => b.STARTDATE)));
                var v = kq.GroupBy(p => new { p.SUBDIVISIONID, p.NAME }).Select(k => new
                {
                    SUBDIVISIONID = k.Key.SUBDIVISIONID,
                    NAME = k.Key.NAME,
                    STARTDATE = k.Max(m => m.STARTDATE),
                });
                var w = (from p in kq
                         join m in v on new { p.SUBDIVISIONID, p.NAME, p.STARTDATE } equals new { m.SUBDIVISIONID, m.NAME, m.STARTDATE }
                         select new
                         {
                             p.SUBDIVISIONID,
                             STARTDATE = p.STARTDATE.ToShortDateString(),
                             p.NAME,
                             p.PRAVALUE,
                             p.STATE,
                             p.PARAMETERID,
                             p.PARAMETERTYPE,
                             p.DATATYPE,
                             p.DESCRIPTION,
                         }
                              ).ToList();

                DataTable dt = Utility.LINQToDataTable(w);
                dt.TableName = "S_PARAMETER";
                if (dt != null)
                {
                    dt.AcceptChanges();
                    ds.Tables.Add(dt);
                    return ds;
                }
                else
                    return null;
            }
            catch (Exception ex)
            {
                CMIS2.ResetDB();
                Utility.ShowMsg(ex.ToString());
                return null;
            }
        }
    }
}
