using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace LogManagementObject
{
    public class cls_S_LOG_Info
    {
        #region string ASSEMBLYNAME

        private string assemblyname;
        public string ASSEMBLYNAME
        {
            get
            {
                return assemblyname;
            }
            set
            {
                assemblyname = value;
            }
        }

        #endregion

        #region string BOOKID

        private string bookid;
        public string BOOKID
        {
            get
            {
                return bookid;
            }
            set
            {
                bookid = value;
            }
        }

        #endregion

        #region string DETAIL

        private string detail;
        public string DETAIL
        {
            get
            {
                return detail;
            }
            set
            {
                detail = value;
            }
        }

        #endregion

        #region long LOGID

        private long logid;
        public long LOGID
        {
            get
            {
                return logid;
            }
            set
            {
                logid = value;
            }
        }

        #endregion

        #region string METHODNAME

        private string methodname;
        public string METHODNAME
        {
            get
            {
                return methodname;
            }
            set
            {
                methodname = value;
            }
        }

        #endregion

        #region string NAMESPACE

        private string @namespace;
        public string NAMESPACE
        {
            get
            {
                return @namespace;
            }
            set
            {
                @namespace = value;
            }
        }

        #endregion

        #region string SUBDIVISIONID

        private string subdivisionid;
        public string SUBDIVISIONID
        {
            get
            {
                return subdivisionid;
            }
            set
            {
                subdivisionid = value;
            }
        }

        #endregion

        #region System.DateTime TIME

        private System.DateTime time;
        public System.DateTime TIME
        {
            get
            {
                return time;
            }
            set
            {
                time = value;
            }
        }

        #endregion

    }
}
