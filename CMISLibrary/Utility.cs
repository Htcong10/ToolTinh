using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.Reflection;
using Microsoft.VisualBasic;

namespace CMISLibrary
{
    public class Utility
    {        
        /// <summary>
        /// Chuyển dữ liệu từ IEnumerable vào DataTable
        /// </summary>
        /// <typeparam name="T">Kiểu đối tượng</typeparam>
        /// <param name="varlist">IEnumerable</param>
        /// <returns>Table chứa dữ liệu</returns>
        public static DataTable LINQToDataTable<T>(IEnumerable<T> varlist)
        {
            try
            {
                DataTable dtReturn = new DataTable();
                // column names
                PropertyInfo[] oProps = null;
                if (varlist == null) return dtReturn;
                foreach (T rec in varlist)
                {
                    // Use reflection to get property names, to create table, Only first time, others
                    //will follow
                    if (oProps == null)
                    {
                        oProps = ((Type)rec.GetType()).GetProperties();
                        foreach (PropertyInfo pi in oProps)
                        {
                            Type colType = pi.PropertyType;
                            if ((colType.IsGenericType) && (colType.GetGenericTypeDefinition() == typeof(Nullable<>)))
                            {
                                colType = colType.GetGenericArguments()[0];
                            }
                            if (!dtReturn.Columns.Contains(pi.Name.ToUpper()))
                                dtReturn.Columns.Add(new DataColumn(pi.Name.ToUpper(), colType));
                        }
                    }
                    DataRow dr = dtReturn.NewRow();
                    foreach (PropertyInfo pi in oProps)
                    {
                        dr[pi.Name.ToUpper()] = pi.GetValue(rec, null) == null ? DBNull.Value : pi.GetValue
                        (rec, null);
                    }

                    dtReturn.Rows.Add(dr);
                }
                return dtReturn;
            }
            catch
            {
                return null;
            }
        }
    }
}
