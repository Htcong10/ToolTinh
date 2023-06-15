using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.Reflection;

namespace BillingLibrary
{
    public class BillingLibrary
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
        /// <summary>
        /// Hiển thị hộp thoại
        /// </summary>
        /// <param name="Message">Nội dung</param>
        /// <param name="Title">Tiêu đề</param>
        public static void ShowMsg(string Message, string Title)
        {
            System.Windows.Forms.MessageBox.Show(Message, Title, System.Windows.Forms.MessageBoxButtons.OK, System.Windows.Forms.MessageBoxIcon.Information, System.Windows.Forms.MessageBoxDefaultButton.Button1, System.Windows.Forms.MessageBoxOptions.ServiceNotification);
        }
        public static T MapDatarowToObject<T>(DataRow dr, ref string strError)
        {
            try
            {
                T instance = Activator.CreateInstance<T>();
                PropertyInfo[] properties = instance.GetType().GetProperties();
                if ((properties.Length > 0))
                {
                    foreach (PropertyInfo propertyObject in properties)
                    {
                        bool valueSet = false;
                        foreach (object attributeObject in propertyObject.GetCustomAttributes(false))
                        {
                            if (object.ReferenceEquals(attributeObject.GetType(), typeof(MapperColumn)))
                            {
                                MapperColumn columnAttributeObject = (MapperColumn)attributeObject;

                                if ((columnAttributeObject.ColumnName != string.Empty))
                                {
                                    if (dr.Table.Columns.Contains(columnAttributeObject.ColumnName) && (!object.ReferenceEquals(dr[columnAttributeObject.ColumnName], DBNull.Value)))
                                    {
                                        propertyObject.SetValue(instance, dr[columnAttributeObject.ColumnName], null);
                                        valueSet = true;
                                    }
                                }
                            }
                        }
                        if (dr.Table.Columns.Contains(propertyObject.Name) && (!object.ReferenceEquals(dr[propertyObject.Name], DBNull.Value)))
                        {
                            try
                            {
                                propertyObject.SetValue(instance, dr[propertyObject.Name], null);
                            }
                            catch
                            {
                            }
                        }
                    }
                }
                return instance;
            }
            catch (Exception ex)
            {
                strError = "Lỗi khi gán dữ liệu cho Object: " + ex.Message;
                return Activator.CreateInstance<T>();
            }
        }
        private static bool IsNullableType(Type type)
        {
            return type.IsGenericType && type.GetGenericTypeDefinition().Equals(typeof(Nullable<>));
        }
        public static T MapDatarowToObjectCDPlus<T>(DataRow dr, ref string strError)
        {
            try
            {
                T instance = Activator.CreateInstance<T>();
                PropertyInfo[] properties = instance.GetType().GetProperties();
                if ((properties.Length > 0))
                {
                    foreach (PropertyInfo propertyObject in properties)
                    {
                        bool valueSet = false;
                        foreach (object attributeObject in propertyObject.GetCustomAttributes(false))
                        {
                            if (object.ReferenceEquals(attributeObject.GetType(), typeof(MapperColumn)))
                            {
                                MapperColumn columnAttributeObject = (MapperColumn)attributeObject;

                                if ((columnAttributeObject.ColumnName != string.Empty))
                                {
                                    if (dr.Table.Columns.Contains(columnAttributeObject.ColumnName) && (!object.ReferenceEquals(dr[columnAttributeObject.ColumnName], DBNull.Value)))
                                    {
                                        if (dr.Table.Columns[propertyObject.Name].DataType == typeof(DateTime))
                                            propertyObject.SetValue(instance, Convert.ToDateTime(dr[propertyObject.Name]).ToString("dd/MM/yyyy"), null);
                                        else
                                            propertyObject.SetValue(instance, dr[propertyObject.Name], null);
                                        valueSet = true;
                                    }
                                }
                            }
                        }
                        if (dr.Table.Columns.Contains(propertyObject.Name) && (!object.ReferenceEquals(dr[propertyObject.Name], DBNull.Value)))
                        {
                            try
                            {
                                if (dr.Table.Columns[propertyObject.Name].DataType == typeof(DateTime))
                                    propertyObject.SetValue(instance, Convert.ToDateTime(dr[propertyObject.Name]).ToString("dd/MM/yyyy"), null);
                                else
                                // propertyObject.SetValue(instance, dr[propertyObject.Name], null);

                                {
                                    Type propertyType = propertyObject.PropertyType;

                                    //Convert.ChangeType does not handle conversion to nullable types
                                    //if the property type is nullable, we need to get the underlying type of the property
                                    var targetType = IsNullableType(propertyType) ? Nullable.GetUnderlyingType(propertyType) : propertyType;
                                    object propertyVal = dr[propertyObject.Name];
                                    //Returns an System.Object with the specified System.Type and whose value is
                                    //equivalent to the specified object.
                                    propertyVal = Convert.ChangeType(propertyVal, targetType);

                                    //Set the value of the property
                                    propertyObject.SetValue(instance, propertyVal, null);
                                }


                            }
                            catch (Exception ex)
                            {
                            }
                        }
                    }
                }
                return instance;
            }
            catch (Exception ex)
            {
                strError = "Lỗi khi gán dữ liệu cho Object: " + ex.Message;
                return Activator.CreateInstance<T>();
            }
        }
        [AttributeUsage(AttributeTargets.Property)]
        public class MapperColumn : Attribute
        {
            private string mColumnName;
            public MapperColumn(string columnName)
            {
                mColumnName = columnName;
            }

            public string ColumnName
            {
                get { return mColumnName; }
                set { mColumnName = value; }
            }
        }
        public static List<T> DataTableToList<T>(DataTable table)
        {
            try
            {
                List<T> list = new List<T>();
                foreach (DataRow row in table.Rows)
                {
                    //T obj = new T();
                    T obj = Activator.CreateInstance<T>();
                    PropertyInfo[] properties = obj.GetType().GetProperties();

                    foreach (PropertyInfo prop in properties)
                    {

                        if (!object.ReferenceEquals(row[prop.Name], DBNull.Value))
                        {
                            prop.SetValue(obj, Convert.ChangeType(row[prop.Name], prop.PropertyType), null);
                            //valueSet = true;
                        }
                    }

                    list.Add(obj);
                }

                return list;
            }
            catch
            {
                return null;
            }
        }

    }
}
