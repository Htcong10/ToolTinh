
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using DbConnect.DB;
using System.Data;

public class cls_D_TY_GIA_Controller
{
	#region   Atributes

	private DataSet ds = new DataSet();
	private DataTable dt = new DataTable();
	private D_TY_GIA info;

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
	#endregion

	#region   Constructor

	public cls_D_TY_GIA_Controller()
	{
		info = new D_TY_GIA();
	}
	public cls_D_TY_GIA_Controller(D_TY_GIA Info)
	{
		this.info = Info;
	}

	#endregion

	#region   Properties

	public D_TY_GIA pInfor
	{
		get { return info; }
		set { info = value; }
	}

	#endregion

	#region Method
    public DataTable getD_TY_GIA()
    {
        try
        {
            var q = from a in this.CMIS2.DB.D_TY_GIA
                    select new
                    {
                        a.ID_TY_GIA,
                        a.LOAI_TIEN,
                        a.MA_CNANG,
                        a.MA_DVIQLY,
                        a.NAM,
                        a.NGAY_NHAP,
                        a.NGUOI_NHAP,
                        a.THANG,
                        a.TYGIA_QDOI,
                    };
            if (q != null && q.Take(1).Count() > 0)
            {
                dt = BillingLibrary.BillingLibrary.LINQToDataTable(q);
                dt.TableName = "D_TY_GIA";
                dt.AcceptChanges();
                return dt;
            }
            return null;
        }
        catch
        {
            return null;
        }
    }
    #endregion

}
