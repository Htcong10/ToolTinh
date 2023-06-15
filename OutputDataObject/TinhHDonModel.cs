using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace OutputDataObject
{
    public class TinhHDonModel
    {
    }
    public class inpDLieuHDon
    {
        public inpDLieuHDon()
        {
            LST_HDON = new List<HDN_HDON_PLUS>();
            LST_HDCT = new List<HDN_HDONCTIET_PLUS>();
            LST_HDCF = new List<HDN_HDONCOSFI_PLUS>();
            LST_HDON_BT = new List<HDN_HDON_PLUS>();
            LST_HDCT_BT = new List<HDN_HDONCTIET_PLUS>();
            LST_HDCF_BT = new List<HDN_HDONCOSFI_PLUS>();           
            LST_CSO = new List<GCS_CHISO_PLUS>();
            LST_HDN_THOP_GTRU = new List<HDN_THOP_GTRU>();
        }
        public List<HDN_HDON_PLUS> LST_HDON { get; set; }
        public List<HDN_HDONCTIET_PLUS> LST_HDCT { get; set; }
        public List<HDN_HDONCOSFI_PLUS> LST_HDCF { get; set; }
        public List<HDN_HDON_PLUS> LST_HDON_BT { get; set; }
        public List<HDN_HDONCTIET_PLUS> LST_HDCT_BT { get; set; }
        public List<HDN_HDONCOSFI_PLUS> LST_HDCF_BT { get; set; }
        public List<GCS_CHISO_PLUS> LST_CSO { get; set; }

        public List<HDN_THOP_GTRU> LST_HDN_THOP_GTRU { get; set; }
    }
    public class inpDLieuHDonPerformance
    {
        public inpDLieuHDonPerformance()
        {
            LST_HDON = new List<HDN_HDON_PLUS>();            
            LST_HDON_BT = new List<HDN_HDON_PLUS>();            
            LST_CSO = new List<GCS_CHISO_PLUS>();            
        }
        public List<HDN_HDON_PLUS> LST_HDON { get; set; }
        
        public List<HDN_HDON_PLUS> LST_HDON_BT { get; set; }
       
        public List<GCS_CHISO_PLUS> LST_CSO { get; set; }

    }
    public class GCS_CHISO_PLUS
    {
        public string BCS { get; set; }
        public decimal CHISO_CU { get; set; }
        public decimal CHISO_MOI { get; set; }
        public decimal HS_NHAN { get; set; }
        public decimal ID_BCS { get; set; }
        public decimal ID_CHISO { get; set; }
        public decimal KY { get; set; }
        public string LOAI_CHISO { get; set; }
        public string MA_CTO { get; set; }
        public string MA_DDO { get; set; }
        public string MA_DVIQLY { get; set; }
        public string MA_KHANG { get; set; }
        public string MA_TTCTO { get; set; }
        public decimal NAM { get; set; }
        public DateTime NGAY_CKY { get; set; }
        public DateTime NGAY_DKY { get; set; }
        public decimal SAN_LUONG { get; set; }
        public decimal? SLUONG_1 { get; set; }
        public decimal? SLUONG_2 { get; set; }
        public decimal? SLUONG_3 { get; set; }
        public decimal SLUONG_TRPHU { get; set; }
        public decimal SLUONG_TTIEP { get; set; }
        public string SO_CTO { get; set; }
        public decimal THANG { get; set; }
        public decimal SLUONG_TRPHU_COSFI { get; set; }

    }
    public class HDN_HDON_PLUS
    {
        public short? CHI_TIET { get; set; }
        public decimal? COSFI { get; set; }
        public string DCHI_KHANG { get; set; }
        public string DCHI_KHANGTT { get; set; }
        public string DCHI_TTOAN { get; set; }
        public decimal DIEN_TTHU { get; set; }
        public long ID_BBANPHANH { get; set; }
        public long ID_HDON { get; set; }
        public decimal? KCOSFI { get; set; }
        public string KIHIEU_SERY { get; set; }
        public int KY { get; set; }
        public string LOAI_HDON { get; set; }
        public int LOAI_KHANG { get; set; }
        public string MA_CNANG { get; set; }
        public string MA_DVIQLY { get; set; }
        public string MA_HTTT { get; set; }
        public string MA_KHANG { get; set; }
        public string MA_KHANGTT { get; set; }
        public string MA_KVUC { get; set; }
        public string MA_LOAIDN { get; set; }
        public string MA_NHANG { get; set; }
        public string MA_NVIN { get; set; }
        public string MA_NVPHANH { get; set; }
        public string MA_PTTT { get; set; }
        public string MA_SOGCS { get; set; }
        public string MA_TO { get; set; }
        public string MANHOM_KHANG { get; set; }
        public string MASO_THUE { get; set; }
        public int NAM { get; set; }
        public string NGAY_CKY { get; set; }
        public string NGAY_DKY { get; set; }
        public string NGAY_IN { get; set; }
        public string NGAY_PHANH { get; set; }
        public DateTime NGAY_SUA { get; set; }
        public DateTime NGAY_TAO { get; set; }
        public string NGUOI_SUA { get; set; }
        public string NGUOI_TAO { get; set; }
        public string SO_CTO { get; set; }
        public decimal SO_HO { get; set; }
        public int SO_LANIN { get; set; }
        public string SO_SERY { get; set; }
        public decimal SO_TIEN { get; set; }

        public decimal TIEN_GTRU { get; set; }
        public decimal TIEN_GOC { get; set; }
        public string STT { get; set; }
        public int STT_IN { get; set; }
        public string STT_TRANG { get; set; }
        public string TEN_KHANG { get; set; }
        public string TEN_KHANGTT { get; set; }
        public int THANG { get; set; }
        public decimal TIEN_GTGT { get; set; }
        public string TKHOAN_KHANG { get; set; }
        public decimal TONG_TIEN { get; set; }
        public decimal TYLE_THUE { get; set; }
        public decimal TIEN_TD { get; set; }
        public decimal THUE_TD { get; set; }
        public decimal TIEN_VC { get; set; }
        public decimal THUE_VC { get; set; }

        public decimal TIEN_TD_GTRU { get; set; }
        public decimal TIEN_TD_GOC { get; set; }
        public decimal TIEN_VC_GTRU { get; set; }
        public decimal TIEN_VC_GOC { get; set; }

        public string DTHOAI { get; set; }
        public List<HDN_HDONCTIET_PLUS> LST_HDCT { get; set; }
        public List<HDN_HDONCOSFI_PLUS> LST_HDCF { get; set; }
        
    }


    public class HDN_THOP_GTRU
    {
        public string MA_DVIQLY { get; set; }
        public string MA_SOGCS { get; set; }
        public long ID_HDON { get; set; }
        public int KY { get; set; }
        public int THANG { get; set; }
        public int NAM { get; set; }     
        public string MA_KHANG { get; set; }
        public string LOAI_HDON { get; set; }
        public decimal DNANG_GTRU { get; set; }
        public decimal TIEN_GTRU { get; set; }
        public decimal THUE_GTRU { get; set; }
        public decimal TONG_GTRU { get; set; }
        public string GHI_CHU { get; set; }
        public DateTime NGAY_SUA { get; set; }
        public DateTime NGAY_TAO { get; set; }
        public string NGUOI_SUA { get; set; }
        public string NGUOI_TAO { get; set; }
        public string MA_CNANG { get; set; }
    }
    public class HDN_HDONCTIET_PLUS
    {
        public string BCS { get; set; }
        public decimal DIEN_TTHU { get; set; }
        public string DINH_MUC { get; set; }
        public decimal DON_GIA { get; set; }
        public long ID_CHISO { get; set; }
        public long ID_HDON { get; set; }
        public long ID_HDONCTIET { get; set; }
        public int KY { get; set; }
        public int LOAI_DDO { get; set; }
        public string LOAI_DMUC { get; set; }
        public int LOAI_KHANG { get; set; }
        public string MA_CAPDA { get; set; }
        public string MA_CNANG { get; set; }
        public string MA_DDO { get; set; }
        public string MA_DVIQLY { get; set; }
        public string MA_KHANG { get; set; }
        public string MA_KVUC { get; set; }
        public string MA_LO { get; set; }
        public string MA_NGIA { get; set; }
        public string MA_NHOMNN { get; set; }
        public string MA_NN { get; set; }
        public string MA_SOGCS { get; set; }
        public string MA_TO { get; set; }
        public string MA_TRAM { get; set; }
        public int NAM { get; set; }
        public string NGAY_APDUNG { get; set; }
        public DateTime NGAY_SUA { get; set; }
        public DateTime NGAY_TAO { get; set; }
        public string NGUOI_SUA { get; set; }
        public string NGUOI_TAO { get; set; }
        public string SO_CTO { get; set; }
        public int SO_PHA { get; set; }
        public decimal SO_TIEN { get; set; }
        public string STT { get; set; }
        public string TGIAN_BDIEN { get; set; }
        public int THANG { get; set; }
        public decimal TY_LE { get; set; }
        public decimal TIEN_GTRU { get; set; }
        public decimal TIEN_GOC { get; set; }
        public string NOI_DUNG { get; set; }
    }
    public class HDN_HDONCOSFI_PLUS
    {
        public decimal COSFI { get; set; }
        public decimal HUU_CONG { get; set; }
        public long ID_HDON { get; set; }
        public long ID_HDONCOSFI { get; set; }
        public decimal KCOSFI { get; set; }
        public int KIMUA_CSPK { get; set; }
        public int KY { get; set; }
        public string MA_CNANG { get; set; }
        public string MA_DDO { get; set; }
        public string MA_DVIQLY { get; set; }
        public string MA_KHANG { get; set; }
        public string MA_KVUC { get; set; }
        public string MA_LO { get; set; }
        public string MA_SOGCS { get; set; }
        public string MA_TO { get; set; }
        public string MA_TRAM { get; set; }
        public int NAM { get; set; }
        public DateTime NGAY_SUA { get; set; }
        public DateTime NGAY_TAO { get; set; }
        public string NGUOI_SUA { get; set; }
        public string NGUOI_TAO { get; set; }
        public string STT { get; set; }
        public int THANG { get; set; }
        public decimal TIEN_HUUCONG { get; set; }
        public decimal TIEN_VOCONG { get; set; }
        public decimal VO_CONG { get; set; }
        public long ID_CHISO { get; set; }
        public decimal TIEN_HC_GTRU { get; set; }
        public decimal TIEN_HC_GOC { get; set; }
        public decimal TIEN_VC_GTRU { get; set; }
        public decimal TIEN_VC_GOC { get; set; }
        public string NOI_DUNG { get; set; }
    }

    public class inpListLapDC
    {
        public List<inpLapDC> LST_OBJ { get; set; }
        public List<LST_TIEN_TRINH> LST_TIEN_TRINH { get; set; }
        public List<LST_PAN_PHAT> LST_PAN_PHAT { get; set; }
        public List<LST_KHANG_DDO> LST_KHANG_DDO { get; set; }
        public List<LST_BBAN_PLUC> LST_BBAN_PLUC { get; set; }
    }

    public class inpLapDC
    {
        public HDN_HDON_DC_PLUS HDN_HDON_DC { get; set; }
        public List<HDN_HDONCTIET_DC_PLUS> HDN_HDONCTIET_DC { get; set; }
        public List<HDN_HDONCOSFI_DC_PLUS> HDN_HDONCOSFI_DC { get; set; }
        public HDN_KHANG_DC_PLUS HDN_KHANG_DC { get; set; }
        public List<HDN_DIEMDO_DC_PLUS> HDN_DIEMDO_DC { get; set; }
        public List<HDN_BBAN_APGIA_DC_PLUS> HDN_BBAN_APGIA_DC { get; set; }
        public HDN_BBAN_DCHINH_PLUS HDN_BBAN_DCHINH { get; set; }
        public List<HDN_CHISO_DC_PLUS> HDN_CHISO_DC { get; set; }
        public List<GCS_CHISO_DUP1> GCS_CHISO { get; set; }

    }
    public class inpDLieuHDonDC
    {
        public List<HDN_HDON_DC_PLUS> HDN_HDON_DC { get; set; }
        public List<HDN_HDONCTIET_DC_PLUS> HDN_HDONCTIET_DC { get; set; }
        public List<HDN_HDONCOSFI_DC_PLUS> HDN_HDONCOSFI_DC { get; set; }
        public List<HDN_KHANG_DC_PLUS> HDN_KHANG_DC { get; set; }
        public List<HDN_DIEMDO_DC_PLUS> HDN_DIEMDO_DC { get; set; }
        public List<HDN_BBAN_APGIA_DC_PLUS> HDN_BBAN_APGIA_DC { get; set; }
        public List<HDN_BBAN_DCHINH_PLUS> HDN_BBAN_DCHINH { get; set; }
        public List<HDN_CHISO_DC_PLUS> HDN_CHISO_DC { get; set; }
        public List<GCS_CHISO_DUP1> GCS_CHISO { get; set; }
        public List<LST_TIEN_TRINH> LST_TIEN_TRINH { get; set; }
        public List<LST_PAN_PHAT> LST_PAN_PHAT { get; set; }
        public List<LST_KHANG_DDO> LST_KHANG_DDO { get; set; }
        public List<LST_BBAN_PLUC> LST_BBAN_PLUC { get; set; }

    }
    public class HDN_HDON_DC_PLUS
    {
        #region Instance Properties

        public short? CHI_TIET { get; set; }
        public decimal? COSFI { get; set; }
        public short DA_CAPNHAT { get; set; }
        public string DCHI_KHANG { get; set; }
        public string DCHI_KHANGTT { get; set; }
        public decimal DIEN_TTHU { get; set; }
        public string DTHOAI { get; set; }
        public long? ID_BBANPHANH { get; set; }
        public long ID_HDON { get; set; }
        public long ID_HDON_DC { get; set; }
        public decimal? KCOSFI { get; set; }
        public string KIEU_PSINH { get; set; }
        public string KIHIEU_SERY { get; set; }
        public short KY { get; set; }
        public string LOAI_DCHINH { get; set; }
        public string LOAI_HDON { get; set; }
        public short LOAI_KHANG { get; set; }
        public string MANHOM_KHANG { get; set; }
        public string MASO_THUE { get; set; }
        public string MA_CNANG { get; set; }
        public string MA_DVICTREN { get; set; }
        public string MA_DVIQLY { get; set; }
        public string MA_HTTT { get; set; }
        public string MA_KHANG { get; set; }
        public string MA_KHANGTT { get; set; }
        public string MA_KVUC { get; set; }
        public string MA_NHANG { get; set; }
        public string MA_NVPHANH { get; set; }
        public string MA_PTTT { get; set; }
        public string MA_SOGCS { get; set; }
        public string MA_TO { get; set; }
        public short NAM { get; set; }
        public string NGAY_CKY { get; set; }
        public string NGAY_DKY { get; set; }
        public string NGAY_PHANH { get; set; }
        public string NGAY_SUA { get; set; }
        public string NGAY_TAO { get; set; }
        public string NGUOI_SUA { get; set; }
        public string NGUOI_TAO { get; set; }
        public short QUY { get; set; }
        public string SO_CTO { get; set; }
        public decimal SO_HO { get; set; }
        public string SO_SERY { get; set; }
        public decimal SO_TIEN { get; set; }
        public string STT { get; set; }
        public string TEN_KHANG { get; set; }
        public string TEN_KHANGTT { get; set; }
        public short THANG { get; set; }
        public decimal? THUE_TD { get; set; }
        public decimal? THUE_VC { get; set; }
        public decimal TIEN_GTGT { get; set; }
        public decimal? TIEN_TD { get; set; }
        public decimal? TIEN_VC { get; set; }
        public string TKHOAN_KHANG { get; set; }
        public decimal TONG_TIEN { get; set; }
        public decimal TYLE_THUE { get; set; }
        public string LOAI_TTHUAN { get; set; }
        public string MA_YCAU_KNAI { get; set; }
        #endregion Instance Properties
    }
    public class HDN_HDONCTIET_DC_PLUS
    {
        #region Instance Properties

        public string BCS { get; set; }
        public decimal DIEN_TTHU { get; set; }
        public string DINH_MUC { get; set; }
        public decimal? DON_GIA { get; set; }
        public long ID_CHISO_DC { get; set; }
        public long ID_HDONCTIET_DC { get; set; }
        public long ID_HDON_DC { get; set; }
        public short KY { get; set; }
        public short LOAI_DDO { get; set; }
        public string LOAI_DMUC { get; set; }
        public string MA_CAPDA { get; set; }
        public string MA_CNANG { get; set; }
        public string MA_DDO { get; set; }
        public string MA_DVICTREN { get; set; }
        public string MA_DVIQLY { get; set; }
        public string MA_KHANG { get; set; }
        public string MA_KVUC { get; set; }
        public string MA_LO { get; set; }
        public string MA_NGIA { get; set; }
        public string MA_NHOMNN { get; set; }
        public string MA_NN { get; set; }
        public string MA_SOGCS { get; set; }
        public string MA_TO { get; set; }
        public string MA_TRAM { get; set; }
        public short NAM { get; set; }
        public string NGAY_APDUNG { get; set; }
        public string NGAY_SUA { get; set; }
        public string NGAY_TAO { get; set; }
        public string NGUOI_SUA { get; set; }
        public string NGUOI_TAO { get; set; }
        public string SO_COT { get; set; }
        public string SO_CTO { get; set; }
        public string SO_HOP { get; set; }
        public short SO_PHA { get; set; }
        public decimal SO_TIEN { get; set; }
        public string STT { get; set; }
        public string TGIAN_BDIEN { get; set; }
        public short THANG { get; set; }
        public string NOI_DUNG { get; set; }

        #endregion Instance Properties
    }
    public class HDN_HDONCOSFI_DC_PLUS
    {
        #region Instance Properties

        public decimal COSFI { get; set; }
        public decimal HUU_CONG { get; set; }
        public decimal? ID_CHISO_DC { get; set; }
        public long ID_HDONCOSFI_DC { get; set; }
        public long ID_HDON_DC { get; set; }
        public decimal KCOSFI { get; set; }
        public decimal KIMUA_CSPK { get; set; }
        public short KY { get; set; }
        public string MA_CNANG { get; set; }
        public string MA_DDO { get; set; }
        public string MA_DVICTREN { get; set; }
        public string MA_DVIQLY { get; set; }
        public string MA_KHANG { get; set; }
        public string MA_KVUC { get; set; }
        public string MA_LO { get; set; }
        public string MA_SOGCS { get; set; }
        public string MA_TO { get; set; }
        public string MA_TRAM { get; set; }
        public short NAM { get; set; }
        public string NGAY_SUA { get; set; }
        public string NGAY_TAO { get; set; }
        public string NGUOI_SUA { get; set; }
        public string NGUOI_TAO { get; set; }
        public string STT { get; set; }
        public short THANG { get; set; }
        public decimal TIEN_HUUCONG { get; set; }
        public decimal TIEN_VOCONG { get; set; }
        public decimal VO_CONG { get; set; }

        public string NOI_DUNG { get; set; }
        #endregion Instance Properties
    }
    public class HDN_KHANG_DC_PLUS
    {
        #region Instance Properties

        public short DA_CAPNHAT { get; set; }
        public string DUONG_PHO { get; set; }
        public long ID_HDON_DC { get; set; }
        public string MASO_THUE { get; set; }
        public string MA_CNANG { get; set; }
        public string MA_DVIQLY { get; set; }
        public string MA_KHANG { get; set; }
        public string MA_KHTT { get; set; }
        public string MA_NHANG { get; set; }
        public string NGAY_SUA { get; set; }
        public string NGAY_TAO { get; set; }
        public string NGUOI_SUA { get; set; }
        public string NGUOI_TAO { get; set; }
        public string SO_NHA { get; set; }
        public string TEN_KHANG { get; set; }
        public string TKHOAN_KHANG { get; set; }
        public decimal TLE_THUE { get; set; }

        #endregion Instance Properties
    }
    public class HDN_DIEMDO_DC_PLUS
    {
        #region Instance Properties

        public short DA_CAPNHAT { get; set; }
        public string DIA_CHI { get; set; }
        public long ID_HDON_DC { get; set; }
        public short KIMUA_CSPK { get; set; }
        public string MA_CAPDA { get; set; }
        public string MA_CNANG { get; set; }
        public string MA_DDO { get; set; }
        public string MA_DVIQLY { get; set; }
        public string MA_KHANG { get; set; }
        public string NGAY_SUA { get; set; }
        public string NGAY_TAO { get; set; }
        public string NGUOI_SUA { get; set; }
        public string NGUOI_TAO { get; set; }
        public decimal SO_HO { get; set; }

        #endregion Instance Properties
    }
    public class HDN_BBAN_APGIA_DC_PLUS
    {
        #region Instance Properties

        public short DA_CAPNHAT { get; set; }
        public string DINH_MUC { get; set; }
        public long ID_BBANAGIA { get; set; }
        public long ID_HDON_DC { get; set; }
        public string LOAI_BCS { get; set; }
        public string LOAI_DMUC { get; set; }
        public string MA_BBANAGIA { get; set; }
        public string MA_CAPDAP { get; set; }
        public string MA_CNANG { get; set; }
        public string MA_DDO { get; set; }
        public string MA_DVIQLY { get; set; }
        public string MA_NGIA { get; set; }
        public string MA_NHOMNN { get; set; }
        public string MA_NN { get; set; }
        public string NGAY_HLUC { get; set; }
        public string NGAY_SUA { get; set; }
        public string NGAY_TAO { get; set; }
        public string NGUOI_SUA { get; set; }
        public string NGUOI_TAO { get; set; }
        public decimal SO_HO { get; set; }
        public short SO_THUTU { get; set; }
        public string TGIAN_BDIEN { get; set; }

        #endregion Instance Properties
    }
    public class HDN_BBAN_DCHINH_PLUS
    {
        #region Instance Properties

        public long ID_BBAN { get; set; }
        public long ID_HDON { get; set; }
        public long ID_HDON_DC { get; set; }
        public short KY { get; set; }
        public short KY_DC { get; set; }
        public string MA_CNANG { get; set; }
        public string MA_DVIQLY { get; set; }
        public string MA_LDOSAI { get; set; }
        public short NAM { get; set; }
        public short NAM_DC { get; set; }
        public string NGAY_LAP { get; set; }
        public string NGAY_SUA { get; set; }
        public string NGAY_TAO { get; set; }
        public string NGUOI_LAP { get; set; }
        public string NGUOI_SUA { get; set; }
        public string NGUOI_TAO { get; set; }
        public string NOI_DUNG { get; set; }
        public string SO_BBAN { get; set; }
        public short THANG { get; set; }
        public short THANG_DC { get; set; }
        public short IS_DUP1 { get; set; }
        #endregion Instance Properties
    }
    public class HDN_CHISO_DC_PLUS
    {
        #region Instance Properties

        public string BCS { get; set; }
        public decimal CHISO_CU { get; set; }
        public decimal CHISO_MOI { get; set; }
        public short DA_CAPNHAT { get; set; }
        public decimal HS_NHAN { get; set; }
        public long ID_BCS_DC { get; set; }
        public long ID_CHISO { get; set; }
        public long ID_HDON_DC { get; set; }
        public short KY { get; set; }
        public string LOAI_CHISO { get; set; }
        public string MA_CNANG { get; set; }
        public string MA_CTO { get; set; }
        public string MA_DDO { get; set; }
        public string MA_DVIQLY { get; set; }
        public string MA_TTCTO { get; set; }
        public short NAM { get; set; }
        public string NGAY_CKY { get; set; }
        public string NGAY_DKY { get; set; }
        public string NGAY_SUA { get; set; }
        public string NGAY_TAO { get; set; }
        public string NGUOI_SUA { get; set; }
        public string NGUOI_TAO { get; set; }
        public decimal? SAN_LUONG { get; set; }
        public decimal? SLUONG_TRPHU { get; set; }
        public decimal? SLUONG_TTIEP { get; set; }
        public string SO_CTO { get; set; }
        public short THANG { get; set; }

        #endregion Instance Properties
    }
    public class GCS_CHISO_DUP1
    {
        public string BCS { get; set; }
        public decimal CHISO_CU { get; set; }
        public decimal CHISO_MOI { get; set; }
        public decimal HS_NHAN { get; set; }
        public long ID_BCS { get; set; }
        public long ID_CHISO { get; set; }
        public short KY { get; set; }
        public string LOAI_CHISO { get; set; }
        public string MA_CTO { get; set; }
        public string MA_DDO { get; set; }
        public string MA_DVIQLY { get; set; }
        public string MA_KHANG { get; set; }
        public string MA_TTCTO { get; set; }
        public short NAM { get; set; }
        public string NGAY_CKY { get; set; }
        public string NGAY_DKY { get; set; }
        public long SAN_LUONG { get; set; }
        public long? SLUONG_1 { get; set; }
        public long? SLUONG_2 { get; set; }
        public long? SLUONG_3 { get; set; }
        public long SLUONG_TRPHU { get; set; }
        public long SLUONG_TTIEP { get; set; }
        public string SO_CTO { get; set; }
        public short THANG { get; set; }
        public long SLUONG_TRPHU_COSFI { get; set; }
        public string NGAY_SUA { get; set; }
        public string NGAY_TAO { get; set; }
        public string NGUOI_SUA { get; set; }
        public string NGUOI_TAO { get; set; }
        public string MA_CNANG { get; set; }
    }


    public class LST_TIEN_TRINH
    {
        public string MA_DVIQLY { get; set; }
        public string MA_YCAU_KNAI { get; set; }
        public string MA_DDO_DDIEN { get; set; }
        public string MA_CVIEC { get; set; }
        public string MA_CVIECTIEP { get; set; }
        public string NDUNG_XLY { get; set; }
        public string SO_LAN { get; set; }
        public string NGAY_BDAU { get; set; }
        public string NGUOI_TAO { get; set; }
        public string NGUOI_SUA { get; set; }
        public string MA_CNANG { get; set; }
        public string LOAI_XULY { get; set; }
        public string LOAI_TTHUAN { get; set; }
        public string IS_CHUAHTAT { get; set; }
    }

    public class LST_PAN_PHAT
    {
        public string MA_DVIQLY { get; set; }
        public string MA_KHANG { get; set; }
        public string MA_YCAU_KNAI { get; set; }
        public string MA_DVICTREN { get; set; }
        public string SO_BBAN { get; set; }
        public string MA_DDO { get; set; }
        public string NGAY_LAP { get; set; }
        public string MA_PPHAP { get; set; }
        public string DIEN_NANG { get; set; }
        public string MA_HTHUC_VPHAM { get; set; }
        public string MA_HANHVI { get; set; }
        public string TU_NGAY { get; set; }
        public string CACH_CAN_CU { get; set; }
        public string DEN_NGAY { get; set; }
        public string SO_NGAY_VPHAM { get; set; }
        public string TONG_TIEN { get; set; }
        public string TIEN_THUE { get; set; }
        public string PHAN_TRAMVP { get; set; }
        public string CPHI_KHAC { get; set; }
        public string GIA_CAONHAT { get; set; }
        public string TU_KY { get; set; }
        public string DEN_KY { get; set; }
        public string TU_THANG { get; set; }
        public string DEN_THANG { get; set; }
        public string TU_NAM { get; set; }
        public string DEN_NAM { get; set; }
        public string ID_FILE_DKEM { get; set; }
        public string ALTV1 { get; set; }
        public string NGUOI_TAO { get; set; }
        public string MA_CNANG { get; set; }
        public string MA_BPHAN_GIAO { get; set; }
        public string MA_CVIECTIEP { get; set; }
        public string MA_NVIEN_GIAO { get; set; }
        public string SO_NGAY_KDUNG { get; set; }
        public string SO_NGAY_MDIEN { get; set; }
        public string SO_TIEN { get; set; }
        public string NGUOI_SUA { get; set; }
        public string KY { get; set; }
        public string THANG { get; set; }
        public string QUY { get; set; }
        public string NAM { get; set; }
        public string TIEN_LAI { get; set; }
        public string TYLE_THUE { get; set; }
        public string DIEN_TTHU { get; set; }
        public string TIEN_GTGT { get; set; }
        public string TIEN_PHATVP { get; set; }
        public string ID_YCAU { get; set; }

    }

    public class LST_KHANG_DDO
    {
        public string MA_DVIQLY { get; set; }
        public string MA_YCAU_KNAI { get; set; }
        public string MA_DVICTREN { get; set; }
        public string MA_KHANG { get; set; }
        public string TEN_KHANG { get; set; }
        public string DCHI_KHANG { get; set; }
        public string MA_KHANGTT { get; set; }
        public string TEN_KHANGTT { get; set; }
        public string DCHI_KHANGTT { get; set; }
        public string TKHOAN_KHANG { get; set; }
        public string MASO_THUE { get; set; }
        public string DTHOAI { get; set; }
        public string LOAI_KHANG { get; set; }
        public string MANHOM_KHANG { get; set; }
        public string MA_PTTT { get; set; }
        public string MA_HTTT { get; set; }
        public string MA_SOGCS { get; set; }
        public string MA_TO { get; set; }
        public string STT { get; set; }
        public string SO_CTO { get; set; }
        public string SO_HO { get; set; }
        public string NGUOI_TAO { get; set; }
        public string NGUOI_SUA { get; set; }
        public string MA_CNANG { get; set; }
    }
    public class LST_BBAN_PLUC
    {
        public string MA_DVIQLY { get; set; }
        public string MA_YCAU_KNAI { get; set; }
        public string LOAI_BBAN { get; set; }
        public string DU_LIEU { get; set; }
    }

}
