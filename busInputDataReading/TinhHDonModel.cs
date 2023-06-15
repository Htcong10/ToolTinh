using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;

namespace busInputDataReading
{
    public class TinhHDonModel
    {
        public string Mesage { get; set; }
        public DataSet Data { get; set; }
        public TinhHDonModel(string v, DataSet dsCustomerData)
        {
            this.Mesage = v;
            this.Data = dsCustomerData;
        }



    }
    public class outCustomerData
    {
        public List<List<string>> HDG_DDO_SOGCS { get; set; }
        public List<List<string>> HDG_DIEM_DO { get; set; }
        public List<List<string>> HDG_KHACH_HANG { get; set; }
        public List<List<string>> HDG_PTHUC_TTOAN { get; set; }
        public List<List<string>> HDG_VITRI_DDO { get; set; }
        public List<List<string>> HDG_QHE_DDO { get; set; }
        public List<List<string>> HDG_DDO_SOGCS_GT { get; set; }
        public List<List<string>> HDG_QHE_DDO_TP { get; set; }
        public List<List<string>> HDG_DIEM_DO_GT { get; set; }
        public List<List<string>> HDG_VITRI_DDO_GT { get; set; }
        public List<List<string>> HDG_BBAN_APGIA_GT { get; set; }
        public List<List<string>> HDG_QHE_DDO_GT { get; set; }
        public List<List<string>> HDG_KHACH_HANG_TT { get; set; }
        public List<List<string>> HDG_BBAN_APGIA { get; set; }
        public List<List<string>> HDG_QHE_DDO_BQ { get; set; }
        public List<List<string>> HDG_DDO_GTRU { get; set; }
    }
    public class outIndexData
    {
        public List<List<string>> GCS_CHISO { get; set; }
        public List<List<string>> GCS_SLMDK_SHBB { get; set; }
        public List<List<string>> GCS_SLMDK_SHBB_GT { get; set; }
        public List<List<string>> GCS_CHISO_GT { get; set; }
        public List<List<string>> GCS_CHISO_BQ { get; set; }
        public List<List<string>> GCS_CHISO_TP { get; set; }
        public List<List<string>> GCS_LICH_DCN { get; set; }
        public List<List<string>> GCS_CSO_DCN_GT { get; set; }
        public List<List<string>> GCS_CSO_DCN_BQ { get; set; }
        public List<List<string>> GCS_CSO_DCN_TP { get; set; }
        public List<List<string>> GCS_CSO_DCN { get; set; }

    }
    public class intIndexData
    {
        public string MA_DVIQLY { get; set; }
        public string MA_SOGCS { get; set; }
        public short KY { get; set; }
        public List<short> LST_KYP { get; set; }
        public short THANG { get; set; }
        public short NAM { get; set; }
        public List<string> LST_DDO { get; set; }
        public List<string> LST_DDO_GT { get; set; }
        public List<string> LST_DDO_TP { get; set; }
        public List<string> LST_DDO_BQ { get; set; }
    }
    public class inpReductionData
    {
        public string MA_DVIQLY { get; set; }        
        public List<string> LST_THANGNAM { get; set; }
        public List<string> LST_KHANG { get; set; }
        
    }
    public class outReductionData
    {
        public List<List<string>> HDN_HDON_GTRU { get; set; }
        public List<List<string>> HDN_HDONCTIET_GTRU { get; set; }
        public List<List<string>> HDN_HDONCOSFI_GTRU { get; set; }
       
    }
    public class StaticData
    {
        public List<List<string>> D_BAC_THANG { get; set; }
        public List<List<string>> D_GIA_NHOMNN { get; set; }
        public List<List<string>> D_NHOM_NN { get; set; }
        public List<List<string>> D_CAP_DAP { get; set; }
        public List<List<string>> D_COSFI { get; set; }
        public List<List<string>> D_NGANH_NGHE { get; set; }
        public List<List<string>> D_TY_GIA { get; set; }
        public List<List<string>> D_THAMCHIEU_CAPDA { get; set; }
        public List<List<string>> D_DVI_HOTRO_COVID { get; set; }
        public List<List<string>> D_SO_HOTRO_COVID { get; set; }
    }
    public class S_PARAMETER_PLUS
    {
        public List<List<string>> S_PARAMETER { get; set; }
    }
    public class outDataDChinhDC
    {
        public string ID_HDON { get; set; }
        public string THANG { get; set; }
        public string MA_DVIQLY { get; set; }
        public string NAM { get; set; }
        public string KY { get; set; }
        public string DATA_DCHINH { get; set; }
    }
    #region HDDC

    public class HDGDIEMDO
    {
        public string MA_DDO { get; set; }
        public int SO_PHA { get; set; }
        public string NGAY_DOI_CSPK { get; set; }
        public int SOHUU_LUOI { get; set; }
        public string THAO_TACDL { get; set; }
        public string MA_DVIQLY { get; set; }
        public double CSUAT { get; set; }
        public int LAN_CAPNHAT { get; set; }
        public int KIMUA_CSPK { get; set; }
        public string DIA_CHI { get; set; }
        public int ID_DIA_CHINH { get; set; }
        public int LOAI_DDO { get; set; }
        public string MA_KHANG { get; set; }
        public int SO_HO { get; set; }
        public string NGAY_HLUC { get; set; }
    }

    public class HDGDIEMDOGT
    {
        public string MA_DDO { get; set; }
        public int SO_PHA { get; set; }
        public string NGAY_DOI_CSPK { get; set; }
        public int SOHUU_LUOI { get; set; }
        public string THAO_TACDL { get; set; }
        public string MA_DVIQLY { get; set; }
        public double CSUAT { get; set; }
        public int LAN_CAPNHAT { get; set; }
        public int KIMUA_CSPK { get; set; }
        public string DIA_CHI { get; set; }
        public int ID_DIA_CHINH { get; set; }
        public int LOAI_DDO { get; set; }
        public string MA_KHANG { get; set; }
        public int SO_HO { get; set; }
        public string NGAY_HLUC { get; set; }
    }

    public class HDGKHACHHANG
    {
        public string MA_KHTT { get; set; }
        public int GIOI_TINH { get; set; }
        public string SO_NHA { get; set; }
        public string DUONG_PHO { get; set; }
        public int THANG { get; set; }
        public int TLE_THUE { get; set; }
        public string MA_DVIQLY { get; set; }
        public string TKHOAN_KHANG { get; set; }
        public string EMAIL { get; set; }
        public string MA_NHANG { get; set; }
        public string MANHOM_KHANG { get; set; }
        public string TEN_KHANG { get; set; }
        public string DTHOAI { get; set; }
        public int LOAI_KHANG { get; set; }
        public string MA_NN { get; set; }
        public string DCHI_TTOAN { get; set; }
        public string MA_LOAIDN { get; set; }
        public int NAM { get; set; }
        public string MA_KHANG { get; set; }
        public string FAX { get; set; }
        public string NGAY_HLUC { get; set; }
        public string WEBSITE { get; set; }
    }

    public class HDGPTHUCTTOAN
    {
        public string HTHUC_TTOAN { get; set; }
        public string PTHUC_TTOAN { get; set; }
        public int ID_PTHUC_TTOAN { get; set; }
        public string MA_DVIQLY { get; set; }
        public string MA_KHANG { get; set; }
        public string NGAY_HLUC { get; set; }
    }

    public class HDGVITRIDDO
    {
        public int ID_VITRI_DDO { get; set; }
        public string MA_DDO { get; set; }
        public string MA_LO { get; set; }
        public string SO_COT { get; set; }
        public string SO_HOM { get; set; }
        public string PHA { get; set; }
        public string MA_TRAM { get; set; }
        public string MA_TO { get; set; }
        public string MA_DVIQLY { get; set; }
        public string NGAY_HLUC { get; set; }
    }

    public class HDNKHANGDC
    {
        public string MA_KHTT { get; set; }
        public int GIOI_TINH { get; set; }
        public string SO_NHA { get; set; }
        public string DUONG_PHO { get; set; }
        public int THANG { get; set; }
        public int TLE_THUE { get; set; }
        public string MA_DVIQLY { get; set; }
        public string TKHOAN_KHANG { get; set; }
        public string EMAIL { get; set; }
        public string MA_NHANG { get; set; }
        public string MANHOM_KHANG { get; set; }
        public string TEN_KHANG { get; set; }
        public string DTHOAI { get; set; }
        public int LOAI_KHANG { get; set; }
        public string MA_NN { get; set; }
        public string DCHI_TTOAN { get; set; }
        public string MA_LOAIDN { get; set; }
        public int NAM { get; set; }
        public string MA_KHANG { get; set; }
        public string FAX { get; set; }
        public string NGAY_HLUC { get; set; }
        public string WEBSITE { get; set; }
    }

    public class HDGDDOSOGC
    {
        public string MA_DDO { get; set; }
        public string MA_SOGCS { get; set; }
        public string STT { get; set; }
        public long ID_QHE { get; set; }
        public string MA_DVIQLY { get; set; }
        public string MA_KHANG { get; set; }
        public string MA_KVUC { get; set; }
        public string NGAY_HLUC { get; set; }
    }

    public class GCSCHISO
    {
        public string MA_DDO { get; set; }
        public int CHISO_MOI { get; set; }
        public int SLUONG_3 { get; set; }
        public int THANG { get; set; }
        public string NGAY_TAO { get; set; }
        public string NGUOI_SUA { get; set; }
        public int CHISO_CU { get; set; }
        public string MA_DVIQLY { get; set; }
        public DateTime NGAY_CKY { get; set; }
        public int SAN_LUONG { get; set; }
        public DateTime NGAY_DKY { get; set; }
        public int HS_NHAN { get; set; }
        public int SLUONG_TRPHU { get; set; }
        public string MA_KHANG { get; set; }
        public string LOAI_CHISO { get; set; }
        public DateTime NGAY_SUA { get; set; }
        public string MA_CNANG { get; set; }
        public string THD_LE { get; set; }
        public string NGUOI_TAO { get; set; }
        public int KY { get; set; }
        public int ID_BCS { get; set; }
        public string BCS { get; set; }
        public string MA_CTO { get; set; }
        public int SLUONG_TTIEP { get; set; }
        public int ID_CHISO { get; set; }
        public int SLUONG_1 { get; set; }
        public string MA_DVICTREN { get; set; }
        public int NAM { get; set; }
        public int SLUONG_2 { get; set; }
        public string SO_CTO { get; set; }
    }
    public class GCSCHISOBQ
    {
        public string MA_DDO { get; set; }
        public int CHISO_MOI { get; set; }
        public int SLUONG_3 { get; set; }
        public int THANG { get; set; }
        public string NGAY_TAO { get; set; }
        public string NGUOI_SUA { get; set; }
        public int CHISO_CU { get; set; }
        public string MA_DVIQLY { get; set; }
        public DateTime NGAY_CKY { get; set; }
        public int SAN_LUONG { get; set; }
        public DateTime NGAY_DKY { get; set; }
        public int HS_NHAN { get; set; }
        public int SLUONG_TRPHU { get; set; }
        public string MA_KHANG { get; set; }
        public string LOAI_CHISO { get; set; }
        public DateTime NGAY_SUA { get; set; }
        public string MA_CNANG { get; set; }
        public string THD_LE { get; set; }
        public string NGUOI_TAO { get; set; }
        public int KY { get; set; }
        public int ID_BCS { get; set; }
        public string BCS { get; set; }
        public string MA_CTO { get; set; }
        public int SLUONG_TTIEP { get; set; }
        public int ID_CHISO { get; set; }
        public int SLUONG_1 { get; set; }
        public string MA_DVICTREN { get; set; }
        public int NAM { get; set; }
        public int SLUONG_2 { get; set; }
        public string SO_CTO { get; set; }
    }
    public class GCSCHISOTP
    {
        public string MA_DDO { get; set; }
        public int CHISO_MOI { get; set; }
        public int SLUONG_3 { get; set; }
        public int THANG { get; set; }
        public string NGAY_TAO { get; set; }
        public string NGUOI_SUA { get; set; }
        public int CHISO_CU { get; set; }
        public string MA_DVIQLY { get; set; }
        public DateTime NGAY_CKY { get; set; }
        public int SAN_LUONG { get; set; }
        public DateTime NGAY_DKY { get; set; }
        public int HS_NHAN { get; set; }
        public int SLUONG_TRPHU { get; set; }
        public string MA_KHANG { get; set; }
        public string LOAI_CHISO { get; set; }
        public DateTime NGAY_SUA { get; set; }
        public string MA_CNANG { get; set; }
        public string THD_LE { get; set; }
        public string NGUOI_TAO { get; set; }
        public int KY { get; set; }
        public int ID_BCS { get; set; }
        public string BCS { get; set; }
        public string MA_CTO { get; set; }
        public int SLUONG_TTIEP { get; set; }
        public int ID_CHISO { get; set; }
        public int SLUONG_1 { get; set; }
        public string MA_DVICTREN { get; set; }
        public int NAM { get; set; }
        public int SLUONG_2 { get; set; }
        public string SO_CTO { get; set; }
    }
    public class GCSCHISOGT
    {
        public string MA_DDO { get; set; }
        public int CHISO_MOI { get; set; }
        public int SLUONG_3 { get; set; }
        public int THANG { get; set; }
        public string NGAY_TAO { get; set; }
        public string NGUOI_SUA { get; set; }
        public int CHISO_CU { get; set; }
        public string MA_DVIQLY { get; set; }
        public DateTime NGAY_CKY { get; set; }
        public int SAN_LUONG { get; set; }
        public DateTime NGAY_DKY { get; set; }
        public int HS_NHAN { get; set; }
        public int SLUONG_TRPHU { get; set; }
        public string MA_KHANG { get; set; }
        public string LOAI_CHISO { get; set; }
        public DateTime NGAY_SUA { get; set; }
        public string MA_CNANG { get; set; }
        public string THD_LE { get; set; }
        public string NGUOI_TAO { get; set; }
        public int KY { get; set; }
        public int ID_BCS { get; set; }
        public string BCS { get; set; }
        public string MA_CTO { get; set; }
        public int SLUONG_TTIEP { get; set; }
        public int ID_CHISO { get; set; }
        public int SLUONG_1 { get; set; }
        public string MA_DVICTREN { get; set; }
        public int NAM { get; set; }
        public int SLUONG_2 { get; set; }
        public string SO_CTO { get; set; }
    }

    public class HDNDIEMDODC
    {
        public string MA_DDO { get; set; }
        public int SO_PHA { get; set; }
        public string NGAY_DOI_CSPK { get; set; }
        public int SOHUU_LUOI { get; set; }
        public string THAO_TACDL { get; set; }
        public string MA_DVIQLY { get; set; }
        public double CSUAT { get; set; }
        public int LAN_CAPNHAT { get; set; }
        public int KIMUA_CSPK { get; set; }
        public string DIA_CHI { get; set; }
        public int ID_DIA_CHINH { get; set; }
        public int LOAI_DDO { get; set; }
        public string MA_KHANG { get; set; }
        public int SO_HO { get; set; }
        public string NGAY_HLUC { get; set; }
    }

    public class HDNBBANAPGIADC
    {
        public string MA_DDO { get; set; }
        public string MA_BBANAGIA { get; set; }
        public string LOAI_DMUC { get; set; }
        public string MA_NHOMNN { get; set; }
        public string LOAI_TIEN { get; set; }
        public string DINH_MUC { get; set; }
        public string TGIAN_BDIEN { get; set; }
        public string THAO_TACDL { get; set; }
        public string MA_DVIQLY { get; set; }
        public int DON_GIA { get; set; }
        public string KHOANG_DA { get; set; }
        public int IS_SOHO { get; set; }
        public string MA_CAPDAP { get; set; }
        public int ID_BBANAGIA { get; set; }
        public int SO_THUTU { get; set; }
        public string MA_NGIA { get; set; }
        public string MA_NN { get; set; }
        public int SO_HO { get; set; }
        public string DIEN_GIAI { get; set; }
        public DateTime NGAY_HLUC { get; set; }
        public int TRANG_THAI { get; set; }
        public string LOAI_BCS { get; set; }
    }

    public class HDNCHISODC
    {
        public string MA_DDO { get; set; }
        public int CHISO_MOI { get; set; }
        public int SLUONG_3 { get; set; }
        public int THANG { get; set; }
        public string NGAY_TAO { get; set; }
        public string NGUOI_SUA { get; set; }
        public int CHISO_CU { get; set; }
        public string MA_DVIQLY { get; set; }
        public DateTime NGAY_CKY { get; set; }
        public int SAN_LUONG { get; set; }
        public DateTime NGAY_DKY { get; set; }
        public int HS_NHAN { get; set; }
        public int SLUONG_TRPHU { get; set; }
        public string MA_KHANG { get; set; }
        public string LOAI_CHISO { get; set; }
        public DateTime NGAY_SUA { get; set; }
        public string MA_CNANG { get; set; }
        public string THD_LE { get; set; }
        public string NGUOI_TAO { get; set; }
        public int KY { get; set; }
        public int ID_BCS { get; set; }
        public string BCS { get; set; }
        public string MA_CTO { get; set; }
        public int SLUONG_TTIEP { get; set; }
        public int ID_CHISO { get; set; }
        public int SLUONG_1 { get; set; }
        public string MA_DVICTREN { get; set; }
        public int NAM { get; set; }
        public int SLUONG_2 { get; set; }
        public string SO_CTO { get; set; }
    }

    public class HDNHDONCTIET
    {
        public string MA_DDO { get; set; }
        public int SO_PHA { get; set; }
        public string LOAI_DMUC { get; set; }
        public string MA_NHOMNN { get; set; }
        public string NGAY_TAO { get; set; }
        public int THANG { get; set; }
        public string NGUOI_SUA { get; set; }
        public string TGIAN_BDIEN { get; set; }
        public string MA_DVIQLY { get; set; }
        public string MA_LO { get; set; }
        public string STT { get; set; }
        public int DIEN_TTHU { get; set; }
        public string MA_NGIA { get; set; }
        public string MA_NN { get; set; }
        public string MA_KHANG { get; set; }
        public string MA_CAPDA { get; set; }
        public string NGAY_APDUNG { get; set; }
        public string NGAY_SUA { get; set; }
        public string MA_CNANG { get; set; }
        public string NGUOI_TAO { get; set; }
        public string DINH_MUC { get; set; }
        public int DON_GIA { get; set; }
        public int KY { get; set; }
        public string MA_SOGCS { get; set; }
        public string BCS { get; set; }
        public int ID_HDON { get; set; }
        public int ID_CHISO { get; set; }
        public int SO_TIEN { get; set; }
        public string MA_TRAM { get; set; }
        public int LOAI_DDO { get; set; }
        public int NAM { get; set; }
        public string SO_CTO { get; set; }
        public int ID_HDONCTIET { get; set; }
    }

    public class HDNHDONTIEPNHAN
    {
        public string NGAY_SUA { get; set; }
        public string NGUOI_TRA { get; set; }
        public string MA_CNANG { get; set; }
        public int THANG { get; set; }
        public string NGAY_TAO { get; set; }
        public string NGUOI_TAO { get; set; }
        public int DA_TRA { get; set; }
        public string NGUOI_SUA { get; set; }
        public string MA_DVIQLY { get; set; }
        public int KY { get; set; }
        public int TTHAI_DUYET { get; set; }
        public string MA_NNHAN { get; set; }
        public string MA_SOGCS { get; set; }
        public int ID_HDON { get; set; }
        public int DIEN_TTHU { get; set; }
        public int TIEN_GTGT { get; set; }
        public int SO_TIEN { get; set; }
        public string NGAY_DUYETXLY { get; set; }
        public int NAM { get; set; }
        public string MA_KHANG { get; set; }
        public string LOAI_HDON { get; set; }
        public string NGAY_TNHAN { get; set; }
    }

    public class HDNHDON
    {
        public string NGAY_PHANH { get; set; }
        public string MA_PTTT { get; set; }
        public string NGAY_TAO { get; set; }
        public int THANG { get; set; }
        public string NGUOI_SUA { get; set; }
        public string SO_SERY { get; set; }
        public string MA_DVIQLY { get; set; }
        public string MA_HTTT { get; set; }
        public DateTime NGAY_CKY { get; set; }
        public string TEN_KHANG { get; set; }
        public string MANHOM_KHANG { get; set; }
        public string STT { get; set; }
        public int TYLE_THUE { get; set; }
        public int DIEN_TTHU { get; set; }
        public DateTime NGAY_DKY { get; set; }
        public int CHI_TIET { get; set; }
        public string MA_KHANG { get; set; }
        public string MA_KHANGTT { get; set; }
        public string LOAI_HDON { get; set; }
        public int ID_BBANPHANH { get; set; }
        public int TIEN_VC { get; set; }
        public string NGAY_SUA { get; set; }
        public string MA_CNANG { get; set; }
        public string MA_NVPHANH { get; set; }
        public string NGUOI_TAO { get; set; }
        public int TONG_TIEN { get; set; }
        public string KIHIEU_SERY { get; set; }
        public string TKHOAN_KHANG { get; set; }
        public string MA_NHANG { get; set; }
        public int KY { get; set; }
        public string DCHI_KHANGTT { get; set; }
        public string MA_SOGCS { get; set; }
        public int ID_HDON { get; set; }
        public string TEN_KHANGTT { get; set; }
        public int TIEN_GTGT { get; set; }
        public int SO_TIEN { get; set; }
        public int LOAI_KHANG { get; set; }
        public string DCHI_KHANG { get; set; }
        public int NAM { get; set; }
        public string SO_CTO { get; set; }
        public int SO_HO { get; set; }
        public int THUE_TD { get; set; }
        public int TIEN_TD { get; set; }
        public int THUE_VC { get; set; }
    }

    public class HDGKHACHHANGTT
    {
        public string TEN_KHANG { get; set; }
        public string SO_NHA { get; set; }
        public string DUONG_PHO { get; set; }
        public string MA_DVIQLY { get; set; }
        public string MA_KHANG { get; set; }
    }

    public class HDNBBANDCHINH
    {
        public int THANG_DC { get; set; }
        public int THANG { get; set; }
        public string MA_LDOSAI { get; set; }
        public string MA_DVIQLY { get; set; }
        public string NGUOI_LAP { get; set; }
        public short KY { get; set; }
        public long ID_HDON { get; set; }
        public string NOI_DUNG { get; set; }
        public int NAM_DC { get; set; }
        public int KY_DC { get; set; }
        public int NAM { get; set; }
        public string SO_BBAN { get; set; }
        public string NGAY_LAP { get; set; }
    }

    public class HDGBBANAPGIA
    {
        public string MA_DDO { get; set; }
        public string MA_BBANAGIA { get; set; }
        public string LOAI_DMUC { get; set; }
        public string MA_NHOMNN { get; set; }
        public string LOAI_TIEN { get; set; }
        public string DINH_MUC { get; set; }
        public string TGIAN_BDIEN { get; set; }
        public string THAO_TACDL { get; set; }
        public string MA_DVIQLY { get; set; }
        public int DON_GIA { get; set; }
        public string KHOANG_DA { get; set; }
        public int IS_SOHO { get; set; }
        public string MA_CAPDAP { get; set; }
        public int ID_BBANAGIA { get; set; }
        public int SO_THUTU { get; set; }
        public string MA_NGIA { get; set; }
        public string MA_NN { get; set; }
        public int SO_HO { get; set; }
        public string DIEN_GIAI { get; set; }
        public DateTime NGAY_HLUC { get; set; }
        public int TRANG_THAI { get; set; }
        public string LOAI_BCS { get; set; }
    }
    public class HDGBBANAPGIAGT
    {
        public string MA_DDO { get; set; }
        public string MA_BBANAGIA { get; set; }
        public string LOAI_DMUC { get; set; }
        public string MA_NHOMNN { get; set; }
        public string LOAI_TIEN { get; set; }
        public string DINH_MUC { get; set; }
        public string TGIAN_BDIEN { get; set; }
        public string THAO_TACDL { get; set; }
        public string MA_DVIQLY { get; set; }
        public int DON_GIA { get; set; }
        public string KHOANG_DA { get; set; }
        public int IS_SOHO { get; set; }
        public string MA_CAPDAP { get; set; }
        public int ID_BBANAGIA { get; set; }
        public int SO_THUTU { get; set; }
        public string MA_NGIA { get; set; }
        public string MA_NN { get; set; }
        public int SO_HO { get; set; }
        public string DIEN_GIAI { get; set; }
        public DateTime NGAY_HLUC { get; set; }
        public int TRANG_THAI { get; set; }
        public string LOAI_BCS { get; set; }
    }

    public class customersDataDC
    {
        public List<HDGDIEMDO> HDG_DIEM_DO { get; set; }
        public List<HDGDIEMDOGT> HDG_DIEM_DO_GT { get; set; }
        public List<HDGKHACHHANG> HDG_KHACH_HANG { get; set; }
        public List<HDGPTHUCTTOAN> HDG_PTHUC_TTOAN { get; set; }
        public List<HDGVITRIDDO> HDG_VITRI_DDO { get; set; }
        public List<HDNKHANGDC> HDN_KHANG_DC { get; set; }
        public List<HDGDDOSOGC> HDG_DDO_SOGCS { get; set; }
        public List<GCSCHISO> GCS_CHISO { get; set; }
        public List<GCSCHISOBQ> GCS_CHISO_BQ { get; set; }
        public List<GCSCHISOTP> GCS_CHISO_TP { get; set; }
        public List<GCSCHISOGT> GCS_CHISO_GT { get; set; }
        public List<HDNDIEMDODC> HDN_DIEMDO_DC { get; set; }
        public List<HDNBBANAPGIADC> HDN_BBAN_APGIA_DC { get; set; }
        public List<HDNCHISODC> HDN_CHISO_DC { get; set; }
        public List<HDNHDONCTIET> HDN_HDONCTIET { get; set; }
        public List<HDNHDONTIEPNHAN> HDN_HDON_TIEPNHAN { get; set; }
        public List<HDNHDON> HDN_HDON { get; set; }
        public List<HDGKHACHHANGTT> HDG_KHACH_HANG_TT { get; set; }
        public List<HDNBBANDCHINH> HDN_BBAN_DCHINH { get; set; }
        public List<HDGBBANAPGIA> HDG_BBAN_APGIA { get; set; }
        public List<HDGBBANAPGIAGT> HDG_BBAN_APGIA_GT { get; set; }
    }


    #endregion
}
