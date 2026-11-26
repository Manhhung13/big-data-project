import os
import sys
import pandas as pd

if sys.stdout.encoding and sys.stdout.encoding.lower() != 'utf-8':
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except Exception:
        pass

# Bảng ánh xạ 30 mã VN30 phục vụ bóc tách thực thể và map tin tức ở tầng Spark
REFERENCE_DATA = [
    {
        "symbol": "FPT",
        "company_name": "CTCP FPT",
        "aliases": "FPT|Tập đoàn FPT|FPT Software|FPT Telecom|FPT Retail",
        "industry": "Công nghệ"
    },
    {
        "symbol": "VNM",
        "company_name": "CTCP Sữa Việt Nam",
        "aliases": "VNM|Vinamilk|Sữa Việt Nam",
        "industry": "Thực phẩm & Đồ uống"
    },
    {
        "symbol": "VIC",
        "company_name": "Tập đoàn Vingroup",
        "aliases": "VIC|Vingroup|Tập đoàn Vingroup|VinFast",
        "industry": "Bất động sản"
    },
    {
        "symbol": "VHM",
        "company_name": "CTCP Vinhomes",
        "aliases": "VHM|Vinhomes|Vinhomes Ocean Park|Vinhomes Grand Park",
        "industry": "Bất động sản"
    },
    {
        "symbol": "HPG",
        "company_name": "CTCP Tập đoàn Hòa Phát",
        "aliases": "HPG|Hòa Phát|Thép Hòa Phát|Tập đoàn Hòa Phát|Dung Quất",
        "industry": "Thép & Vật liệu xây dựng"
    },
    {
        "symbol": "VCB",
        "company_name": "Ngân hàng TMCP Ngoại thương Việt Nam",
        "aliases": "VCB|Vietcombank|Ngoại thương Việt Nam|Ngân hàng Ngoại thương",
        "industry": "Ngân hàng"
    },
    {
        "symbol": "BID",
        "company_name": "Ngân hàng TMCP Đầu tư và Phát triển Việt Nam",
        "aliases": "BID|BIDV|Đầu tư và Phát triển Việt Nam|Ngân hàng BIDV",
        "industry": "Ngân hàng"
    },
    {
        "symbol": "CTG",
        "company_name": "Ngân hàng TMCP Công thương Việt Nam",
        "aliases": "CTG|VietinBank|Vietinbank|Công thương Việt Nam|Ngân hàng Công thương",
        "industry": "Ngân hàng"
    },
    {
        "symbol": "TCB",
        "company_name": "Ngân hàng TMCP Kỹ thương Việt Nam",
        "aliases": "TCB|Techcombank|Kỹ thương Việt Nam|Ngân hàng Kỹ thương",
        "industry": "Ngân hàng"
    },
    {
        "symbol": "MBB",
        "company_name": "Ngân hàng TMCP Quân đội",
        "aliases": "MBB|MBBank|MB Bank|Ngân hàng Quân đội",
        "industry": "Ngân hàng"
    },
    {
        "symbol": "VPB",
        "company_name": "Ngân hàng TMCP Việt Nam Thịnh Vượng",
        "aliases": "VPB|VPBank|VP Bank|Việt Nam Thịnh Vượng",
        "industry": "Ngân hàng"
    },
    {
        "symbol": "ACB",
        "company_name": "Ngân hàng TMCP Á Châu",
        "aliases": "ACB|Á Châu|Ngân hàng Á Châu",
        "industry": "Ngân hàng"
    },
    {
        "symbol": "SSI",
        "company_name": "CTCP Chứng khoán SSI",
        "aliases": "SSI|Chứng khoán SSI",
        "industry": "Chứng khoán"
    },
    {
        "symbol": "VND",
        "company_name": "CTCP Chứng khoán VNDIRECT",
        "aliases": "VND|VNDIRECT|Vndirect|Chứng khoán VNDIRECT",
        "industry": "Chứng khoán"
    },
    {
        "symbol": "MSN",
        "company_name": "CTCP Tập đoàn Masan",
        "aliases": "MSN|Masan|Masan Group|WinCommerce|Masan Consumer",
        "industry": "Hàng tiêu dùng"
    },
    {
        "symbol": "MWG",
        "company_name": "CTCP Đầu tư Thế Giới Di Động",
        "aliases": "MWG|Thế Giới Di Động|Bách Hóa Xanh|Điện Máy Xanh|Mobile World",
        "industry": "Bán lẻ"
    },
    {
        "symbol": "GAS",
        "company_name": "Tổng Công ty Khí Việt Nam - CTCP",
        "aliases": "GAS|PV GAS|PVGas|Khí Việt Nam|Tổng Công ty Khí",
        "industry": "Dầu khí & Tiện ích"
    },
    {
        "symbol": "PLX",
        "company_name": "Tập đoàn Xăng dầu Việt Nam",
        "aliases": "PLX|Petrolimex|Xăng dầu Việt Nam|Tập đoàn Xăng dầu",
        "industry": "Dầu khí"
    },
    {
        "symbol": "POW",
        "company_name": "Tổng Công ty Điện lực Dầu khí Việt Nam - CTCP",
        "aliases": "POW|PV Power|Điện lực Dầu khí",
        "industry": "Năng lượng & Điện toán"
    },
    {
        "symbol": "SAB",
        "company_name": "Tổng CTCP Bia - Rượu - Nước giải khát Sài Gòn",
        "aliases": "SAB|Sabeco|Bia Sài Gòn|Bia Saigon",
        "industry": "Thực phẩm & Đồ uống"
    },
    {
        "symbol": "STB",
        "company_name": "Ngân hàng TMCP Sài Gòn Thương Tín",
        "aliases": "STB|Sacombank|Sài Gòn Thương Tín",
        "industry": "Ngân hàng"
    },
    {
        "symbol": "HDB",
        "company_name": "Ngân hàng TMCP Phát triển TP.HCM",
        "aliases": "HDB|HDBank|HD Bank|Phát triển TP.HCM",
        "industry": "Ngân hàng"
    },
    {
        "symbol": "VIB",
        "company_name": "Ngân hàng TMCP Quốc tế Việt Nam",
        "aliases": "VIB|VIBank|Ngân hàng Quốc tế",
        "industry": "Ngân hàng"
    },
    {
        "symbol": "SHB",
        "company_name": "Ngân hàng TMCP Sài Gòn - Hà Nội",
        "aliases": "SHB|Sài Gòn - Hà Nội|Ngân hàng SHB",
        "industry": "Ngân hàng"
    },
    {
        "symbol": "TPB",
        "company_name": "Ngân hàng TMCP Tiên Phong",
        "aliases": "TPB|TPBank|TP Bank|Tiên Phong Bank|Ngân hàng Tiên Phong",
        "industry": "Ngân hàng"
    },
    {
        "symbol": "LPB",
        "company_name": "Ngân hàng TMCP Lộc Phát Việt Nam",
        "aliases": "LPB|LPBank|Lộc Phát Việt Nam|Bưu điện Liên Việt|LienVietPostBank",
        "industry": "Ngân hàng"
    },
    {
        "symbol": "GVR",
        "company_name": "Tập đoàn Công nghiệp Cao su Việt Nam",
        "aliases": "GVR|Cao su Việt Nam|Vietnam Rubber Group",
        "industry": "Cao su & Hóa chất"
    },
    {
        "symbol": "BCM",
        "company_name": "Tổng Công ty Đầu tư và Phát triển Công nghiệp - CTCP",
        "aliases": "BCM|Becamex|Becamex IDC|Đầu tư và Phát triển Công nghiệp",
        "industry": "Bất động sản & KCN"
    },
    {
        "symbol": "VRE",
        "company_name": "CTCP Vincom Retail",
        "aliases": "VRE|Vincom Retail|Vincom Mega Mall|Vincom Plaza",
        "industry": "Bất động sản bán lẻ"
    },
    {
        "symbol": "VJC",
        "company_name": "CTCP Hàng không VietJet",
        "aliases": "VJC|Vietjet|Vietjet Air|Hàng không VietJet",
        "industry": "Hàng không & Du lịch"
    }
]

def generate_reference_csv(output_path="reference_symbols.csv"):
    df = pd.DataFrame(REFERENCE_DATA)
    script_dir = os.path.dirname(os.path.abspath(__file__))
    target_file = os.path.join(script_dir, output_path) if not os.path.isabs(output_path) else output_path
    df.to_csv(target_file, index=False, encoding="utf-8-sig")
    print(f"Đã tạo thành công file: {target_file} với {len(df)} mã cổ phiếu VN30!")
    return df

if __name__ == "__main__":
    generate_reference_csv()
