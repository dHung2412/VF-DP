# Hướng Dẫn Nhúng Bản Đồ Mô Phỏng Sạc Xe Điện Vào Apache Superset

Bản đồ mô phỏng xe điện & gợi ý trạm sạc thông minh được xây dựng tại:
- Giao diện độc lập: [`bi/dashboard/charging_simulation_map.html`](../bi/dashboard/charging_simulation_map.html)
- Server hosting: [`bi/serve_dashboard.py`](../bi/serve_dashboard.py)

---

## 1. Khởi động Dashboard Server

Khởi động server phục vụ dashboard trên cổng `8050` (hoặc cổng tùy chọn):

```bash
# Cách 1: Qua Makefile
make dashboard

# Cách 2: Qua Python trực tiếp
python3 bi/serve_dashboard.py --port 8050
```

Kiểm tra truy cập: Mở trình duyệt tại `http://localhost:8050`.

---

## 2. Các phương án nhúng vào Apache Superset

### Phương án A: Dùng Component Iframe chính thức của Superset (Khuyến nghị)
1. Mở giao diện Superset $\rightarrow$ Vào **Dashboards** $\rightarrow$ Chọn hoặc tạo Dashboard mới (VD: `Quy Hoạch Trạm Sạc Xe Điện`).
2. Nhấn nút **Edit Dashboard** (góc trên bên phải).
3. Trong thanh bên trái các components, tìm và kéo thẻ **"Iframe"** vào vị trí mong muốn trên lưới dashboard.
4. Nhấn biểu tượng cài đặt (⚙️) của thẻ Iframe:
   - **URL:** `http://localhost:8050` (hoặc `http://<IP-SERVER>:8050` nếu Superset chạy trên server khác).
   - **Height:** Đặt `850px` để hiển thị trọn vẹn bản đồ và 2 thanh điều khiển.
5. Nhấn **Save**.

---

### Phương án B: Nhúng qua Component Markdown của Superset
Nếu Superset phiên bản hiện tại ẩn thẻ Iframe, bạn có thể nhúng trực tiếp qua thẻ **Markdown**:
1. Trong màn hình **Edit Dashboard**, kéo component **"Markdown"** vào dashboard.
2. Nhập đoạn mã HTML sau:

```html
<div style="width: 100%; height: 850px; border-radius: 12px; overflow: hidden; box-shadow: 0 4px 20px rgba(0,0,0,0.4);">
  <iframe 
    src="http://localhost:8050" 
    width="100%" 
    height="100%" 
    frameborder="0" 
    allow="geolocation"
    style="border: 1px solid #1e293b;">
  </iframe>
</div>
```

3. Nhấn **Save**.

---

### Phương án C: Cấu hình Header an toàn trong Superset (`superset_config.py`)
Khi nhúng iframe từ một domain/port khác vào Superset, đảm bảo file cấu hình `superset_config.py` cho phép hiển thị iframe:

```python
# Cho phép nhúng iframe ngoài trong dashboard Markdown
TALISMAN_CONFIG = {
    "content_security_policy": {
        "frame-src": ["'self'", "http://localhost:8050", "http://*:8050"]
    }
}
HTTP_HEADERS = {"X-Frame-Options": "ALLOWALL"}
```
*(Server [`bi/serve_dashboard.py`](../bi/serve_dashboard.py) đã được cấu hình sẵn các response header `X-Frame-Options: ALLOWALL` và `Access-Control-Allow-Origin: *`).*

---

## 3. Các tính năng nổi bật của Dashboard

1. **Bản đồ tương tác toàn quốc (Leaflet.js):**
   - Định vị 15 tỉnh/thành trọng điểm trải dài 3 miền Bắc — Trung — Nam.
   - Vòng tròn kích thước động thể hiện mật độ xe điện.
   - Vị trí các trạm sạc hiện hữu (icon xanh dương ⚡).
2. **Bộ điều khiển tăng/giảm xe:**
   - Các nút chỉnh nhanh: `[-1K]`, `[-200]`, `[+200]`, `[+1K]`, `[+5K]` trực tiếp trên từng tỉnh.
   - Các kịch bản có sẵn: Tăng trưởng 20% toàn quốc, Tăng đột biến Hà Nội / TP.HCM, Dịp lễ cao điểm du lịch.
3. **Thuật toán gợi ý điểm đặt trạm sạc (Realtime Engine):**
   - Tự động chạy và tính toán lại ngay khi số lượng xe thay đổi:
     - So sánh nhu cầu năng lượng sạc ($E_{\text{demand}}$) vs công suất cung ứng của trạm ($E_{\text{supply}}$).
     - Tính toán tỷ lệ quá tải và số cổng sạc thiếu hụt.
   - Phân cấp 3 mức độ màu (từ Cao đến Thấp):
     - 🔴 **Mức Cao (P0 - Cấp thiết):** Tỷ lệ quá tải $\ge 175\%$ hoặc thiếu $\ge 12$ cổng $\rightarrow$ Đề xuất Superhub sạc siêu nhanh 250kW.
     - 🟡 **Mức Trung bình (P1 - Bổ sung):** Tỷ lệ quá tải $130\% - 175\%$ $\rightarrow$ Đề xuất Standard Hub sạc nhanh 150kW.
     - 🟢 **Mức Tiềm năng (P2 - Đón đầu):** Tỷ lệ quá tải $105\% - 130\%$ $\rightarrow$ Đề xuất trạm vệ tinh đô thị 60–120kW.
4. **Bảng xếp hạng & Xuất báo cáo:**
   - Danh sách đề xuất bên phải tự động sắp xếp theo độ cấp thiết. Nhấp vào từng đề xuất để bản đồ tự động bay (flyTo) đến tọa độ đó.
   - Hỗ trợ nút **Xuất báo cáo (CSV)** chứa đầy đủ tọa độ, quy mô cổng đề xuất, loại trạm và ước tính vốn đầu tư.
