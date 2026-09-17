"""Server nhẹ phục vụ Dashboard Mô Phỏng & Gợi Ý Trạm Sạc (nhúng được vào Superset).

Sử dụng thư viện chuẩn của Python (không cần cài thêm framework).
Chạy:
  python3 bi/serve_dashboard.py --port 8050
Sau đó:
  - Mở trực tiếp: http://localhost:8050
  - Hoặc nhúng vào Superset Dashboard qua URL Iframe: http://localhost:8050
"""
from __future__ import annotations

import argparse
import http.server
import json
import socketserver
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DASHBOARD_FILE = ROOT / "bi" / "dashboard" / "charging_simulation_map.html"


class DashboardHandler(http.server.SimpleHTTPRequestHandler):
    def do_GET(self) -> None:
        if self.path in ("/", "/dashboard", "/index.html"):
            if DASHBOARD_FILE.exists():
                content = DASHBOARD_FILE.read_bytes()
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(content)))
                # Cho phép nhúng vào Superset iframe từ domain khác
                self.send_header("X-Frame-Options", "ALLOWALL")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(content)
                return
            else:
                self.send_error(404, "Dashboard HTML file not found")
                return

        elif self.path == "/api/stats":
            # API trả về dữ liệu thực tế từ DuckDB warehouse nếu có
            data = {"status": "ok", "source": "duckdb"}
            db_path = ROOT / "data" / "warehouse.duckdb"
            if db_path.exists():
                try:
                    import duckdb
                    con = duckdb.connect(str(db_path), read_only=True)
                    data["counts"] = {
                        "stations": con.execute("SELECT count(*) FROM dim_station").fetchone()[0],
                        "vehicles": con.execute("SELECT count(*) FROM dim_vehicle").fetchone()[0],
                        "telemetry": con.execute("SELECT count(*) FROM fact_vehicle_telemetry").fetchone()[0],
                    }
                    con.close()
                except Exception as e:
                    data["error"] = str(e)
            payload = json.dumps(data).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
            return

        elif self.path in ("/vietnam_geojson.js", "/bi/dashboard/vietnam_geojson.js"):
            js_file = ROOT / "bi" / "dashboard" / "vietnam_geojson.js"
            if js_file.exists():
                content = js_file.read_bytes()
                self.send_response(200)
                self.send_header("Content-Type", "application/javascript; charset=utf-8")
                self.send_header("Content-Length", str(len(content)))
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(content)
                return

        elif self.path in ("/vietnam_provinces.geojson", "/bi/dashboard/vietnam_provinces.geojson"):
            json_file = ROOT / "bi" / "dashboard" / "vietnam_provinces.geojson"
            if json_file.exists():
                content = json_file.read_bytes()
                self.send_response(200)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Content-Length", str(len(content)))
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                self.wfile.write(content)
                return

        # Fallback to standard static files
        super().do_GET()


def main() -> None:
    ap = argparse.ArgumentParser(description="Chạy server Dashboard mô phỏng trạm sạc")
    ap.add_argument("--port", type=int, default=8050, help="Port lắng nghe (mặc định 8050)")
    ap.add_argument("--host", default="0.0.0.0", help="Host lắng nghe (mặc định 0.0.0.0)")
    args = ap.parse_args()

    # Allow port reuse immediately
    socketserver.TCPServer.allow_reuse_address = True
    with socketserver.TCPServer((args.host, args.port), DashboardHandler) as httpd:
        print("=" * 70)
        print(f"🚀 VF-DP Simulation Dashboard đang chạy tại:")
        print(f"   👉 Truy cập trực tiếp: http://localhost:{args.port}")
        print(f"   👉 URL nhúng vào Superset (Iframe): http://localhost:{args.port}")
        print("=" * 70)
        print("Nhấn Ctrl+C để dừng server.")
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print("\nĐã dừng server.")


if __name__ == "__main__":
    main()
