import pandas as pd
import sys
import os

STORAGE_OPTS = {
    "key": "minioadmin", 
    "secret": "minioadmin", 
    "client_kwargs": {"endpoint_url": "http://minio:9000"}
}

def generate_html_report(results):
    """Creates an HTML file with the results table"""
    html_content = """
    <html>
    <head>
        <title>Data Quality Report</title>
        <style>
            body { font-family: Arial, sans-serif; margin: 20px; }
            table { border-collapse: collapse; width: 100%; }
            th, td { border: 1px solid #dddddd; text-align: left; padding: 8px; }
            th { background-color: #f2f2f2; }
            .pass { color: green; font-weight: bold; }
            .fail { color: red; font-weight: bold; }
        </style>
    </head>
    <body>
        <h1>Data Quality Report (Gold Layer)</h1>
        <table>
            <tr>
                <th>Test Name</th>
                <th>Status</th>
                <th>Details</th>
            </tr>
    """
    
    for test_name, status, msg in results:
        css_class = "pass" if status == "PASS" else "fail"
        html_content += f"""
            <tr>
                <td>{test_name}</td>
                <td class="{css_class}">{status}</td>
                <td>{msg}</td>
            </tr>
        """
    
    html_content += "</table></body></html>"
    
    output_path = "/opt/airflow/dags/data_quality_report.html"
    with open(output_path, "w") as f:
        f.write(html_content)
    print(f">>> [HTML] Report saved at: {output_path}")

def run_validation():
    print(">>> [Data Quality] Downloading data from Gold (MinIO)...")
    try:
        df = pd.read_parquet("s3://gold/data/product_stats/", storage_options=STORAGE_OPTS)
    except Exception as e:
        print(f"Data read error: {e}")
        sys.exit(1)

    print(f"Downloaded {len(df)} rows.")
    
    results = []
    failures = []

    required_cols = ["product_name", "total_quantity", "avg_price"]
    missing = [c for c in required_cols if c not in df.columns]
    
    if not missing:
        msg = "All required columns are present."
        print(f">>> [PASS] {msg}")
        results.append(("Schema Check", "PASS", msg))
    else:
        msg = f"Missing columns: {missing}"
        print(f">>> [FAIL] {msg}")
        results.append(("Schema Check", "FAIL", msg))
        failures.append(msg)

    if "total_quantity" in df.columns:
        if not df["total_quantity"].isnull().any():
            msg = "No empty values (NULL)."
            print(f">>> [PASS] {msg}")
            results.append(("Null Check", "PASS", msg))
        else:
            msg = "NULL values detected in total_quantity column."
            print(f">>> [FAIL] {msg}")
            results.append(("Null Check", "FAIL", msg))
            failures.append(msg)

    if "avg_price" in df.columns:
        invalid_prices = df[~df["avg_price"].between(0, 100000)]
        if invalid_prices.empty:
            msg = "All prices in valid range."
            print(f">>> [PASS] {msg}")
            results.append(("Price Range Check", "PASS", msg))
        else:
            msg = f"Found {len(invalid_prices)} invalid prices!"
            print(f">>> [FAIL] {msg}")
            results.append(("Price Range Check", "FAIL", msg))
            failures.append(msg)

    generate_html_report(results)

    if failures:
        print("\n=== VALIDATION FAILED ===")
        sys.exit(1)
    else:
        print("\n=== VALIDATION SUCCESSFUL ===")

if __name__ == "__main__":
    run_validation()