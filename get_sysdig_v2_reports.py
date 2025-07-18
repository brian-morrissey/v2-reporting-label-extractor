import os
import requests
import sys
import datetime
import pytz
import time
import gzip
import shutil
from time import time as get_time

def main():
    api_key = os.getenv("SECURE_API_KEY")
    if not api_key:
        print("SECURE_API_KEY environment variable not set.")
        return
    
    sysdig_tenant = os.getenv("SYSDIG_TENANT")
    if not sysdig_tenant:
        print("SYSDIG_TENANT environment variable not set.")
        return
    
    api_url = f"https://{sysdig_tenant}/api/platform/reporting/v1/reports"

    headers = {
        "Authorization": f"Bearer {api_key}"
    }

    response = requests.get(api_url, headers=headers)
    if response.status_code != 200:
        print(f"Request failed with status code {response.status_code}: {response.text}")
        return

    data = response.json()
    
    # Check if any valid command line arguments are provided
    if "--list" not in sys.argv and "--id" not in sys.argv:
        print("Error: No command line arguments specified.")
        print("Usage:")
        print("  --list              List all available reports")
        print("  --id <report_id>    Generate report for specified ID")
        return
    
    # Build a set of valid report IDs for validation
    valid_report_ids = {report.get("id") for report in data if report.get("id") is not None}
    
    if "--list" in sys.argv:
        for report in data:
            report_id = report.get("id")
            report_name = report.get("name")
            print(f"ID: {report_id}, Name: {report_name}")

    if "--id" in sys.argv:
        try:
            id_index = sys.argv.index("--id") + 1
            report_id = int(sys.argv[id_index])
        except (ValueError, IndexError):
            print("Please provide a valid report id after --id.")
            return
        
        # Validate that the report ID exists
        if report_id not in valid_report_ids:
            print(f"Error: Report ID {report_id} not found in available reports.")
            print("Use --list to see available report IDs.")
            return


        # Set timezone to America/New_York
        tz = pytz.timezone("America/New_York")
        now = datetime.datetime.now(tz)
        scheduled_on = now.isoformat()

        # Latest 24 hours (in seconds since epoch)
        to_ts = int(get_time())
        from_ts = to_ts - 24 * 60 * 60

        payload = {
            "jobType": "ON_DEMAND",
            "reportFormat": "csv",
            "compression": "gzip",
            "scheduledOn": scheduled_on,
            "zones": [],
            "timeFrame": {
                "from": from_ts,
                "to": to_ts
            },
            "reportId": report_id,
            "isReportTemplate": False,
            "jobName": "Kubernetes Workload Vulnerability Findings",
            "fileName": "Kubernetes Workload Vulnerability Findings",
            "timezone": "America/New_York"
        }

        jobs_url = f"https://{sysdig_tenant}/api/platform/reporting/v1/jobs"
        post_response = requests.post(jobs_url, headers=headers, json=payload)
        if post_response.status_code != 200:
            print(f"POST failed with status code {post_response.status_code}: {post_response.text}")
            return
        
        job_data = post_response.json()
        print("Reporting Job created successfully")

        # Extract job ID from response
        job_id = job_data.get("id")
        if not job_id:
            print("Error: No job ID returned in response")
            return
        
        print(f"Job ID: {job_id}")a
        print("Polling job status every 30 seconds...")
        
        # Poll job status until complete
        status_url = f"https://{sysdig_tenant}/api/platform/reporting/v1/jobs/{job_id}"
        
        # Set timeout to 2 hours (7200 seconds)
        timeout_seconds = 2 * 60 * 60  # 2 hours
        start_polling_time = get_time()
        
        while True:
            time.sleep(30)  # Wait 30 seconds before checking
            
            # Check if we've exceeded the 2-hour timeout
            elapsed_time = get_time() - start_polling_time
            if elapsed_time > timeout_seconds:
                print(f"Timeout: Job polling exceeded 2 hours ({elapsed_time/3600:.1f} hours). Stopping.")
                print(f"Job ID {job_id} may still be running. Check manually if needed.")
                return
            
            status_response = requests.get(status_url, headers=headers)
            if status_response.status_code != 200:
                print(f"Status check failed with status code {status_response.status_code}: {status_response.text}")
                return
            
            status_data = status_response.json()
            current_status = status_data.get("status")
            print(f"Current job status: {current_status}")
            
            if current_status == "COMPLETED":
                print("Job completed successfully!")
                
                # Get file path and download
                file_path = status_data.get("filePath")
                if file_path:
                    print(f"Downloading file from: {file_path}")
                    
                    # Download the file
                    download_response = requests.get(file_path, headers=headers)
                    if download_response.status_code == 200:
                        # Extract filename from URL or use a default
                        filename = "v2-report.csv.gz"
                        
                        with open(filename, "wb") as f:
                            f.write(download_response.content)
                        print(f"File downloaded successfully: {filename}")
                        
                        # Extract the gzip file
                        try:
                            extracted_filename = "v2-report.csv"
                            with gzip.open(filename, 'rb') as f_in:
                                with open(extracted_filename, 'wb') as f_out:
                                    shutil.copyfileobj(f_in, f_out)
                            print(f"File extracted successfully: {extracted_filename}")
                        except Exception as e:
                            print(f"Error extracting file: {e}")
                    else:
                        print(f"Download failed with status code {download_response.status_code}")
                else:
                    print("Error: No file path found in job response")
                break
            
            elif current_status in ["FAILED"]:
                print(f"Job {current_status.lower()}. Stopping polling.")
                break



if __name__ == "__main__":
    main()
