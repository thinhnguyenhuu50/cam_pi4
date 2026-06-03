import requests
from datetime import datetime
from pathlib import Path

# Your exact API details
API_URL = "http://sauray.abcsolutions.com.vn/api/Images/Upload"
STATION_CODE = "SAURAY1"

def test_upload():
    print(f"[TEST] Preparing to upload to {API_URL}...")
    
    # 1. Load the local test image
    try:
        image_path = Path(__file__).resolve().parent / "test_image.jpg"
        with open(image_path, "rb") as image_file:
            image_bytes = image_file.read()
    except FileNotFoundError:
        print("[ERROR] Please put a 'test_image.jpg' in the same folder as this script.")
        return

    # 2. Generate timestamps
    now = datetime.now()
    filename = now.strftime("%Y-%m-%d_%H-%M-%S_TEST.jpg")
    api_time_str = now.strftime("%d/%m/%Y %H:%M:%S")

    # 3. Format exactly as your main script does
    files = {
        'images': (filename, image_bytes, 'image/jpeg')
    }
    data = {
        'stationcode': STATION_CODE,
        'status': '4',
        'time': api_time_str
    }

    # 4. Send the request
    try:
        response = requests.post(API_URL, files=files, data=data, timeout=15)
        
        print("\n--- RESULTS ---")
        print(f"HTTP Status Code : {response.status_code}")
        print(f"Server Response  : {response.text}")
        
        if response.status_code == 200:
            print("\n✅ SUCCESS! The API accepted the image.")
        else:
            print("\n❌ FAILED. The API rejected the upload. Read the Server Response above to see why.")
            
    except Exception as e:
        print(f"\n❌ CRITICAL ERROR: Could not connect to the server. Details: {e}")

if __name__ == "__main__":
    test_upload()