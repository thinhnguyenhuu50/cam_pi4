import requests
from pathlib import Path

url = "http://sauray.abcsolutions.com.vn/api/Images/GetLatest"

response = requests.get(url, timeout=15)

if response.status_code == 200:
    # Open a new local file in "write binary" (wb) mode
    image_path = Path(__file__).resolve().parent / "downloaded_latest.jpg"
    with open(image_path, "wb") as f:
        f.write(response.content)  # Use .content for images!
    print(f"Success! Image saved as '{image_path}'")
else:
    print(f"Failed to fetch image. Status: {response.status_code}")