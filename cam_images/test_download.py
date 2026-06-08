import requests

url = "http://sauray.abcsolutions.com.vn/api/Images/GetLatest"

# 1. Ask the API for the latest image data
response = requests.get(url, timeout=10)

if response.status_code == 200:
    data = response.json()

    # 2. Extract the URL from the JSON (handle dict or list responses)
    if isinstance(data, list):
        if not data:
            print("JSON response list is empty.")
            raise SystemExit(1)
        item = data[0]
    else:
        item = data

    if not isinstance(item, dict):
        print(f"Unexpected JSON item type: {type(item).__name__}")
        raise SystemExit(1)

    # You may need to change 'imageUrl' to the actual key from your API
    image_url = item.get("imageUrl")
    
    if image_url:
        print(f"Found image URL: {image_url}")
        
        # 3. Download the actual image from that URL
        img_response = requests.get(image_url)
        if img_response.status_code == 200:
            with open("downloaded_latest.jpg", "wb") as f:
                f.write(img_response.content)
            print("Success! Image downloaded and saved.")
        else:
            print("Failed to download the image from the provided URL.")
    else:
        print("JSON response did not contain an image URL.")
else:
    print(f"API request failed. Status: {response.status_code}")