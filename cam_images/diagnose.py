import requests

# url = "http://sauray.abcsolutions.com.vn/api/Images/GetLatest"
url = "http://sauray.abcsolutions.com.vn/api/Images/GetLatest/5488"

# Add the parameters here. I am using 'SMARTFARM' based on your previous upload code.
# You may also need to check Postman to see if the parameter is capitalized or lowercase (e.g., 'stationCode' vs 'stationcode')
payload = {'stationcode': 'SMARTFARM', 'status': '4'}

print("Fetching latest image info for SMARTFARM...")
# response = requests.get(url, params=payload, timeout=10)
response = requests.get(url, data={'stationcode': 'SMARTFARM'})

print(f"Status Code: {response.status_code}")
print(f"Content-Type: {response.headers.get('Content-Type')}")

if 'application/json' in response.headers.get('Content-Type', ''):
    print("\nResult: The server returned JSON data.")
    print(response.json()) 
else:
    print("\nResult: Unknown format.")
    print(response.text[:200])
