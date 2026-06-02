import cv2
import requests
import time
from datetime import datetime

# ================= CONFIGURATION =================

RTSP_URL = "rtsp://admin:Duy15112001@192.168.1.64:554/Streaming/Channels/101"

# HPCC Camera API
API_URL = "https://xanhmarket-camera.hpcc.vn/camera"
LATEST_IMAGE_API = "https://xanhmarket-camera.hpcc.vn/camera/latest-image"

DEVICE_NAME = "tomochanfarm-1"

# Capture interval (seconds)
INTERVAL = 600  # 10 minutes

# =================================================


def capture_and_push():

    print(f"\n[{datetime.now()}] Connecting to camera...")

    # Open RTSP stream
    cap = cv2.VideoCapture(RTSP_URL)

    if not cap.isOpened():
        print("[ERROR] Cannot connect to camera.")
        return

    try:

        # Flush old frames
        for _ in range(5):
            cap.read()

        # Read latest frame
        success, frame = cap.read()

        if not success:
            print("[ERROR] Failed to capture frame.")
            return

        print("[SUCCESS] Frame captured.")

        # Encode image directly in RAM
        success_encode, buffer = cv2.imencode(".jpg", frame)

        if not success_encode:
            print("[ERROR] Failed to encode JPEG.")
            return

        print("[INFO] Uploading image...")

        # Multipart upload
        files = {
            'image': (
                f'{DEVICE_NAME}.jpg',
                buffer.tobytes(),
                'image/jpeg'
            )
        }

        # Form data
        data = {
            'device': DEVICE_NAME
        }

        # Send upload request
        response = requests.post(
            API_URL,
            files=files,
            data=data,
            timeout=30
        )

        # ================= SUCCESS =================

        if response.status_code == 200:

            print("[UPLOAD] Success!")
            print(f"[UPLOAD] Server Response: {response.text}")

        # ================= FAILED =================

        else:

            print(
                f"[UPLOAD] Failed - "
                f"Status Code: {response.status_code}"
            )

            print(f"[UPLOAD] Response: {response.text}")

    except Exception as e:

        print(f"[EXCEPTION] Upload error: {e}")

    finally:

        # Always release camera
        cap.release()


def check_latest_image():

    try:

        response = requests.get(
            f"{LATEST_IMAGE_API}?device={DEVICE_NAME}",
            timeout=10
        )

        if response.status_code == 200:

            data = response.json()

            print("\n[LATEST IMAGE]")
            print(f"Timestamp : {data.get('timestamp')}")
            print(f"URL       : {data.get('url')}")

        else:

            print(f"[LATEST IMAGE] Failed: {response.status_code}")

    except Exception as e:

        print(f"[LATEST IMAGE] Error: {e}")


if __name__ == "__main__":

    print("================================================")
    print("HPCC Camera Streaming Service Started")
    print(f"API URL     : {API_URL}")
    print(f"Device Name : {DEVICE_NAME}")
    print(f"Interval    : {INTERVAL} seconds")
    print("================================================")

    while True:

        try:

            # Capture and upload image
            capture_and_push()

            # Verify latest uploaded image
            check_latest_image()

        except Exception as e:

            print(f"[CRITICAL] System error: {e}")

        print(f"\n[WAIT] Sleeping for {INTERVAL} seconds...")
        time.sleep(INTERVAL)
