import cv2
import time
import requests
from datetime import datetime

# ================= CONFIGURATION =================

RTSP_URL = "rtsp://admin:cselabc5c6@192.168.1.200:554/Streaming/Channels/101"

# API Configuration
API_URL = "http://demo.quantraconline.com/api/Images/Upload"
STATION_CODE = "SMARTFARM"

# Capture interval (seconds)
INTERVAL = 600  # 10 minutes

# =================================================


def upload_to_api(image_bytes, filename, capture_time_str):
    """
    Upload image directly from RAM to API
    """

    print(f"[UPLOAD] Uploading {filename} to server...")

    try:

        # Multipart form-data
        files = {
            'images': (
                filename,
                image_bytes,
                'image/jpeg'
            )
        }

        # Form fields
        data = {
            'stationcode': STATION_CODE,
            'status': '4',
            'time': capture_time_str
        }

        # POST request
        response = requests.post(
            API_URL,
            files=files,
            data=data,
            timeout=30
        )

        # ================= SUCCESS =================

        if response.status_code == 200:

            print(f"[UPLOAD] Success!")
            print(f"[UPLOAD] Response: {response.text}")

        # ================= FAILED =================

        else:

            print(
                f"[UPLOAD] Failed. "
                f"Status code: {response.status_code}"
            )

            print(f"[UPLOAD] Body: {response.text}")

    except Exception as e:

        print(f"[UPLOAD] Error connecting to API: {e}")


def main():

    print("======================================")
    print("Camera Upload Service Started")
    print(f"API Target : {API_URL}")
    print(f"Interval   : {INTERVAL} seconds")
    print("======================================")

    while True:

        cap = None

        try:

            print("\n[INFO] Connecting to camera...")

            cap = cv2.VideoCapture(RTSP_URL)

            if not cap.isOpened():

                print("[ERROR] Camera connection failed.")

                time.sleep(10)

                continue

            # Flush old frames
            for _ in range(5):
                cap.read()

            # Capture frame
            success, frame = cap.read()

            if success:

                now = datetime.now()

                # Filename only for upload metadata
                filename = now.strftime("%Y-%m-%d_%H-%M-%S") + ".jpg"

                # API datetime format
                api_time_str = now.strftime("%d/%m/%Y %H:%M:%S")

                print("[SUCCESS] Frame captured.")

                # Encode JPEG directly in RAM
                success_encode, buffer = cv2.imencode(".jpg", frame)

                if not success_encode:

                    print("[ERROR] JPEG encoding failed.")

                else:

                    # Convert buffer to bytes
                    image_bytes = buffer.tobytes()

                    # Upload directly from memory
                    upload_to_api(
                        image_bytes,
                        filename,
                        api_time_str
                    )

            else:

                print("[ERROR] Failed to grab frame.")

        except Exception as e:

            print(f"[EXCEPTION] System error: {e}")

        finally:

            # Always release camera
            if cap is not None:
                cap.release()

        print(f"\n[WAIT] Sleeping for {INTERVAL} seconds...")

        time.sleep(INTERVAL)


if __name__ == "__main__":
    main()
