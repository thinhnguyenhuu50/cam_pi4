import cv2
import time
import requests
from datetime import datetime

# ================= CONFIGURATION =================

RTSP_URL = "rtsp://admin:cselabc5c6@192.168.1.200:554/Streaming/Channels/101"

BASE_URL = "https://sharefile.hpcc.vn"
LOGIN_URL = BASE_URL + "/api/login"

UPLOAD_FOLDER = "bkit-test"

USERNAME = "iot"
PASSWORD = "123456"

INTERVAL = 600  # 10 minutes

# =================================================


session = requests.Session()
session.verify = "/etc/ssl/certs/ca-certificates.crt"

token = None


def login():

    global token

    print("[LOGIN] Connecting server...")

    payload = {
        "username": USERNAME,
        "password": PASSWORD
    }

    try:

        r = session.post(
            LOGIN_URL,
            json=payload,
            timeout=30
        )

        if r.status_code == 200:

            token = r.text.strip()

            print("[LOGIN] Success")

            return True

        print("[LOGIN] Failed:", r.status_code, r.text)

        return False

    except Exception as e:

        print("[LOGIN] Error:", e)

        return False


def capture_image():

    print("[CAMERA] Connecting RTSP...")

    cap = cv2.VideoCapture(RTSP_URL)

    if not cap.isOpened():

        print("[CAMERA] Connection failed")

        return None

    try:

        # Flush old frames
        for _ in range(5):
            cap.read()

        ret, frame = cap.read()

        if not ret:

            print("[CAMERA] Capture failed")

            return None

        print("[CAMERA] Frame captured")

        # Encode JPEG directly in RAM
        success, buffer = cv2.imencode(".jpg", frame)

        if not success:

            print("[CAMERA] JPEG encode failed")

            return None

        filename = datetime.now().strftime("%Y-%m-%d_%H-%M-%S") + ".jpg"

        return buffer.tobytes(), filename

    finally:

        cap.release()


def upload_image(image_bytes, filename):

    global token

    url = f"{BASE_URL}/api/resources/{UPLOAD_FOLDER}/{filename}"

    headers = {
        "X-Auth": token,
        "Content-Type": "application/octet-stream"
    }

    print("[UPLOAD] Target:", url)

    try:

        r = session.post(
            url,
            headers=headers,
            data=image_bytes,
            timeout=60
        )

        # Token expired
        if r.status_code == 401:

            print("[UPLOAD] Token expired. Re-login required.")

            token = None

            return False

        print("[UPLOAD]", r.status_code, r.text)

        return r.status_code in [200, 201]

    except Exception as e:

        print("[UPLOAD] Error:", e)

        return False


def main():

    global token

    print("======================================")
    print("HPCC Camera Service Started")
    print(f"Upload Folder : {UPLOAD_FOLDER}")
    print(f"Interval      : {INTERVAL} seconds")
    print("======================================")

    while True:

        try:

            # Login if needed
            if not token:

                success = login()

                if not success:

                    print("[SYSTEM] Login failed")

                    time.sleep(30)

                    continue

            # Capture image in RAM only
            result = capture_image()

            if result:

                image_bytes, filename = result

                # Upload directly from memory
                upload_image(image_bytes, filename)

        except Exception as e:

            print("[SYSTEM ERROR]", e)

        print(f"[WAIT] Sleeping {INTERVAL} seconds...\n")

        time.sleep(INTERVAL)


if __name__ == "__main__":
    main()
