from fastapi import FastAPI
from fastapi import WebSocket
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from starlette.background import BackgroundTask 
import qrcode
import os, json, hashlib, socket
from jsonschema import validate, ValidationError
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import asyncio
import threading
import tempfile
from pydantic import BaseModel
import subprocess
import base64
from PIL import Image, ImageDraw  
import io
import time
from datetime import datetime
import shutil

import logging
import logging.handlers
from pathlib import Path

import fcntl

CEC_LOCK_FILE = '/tmp/cec.lock'

async def acquire_flock():
    fcntl.flock(CEC_LOCK_FILE, fcntl.LOCK_EX)

async def release_flock():
    fcntl.flock(CEC_LOCK_FILE, fcntl.LOCK_UN)

#script basepath irrespective of where it was run from
server_base_path = os.path.dirname(os.path.abspath(__file__))
eka_app_path = os.path.join(server_base_path, "../eka-app")
resources_dir = os.path.join(eka_app_path, "resources")
settings_file_path = os.path.join(server_base_path, "../eka-settings/eka-settings.json")
cloud_connect_url = "http://localhost:8001"
device_serial=""
device_mac=""
registration_status=False
connectivity_status=None
playlist_updated=False
show_qr_code=False
frontend_message=""
version="1.0.0"
show_screen_logs=True

SETTINGS_DISCONNECT_THRESHOLD = 0  # default set in process_settings
SETTINGS_WIFI_RECOVERY_CYCLE_INTERVAL = 7200  # default 2 hours; overridden by process_settings
TIER_2_DELAY_AFTER_TIER_1 = 300  # 5 minutes — gives nmcli's reconnect attempt time to either succeed or clearly fail

# Tracks the last status we set via tv_status_set_async. None on process start.
# Used as the Pi-side edge guard for the HDMI modeset (covers boot, AUTO transition,
# manual reboot, etc.). The CEC pow-0 pre-query inside tv_status_set_async additionally
# covers TV-initiated off->on transitions (e.g. user presses OFF on the TV remote).
# See memory: hdmi-display-desync-fix.
_last_tv_status = None

ws_update_event = asyncio.Event()
send_status_background_tasks = set()

#----------------------------Logging---------------------------------------------------------#
def setup_logging():
    # Set up verbose debugging logging
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - [%(filename)s:%(funcName)s:%(lineno)d] - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    logger = logging.getLogger('eka-local_server')
    logger.setLevel(logging.DEBUG)  # Set to DEBUG for verbose logging
    logger.propagate = False

    Path("/var/log/eka").mkdir(parents=True, exist_ok=True)
    
    # File handler for debug log
    file_handler = logging.handlers.RotatingFileHandler(
        Path("/var/log/eka/eka-local_server-debug.log"),
        maxBytes=10*1024*1024,  # Increased to 10MB for debug logs
        backupCount=10  # Keep more backups for debugging
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(
        logging.Formatter('%(asctime)s - PID:%(process)d - TID:%(thread)d - [%(filename)s:%(funcName)s:%(lineno)d] - %(levelname)s - %(message)s')
    )

    # Console handler for verbose output
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)
    console_handler.setFormatter(
        logging.Formatter('%(asctime)s - [%(filename)s:%(funcName)s:%(lineno)d] - %(levelname)s - %(message)s',
                         datefmt='%H:%M:%S')
    )

    logger.handlers = []
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    # Log system information for debugging context
    import sys
    import platform
    logger.info("="*80)
    logger.info("EKA LOCAL SERVER - DEBUG MODE ENABLED")
    logger.info("="*80)
    logger.info(f"Python version: {sys.version}")
    logger.info(f"Platform: {platform.platform()}")
    logger.info(f"Command line args: {sys.argv}")
    logger.info(f"Current working directory: {os.getcwd()}")
    logger.info(f"Script path: {__file__}")
    logger.info("="*80)
       
    return logger

logger=setup_logging()

def get_device_mac():
    global device_mac, logger
    logger.debug("Starting device MAC address retrieval")
    try:
        mac_file_path = "/sys/class/net/wlan0/address"
        logger.debug(f"Reading MAC address from: {mac_file_path}")
        
        if not os.path.exists(mac_file_path):
            logger.error(f"MAC address file not found: {mac_file_path}")
            return False
            
        with open(mac_file_path, "r") as f:
            device_mac = f.read().strip()
            logger.info(f"Device MAC address retrieved: {device_mac}")
            logger.debug(f"MAC address length: {len(device_mac)} characters")
            return True
    except Exception as e:
        logger.error(f"Error reading device MAC address: {e}", exc_info=True)
        device_mac = ""
        return False
    

def get_device_serial():
    global device_serial, logger
    logger.debug("Starting device serial number retrieval")
    logger.debug(f"Cloud connect URL: {cloud_connect_url}")
    
    # use requests module to get the serial number from the cloud connect server at http://localhost:8001/serial
    # response format is {"serial_number": self.serial_number}
    try:
        request_url = f'{cloud_connect_url}/serial'
        logger.debug(f"Making request to: {request_url}")
        
        response = requests.get(request_url, timeout=5)
        logger.debug(f"Response status code: {response.status_code}")
        logger.debug(f"Response headers: {dict(response.headers)}")
        
        response.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx)
        
        response_data = response.json()
        logger.debug(f"Response JSON: {response_data}")
        
        device_serial = response_data["serial_number"]
        logger.info(f"Device serial number retrieved: {device_serial}")
        logger.debug(f"Serial number length: {len(device_serial)} characters")
        return True
    except requests.exceptions.Timeout as e:
        logger.error(f"Timeout getting serial number from {cloud_connect_url}: {e}")
        return False
    except requests.exceptions.ConnectionError as e:
        logger.error(f"Connection error getting serial number: {e}")
        return False
    except requests.exceptions.HTTPError as e:
        logger.error(f"HTTP error getting serial number: {e}")
        return False
    except KeyError as e:
        logger.error(f"Serial number not found in response: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error getting serial number: {e}", exc_info=True)
        return False

def generate_qr_code(data):
    global eka_app_path, logger
    logger.debug(f"Starting QR code generation for data: {data}")
    logger.debug(f"QR code data length: {len(data)} characters")
    
    try:
        qr = qrcode.QRCode(
            version=1,
            error_correction=qrcode.constants.ERROR_CORRECT_H,
            box_size=8,
            border=4,
        )
        logger.debug("QR code object created with parameters")
        
        qr.add_data(data)
        logger.debug("Data added to QR code")
        
        qr.make(fit=True)
        logger.debug("QR code optimized and finalized")

        img = qr.make_image(fill_color="black", back_color="white")
        logger.debug(f"QR code image created with size: {img.size}")
        
        qr_image_path = os.path.join(eka_app_path, "devmac_qr.png")
        logger.debug(f"Saving QR code to: {qr_image_path}")
        
        img.save(qr_image_path)
        
        # Verify file was created
        if os.path.exists(qr_image_path):
            file_size = os.path.getsize(qr_image_path)
            logger.info(f"QR Code generated successfully - Size: {file_size} bytes")
            logger.debug(f"QR Code saved at: {qr_image_path}")
        else:
            logger.error("QR code file was not created successfully")
            
    except Exception as e:
        logger.error(f"Error generating QR code: {e}", exc_info=True)
            
def generate_status_qr(status=""):
    global device_mac, logger
    logger.debug(f"Starting status QR code generation with status: '{status}'")
    logger.debug(f"Device MAC: {device_mac}")
    logger.debug(f"Device Serial: {device_serial}")
    
    qr_string = f"{device_mac}|{device_serial}"
    if status:
        qr_string += f"|{status}"
        logger.debug(f"Status added to QR string")
    
    logger.info(f"Generating QR code with string: {qr_string}")
    generate_qr_code(qr_string)

def display_message(message):
    global frontend_message, logger
    logger.debug(f"Setting frontend message: '{message}'")
    frontend_message = message
    logger.debug("Triggering WebSocket update event")
    ws_update_event.set()
    logger.info(f"Frontend message updated and event triggered: {message}")

async def query_tv_power_via_cec(env, debug_msg='', retries=3, retry_delay=1.0):
    """Query the TV's actual power state via CEC `pow 0`.

    Returns one of 'on', 'standby', 'transition', or 'unknown' (all retries errored
    or produced no parseable status). Caller is responsible for CEC bus locking.
    """
    for attempt in range(retries):
        try:
            proc = await asyncio.create_subprocess_exec(
                "cec-client", "-s", "-d", "1", "-o", "ekadevice",
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                env=env
            )
            stdout, _ = await proc.communicate(input=b"pow 0\n")
            out = stdout.decode().lower()
            if "power status: on" in out:
                logger.debug(debug_msg + f"query_tv_power attempt {attempt+1}: on")
                return "on"
            if "power status: standby" in out:
                logger.debug(debug_msg + f"query_tv_power attempt {attempt+1}: standby")
                return "standby"
            if "in transition" in out:
                logger.debug(debug_msg + f"query_tv_power attempt {attempt+1}: transition")
                return "transition"
            logger.debug(debug_msg + f"query_tv_power attempt {attempt+1}: no parseable status in output")
        except Exception as e:
            logger.debug(debug_msg + f"query_tv_power attempt {attempt+1} exception: {e}")
        if attempt < retries - 1:
            await asyncio.sleep(retry_delay)
    return "unknown"


async def tv_status_set_async(status: bool, use_lock=True, debug_msg='normal: '):
    """Asynchronous version of tv_status_set that runs in the background"""
    global _last_tv_status
    logger.debug(debug_msg + f"Starting TV status change to: {'ON' if status else 'OFF'}")

    try:
        env = os.environ.copy()
        if 'XDG_RUNTIME_DIR' not in env:
            env['XDG_RUNTIME_DIR'] = f'/run/user/1000' #ekausers uid is 1000
            logger.debug(debug_msg + "Set XDG_RUNTIME_DIR to /run/user/1000")

        if status:
            logger.info(debug_msg + "Turning TV ON - sending CEC and display commands")

            # Use asyncio.create_subprocess_exec for better async behavior
            logger.debug(debug_msg + "Starting CEC client to turn TV on")

            # Pre-query the TV's actual power state so we can catch off->on transitions
            # the Pi didn't initiate (Scenario 7 — user presses OFF on the TV remote).
            # "unknown" (all retries errored) falls back to Pi-side edge only.
            tv_power_before = "unknown"

            if use_lock:
                logger.debug(debug_msg + "local server tv_status_set_async() ON, opening lock file...")
                with open(CEC_LOCK_FILE, 'w') as lockfile:
                    try:
                        # Try to acquire the lock (blocking)
                        logger.debug(debug_msg + "local server tv_status_set_async() ON, attempting to acquire lock...")
                        # fcntl.flock(lockfile, fcntl.LOCK_EX)
                        await asyncio.to_thread(acquire_flock)
                        logger.debug(debug_msg + "local server tv_status_set_async() ON, lock acquired.")

                        tv_power_before = await query_tv_power_via_cec(env, debug_msg=debug_msg + "pre-query: ")
                        logger.debug(debug_msg + f"TV power state before CEC as: {tv_power_before}")

                        cec_proc = await asyncio.create_subprocess_exec(
                            "cec-client", "-s", "-d", "1", "-o", "ekadevice",
                            stdin=asyncio.subprocess.PIPE,
                            stdout=asyncio.subprocess.PIPE,
                            stderr=asyncio.subprocess.PIPE,
                            env=env
                        )
                        stdout, stderr = await cec_proc.communicate(input=b"as\n")
                        logger.debug(debug_msg + f"CEC ON command - return code: {cec_proc.returncode}")
                        logger.debug(debug_msg + f"CEC ON stdout: {stdout.decode()}")
                        if stderr:
                            logger.debug(debug_msg + f"CEC ON stderr: {stderr.decode()}")

                    except Exception as e:
                        logger.debug(debug_msg + f"tv_status_set_async() ON Unexpected error: {e}")

                    finally:
                        logger.debug(debug_msg + "local server tv_status_set_async() ON, attempting to release lock...")
                        # fcntl.flock(lockfile, fcntl.LOCK_UN)
                        await asyncio.to_thread(release_flock)
                        logger.debug(debug_msg + "local server tv_status_set_async() ON, lock released.")
            else:
                logger.debug(debug_msg + "NO LOCK, running cec-client")
                tv_power_before = await query_tv_power_via_cec(env, debug_msg=debug_msg + "pre-query: ")
                logger.debug(debug_msg + f"TV power state before CEC as: {tv_power_before}")
                cec_proc = await asyncio.create_subprocess_exec(
                            "cec-client", "-s", "-d", "1", "-o", "ekadevice",
                            stdin=asyncio.subprocess.PIPE,
                            stdout=asyncio.subprocess.PIPE,
                            stderr=asyncio.subprocess.PIPE,
                            env=env
                        )
                stdout, stderr = await cec_proc.communicate(input=b"as\n")
                logger.debug(debug_msg + f"CEC ON command - return code: {cec_proc.returncode}")
                logger.debug(debug_msg + f"CEC ON stdout: {stdout.decode()}")
                if stderr:
                    logger.debug(debug_msg + f"CEC ON stderr: {stderr.decode()}")

            # Decide whether to re-apply the HDMI modeset. Two independent triggers:
            #   - tv_power_before is standby/transition: TV was off, CEC `as` just woke it
            #     (covers Scenario 7 even when _last_tv_status didn't change)
            #   - _last_tv_status != status: Pi-side edge (boot None->True, AUTO False->True, etc.)
            # If CEC pre-query returned "unknown" (all retries errored), we fall back to the
            # Pi-side edge alone — avoids spurious flicker on transient CEC failures.
            tv_was_off = tv_power_before in ("standby", "transition")
            if tv_was_off or _last_tv_status != status:
                logger.info(debug_msg + f"Running wlr-randr modeset (tv_was_off={tv_was_off}, last_status={_last_tv_status})")
                # Brief settle window so the TV is past mid-wake when modeset lands.
                await asyncio.sleep(3)
                wlr_proc = await asyncio.create_subprocess_exec(
                    "wlr-randr", "--output", "HDMI-A-1", "--custom-mode", "1920x1080@60Hz",
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                    env=env
                )
                wlr_stdout, wlr_stderr = await wlr_proc.communicate()
                logger.debug(debug_msg + f"wlr-randr modeset return code: {wlr_proc.returncode}")
                if wlr_stdout:
                    logger.debug(debug_msg + f"wlr-randr stdout: {wlr_stdout.decode()}")
                if wlr_stderr:
                    logger.debug(debug_msg + f"wlr-randr stderr: {wlr_stderr.decode()}")


        else:
            logger.info(debug_msg + "Turning TV OFF - sending CEC and display commands")
            
            logger.debug(debug_msg + "Starting CEC client to turn TV off")
            
            if use_lock:
                logger.debug(debug_msg + "local server tv_status_set_async() OFF, opening lock file...")
                with open(CEC_LOCK_FILE, 'w') as lockfile:
                    try:
                        # Try to acquire the lock (blocking)
                        logger.debug(debug_msg + "local server tv_status_set_async() OFF, attempting to acquire lock...")
                        # fcntl.flock(lockfile, fcntl.LOCK_EX)
                        await asyncio.to_thread(acquire_flock)
                        logger.debug(debug_msg + "local server tv_status_set_async() OFF, lock acquired.")
                        
                        cec_proc = await asyncio.create_subprocess_exec(
                            "cec-client", "-s", "-d", "1", "-o", "ekadevice",
                            stdin=asyncio.subprocess.PIPE,
                            stdout=asyncio.subprocess.PIPE,
                            stderr=asyncio.subprocess.PIPE,
                            env=env
                        )
                        stdout, stderr = await cec_proc.communicate(input=b"standby 0\n") 
                        logger.debug(debug_msg + f"CEC OFF command - return code: {cec_proc.returncode}")
                        logger.debug(debug_msg + f"CEC OFF stdout: {stdout.decode()}")
                        if stderr:
                            logger.debug(debug_msg + f"CEC OFF stderr: {stderr.decode()}")
                    
                    except Exception as e:
                        logger.debug(debug_msg + f"tv_status_set_async() OFF Unexpected error: {e}")

                    finally:
                        logger.debug(debug_msg + "local server tv_status_set_async() OFF, attempting to release lock...")
                        # fcntl.flock(lockfile, fcntl.LOCK_UN)
                        await asyncio.to_thread(release_flock)
                        logger.debug(debug_msg + "local server tv_status_set_async() OFF, lock released.")
            else:
                logger.debug(debug_msg + "NO LOCK, running cec-client")
                cec_proc = await asyncio.create_subprocess_exec(
                    "cec-client", "-s", "-d", "1", "-o", "ekadevice",
                    stdin=asyncio.subprocess.PIPE,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                    env=env
                )
                stdout, stderr = await cec_proc.communicate(input=b"standby 0\n")
                logger.debug(debug_msg + f"CEC OFF command - return code: {cec_proc.returncode}")
                logger.debug(debug_msg + f"CEC OFF stdout: {stdout.decode()}")
                if stderr:
                    logger.debug(debug_msg + f"CEC OFF stderr: {stderr.decode()}")
            
            # logger.debug(debug_msg + "Starting wlr-randr to turn display off")
            # wlr_proc = await asyncio.create_subprocess_exec(
            #     "wlr-randr", "--output", "HDMI-A-1", "--off",
            #     stdout=asyncio.subprocess.PIPE,
            #     stderr=asyncio.subprocess.PIPE,
            #     env=env
            # )
            # stdout, stderr = await wlr_proc.communicate()
            # logger.debug(debug_msg + f"WLR OFF command - return code: {wlr_proc.returncode}")
            # logger.debug(debug_msg + f"WLR OFF stdout: {stdout.decode()}")
            # if stderr:
            #     logger.debug(debug_msg + f"WLR OFF stderr: {stderr.decode()}")
            
        _last_tv_status = status
        logger.info(debug_msg + f"TV status change completed successfully: {'ON' if status else 'OFF'}")
        return True
    except Exception as e:
        logger.error(debug_msg + f"Error setting TV status to {'ON' if status else 'OFF'}: {e}", exc_info=True)
        return False

def tv_status_set(status: bool, use_lock=True, debug_msg='normal: '):
    """Non-blocking wrapper for tv_status_set_async"""
    logger.debug(debug_msg + f"TV status set called with status: {'ON' if status else 'OFF'}")
    
    # Create a task that will run in the background
    try:
        # Get or create an event loop
        try:
            loop = asyncio.get_running_loop()
            logger.debug(debug_msg + "Using existing event loop")
        except RuntimeError:
            # If no event loop exists, create one
            logger.debug(debug_msg + "Creating new event loop")
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        
        # Create task without waiting for it
        task = asyncio.create_task(tv_status_set_async(status, use_lock=use_lock, debug_msg=debug_msg))
        logger.debug(debug_msg + f"Created background task for TV status change: {task}")
        
        # Optional: Add a callback to handle errors
        def handle_task_result(task):
            try:
                result = task.result()  # This will raise any exceptions from the task
                logger.debug(debug_msg + f"Background TV status task completed successfully: {result}")
            except Exception as e:
                logger.error(debug_msg + f"Background TV status task error: {e}", exc_info=True)
                
        task.add_done_callback(handle_task_result)
        logger.debug(debug_msg + "Added callback handler to TV status task")
        
        return True
    except Exception as e:
        logger.error(debug_msg + f"Failed to start TV status task: {e}", exc_info=True)
        # Fall back to synchronous execution if creating the task fails
        logger.warning(debug_msg + "Falling back to synchronous TV status execution")
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            result = loop.run_until_complete(tv_status_set_async(status, use_lock=use_lock, debug_msg=debug_msg))
            logger.debug(debug_msg + f"Synchronous TV status change result: {result}")
            return result
        except Exception as sync_error:
            logger.error(debug_msg + f"Synchronous TV status change also failed: {sync_error}", exc_info=True)
            return False

def tv_power_get() -> bool:
    logger.debug("Checking TV power status")
    
    try:
        logger.debug("tv power get() local server (shouldn't be here ?)")
        command = "echo pow 0.0.0.0 | cec-client -s -d 1"
        logger.debug(f"Executing command: {command}")
        
        # Use shell=True when using pipe operators
        result = subprocess.run(command, 
                            shell=True, 
                            capture_output=True, 
                            text=True,
                            timeout=10)
        
        logger.debug(f"Command return code: {result.returncode}")
        logger.debug(f"Command stdout: {result.stdout}")
        logger.debug(f"Command stderr: {result.stderr}")
        
        if "power status: on" in result.stdout:
            logger.info("TV power status: ON")
            return True
        else:
            logger.info("TV power status: OFF")
            logger.debug(f"Full output for OFF status: {result.stdout}")
            return False
    except subprocess.TimeoutExpired:
        logger.error("TV power check timed out after 10 seconds")
        return False
    except Exception as e:
        logger.error(f"Error getting TV power status: {e}", exc_info=True)
        return False

def atomic_json_write(data, filename):
    """Write JSON data atomically to avoid file corruption."""
    tmpfile = filename + ".tmp"
    with open(tmpfile, "w") as f:
        json.dump(data, f)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmpfile, filename)

def safe_remove(path):
    """Remove a file, ignoring errors if it doesn't exist."""
    try:
        os.remove(path)
    except FileNotFoundError:
        pass
    except Exception as e:
        logger.error(f"Error removing file {path}: {e}")

def download_file(url, dest_path, max_retries=5, backoff_factor=0.3):
    global logger
    logger.debug(f"Starting file download from: {url}")
    logger.debug(f"Destination path: {dest_path}")
    logger.debug(f"Max retries: {max_retries}, Backoff factor: {backoff_factor}")
    
    retry_strategy = Retry(
        total=max_retries,
        backoff_factor=backoff_factor,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["HEAD", "GET", "OPTIONS"]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session = requests.Session()
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    
    tmp_dest = dest_path + ".tmp"
    logger.debug(f"Using temporary file: {tmp_dest}")
    
    try:
        logger.debug("Making HTTP request...")
        response = session.get(url, stream=True, timeout=10)
        logger.debug(f"Response status: {response.status_code}")
        logger.debug(f"Response headers: {dict(response.headers)}")
        
        # Log content information
        content_length = response.headers.get('content-length')
        if content_length:
            logger.debug(f"Content length: {content_length} bytes")
            
        response.raise_for_status()
        
        bytes_downloaded = 0
        with open(tmp_dest, "wb") as file:
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)
                bytes_downloaded += len(chunk)
                
        logger.debug(f"Downloaded {bytes_downloaded} bytes")
        logger.debug(f"Moving {tmp_dest} to {dest_path}")
        os.replace(tmp_dest, dest_path)
        
        # Verify final file
        if os.path.exists(dest_path):
            final_size = os.path.getsize(dest_path)
            logger.info(f"File download completed successfully: {final_size} bytes")
            return True
        else:
            logger.error("File was not created after download")
            return False
            
    except requests.exceptions.RequestException as e:
        logger.error(f"Request exception downloading file from {url}: {e}")
        safe_remove(tmp_dest)
        return False
    except Exception as e:
        logger.error(f"Unexpected error downloading file from {url}: {e}", exc_info=True)
        safe_remove(tmp_dest)
        return False

def check_file_hash(file_path, expected_hash):
    global logger
    logger.debug(f"Checking file hash for: {file_path}")
    logger.debug(f"Expected hash: {expected_hash}")
    
    if not os.path.exists(file_path):
        logger.error(f"File does not exist for hash check: {file_path}")
        return False, None
        
    file_size = os.path.getsize(file_path)
    logger.debug(f"File size: {file_size} bytes")
    
    sha256_hash = hashlib.sha256()
    bytes_processed = 0
    
    try:
        with open(file_path, "rb") as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
                bytes_processed += len(byte_block)
                
        file_hash = sha256_hash.hexdigest()
        logger.debug(f"Processed {bytes_processed} bytes")
        logger.debug(f"Calculated hash: {file_hash}")
        
        hash_match = file_hash == expected_hash
        logger.info(f"Hash verification {'PASSED' if hash_match else 'FAILED'} for {file_path}")
        
        if not hash_match:
            logger.warning(f"Hash mismatch - Expected: {expected_hash}, Got: {file_hash}")
            
        return hash_match, file_hash
    except Exception as e:
        logger.error(f"Error checking file hash for {file_path}: {e}", exc_info=True)
        return False, None

def create_fallback_playlist(playlist_file=os.path.join(resources_dir, "playlist.json")):
    global playlist_updated, logger
    logo_file_src = os.path.join(eka_app_path, "logo.jpg")
    logo_file_dest = os.path.join(resources_dir, "logo.jpg")
    try:
        if not os.path.exists(logo_file_dest):
            shutil.copy2(logo_file_src, logo_file_dest)
        with open(logo_file_dest, "rb") as f:
            sha256_hash = hashlib.sha256()
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
            logo_hash = sha256_hash.hexdigest()
        playlist = {
            "playlist": {
                "repeat": True,
                "id": 0, # THIS NEEDS TO BE 0, DON'T CHANGE
                "ads": [
                    {
                        "slot_id": "1",
                        "image_file": "logo.jpg",
                        "image_hash": logo_hash,
                        "image_url": "https://eka.com/logo.jpg",
                        "duration_seconds": 600
                    }
                ]
            }
        }
        atomic_json_write(playlist, playlist_file)
        playlist_updated = True
        ws_update_event.set()
    except Exception as e:
        logger.error(f"Error creating fallback playlist: {e}")

def get_registration_status():    
    global logger, cloud_connect_url
    logger.debug("Checking device registration status")
    logger.debug(f"Cloud connect URL: {cloud_connect_url}")
    
    try:
        request_url = f'{cloud_connect_url}/status'
        logger.debug(f"Making request to: {request_url}")
        
        response = requests.get(request_url, timeout=5)
        logger.debug(f"Response status code: {response.status_code}")
        
        response.raise_for_status()
        
        response_data = response.json()
        logger.debug(f"Response data: {response_data}")
        
        registration_status = response_data["connection"]
        logger.info(f"Registration status retrieved: {registration_status}")
        
        logger.debug("Triggering WebSocket update event")
        ws_update_event.set()
        return registration_status
        
    except requests.exceptions.Timeout as e:
        logger.error(f"Timeout getting registration status: {e}")
        ws_update_event.set()
        return False
    except requests.exceptions.ConnectionError as e:
        logger.error(f"Connection error getting registration status: {e}")
        ws_update_event.set()
        return False
    except requests.exceptions.HTTPError as e:
        logger.error(f"HTTP error getting registration status: {e}")
        ws_update_event.set()
        return False
    except KeyError as e:
        logger.error(f"Connection status not found in response: {e}")
        ws_update_event.set()
        return False
    except requests.exceptions.RequestException as e:
        logger.error(f"Request exception getting registration status: {e}")
        ws_update_event.set()
        return False
    except Exception as e:
        logger.error(f"Unexpected error getting registration status: {e}", exc_info=True)
        ws_update_event.set()
        return False

def process_ad_data(playlist_file_name=None, delete_unused_files=False, hash_error_ok=False):
    """
    Process playlist data with robust fallback mechanisms
    
    Args:
        playlist_file_name: Optional path to a new playlist file
        delete_unused_files: Whether to delete files not referenced in the playlist
        hash_error_ok: Whether to ignore hash verification errors
        
    Returns:
        tuple: (success_bool, status_dict)
    """
    global playlist_updated, show_qr_code, logger, resources_dir
    display_message("Processing playlist")
    
    # Define default playlist location and check registration status
    default_playlist_path = os.path.join(resources_dir, "playlist.json")
    playlist_file = playlist_file_name or default_playlist_path
    registration = get_registration_status()
    
    # Set up error tracking
    errorlog = {
        "status": "error",
        "playlist_id": "",
        "errors": []
    }
    
    # Determine playlist status
    playlist_exists = os.path.exists(playlist_file)
    using_default_path = playlist_file == default_playlist_path
    
    # Handle registered/unregistered device scenarios
    if registration:
        # Device is registered, don't show QR code
        show_qr_code = False
    else:
        # Handle unregistered device scenarios
        show_qr_code = True
        
        # Case 1: Not registered but has existing playlist - keep using it
        if playlist_exists and using_default_path:
            # Only maintain current playlist when checking default path (not when processing a new update)
            logger.info("Device not registered, but playlist exists. Continuing to display existing playlist.")
            return True, {"status": "success", "message": "Using existing playlist despite registration issues"}
        
        # Case 2: Not registered and no playlist - create fallback
        elif not playlist_file_name:  # Only create fallback when checking default playlist
            logger.info("Device not registered and no playlist exists. Creating initial fallback playlist.")
            # TODO - decided whether to keep this create_fallback_playlist() or not
            create_fallback_playlist(default_playlist_path)
            playlist_file = default_playlist_path
            # Continue processing with the new fallback playlist
    
    # Handle missing playlist file (after all other checks)
    if not os.path.exists(playlist_file):
        logger.error("Playlist file not found. Creating a fallback playlist file")
        # TODO - decided whether to keep this create_fallback_playlist() or not
        create_fallback_playlist(default_playlist_path)
        show_qr_code = True
        errorlog["errors"].append({"error": "Playlist file not found", "action": "Created fallback playlist"})
        return False, errorlog
        
    # Process the actual playlist content
    try:
        # Read the playlist file
        with open(playlist_file, "r") as f:
            playlist_data = json.load(f)
            
        # Create a backup of the current playlist before any changes
        backup_path = None
        if os.path.exists(default_playlist_path) and not using_default_path:
            backup_path = default_playlist_path + ".bak"
            logger.info(f"Backing up current playlist to {backup_path}")
            shutil.copy2(default_playlist_path, backup_path)
        
        # Process media files referenced in the playlist
        referenced_files = set()  # Track all files referenced in the playlist
        download_errors = []
        critical_errors = False   # Track if any required files failed to download
        
        # Extract playlist ID for logging
        playlist_id = playlist_data.get("playlist", {}).get("id", "unknown")
        errorlog["playlist_id"] = playlist_id
        
        # Process each ad in the playlist
        for ad in playlist_data.get("playlist", {}).get("ads", []):
            #display_message(f"Processing media file: {ad.get('image_file', 'unknown')}")
            
            # Get file details
            file_name = ad.get("image_file")
            file_hash = ad.get("image_hash")
            file_url = ad.get("image_url")
            
            if not file_name or not file_url:
                error_msg = f"Missing file name or URL in playlist item: {ad.get('slot_id', 'unknown')}"
                logger.error(error_msg)
                download_errors.append({"error": error_msg})
                critical_errors = True  # Missing file info is a critical error
                continue
                
            # Add to referenced files set
            referenced_files.add(file_name)
            
            # Local file path
            local_file_path = os.path.join(resources_dir, file_name)
            
            # Check if file exists and has correct hash
            needs_download = True
            if os.path.exists(local_file_path):
                if file_hash:
                    hash_match, actual_hash = check_file_hash(local_file_path, file_hash)
                    if hash_match:
                        # File exists and hash matches - no download needed
                        logger.info(f"File {file_name} already exists with matching hash")
                        needs_download = False
                    else:
                        logger.warning(f"Hash mismatch for {file_name}: expected {file_hash}, got {actual_hash}")
                        # Will download to replace the file
                else:
                    # No hash provided - assume file is ok if it exists
                    logger.info(f"File {file_name} exists but no hash provided for verification")
                    needs_download = False
            
            # Download file if needed
            if needs_download:
                logger.info(f"Downloading {file_name} from {file_url}")
                success = download_file(file_url, local_file_path)
                
                if not success:
                    error_msg = f"Failed to download {file_name} from {file_url}"
                    download_errors.append({"error": error_msg})
                    logger.error(error_msg)
                    critical_errors = True  # Failed download is a critical error
                    continue
                
                # Verify hash after download if hash is provided
                if file_hash:
                    hash_match, actual_hash = check_file_hash(local_file_path, file_hash)
                    if not hash_match:
                        error_msg = f"Hash verification failed for {file_name}: expected {file_hash}, got {actual_hash}"
                        download_errors.append({"error": error_msg})
                        logger.error(error_msg)
                        
                        # Handle hash error based on parameter
                        if not hash_error_ok:
                            # Remove the file since hash doesn't match
                            safe_remove(local_file_path)
                            critical_errors = True  # Hash verification failure is a critical error
                            continue
        
        # Clean up unused files if requested
        if delete_unused_files:
            logger.info("Checking for unused files to clean up")
            try:
                for file_name in os.listdir(resources_dir):
                    # Skip playlist files and non-media files
                    if file_name == "playlist.json" or file_name.endswith(".bak") or file_name.startswith("."):
                        continue
                        
                    if file_name not in referenced_files:
                        file_path = os.path.join(resources_dir, file_name)
                        logger.info(f"Removing unused file: {file_name}")
                        safe_remove(file_path)
            except Exception as e:
                logger.error(f"Error while cleaning up unused files: {e}")
        
        # Only update the playlist file if all critical files were processed successfully
        if not using_default_path:
            if not critical_errors:
                # All files processed successfully, now safe to update the playlist
                logger.info(f"All media files processed successfully. Updating playlist file.")
                shutil.copy2(playlist_file, default_playlist_path)
                
                # Mark playlist as updated to notify clients
                playlist_updated = True
                ws_update_event.set()
                display_message("Playlist updated successfully")
            else:
                # Critical errors occurred, don't update the playlist
                logger.error("Critical errors occurred during processing. Keeping existing playlist.")
                if backup_path:
                    logger.info(f"Restoring backup playlist from {backup_path}")
                    # In case partial processing happened, restore the backup
                    if os.path.exists(backup_path):
                        shutil.copy2(backup_path, default_playlist_path)
                
                # Notify clients about the error
                display_message("Playlist update failed - check logs")
        
        # Cleanup - Remove temporary file if one was created
        if not using_default_path and os.path.exists(playlist_file) and playlist_file_name:
            try:
                os.remove(playlist_file)
            except Exception as e:
                logger.error(f"Error removing temporary playlist file: {e}")
        
        # Report any download errors
        if download_errors:
            errorlog["errors"].extend(download_errors)
            return not critical_errors, errorlog
        
        return True, {"status": "success", "message": "Playlist processed successfully"}
            
    except Exception as e:
        logger.error(f"Error processing playlist: {e}", exc_info=True)
        errorlog["errors"].append({"error": f"Processing error: {str(e)}"})
        
        # If an exception occurred and we were processing a new playlist, restore the backup
        if not using_default_path and os.path.exists(default_playlist_path + ".bak"):
            logger.info("Restoring backup playlist after exception")
            shutil.copy2(default_playlist_path + ".bak", default_playlist_path)
        
        return False, errorlog

def create_thumbnail(file_path: str, is_video: bool, size: tuple = (512, 512)) -> str:
    """Create a thumbnail from an image or video file"""
    global logger
    try:
        if is_video:
            # Extract frame using ffmpeg
            cmd = [
                'ffmpeg',
                '-i', file_path,      # Input file
                '-ss', '00:00:01',    # Seek to 1 second
                '-vframes', '1',      # Extract 1 frame
                '-vf', f'scale={size[0]}:{size[1]}:force_original_aspect_ratio=decrease,pad={size[0]}:{size[1]}:(ow-iw)/2:(oh-ih)/2:color=white',
                '-f', 'image2pipe',   # Output to pipe
                '-vcodec', 'mjpeg',   # Use JPEG format
                '-'                   # Output to stdout
            ]
            
            process = subprocess.run(cmd, capture_output=True)
            if process.returncode != 0:
                logger.error(f"Error creating video thumbnail: {process.stderr.decode()}")
                return None
                
            # Encode the thumbnail
            encoded = base64.b64encode(process.stdout).decode()
            return f"data:image/jpeg;base64,{encoded}"
                
        else:
            # Handle images
            with Image.open(file_path) as img:
                # Convert RGBA to RGB if needed
                if img.mode in ('RGBA', 'LA'):
                    background = Image.new('RGB', img.size, (255, 255, 255))
                    background.paste(img, mask=img.split()[-1])
                    img = background
                
                # Create thumbnail
                img.thumbnail(size, Image.Resampling.LANCZOS)
                
                # Center image on white background
                thumb = Image.new('RGB', size, (255, 255, 255))
                offset = ((size[0] - img.size[0]) // 2, (size[1] - img.size[1]) // 2)
                thumb.paste(img, offset)
                
                # Save to bytes
                buffer = io.BytesIO()
                thumb.save(buffer, format='JPEG', quality=85)
                encoded = base64.b64encode(buffer.getvalue()).decode()
                return f"data:image/jpeg;base64,{encoded}"
                
    except Exception as e:
        logger.error(f"Error creating thumbnail for {file_path}: {e}")
        return None

# removing thumbnail logic to reduce device load during playlist-upload flow
# def get_playlist_with_thumbnails():
def get_playlist_json():
    global resources_dir
    playlist_file = os.path.join(resources_dir, "playlist.json")
    playlist = {}
    
    try:
        with open(playlist_file, "r") as f:
            playlist = json.load(f)
    except json.JSONDecodeError as e:
        logger.error(f"Error loading playlist file: {e}")
        return {"status": "error", "log": f'Error loading playlist file: {e}'}

    return playlist
    # thumbnail_playlist = playlist.copy()
    # for ad in thumbnail_playlist["playlist"]["ads"]:
    #     # Determine file path based on content type
    #     is_video = ad.get("video", False)
    #     file_path = os.path.join(resources_dir,ad["image_file"])
        
    #     if os.path.exists(file_path):
    #         thumbnail = create_thumbnail(file_path, is_video)
    #         ad["thumbnail"] = thumbnail if thumbnail else None
    #     else:
    #         ad["thumbnail"] = None

    # return thumbnail_playlist

def get_device_screenshot():
    global logger
    """Take a screenshot and return path to the resized image"""
    screenshot_files = []  # Track all created files for cleanup
    
    try:
        # Create temporary file
        screenshot_file = tempfile.mktemp(suffix='.png')
        screenshot_files.append(screenshot_file)
        
        # Set environment variables for Wayland/LabWC
        env = os.environ.copy()
        if 'XDG_RUNTIME_DIR' not in env:
            env['XDG_RUNTIME_DIR'] = f'/run/user/1000'  # ekauser's uid is 1000
        
        # Run with timeout to prevent hanging
        try:
            result = subprocess.run(
                ['grim', screenshot_file], 
                capture_output=True,
                text=True,
                env=env,
                timeout=15  # Timeout after 15 seconds
            )
            
            if result.returncode != 0:
                logger.error(f"Screenshot failed: {result.stderr}")
                raise RuntimeError(f"Screenshot command failed: {result.stderr}")
                
        except subprocess.TimeoutExpired:
            logger.error("Screenshot timed out after 15 seconds")
            raise RuntimeError("Screenshot timed out")
            
        # Check file exists and has content
        if not os.path.exists(screenshot_file) or os.path.getsize(screenshot_file) == 0:
            logger.error("Screenshot file is empty or missing")
            raise FileNotFoundError("Screenshot file is empty or missing")
            
        # Process and resize image
        with Image.open(screenshot_file) as img:
            if img.size[0] > 512 or img.size[1] > 1024:
                resized_file = tempfile.mktemp(suffix='.png')
                screenshot_files.append(resized_file)
                img.thumbnail((512, 512), Image.Resampling.LANCZOS)
                img.save(resized_file, 'PNG')
                
                # Only remove original after successfully saving resized
                os.remove(screenshot_file)
                screenshot_files.remove(screenshot_file)
                return resized_file
                
        return screenshot_file
        
    except Exception as e:
        logger.error(f"Screenshot error: {str(e)}", exc_info=True)
        
        # Clean up any temporary files created before the error
        for file in screenshot_files:
            try:
                if os.path.exists(file):
                    os.remove(file)
            except Exception:
                pass
                
        # Create error image
        error_file = tempfile.mktemp(suffix='.png')
        try:
            error_img = Image.new('RGB', (512, 512), color='white')
            draw = ImageDraw.Draw(error_img)
            message = str(e)[:100]  # Limit message length
            draw.text((256, 256), f"Screenshot Failed: {message}", fill='black', anchor='mm')
            error_img.save(error_file, 'PNG')
            return error_file
        except Exception as fallback_error:
            logger.error(f"Failed to create error image: {fallback_error}")
            if os.path.exists(error_file):
                os.remove(error_file)
            raise RuntimeError("Complete screenshot failure")

def ble_advertise(status: bool):
    global logger
    logger.debug(f"BLE advertise called with status: {'START' if status else 'STOP'}")
    
    try:
        if status:
            logger.debug("Checking eka-netconnect service status")
            result = subprocess.run(["systemctl", "is-active", "eka-netconnect.service"], 
                                    capture_output=True, text=True, timeout=5)
            service_status = result.stdout.strip()
            logger.debug(f"Service status: {service_status}")
            
            if service_status != "active":
                logger.info("Starting eka-netconnect service for BLE advertising")
                start_result = os.system("systemctl start eka-netconnect.service")
                logger.debug(f"Start service result: {start_result}")
            else:
                logger.debug("eka-netconnect service already active")
                
            display_message("BLE advertising started")
            logger.info("BLE advertising started successfully")
        else:
            logger.info("Stopping eka-netconnect service (BLE advertising)")
            stop_result = os.system("systemctl stop eka-netconnect.service")
            logger.debug(f"Stop service result: {stop_result}")
            display_message("BLE advertising stopped")
            logger.info("BLE advertising stopped successfully")
            
    except subprocess.TimeoutExpired:
        logger.error("Timeout checking eka-netconnect service status")
    except Exception as e:
        logger.error(f"Error in BLE advertise: {e}", exc_info=True)

async def monitor_connectivity():
    global connectivity_status, logger, show_qr_code
    last_connected_time = time.time()  # Track when we were last connected
    global SETTINGS_DISCONNECT_THRESHOLD, SETTINGS_WIFI_RECOVERY_CYCLE_INTERVAL

    # Define thresholds for connectivity actions
    # QR_CODE_DISPLAY_THRESHOLD = 300  # 5 minutes
    BLE_ADVERTISING_THRESHOLD = 30   # Start BLE advertising after 30 s
    # qr_code_displayed = False
    ble_advertising_active = False
    connectivity_check_attempts = 0
    max_check_attempts = 3  # Number of failed attempts before considering disconnected
    # Escalating wifi recovery state: 0 = no recovery attempted yet, 1 = tier 1 done, 2 = tier 2 done
    recovery_tier_attempted = 0
    last_tier_action_time = 0  # time.time() of the most recent tier action; used for spacing tier 2 and the re-cycle

    while True:
        current_status = connectivity_status
        
        logger.info(f"Start of monitor connectivity while loop, current_status: {current_status}")
        try:
            # Try multiple DNS servers for more reliable connectivity check
            for dns_server in ["8.8.8.8", "1.1.1.1"]:
                try:
                    logger.info(f"check dns to {dns_server}")
                    socket.create_connection((dns_server, 53), timeout=3)
                    connectivity_status = True
                    connectivity_check_attempts = 0
                    last_connected_time = time.time()  # Reset timer when connected
                    recovery_tier_attempted = 0  # Reset escalation ladder for the next outage
                    last_tier_action_time = 0
                    
                    # If connectivity is restored, hide the QR code and stop BLE advertising
                    # if qr_code_displayed:
                        # show_qr_code = False
                        # qr_code_displayed = False
                        # logger.info("Connectivity restored. Hiding QR code.")
                        # ws_update_event.set()
                    
                    if ble_advertising_active:
                        logger.info("Connectivity restored. Stopping BLE advertising.")
                        ble_advertise(False)
                        ble_advertising_active = False

                        try:
                            
                            request_url = f'{cloud_connect_url}/network_restored'
                            logger.info("Notifying cloudconnect of network restoration to trigger sync, Making request to: {request_url}...")
                            requests.post(request_url, timeout=5)
                        except Exception as e:
                            logger.error(f"Failed to notify cloudconnect of network restoration: {e}")
                    
                    break  # Exit DNS server loop if connection successful
                except (socket.timeout, OSError):
                    logger.info(f"Failed to check dns to {dns_server}, trying next dns server")
                    continue  # Try next DNS server
            else:  # This runs if no DNS servers connected successfully
                connectivity_check_attempts += 1
                logger.info(f"no DNS servers connected successfully, connectivity_check_attempts = {connectivity_check_attempts}")
                if connectivity_check_attempts >= max_check_attempts:
                    logger.info(f"Gone over max_check_attempts, setting connectivity_status to False")
                    connectivity_status = False
        except Exception as e:
            logger.error(f"Error checking connectivity: {e}")
            connectivity_status = False
            
        # If disconnected, check thresholds and take appropriate actions
        if not connectivity_status:
            disconnect_duration = time.time() - last_connected_time
            logger.info(f"connectivity_status is False. Disconnect duration: {disconnect_duration} seconds")
            
            # Start BLE advertising after short threshold
            if disconnect_duration >= BLE_ADVERTISING_THRESHOLD and not ble_advertising_active:
                logger.info(f"No connectivity for {BLE_ADVERTISING_THRESHOLD} seconds. Starting BLE advertising.")
                ble_advertise(True)
                ble_advertising_active = True
            
            # Show QR code only after longer threshold to avoid disruption for brief outages
            # if disconnect_duration >= QR_CODE_DISPLAY_THRESHOLD and not qr_code_displayed:
            #     logger.warning(f"No connectivity for {QR_CODE_DISPLAY_THRESHOLD} seconds. Displaying QR code.")
            #     show_qr_code = True
            #     qr_code_displayed = True
            #     ws_update_event.set()
            
            # Escalating wifi recovery (no reboot — kiosk stays up).
            # Tier 1 at watchdog_timeout seconds offline: nudge NetworkManager to retry the saved wlan0 profile.
            # Tier 2 TIER_2_DELAY_AFTER_TIER_1 seconds after tier 1: restart NetworkManager (handles a wedged NM).
            # After tier 2, wait wifi_recovery_cycle_interval seconds then re-run the ladder from tier 1.
            if SETTINGS_DISCONNECT_THRESHOLD > 0:
                now = time.time()
                if recovery_tier_attempted == 0 and disconnect_duration >= SETTINGS_DISCONNECT_THRESHOLD:
                    logger.error(f"No connectivity for {disconnect_duration:.0f}s. Tier 1: nudging NetworkManager to reconnect wlan0...")
                    try:
                        result = subprocess.run(
                            ["timeout", "10", "nmcli", "device", "connect", "wlan0"],
                            capture_output=True, text=True, timeout=15,
                        )
                        logger.info(f"Tier 1 nmcli rc={result.returncode} stdout={result.stdout.strip()!r} stderr={result.stderr.strip()!r}")
                    except Exception as e:
                        logger.error(f"Tier 1 recovery action failed: {e}")
                    recovery_tier_attempted = 1
                    last_tier_action_time = now
                elif recovery_tier_attempted == 1 and (now - last_tier_action_time) >= TIER_2_DELAY_AFTER_TIER_1:
                    logger.error(f"No connectivity for {disconnect_duration:.0f}s. Tier 2: restarting NetworkManager...")
                    try:
                        result = subprocess.run(
                            ["systemctl", "restart", "NetworkManager"],
                            capture_output=True, text=True, timeout=30,
                        )
                        logger.info(f"Tier 2 systemctl restart rc={result.returncode} stderr={result.stderr.strip()!r}")
                    except Exception as e:
                        logger.error(f"Tier 2 recovery action failed: {e}")
                    recovery_tier_attempted = 2
                    last_tier_action_time = now
                elif recovery_tier_attempted == 2 and (now - last_tier_action_time) >= SETTINGS_WIFI_RECOVERY_CYCLE_INTERVAL:
                    logger.warning(f"No connectivity for {disconnect_duration:.0f}s. Restarting recovery ladder from tier 1.")
                    recovery_tier_attempted = 0
                    # tier 1 will fire on the next loop iteration since disconnect_duration >= SETTINGS_DISCONNECT_THRESHOLD
        
        # Update the websocket client if the status changes
        if current_status != connectivity_status:
            logger.info(f"Connectivity status changed to: {'Connected' if connectivity_status else 'Disconnected'}")
            ws_update_event.set()
        
        # Adaptive sleep time - check more frequently when disconnected, less when connected
        sleep_time = 5 if not connectivity_status else 30
        await asyncio.sleep(sleep_time)

# Create and start the monitoring task
async def start_connectivity_monitor():
    await monitor_connectivity()
connectivity_monitor_task = asyncio.create_task(start_connectivity_monitor())

def process_tv_on_off(settings, use_lock=True, debug_msg='normal: '):
    tv_state = settings.get("tv_state", "AUTO").upper()
    
    if tv_state == "ON":
        logger.info(debug_msg + "TV state: ON - Forcing TV on")
        tv_status_set(True, use_lock=use_lock, debug_msg=debug_msg)
    elif tv_state == "OFF":
        logger.info(debug_msg + "TV state: OFF - Forcing TV off")
        tv_status_set(False, use_lock=use_lock, debug_msg=debug_msg)
    elif tv_state == "AUTO":
        logger.info(debug_msg + "TV state: AUTO - Using schedule")
        current_time = datetime.now().time()
        try:
            # Clean up time strings in case there are extra characters
            tv_on_time_str = settings["tv_on_time"].strip()
            tv_off_time_str = settings["tv_off_time"].strip()
            
            # Parse the times, handling potential format issues
            try:
                tv_on_time = datetime.strptime(tv_on_time_str, "%H:%M").time()
            except ValueError:
                logger.error(debug_msg + f"Invalid tv_on_time format: {tv_on_time_str}. Using 08:00.")
                tv_on_time = datetime.strptime("08:00", "%H:%M").time()
                
            try:
                tv_off_time = datetime.strptime(tv_off_time_str, "%H:%M").time()
            except ValueError:
                logger.error(f"Invalid tv_off_time format: {tv_off_time_str}. Using 22:00.")
                tv_off_time = datetime.strptime("22:00", "%H:%M").time()
            
            logger.info(debug_msg + f"Current time: {current_time}")
            logger.info(debug_msg + f"TV on time: {tv_on_time}")
            logger.info(debug_msg + f"TV off time: {tv_off_time}")
            
            # Handle both normal schedules and overnight schedules
            tv_should_be_on = False
            
            # Check if this is an overnight schedule (on time > off time)
            if tv_on_time > tv_off_time:
                # TV should be ON if current time is after on time OR before off time
                if current_time >= tv_on_time or current_time <= tv_off_time:
                    tv_should_be_on = True
            else:
                # Normal schedule: TV should be ON if current time is between on and off times
                if tv_on_time <= current_time <= tv_off_time:
                    tv_should_be_on = True
            
            if tv_should_be_on:
                logger.info(debug_msg + "Turning TV on based on schedule")
                tv_status_set(True, use_lock=use_lock, debug_msg=debug_msg)
            else:
                display_message("TV will be turned off in 10 seconds")
                logger.info(debug_msg + "Turning TV off based on schedule")
                time.sleep(10)
                tv_status_set(False, use_lock=use_lock, debug_msg=debug_msg)
        except Exception as e:
            logger.error(debug_msg + f"Error processing TV schedule: {e}")
    else:
        logger.error(debug_msg + f"Invalid tv_state: {tv_state}. Using AUTO.")
        # Fall back to AUTO behavior
        process_tv_on_off({**settings, "tv_state": "AUTO"}, use_lock=use_lock, debug_msg=debug_msg)

#every 30s make sure the tv is on/off based on the settings
async def start_tv_monitor():
    while True:
        with open(settings_file_path, "r") as f:
            settings = json.load(f)
            # if settings has "tv_force_ekadev": true, then force process settings
            if settings.get("tv_force_ekadev"):
                if settings["tv_force_ekadev"]:
                    process_tv_on_off(settings, debug_msg='FLEEP')
                    # logger.debug('PENCIL skipping process_tv_on_off')

        await asyncio.sleep(30)
tv_monitor_task = asyncio.create_task(start_tv_monitor())

def process_settings(use_lock=True, debug_msg='normal: '):
    global settings_file_path, logger
    global SETTINGS_DISCONNECT_THRESHOLD, SETTINGS_WIFI_RECOVERY_CYCLE_INTERVAL
    
    logger.debug(debug_msg + f"Processing settings from: {settings_file_path}")
    
    # if settings file is not present or cannot be loaded as a json file, set the default values
    try:
        if not os.path.exists(settings_file_path):
            logger.warning(debug_msg + f"Settings file does not exist: {settings_file_path}")
            return False
            
        logger.debug(debug_msg + "Reading settings file")
        with open(settings_file_path, "r") as f:
            settings = json.load(f)
            
        logger.debug(debug_msg + f"Loaded settings: {settings}")
        
        # Process watchdog timeout
        if settings.get("watchdog_timeout"):
            SETTINGS_DISCONNECT_THRESHOLD = settings["watchdog_timeout"]
            logger.info(debug_msg + f"Watchdog timeout set to {SETTINGS_DISCONNECT_THRESHOLD} seconds")
        else:
            logger.debug(debug_msg + "No watchdog_timeout in settings, keeping default")

        # Process wifi recovery cycle interval (time after tier 2 before re-running the ladder)
        if settings.get("wifi_recovery_cycle_interval"):
            SETTINGS_WIFI_RECOVERY_CYCLE_INTERVAL = settings["wifi_recovery_cycle_interval"]
            logger.info(debug_msg + f"Wifi recovery cycle interval set to {SETTINGS_WIFI_RECOVERY_CYCLE_INTERVAL} seconds")
        else:
            logger.debug(debug_msg + "No wifi_recovery_cycle_interval in settings, keeping default")
        
        # Process audio mute setting
        if settings.get("mute"):
            logger.info(debug_msg + "Muting audio based on settings file")
            mute_result = os.system("amixer set PCM 0%")
            logger.debug(debug_msg + f"Mute command result: {mute_result}")
            display_message("Audio muted")
        else:
            logger.info(debug_msg + "Unmuting audio based on settings file")
            unmute_result = os.system("amixer set PCM 100%")
            logger.debug(debug_msg + f"Unmute command result: {unmute_result}")
            display_message("Audio unmuted")
        
        # Process BLE setting
        if settings.get("ble_on"):
            logger.info(debug_msg + "Starting BLE advertising based on settings file")
            ble_advertise(True)
        else:
            logger.info(debug_msg + "Stopping BLE advertising based on settings file")
            ble_advertise(False)
        
        # Process Show Screen Logs setting
        global show_screen_logs
        if "show_screen_logs" in settings:
            if settings["show_screen_logs"] != show_screen_logs:
                show_screen_logs = settings["show_screen_logs"]
                logger.info(debug_msg + f"Screen logs setting changed to: {show_screen_logs}")
                ws_update_event.set() # Trigger WS broadcast
            
        # Process daily reboot setting
        if settings.get("daily_reboot"):
            if settings.get("daily_reboot_time"):
                try:
                    reboot_time = settings["daily_reboot_time"].strip()
                    logger.info(debug_msg + f"Daily reboot time set to {reboot_time}")
                    
                    # issue "shutdown -r {reboot_time} --no-wall" and check status
                    logger.debug(debug_msg + f"Scheduling reboot for {reboot_time}")
                    command_status = subprocess.run(["/usr/sbin/shutdown", "-r", reboot_time, "--no-wall"], 
                                                   capture_output=True, text=True, timeout=10)
                    logger.debug(debug_msg + f"Shutdown command return code: {command_status.returncode}")
                    logger.debug(debug_msg + f"Shutdown stdout: {command_status.stdout}")
                    logger.debug(debug_msg + f"Shutdown stderr: {command_status.stderr}")
                    
                    if command_status.returncode != 0:
                        logger.error(debug_msg + f"Error scheduling reboot: {command_status.stderr}")
                    else:
                        logger.info(debug_msg + f"Daily reboot scheduled successfully for {reboot_time}")
                        
                except subprocess.TimeoutExpired:
                    logger.error(debug_msg + "Timeout scheduling daily reboot")
                except Exception as e:
                    logger.error(debug_msg + f"Error scheduling daily reboot: {e}", exc_info=True)
            else:
                logger.warning(debug_msg + "daily_reboot enabled but no daily_reboot_time specified")
        else:
            logger.info(debug_msg + "Disabling daily reboot based on settings file")
            try:
                logger.debug(debug_msg + "Cancelling any scheduled reboots")
                command_status = subprocess.run(["/usr/sbin/shutdown", "-c"], 
                                               capture_output=True, text=True, timeout=10)
                logger.debug(debug_msg + f"Cancel shutdown return code: {command_status.returncode}")
                logger.debug(debug_msg + f"Cancel shutdown stdout: {command_status.stdout}")
                logger.debug(debug_msg + f"Cancel shutdown stderr: {command_status.stderr}")
                
                if command_status.returncode != 0:
                    logger.debug(debug_msg + f"Cancel reboot result (may be normal if no reboot scheduled): {command_status.stderr}")
                else:
                    logger.info(debug_msg + "Cancelled any scheduled reboots")
                    
            except subprocess.TimeoutExpired:
                logger.error(debug_msg + "Timeout cancelling scheduled reboot")
            except Exception as e:
                logger.error(debug_msg + f"Error cancelling scheduled reboot: {e}", exc_info=True)

        # Process TV settings
        logger.debug(debug_msg + "Processing TV on/off settings")
        process_tv_on_off(settings, use_lock=use_lock, debug_msg=debug_msg)

        logger.info(debug_msg + "Settings processed successfully")
        return True
        
    except json.JSONDecodeError as e:
        logger.error(debug_msg + f"Invalid JSON in settings file: {e}")
        return False
    except Exception as e:
        logger.error(debug_msg + f"Error loading settings file: {e}", exc_info=True)
        return False    

# generate files that are required for the app
logger.info("Creating required directories")
logger.debug("Creating /opt/eka/eka-app directory")
os.makedirs("/opt/eka/eka-app", exist_ok=True)

#the resources folder contains the ad playlist and the ad images
logger.debug("Creating /opt/eka/eka-app/resources directory")
os.makedirs("/opt/eka/eka-app/resources", exist_ok=True)

logger.info("Turning TV on at startup")
tv_status_set(True, use_lock=False, debug_msg="MEEP: ")

logger.info("Processing initial settings")
settings_result = process_settings(use_lock=False, debug_msg='GLEEP: ')
logger.debug(f"Settings processing result: {settings_result}")

logger.info("Getting device identifiers")
mac_result = get_device_mac()
logger.debug(f"MAC retrieval result: {mac_result}")

serial_result = get_device_serial()
logger.debug(f"Serial retrieval result: {serial_result}")

logger.info("Generating status QR code")
generate_status_qr()

# At startup:
logger.info("Checking for existing playlist")
playlist_file = os.path.join(resources_dir, "playlist.json")
if not os.path.exists(playlist_file):
    logger.info("No playlist found on first boot. Do nothing, show mpv eka logo default")
    # create_fallback_playlist()
    # logger.debug("Processing fallback playlist")
    # process_result = process_ad_data(None, delete_unused_files=False)
    # logger.debug(f"Fallback playlist processing result: {process_result}")
else:
    logger.info("Existing playlist found on boot. Using it.")
    logger.debug("Processing existing playlist")
    process_result = process_ad_data(None, delete_unused_files=False)
    logger.debug(f"Existing playlist processing result: {process_result}")

logger.info("EKA Local Server startup completed successfully")
logger.info("="*80)



#----------------------------Local HTTP Server---------------------------------------------------------#

class PlaylistUpdate(BaseModel):
    file_path: str

app = FastAPI(
    #docs_url=None,  # Disable Swagger UI
    #redoc_url=None,  # Disable ReDoc
    #openapi_url=None  # Disable OpenAPI schema
)

# enabe CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Or specify allowed origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)   


# Mount the /opt/eka/app directory to serve all static files under /
static_dir = '/opt/eka/eka-app/'
app.mount("/static", StaticFiles(directory=static_dir, html=True), name="static")

@app.get("/")
async def serve_index():
    # Serve index.html at the root URL (/)
    index_path = os.path.join(static_dir, "index.html")
    return FileResponse(index_path)

#route to get the device registration status. This will be used by the EKA app to check if the device is registered and show the corresponding prompts to the user
@app.get("/registration")
async def serve_registration():
    # Check if the device is registered
    global registration_status
    registration_status = get_registration_status()
    return {"status": registration_status}    

#route to check the device internet connectivity status
# monitor_connectivity function is running in a separate thread to check the connectivity status
@app.get("/connectivity")
async def serve_connectivity():
    # Check if the device has internet connectivity
    global connectivity_status
    return {"status": connectivity_status}

@app.post("/force_default_playlist")
async def force_default_playlist():
    global show_qr_code, registration_status
    create_fallback_playlist()
    display_message("Default playlist")
    # Only show QR code if device is not registered
    show_qr_code = not registration_status
    return {"status": "success", "log": "Default playlist created successfully"}

#route used by cloudconect to inform the local server about changes in the device registration 
# input: {"registration": True/False}
@app.post("/status")
async def serve_status(status: dict):
    global registration_status
    global show_qr_code
    
    # Cloud connect sends MQTT connection status under the "registration" key
    is_connected_to_cloud = status.get("registration", False)
    
    # Check if we have a real downloaded playlist cache
    playlist_file = os.path.join(resources_dir, "playlist.json")
    has_custom_playlist = False
    if os.path.exists(playlist_file):
        try:
            with open(playlist_file, "r") as f:
                data = json.load(f)
                if str(data.get("playlist", {}).get("id")) != "0":
                    has_custom_playlist = True
        except:
            pass

    # 1. TEXT PROMPT LOGIC
    # If we have a custom playlist, the device IS registered (just currently offline).
    # This prevents the "Device is not registered" text from showing.
    if has_custom_playlist:
        registration_status = True
    else:
        registration_status = is_connected_to_cloud
        
    # 2. QR CODE LOGIC
    # Show the QR code whenever the device is disconnected from the cloud
    # show_qr_code = not is_connected_to_cloud
    
    # Only show the QR code if the device is genuinely unregistered
    show_qr_code = not registration_status

    ws_update_event.set()
    logger.debug(f"Registration status: {registration_status}, Show QR: {show_qr_code}")
    
    # Only create fallback if no playlist exists at all
    if not is_connected_to_cloud:
        if not os.path.exists(playlist_file):
            logger.info("Device not registered and no playlist exists. NOT creating initial fallback playlist.")
            # TODO - decided whether to keep this create_fallback_playlist() or not
            # create_fallback_playlist()
        else: 
            logger.info("Device not registered but playlist exists. Keeping existing playlist.")
    # <--- Do NOT create fallback playlist if playlist_file exists, regardless of registration status

    return {"status": "success", "log": "Status updated successfully"}

# post request to update playlist from a specific path
#cloud connect will download the playlist file and send it to the local server
@app.post("/update_playlist")
async def serve_update_playlist(playlist_path: PlaylistUpdate):
    logger.info(f"Received playlist update request for: {playlist_path.file_path}")
    playlist_file = playlist_path.file_path
    
    # Validate path to prevent directory traversal
    safe_path = os.path.realpath(playlist_file)
    logger.debug(f"Resolved path: {safe_path}")
    
    if not os.path.exists(safe_path):
        logger.error(f"File not found: {safe_path}")
        return {"status": "error", "log": "File not found"}
    
    # Validate file size to prevent resource exhaustion
    try:
        file_size = os.path.getsize(safe_path)
        logger.debug(f"Playlist file size: {file_size} bytes")
        
        if file_size > 10_000_000:  # 10MB limit
            logger.error(f"File too large: {safe_path} ({file_size} bytes)")
            return {"status": "error", "log": "File too large"}
        
        # Validate that it's a JSON file
        logger.debug("Validating JSON format")
        try:
            with open(safe_path, "r") as f:
                json_data = json.load(f)
                logger.debug(f"JSON validation successful, keys: {list(json_data.keys())}")
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON file: {safe_path} - {e}")
            return {"status": "error", "log": "Invalid JSON file"}
            
        # Short-circuit and don't update if the playlist is identical
        default_playlist_path = os.path.join(resources_dir, "playlist.json")
        if os.path.exists(default_playlist_path):
            try:
                with open(default_playlist_path, "r") as f:
                    current_json_data = json.load(f)

                # Python securely deep-compares the dictionaries regardless of whitespace/formatting
                if json_data == current_json_data:
                    logger.info("Incoming playlist is exactly identical to the currently playing playlist. Bypassing update.")
                    display_message("Playlist update skipped bc identical")
                    # Clean up the downloaded temp file from cloudconnect
                    os.remove(safe_path)
                    # Return success log formatted exactly how cloudconnect and the frontend expect it
                    return {"log": {"status": "success", "message": "Playlist is identical, update bypassed"}}
            except Exception as e:
                logger.error(f"Error reading current playlist for comparison: {e}")

        # Move file safely
        temp_path = tempfile.mktemp()
        logger.debug(f"Moving file to temporary location: {temp_path}")
        
        try:
            shutil.move(safe_path, temp_path)
            logger.debug("File moved successfully")
        except (IOError, OSError) as e:
            logger.error(f"Error moving file: {e}")
            return {"status": "error", "log": f"Failed to move file: {str(e)}"}
            
        # Process the playlist
        logger.info(f"Processing playlist file: {temp_path}")
        # Process the playlist and delete unused files
        status, errorlog = process_ad_data(delete_unused_files=True, playlist_file_name=temp_path)
        logger.info(f"Playlist processing completed with status: {status}")
        logger.debug(f"Processing log: {errorlog}")
        
        return {"log": errorlog}
    except Exception as e:
        logger.error(f"Error in update_playlist endpoint: {e}", exc_info=True)
        return {"status": "error", "log": f"Internal error: {str(e)}"}

#endpoint to send back teh device serial number and mac address to the EKA app
@app.get("/dev_info")
async def serve_dev_info():
    global device_serial, device_mac
    get_device_mac()
    get_device_serial()
    return {"serial": device_serial, "mac": device_mac}

@app.get("/playlist")
async def serve_playlist():
    # playlist=get_playlist_with_thumbnails()
    playlist=get_playlist_json()
    return playlist
    
@app.get("/screenshot")
async def serve_screenshot():
    screenshot_file_path = get_device_screenshot()
    return FileResponse(
        path=screenshot_file_path,
        media_type='image/png',
        background=BackgroundTask(lambda: os.remove(screenshot_file_path))
    )        

@app.get("/update_settings")
async def serve_update_settings():
    if process_settings(debug_msg='SCREEP'):
        return {"status": "success"}
    else:
        return {"status": "error", "log": "Error updating settings"}

@app.post("/display_message")
async def serve_notify_frontend(message: dict):
    display_message(message.get("message", ""))
    return {"status": "success"}

@app.post("/reboot")
async def serve_reboot():
    # the script in /etc/systemd/system/system-shutdown/ will ensure TV is turned on before
    os.system("systemctl reboot")
    return {"status": "success"}

@app.get("/version")
async def serve_version():
    return version


#----------------------------Local WS Server---------------------------------------------------------#
#A websocket server to asynchronously send the registration, connectivity and playlist update status to the EKA app
#The EKA app will connect to this websocket server to get the status updates

async def send_status(websocket: WebSocket):
    global registration_status, connectivity_status, playlist_updated, frontend_message, logger
    is_connected = True
    logger.info("WebSocket connection established, starting status updates")
    
    while is_connected:
        try:
            status = {
                "status":{
                    "registration": registration_status,
                    "connectivity": connectivity_status,
                    "playlist_updated": playlist_updated,
                    "display_qr": show_qr_code,
                    "message": frontend_message,
                    "show_screen_logs": show_screen_logs
                }
            }
            
            logger.debug(f"Sending WebSocket status: {status}")
            await websocket.send_json(status)
            
            # Reset transient flags
            if frontend_message:
                logger.debug(f"Clearing frontend message: {frontend_message}")
            if playlist_updated:
                logger.debug("Clearing playlist_updated flag")
                
            frontend_message = ""
            playlist_updated = False
            
            #if not registered or no internet connectivity, send update every 1 seconds
            wait_time = 1 if not registration_status or not connectivity_status else 10
            logger.debug(f"WebSocket wait time: {wait_time}s (reg: {registration_status}, conn: {connectivity_status})")
            
            # Wait for the next update event or send current status after a timeout
            # this is just for additional redundancy. The status will be sent immediately if there is an update event
            try:
                await asyncio.wait_for(ws_update_event.wait(), timeout=wait_time)
                logger.debug("WebSocket update event triggered")
                ws_update_event.clear()
                logger.debug("try: after update event clear")
            except asyncio.TimeoutError:
                logger.debug("WebSocket timeout reached, sending periodic update")
                pass
        except RuntimeError as e:
            is_connected = False
            logger.info(f"WebSocket connection closed: {e}")
            ws_update_event.clear()
            logger.debug("except 1: after update event clear")
        except Exception as e:
            is_connected = False
            # logger.error(f"WebSocket error: {e}", exc_info=True)
            logger.error(f"WebSocket error: {e}")
            ws_update_event.clear()
            logger.debug("except 2: after update event clear")
        #clear in case of exiting with an exception
        # ws_update_event.clear()

    logger.info("WebSocket connection handler ended")
        
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    logger.info("New WebSocket connection attempt")
    await websocket.accept()
    logger.info("WebSocket connection accepted")
    await send_status(websocket)

                                  
# To run the server, use:
# uvicorn main:app --host 127.0.0.1 --port 8000

