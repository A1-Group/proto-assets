import concurrent.futures
from concurrent.futures import ThreadPoolExecutor, Future
import threading
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from awsiot import mqtt5_client_builder
from awscrt import mqtt5
import json
import os
import time
import signal
import platform
import psutil
import logging
import logging.handlers
from typing import Dict, List
from datetime import datetime
from pathlib import Path
import uvicorn
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import asyncio
import tempfile
import subprocess
import select
import socket
import urllib.parse

import fcntl

CEC_LOCK_FILE = '/tmp/cec.lock'

# tv.power shadow values -- always one of these three strings (never bool/null),
# so a can't-determine result ("UNKNOWN") stays distinct from a genuine "OFF".
TV_POWER_ON = "ON"
TV_POWER_OFF = "OFF"
TV_POWER_UNKNOWN = "UNKNOWN"

def setup_logging():
    # Set up verbose debugging logging
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - [%(filename)s:%(funcName)s:%(lineno)d] - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    logger = logging.getLogger('eka_cloudConnect')
    logger.setLevel(logging.DEBUG)  # Set to DEBUG for verbose logging
    logger.propagate = False

    Path("/var/log/eka").mkdir(parents=True, exist_ok=True)
    
    # File handler for debug log
    file_handler = logging.handlers.RotatingFileHandler(
        Path("/var/log/eka/eka-cloudConnect-debug.log"),
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
    logger.info("="*80)
    logger.info("EKA CLOUD CONNECT - DEBUG MODE ENABLED")
    logger.info("="*80)
    logger.info(f"Python version: {sys.version}")
    logger.info(f"Platform: {platform.platform()}")
    logger.info(f"Command line args: {sys.argv}")
    logger.info(f"Current working directory: {os.getcwd()}")
    logger.info(f"Script path: {__file__}")
    logger.info("="*80)
    
    return logger


class MessagePayload(BaseModel):
    topic: str
    message: str

class PlaylistUpdate(BaseModel):
    playlist_url: str

class TopicHandler:
    def __init__(self, device):
        self.device = device
        self.logger = device.logger
        self.topic_mapping = {}
        self.local_server_url = device.local_server_url
                
    #update topic_mapping is called when the serial number is known from the credentials file
    def update_topic_mapping(self, serial_number):
        self.logger.debug(f"Updating topic mapping for serial number: {serial_number}")
        
        self.topic_mapping = {
            f"eka-device/{serial_number}/get_sys_data": self.handle_sys_data_request,
            f"eka-device/{serial_number}/update_playlist": self.handle_updatePlaylist,
            f"eka-device/{serial_number}/force_default_playlist": self.handle_force_default_playlist,
            f"eka-device/{serial_number}/get_playlist": self.handle_get_playlist,
            f"eka-device/{serial_number}/get_screenshot": self.handle_get_screenshot,
            f"eka-device/{serial_number}/reboot": self.handle_reboot,
            f"$aws/things/{serial_number}/shadow/name/settings/get/accepted": self.handle_settings_shadow_get_accepted,
            f"$aws/things/{serial_number}/shadow/name/settings/get/rejected": self.handle_settings_shadow_get_rejected,
            f"$aws/things/{serial_number}/shadow/name/settings/update/delta": self.handle_settings_shadow_update_delta,
            f"eka-device/{serial_number}/exec": self.handle_exec,
            f"eka-device/{serial_number}/ping": self.handle_ping
         }
        
        self.logger.info(f"Topic mapping updated with {len(self.topic_mapping)} topics:")
        for topic in self.topic_mapping.keys():
            self.logger.debug(f"  - {topic}")    
    
    def handle_sys_data_request(self, payload_str: str):
        self.logger.debug(f"Received system data request with payload: {payload_str}")
        try:
            self.logger.info("Processing system data request - publishing system data once")
            self.device.publish_system_data(once=True)
            self.logger.debug("System data request handled successfully")
        except Exception as e:
            self.logger.error(f"Error handling sys data request: {e}", exc_info=True)
    
    def handle_force_default_playlist(self, payload_str: str):
        self.logger.debug(f"Received force default playlist request with payload: {payload_str}")
        try:
            # http post localhost:8000/force_default_playlist
            self.logger.info("Forcing default playlist on local server")
            request_url = f"{self.local_server_url}/force_default_playlist"
            self.logger.debug(f"Making POST request to: {request_url}")
            
            response = requests.post(request_url, timeout=10)
            self.logger.debug(f"Response status code: {response.status_code}")
            self.logger.debug(f"Response headers: {dict(response.headers)}")
            
            response.raise_for_status()
            self.logger.info(f"Default playlist forced successfully - Response: {response.text}")
            
        except requests.exceptions.Timeout as e:
            self.logger.error(f"Timeout forcing default playlist: {e}")
        except requests.exceptions.ConnectionError as e:
            self.logger.error(f"Connection error forcing default playlist: {e}")
        except requests.exceptions.HTTPError as e:
            self.logger.error(f"HTTP error forcing default playlist: {e}")
        except Exception as e:
            self.logger.error(f"Unexpected error handling force default playlist: {e}", exc_info=True)
    
    def handle_updatePlaylist(self, playlist_url: str):
        self.logger.debug(f"Received playlist update request with URL: {playlist_url}")
        try:
            # download the playlist file from the url and save it to playlist_path
            # http post localhost:8000/update_playlist 
            
            self.logger.info(f"Updating playlist with URL: {playlist_url}")
            playlist_status_topic = f"$aws/things/{self.device.serial_number}/shadow/name/playlist/update"
            self.logger.debug(f"Will report status to topic: {playlist_status_topic}")
            
            playlist_status_payload={
                "state": {
                    "reported": {
                        "log": ""
                    }
                }
            }
            
            # getting the playlist from the url
            self.logger.debug(f"Downloading playlist from: {playlist_url}")
            r = requests.get(playlist_url, timeout=30)
            self.logger.debug(f"Download response status: {r.status_code}")
            self.logger.debug(f"Download response headers: {dict(r.headers)}")
            
            if 'content-length' in r.headers:
                self.logger.debug(f"Downloaded content length: {r.headers['content-length']} bytes")
            
            if r.status_code != 200:
                self.logger.error(f"Error downloading playlist - Status: {r.status_code}, Response: {r.text}")
                #display message on local server
                display_msg = {"message": "Failed to get playlist"}
                self.logger.debug(f"Sending display message: {display_msg}")
                requests.post(f"{self.local_server_url}/display_message", json=display_msg, timeout=5)
                
                #report status to the cloud shadow "playlist"
                errlog={
                    "status": "error", 
                    "errors":[
                        {
                            "error":"Failed getting playlist",
                            "status_code":r.status_code,
                            #truncate the response text to 100 characters
                            "response":r.text[:100],
                            "url":playlist_url
                        }
                    ]
                }
                playlist_status_payload["state"]["reported"]["log"] = json.dumps(errlog)
                self.logger.debug(f"Publishing error status: {errlog}")
                self.device.publish_message(playlist_status_topic, playlist_status_payload, no_prefix=True)
                return False
            
            playlist_path = tempfile.mktemp()
            self.logger.debug(f"Saving playlist to temporary file: {playlist_path}")
            
            with open(playlist_path, 'wb') as f:
                f.write(r.content)
                
            json_response = r.json()
            playlist_id = json_response["playlist"]["id"]
            self.logger.debug(f"Playlist ID: {playlist_id}")
            playlist_status_payload["state"]["reported"]["filename"] = playlist_id
            self.logger.debug(f"Playlist ID added to payload")

            file_size = os.path.getsize(playlist_path)
            self.logger.debug(f"Playlist file saved - Size: {file_size} bytes")
                            
            # updating the playlist by posting the playlist_path as string to localserver
            payload = {'file_path': playlist_path}
            headers = {'Content-Type': 'application/json'}
            
            self.logger.debug(f"Sending playlist update to local server: {payload}")
            response = requests.post(f"{self.local_server_url}/update_playlist", 
                                   data=json.dumps(payload), 
                                   headers=headers, 
                                   timeout=60)
            
            self.logger.debug(f"Local server response status: {response.status_code}")
            self.logger.debug(f"Local server response: {response.text}")
            
            response.raise_for_status()
            self.logger.info(f"Playlist updated successfully - Response: {response.text}")
            
            #send a message to the cloud shadow named playlist and include the response text as a string
            playlist_status_payload["state"]["reported"]["log"] = response.text
            self.logger.debug(f"Publishing success status to cloud")
            self.device.publish_message(playlist_status_topic, playlist_status_payload, no_prefix=True)
            
            # Cleanup temporary file
            try:
                os.remove(playlist_path)
                self.logger.debug(f"Cleaned up temporary file: {playlist_path}")
            except Exception as cleanup_error:
                self.logger.warning(f"Failed to cleanup temporary file: {cleanup_error}")
            
        except requests.exceptions.Timeout as e:
            self.logger.error(f"Timeout during playlist update: {e}")
        except requests.exceptions.ConnectionError as e:
            self.logger.error(f"Connection error during playlist update: {e}")
        except requests.exceptions.HTTPError as e:
            self.logger.error(f"HTTP error during playlist update: {e}")
        except Exception as e:
            self.logger.error(f"Unexpected error handling playlist update: {e}", exc_info=True)

    def handle_get_playlist(self, upload_url: str):
        self.logger.debug(f"Received get playlist request with upload URL: {upload_url}")
        try:
            self.logger.info(f"Getting playlist from local server and uploading to: {upload_url}")
            
            # Get playlist from local server
            playlist_url = f"{self.local_server_url}/playlist"
            self.logger.debug(f"Fetching playlist from: {playlist_url}")
            
            response = requests.get(playlist_url, timeout=30)
            self.logger.debug(f"Local server response status: {response.status_code}")
            self.logger.debug(f"Local server response headers: {dict(response.headers)}")
            
            if 'content-length' in response.headers:
                self.logger.debug(f"Playlist content length: {response.headers['content-length']} bytes")
            
            response.raise_for_status()
            
            #frame the name of the playlist file as playlist_{serial_number}_{timestamp}.json
            playlist_file_name = f"playlist_{self.device.serial_number}_{int(time.time())}.json"
            self.logger.debug(f"Generated playlist filename: {playlist_file_name}")
            
            files = {'file': (playlist_file_name, response.content)}
            self.logger.debug(f"Uploading playlist to: {upload_url}")
            
            upload_response = requests.post(upload_url, files=files, timeout=60)
            self.logger.debug(f"Upload response status: {upload_response.status_code}")
            self.logger.debug(f"Upload response headers: {dict(upload_response.headers)}")
            self.logger.info(f"Playlist uploaded with response code: {upload_response.status_code}")
            
            #Once the playlist is uploaded, update post the status to the cloud shadow named playlist
            playlist_status_topic = f"$aws/things/{self.device.serial_number}/shadow/name/playlist-upload/update"
            self.logger.debug(f"Reporting status to topic: {playlist_status_topic}")
            
            playlist_status_payload={
                "state": {
                    "reported": {
                        "status": False,
                        "filename": "",
                        "error": ""
                    }
                }
            }
            
            if upload_response.status_code != 200:
                error_msg = {"message": "Failed to upload playlist"}
                self.logger.debug(f"Sending error display message: {error_msg}")
                requests.post(f"{self.local_server_url}/display_message", json=error_msg, timeout=5)
                
                self.logger.error(f"Error uploading playlist: {upload_response.text}")
                playlist_status_payload["state"]["reported"]["error"] = upload_response.text
                self.logger.debug(f"Publishing error status: {playlist_status_payload}")
                self.device.publish_message(playlist_status_topic, playlist_status_payload, no_prefix=True)
                
            else:
                success_msg = {"message": "Playlist uploaded successfully"}
                self.logger.debug(f"Sending success display message: {success_msg}")
                requests.post(f"{self.local_server_url}/display_message", json=success_msg, timeout=5)
                
                self.logger.info("Playlist uploaded successfully")
                playlist_status_payload["state"]["reported"]["status"] = True
                playlist_status_payload["state"]["reported"]["filename"] = playlist_file_name
                self.logger.debug(f"Publishing success status: {playlist_status_payload}")
                self.device.publish_message(playlist_status_topic, playlist_status_payload, no_prefix=True)
                
        except requests.exceptions.Timeout as e:
            self.logger.error(f"Timeout during playlist get/upload: {e}")
        except requests.exceptions.ConnectionError as e:
            self.logger.error(f"Connection error during playlist get/upload: {e}")
        except requests.exceptions.HTTPError as e:
            self.logger.error(f"HTTP error during playlist get/upload: {e}")
        except Exception as e:
            self.logger.error(f"Unexpected error handling get playlist: {e}", exc_info=True)
    
    def handle_get_screenshot(self, upload_url: str):
        self.logger.debug(f"Received get screenshot request with upload URL: {upload_url}")
        try:
            screenshot_update_topic = f"$aws/things/{self.device.serial_number}/shadow/name/screenshot/update"
            self.logger.debug(f"Will report status to topic: {screenshot_update_topic}")
            
            publish_payload = {
                    "state": {
                        "reported": {
                            "status": False,
                            "filename": "",
                            "error": ""
                        }
                    }
                }
            
            self.logger.info(f"Getting screenshot from local server and uploading to: {upload_url}")
            
            # Get screenshot from local server
            screenshot_url = f"{self.local_server_url}/screenshot"
            self.logger.debug(f"Fetching screenshot from: {screenshot_url}")
            
            screenshot_file_path = requests.get(screenshot_url, timeout=30)
            self.logger.debug(f"Screenshot response status: {screenshot_file_path.status_code}")
            self.logger.debug(f"Screenshot response headers: {dict(screenshot_file_path.headers)}")
            
            if 'content-length' in screenshot_file_path.headers:
                self.logger.debug(f"Screenshot content length: {screenshot_file_path.headers['content-length']} bytes")
            
            screenshot_file_path.raise_for_status()
            
            #uploaded screenshot must be named screenshot_{serial_number}_{timestamp}.png
            # screenshot_file_name = f"screenshot_{self.device.serial_number}_{int(time.time())}.png"
            # self.logger.debug(f"Generated screenshot filename: {screenshot_file_name}")
            
            # Extract the exact S3 object key (filename) from the pre-signed URL
            parsed_path = urllib.parse.urlparse(upload_url).path
            screenshot_file_name = urllib.parse.unquote(parsed_path.split('/')[-1])
            
            self.logger.debug(f"Extracted S3 filename from URL: {screenshot_file_name}")

            # files = {'file': (screenshot_file_name, screenshot_file_path.content)}
            self.logger.debug(f"Uploading screenshot to: {upload_url}")
            
            # PUT instead of POST because of AWS signature issues
            # response = requests.post(upload_url, files=files, timeout=60)
            response = requests.put(
                upload_url, 
                data=screenshot_file_path.content, 
                headers={'Content-Type': 'image/png'},
                timeout=60
            )
            self.logger.debug(f"Upload response status: {response.status_code}")
            self.logger.debug(f"Upload response headers: {dict(response.headers)}")
            self.logger.info(f"Screenshot uploaded with response code: {response.status_code}")
            
            if response.status_code != 200:
                self.logger.error(f"Error uploading screenshot: {response.text}")
                #report status to the cloud shadow "ERROR"
                publish_payload["state"]["reported"]["error"] = response.text
                self.logger.debug(f"Publishing error status: {publish_payload}")
                self.device.publish_message(screenshot_update_topic, publish_payload, no_prefix=True)
                
                error_msg = {"message": "Failed to upload screenshot"}
                self.logger.debug(f"Sending error display message: {error_msg}")
                requests.post(f"{self.local_server_url}/display_message", json=error_msg, timeout=5)
            else:
                #report status to the cloud shadow "SUCCESS"
                publish_payload["state"]["reported"]["status"] = True
                publish_payload["state"]["reported"]["filename"] = screenshot_file_name
                self.logger.debug(f"Publishing success status: {publish_payload}")
                self.device.publish_message(screenshot_update_topic, publish_payload, no_prefix=True)
                
                success_msg = {"message": "Screenshot captured and uploaded"}
                self.logger.debug(f"Sending success display message: {success_msg}")
                requests.post(f"{self.local_server_url}/display_message", json=success_msg, timeout=5)
                self.logger.info("Screenshot uploaded successfully")
                
        except requests.exceptions.Timeout as e:
            self.logger.error(f"Timeout during screenshot get/upload: {e}")
            publish_payload["state"]["reported"]["error"] = f"Timeout: {str(e)}"
            self.device.publish_message(screenshot_update_topic, publish_payload, no_prefix=True)
            error_msg = {"message": "Screenshot capture failed - timeout"}
            requests.post(f"{self.local_server_url}/display_message", json=error_msg, timeout=5)
        except requests.exceptions.ConnectionError as e:
            self.logger.error(f"Connection error during screenshot get/upload: {e}")
            publish_payload["state"]["reported"]["error"] = f"Connection error: {str(e)}"
            self.device.publish_message(screenshot_update_topic, publish_payload, no_prefix=True)
            error_msg = {"message": "Screenshot capture failed - connection error"}
            requests.post(f"{self.local_server_url}/display_message", json=error_msg, timeout=5)
        except requests.exceptions.HTTPError as e:
            self.logger.error(f"HTTP error during screenshot get/upload: {e}")
            publish_payload["state"]["reported"]["error"] = f"HTTP error: {str(e)}"
            self.device.publish_message(screenshot_update_topic, publish_payload, no_prefix=True)
            error_msg = {"message": "Screenshot capture failed - HTTP error"}
            requests.post(f"{self.local_server_url}/display_message", json=error_msg, timeout=5)
        except Exception as e:
            self.logger.error(f"Unexpected error handling get screenshot: {e}", exc_info=True)
            publish_payload["state"]["reported"]["error"] = str(e)
            self.device.publish_message(screenshot_update_topic, publish_payload, no_prefix=True)
            error_msg = {"message": "Screenshot capture failed"}
            requests.post(f"{self.local_server_url}/display_message", json=error_msg, timeout=5)
            
    def handle_reboot(self, payload_str: str):
        self.logger.debug(f"Received reboot request with payload: {payload_str}")
        try:
            self.logger.warning("Initiating system reboot via local server")
            reboot_url = f"{self.local_server_url}/reboot"
            self.logger.debug(f"Sending reboot request to: {reboot_url}")
            
            response = requests.post(reboot_url, timeout=10)
            self.logger.debug(f"Reboot request response status: {response.status_code}")
            self.logger.info("Reboot request sent successfully")
            
        except requests.exceptions.Timeout as e:
            self.logger.error(f"Timeout sending reboot request: {e}")
        except requests.exceptions.ConnectionError as e:
            self.logger.error(f"Connection error sending reboot request: {e}")
        except Exception as e:
            self.logger.error(f"Error handling reboot request: {e}", exc_info=True)

    
    def handle_settings_shadow_get_accepted(self, payload_str: str):
        try:
            new_settings = json.loads(payload_str)["state"]["desired"]
            current_settings = {}
            
            # Read current settings if file exists
            if os.path.exists(self.device.settings_file_path):
                try:
                    with open(self.device.settings_file_path, 'r') as f:
                        current_settings = json.load(f)
                except json.JSONDecodeError:
                    self.logger.warning("Current settings file is invalid JSON")
                    
            # Check if settings are different
            if current_settings != new_settings:
                self.logger.info("Settings have changed, updating file")
                with open(self.device.settings_file_path, 'w') as f:
                    json.dump(new_settings, f)
                                    
            #process local settings and let local server know
            self.device.process_settings()

            # Publish the reported settings to the cloud
            report_settings = {
                "state": {
                    "reported": new_settings
                }
            }
            self.device.publish_message(
                f"$aws/things/{self.device.serial_number}/shadow/name/settings/update", 
                report_settings, 
                no_prefix=True
            )
            
        except Exception as e:
            self.logger.error(f"Error handling shadow get accepted: {e}")

    def handle_settings_shadow_get_rejected(self, payload_str: str):
        # If the settings get request is rejected, send the current settings to the cloud
        try:
            current_settings = {}
            
            # Read current settings if file exists
            if os.path.exists(self.device.settings_file_path):
                try:
                    with open(self.device.settings_file_path, 'r') as f:
                        current_settings = json.load(f)
                except json.JSONDecodeError:
                    self.logger.warning("Current settings file is invalid JSON")
                    
            # Publish the current settings to the cloud
            report_settings = {
                "state": {
                    "reported": current_settings,
                    "desired": current_settings
                }
            }
            self.device.publish_message(
                f"$aws/things/{self.device.serial_number}/shadow/name/settings/update", 
                report_settings, 
                no_prefix=True
            )
            
        except Exception as e:
            self.logger.error(f"Error handling shadow get rejected: {e}")
            
    
    def handle_settings_shadow_update_delta(self, payload_str: str):
        try:
            self.logger.info(f"Received settings delta: {payload_str}")
            delta = json.loads(payload_str)["state"]
            settings_changed = False
            
            # Read current settings
            current_settings = {}
            if os.path.exists(self.device.settings_file_path):
                with open(self.device.settings_file_path, 'r') as f:
                    current_settings = json.load(f)
            
            # Update only changed settings
            for key, value in delta.items():
                if key not in current_settings or current_settings[key] != value:
                    current_settings[key] = value
                    settings_changed = True
                    
            if settings_changed:
                self.logger.info("Settings have changed, updating file")
                with open(self.device.settings_file_path, 'w') as f:
                    json.dump(current_settings, f)
            
            #process local settings and let local server know
            self.device.process_settings()
            
            # Publish the reported settings to the cloud
            report_settings = {
                "state": {
                    "reported": current_settings
                }
            }
            self.device.publish_message(
                f"$aws/things/{self.device.serial_number}/shadow/name/settings/update", 
                report_settings, 
                no_prefix=True
            )
            
        except Exception as e:
            self.logger.error(f"Error handling shadow update delta: {e}")

    def handle_exec(self, payload_str: str):
        # payload format {'command': 'command to execute including arguments', 'timeout': 'timeout in seconds', execution_id: 'unique id for the execution'}"
        # first parse  the payload to make sure it has the required fields. after execution, send a shadow update with the result including the execution_id
        self.logger.debug(f"Received exec request with payload: {payload_str}")
        
        execution_id = ""
        command = ""
        
        try:
            payload = json.loads(payload_str)
            self.logger.debug(f"Parsed exec payload: {payload}")
            
            command = payload["command"]
            timeout = payload.get("timeout", 10)
            execution_id = payload.get("execution_id", "")
            
            self.logger.info(f"Executing command: {command}")
            self.logger.debug(f"Command timeout: {timeout}s, Execution ID: {execution_id}")
            
            result = subprocess.run(command, shell=True, capture_output=True, text=True, timeout=timeout)
            
            self.logger.debug(f"Command execution completed:")
            self.logger.debug(f"  Return code: {result.returncode}")
            self.logger.debug(f"  Stdout length: {len(result.stdout)} chars")
            self.logger.debug(f"  Stderr length: {len(result.stderr)} chars")
            
            if result.stdout:
                self.logger.debug(f"  Stdout: {result.stdout[:200]}{'...' if len(result.stdout) > 200 else ''}")
            if result.stderr:
                self.logger.debug(f"  Stderr: {result.stderr[:200]}{'...' if len(result.stderr) > 200 else ''}")
            
            # Publish the result to the cloud
            exec_result = {
                "state": {
                    "reported": {
                        "execution_id": execution_id,
                        "command": command,
                        "stdout": result.stdout,
                        "stderr": result.stderr,
                        "returncode": result.returncode
                    }
                }
            }
            
            self.logger.info(f"Command executed successfully with return code: {result.returncode}")
            self.logger.debug(f"Publishing exec result to cloud")
            
            self.device.publish_message(
                f"$aws/things/{self.device.serial_number}/shadow/name/exec/update", 
                exec_result, 
                no_prefix=True
            )

        except json.JSONDecodeError as e:
            self.logger.error(f"Invalid JSON in exec payload: {e}")
            self._publish_exec_error(execution_id, command, f"Invalid JSON: {str(e)}")
        except subprocess.TimeoutExpired as e:
            self.logger.error(f"Command execution timed out: {e}")
            self._publish_exec_error(execution_id, command, f"Command timed out after {timeout}s")
        except KeyError as e:
            self.logger.error(f"Missing required field in exec payload: {e}")
            self._publish_exec_error(execution_id, command, f"Missing required field: {str(e)}")
        except Exception as e:
            self.logger.error(f"Unexpected error handling exec command: {e}", exc_info=True)
            self._publish_exec_error(execution_id, command, str(e))
    
    def handle_ping(self, payload_str: str):
        self.logger.debug(f"Received ping request with payload: {payload_str}")
        try:
            payload = json.loads(payload_str)
            ping_id = payload.get("ping_id", "unknown")
            
            # Instantly bounce the unique ID into the AWS Shadow
            ping_result = {
                "state": {
                    "reported": {
                        "last_ping_id": ping_id,
                        "timestamp": int(time.time())
                    }
                }
            }
            
            self.logger.debug(f"Bouncing PONG back to shadow with ID: {ping_id}")
            self.device.publish_message(
                f"$aws/things/{self.device.serial_number}/shadow/name/connectivity/update", 
                ping_result, 
                no_prefix=True
            )
        except Exception as e:
            self.logger.error(f"Error handling ping request: {e}")
    
    def _publish_exec_error(self, execution_id: str, command: str, error_msg: str):
        """Helper method to publish exec error results"""
        try:
            exec_result = {
                "state": {
                    "reported": {
                        "execution_id": execution_id,
                        "command": command,
                        "stdout": "",
                        "stderr": error_msg,
                        "returncode": -1
                    }
                }
            }
            self.logger.debug(f"Publishing exec error result: {error_msg}")
            self.device.publish_message(
                f"$aws/things/{self.device.serial_number}/shadow/name/exec/update", 
                exec_result, 
                no_prefix=True
            )
        except Exception as e:
            self.logger.error(f"Failed to publish exec error result: {e}")

class EkaDevice:
    instance = None
    
    def __init__(self):
        self.version = "1.0.0"
        self.logger = setup_logging()
        
        self.logger.info("Initializing EkaDevice")
        self.logger.debug(f"Version: {self.version}")
        
        self.base_config_path = "/opt/eka/eka-config/"
        self.sys_data_period = 60
        self.TIMEOUT = 100
        self.local_server_url = "http://localhost:8000"
        self.serial_number = ""
        self.endpoint = ""
        self.client = None
        self.credentials = None
        self.credential_status = False
        self.settings_file_path = "/opt/eka/eka-settings/eka-settings.json"
        
        self.logger.debug(f"Configuration:")
        self.logger.debug(f"  Base config path: {self.base_config_path}")
        self.logger.debug(f"  System data period: {self.sys_data_period}s")
        self.logger.debug(f"  Connection timeout: {self.TIMEOUT}s")
        self.logger.debug(f"  Local server URL: {self.local_server_url}")
        self.logger.debug(f"  Settings file path: {self.settings_file_path}")
        
        # Thread-safe events
        self.connected = threading.Event()
        self.shutdown = threading.Event()
        self.logger.debug("Created threading events for connection and shutdown")
        
        # Executor
        self.executor = ThreadPoolExecutor(max_workers=5)
        self.connection_future = None
        self.logger.debug("Created ThreadPoolExecutor with 5 max workers")
        
        # MQTT state
        self.future_stopped = None
        self.future_connection_success = None
        self.CLEANUP_TIMEOUT = 2  # Shorter timeout
        self.cleaning_up = False  # Track cleanup state

        self.min_reconnect_delay = 1
        self.max_reconnect_delay = 128
        self.current_reconnect_delay = self.min_reconnect_delay
        
        self.logger.debug(f"MQTT reconnection settings:")
        self.logger.debug(f"  Min delay: {self.min_reconnect_delay}s")
        self.logger.debug(f"  Max delay: {self.max_reconnect_delay}s")
        self.logger.debug(f"  Cleanup timeout: {self.CLEANUP_TIMEOUT}s")
        
        self.topic_filter = []
        
        EkaDevice.instance = self
        self.logger.debug("Set global EkaDevice instance")

        # FastAPI setup
        self.logger.debug("Setting up FastAPI application")
        self.api = FastAPI(title="EkaDevice API")
        self.setup_api_routes()
        self.topic_handler = TopicHandler(self)
        
        self.logger.info("EkaDevice initialization completed successfully")


    def setup_api_routes(self):        
        @self.api.get("/status")
        def get_status():
            return {
                "connection": self.connected.is_set(),
                "network": self.wait_for_connectivity(),
                "credentials": self.credential_status,
                "uptime": round(time.time() - self.start_time if hasattr(self, 'start_time') else 0, 2)
            }

        @self.api.post("/publish")
        async def publish_message(payload: MessagePayload):
            if not self.connected.is_set():
                raise HTTPException(status_code=503, detail="Device not connected")
            try:
                self.publish_message(payload.topic, payload.message)
                return {"status": "success"}
            except Exception as e:
                raise HTTPException(status_code=500, detail=str(e))

        @self.api.get("/restart")
        async def restart_connection():
            self.restart()
            return {"status": "restarting connection"}
        
        @self.api.get("/serial")
        async def get_serial():
            return {"serial_number": self.serial_number}
        
        @self.api.post("/report_settings")
        async def report_settings(payload: Dict):
            try:
                settings_update_topic = f"$aws/things/{self.serial_number}/shadow/name/settings/update"
                update_payload={
                    "state": {
                        "reported": payload,
                        "desired": payload
                    }
                }
                self.publish_message(settings_update_topic, update_payload, no_prefix=True)
                return {"status": "success"}
            except Exception as e:
                self.logger.error(f"Error reporting settings: {e}")
                raise HTTPException(status_code=500, detail=str(e))
        
        @self.api.post("/network_restored")
        async def network_restored():
            self.logger.info("Network restored event received from local_server. Initiating sync...")
            try:
                # 1. State Check & Wait: Allow the awscrt background thread to finish connecting
                if not self.connected.is_set():
                    self.logger.warning("MQTT client not ready. Waiting for background TLS handshake...")
                    self.connected.wait(timeout=self.TIMEOUT)

                # 2. Publish Block: Only execute if the state is verifiably online
                if self.connected.is_set():
                    # Formatting payload similar to system telemetry
                    payload = {
                        "status": "online",
                        "trigger": "network_restored",
                        "timestamp": int(time.time())
                    }
                    
                    self.logger.info(f"Publishing sync message to eka-device/{self.serial_number}/reconnected")
                    self.publish_message("reconnected", payload)
                    
                    return {"status": "success", "message": "Sync message published"}
                
                # 3. Graceful Fallback
                else:
                    self.logger.error("Sync aborted: MQTT client failed to reconnect within 5 seconds.")
                    return {"status": "error", "message": "AWS client offline"}
                    
            except Exception as e:
                self.logger.error(f"Exception in network_restored publishing routine: {e}")
                return {"status": "error", "message": "Internal server error"}

    def restart(self):
        self.cleanup()
        # systemd must handle restarting the service
        
    def wait_for_connectivity(self) -> bool:
        self.logger.debug("Checking internet connectivity")
        
        session = requests.Session()
        retries = Retry(total=5, backoff_factor=1, status_forcelist=[502, 503, 504])
        session.mount('http://', HTTPAdapter(max_retries=retries))
        session.mount('https://', HTTPAdapter(max_retries=retries))
        
        test_url = "https://www.amazontrust.com"
        self.logger.debug(f"Testing connectivity to: {test_url}")
        
        try:
            response = session.get(test_url, timeout=5)
            self.logger.debug(f"Connectivity test response status: {response.status_code}")
            self.logger.info("Internet connectivity confirmed")
            return True
        except requests.exceptions.Timeout as e:
            self.logger.warning(f"Connectivity test timed out: {e}")
            return False
        except requests.exceptions.ConnectionError as e:
            self.logger.warning(f"Connectivity test connection error: {e}")
            return False
        except requests.exceptions.RequestException as e:
            self.logger.warning(f"Connectivity test failed: {e}")
            return False
        except Exception as e:
            self.logger.error(f"Unexpected error during connectivity test: {e}", exc_info=True)
            return False

    def setup_credentials(self, root_ca_download=False) -> bool:
        self.logger.debug(f"Setting up credentials (root_ca_download={root_ca_download})")
        
        try:
            # encrypt credentials file if it exists in the first run and remove the clear text file
            credentials_clear_path = f"{self.base_config_path}credentials.json"
            credentials_enc_path = f"{self.base_config_path}credentials.enc"
            
            self.logger.debug(f"Checking for credentials files:")
            self.logger.debug(f"  Clear text: {credentials_clear_path}")
            self.logger.debug(f"  Encrypted: {credentials_enc_path}")
            
            if os.path.exists(credentials_clear_path):
                self.logger.info("Found clear text credentials file - encrypting")
                systemd_creds = f"systemd-creds encrypt --name='eka-credentials' {credentials_clear_path} {credentials_enc_path}"
                self.logger.debug(f"Encryption command: {systemd_creds}")
                
                with os.popen(systemd_creds) as f:
                    pass
                
                self.logger.debug("Removing clear text credentials file")
                os.remove(credentials_clear_path)
                self.logger.info("Credentials encrypted successfully")
            
            if os.path.exists(credentials_enc_path):
                self.logger.info("Decrypting credentials file")
                systemd_creds = f"systemd-creds decrypt --name='eka-credentials' {credentials_enc_path}"
                self.logger.debug(f"Decryption command: {systemd_creds}")
                
                with os.popen(systemd_creds) as f:
                    self.credentials = json.load(f)
                
                self.logger.debug("Credentials decrypted successfully")
                
                if "serial_number" in self.credentials:
                    self.serial_number = self.credentials["serial_number"]
                    self.logger.info(f"Device serial number: {self.serial_number}")
                else:
                    self.logger.error("Serial number not found in credentials")
                    return False
                
                if "endpoint" in self.credentials:
                    self.endpoint = self.credentials["endpoint"]
                    self.logger.info(f"AWS IoT endpoint: {self.endpoint}")
                else:
                    self.logger.error("Endpoint not found in credentials")
                    return False
                    
                # Check if this is first boot and password should be reset
                first_boot_marker = f"/boot/firmware/password_initialized"
                self.logger.debug(f"Checking first boot marker: {first_boot_marker}")
                
                if "password" in self.credentials and not os.path.exists(first_boot_marker):
                    try:
                        self.logger.info("First boot detected - initializing system password")
                        # Create password file for chpasswd
                        password_file = tempfile.NamedTemporaryFile(delete=False, mode='w')
                        try:
                            # Format for chpasswd: username:password
                            password_file.write(f"ekauser:{self.credentials['password']}")
                            password_file.close()
                            
                            self.logger.debug(f"Created temporary password file: {password_file.name}")
                            
                            # Use chpasswd to update password (more secure than echo | passwd)
                            result = subprocess.run(
                                ["sudo", "chpasswd"], 
                                input=open(password_file.name, 'r').read(),
                                text=True,
                                capture_output=True
                            )
                            
                            self.logger.debug(f"chpasswd return code: {result.returncode}")
                            if result.stderr:
                                self.logger.debug(f"chpasswd stderr: {result.stderr}")
                            
                            if result.returncode == 0:
                                self.logger.info("Password updated successfully")
                                # now permenently change the hostname to ekadev_<serial_number>
                                hostname = f"ekadev-{self.serial_number}"
                                self.logger.info(f"Setting hostname to: {hostname}")
                                
                                hostname_result = subprocess.run(["sudo", "hostnamectl", "set-hostname", hostname])
                                self.logger.debug(f"hostnamectl return code: {hostname_result.returncode}")
                                
                                # Update /etc/hosts . replace ekadev with ekadev_<serial_number> using sed
                                self.logger.debug("Updating /etc/hosts file")
                                hosts_result = subprocess.run(["sudo", "sed", "-i", f"s/ekadev/{hostname}/g", "/etc/hosts"])
                                self.logger.debug(f"sed return code: {hosts_result.returncode}")
                                
                                # Create marker file to prevent future password resets
                                Path(first_boot_marker).touch()
                                self.logger.info("Created first boot marker file")
                            else:
                                self.logger.error(f"Password update failed: {result.stderr}")
                        finally:
                            # Always clean up the temporary password file
                            if os.path.exists(password_file.name):
                                os.remove(password_file.name)
                                self.logger.debug("Cleaned up temporary password file")
                    except Exception as e:
                        self.logger.error(f"Failed to set password: {e}", exc_info=True)
                else:
                    self.logger.debug("Password initialization not needed")
                
                if root_ca_download:
                    self.logger.info("Downloading root CA certificates")
                    self.credentials["ca_bytes"] = ""
                    
                    if "root_ca" in self.credentials:
                        for i, root_ca in enumerate(self.credentials["root_ca"]):
                            self.logger.debug(f"Downloading root CA {i+1}/{len(self.credentials['root_ca'])}: {root_ca}")
                            try:
                                r = requests.get(root_ca, timeout=10)
                                r.raise_for_status()
                                self.credentials["ca_bytes"] += r.text
                                self.logger.debug(f"Downloaded CA certificate {i+1} - {len(r.text)} bytes")
                            except Exception as e:
                                self.logger.error(f"Failed to download root CA {root_ca}: {e}")
                                return False
                        
                        self.logger.info(f"Downloaded all root CA certificates - Total: {len(self.credentials['ca_bytes'])} bytes")
                    else:
                        self.logger.error("No root_ca URLs found in credentials")
                        return False
                else:
                    self.logger.debug("Skipping root CA download")
                
                #update topic_handlers with serial number
                self.logger.debug("Updating topic handlers with serial number")
                self.topic_handler.update_topic_mapping(self.serial_number)
                
                #setup subscription topic filter for topics in topic_handler.topic_mapping
                self.topic_filter = []
                for topic in self.topic_handler.topic_mapping.keys():
                    self.topic_filter.append(topic)
                
                self.logger.info(f"Set up {len(self.topic_filter)} topic filters for subscription")

                # export a $EKA_SERIAL variable to the shell as well as the bashrc file if the variable with the right value does not exist
                if os.environ.get("EKA_SERIAL") != self.serial_number:
                    self.logger.debug(f"Setting EKA_SERIAL environment variable to: {self.serial_number}")
                    os.environ["EKA_SERIAL"] = self.serial_number
                    
                    try:
                        with open("/etc/profile", "a") as f:
                            f.write(f"\nexport EKA_SERIAL={self.serial_number}\n")
                        self.logger.debug("Added EKA_SERIAL to /etc/profile")
                    except Exception as e:
                        self.logger.warning(f"Failed to add EKA_SERIAL to /etc/profile: {e}")
                else:
                    self.logger.debug("EKA_SERIAL environment variable already set correctly")
                
                # file eka_<serial_number> will be visible in the bootfs partition. allows easy offline identification
                boot_marker = f"/boot/firmware/eka_{self.serial_number}"
                if not os.path.exists(boot_marker):
                    self.logger.debug(f"Creating boot marker file: {boot_marker}")
                    Path(boot_marker).touch()
                    self.logger.info("Created boot marker file for offline identification")
                else:
                    self.logger.debug("Boot marker file already exists")
                
                self.logger.info("Credentials setup completed successfully")
                return True
            else:
                self.logger.error(f"Credentials file not found: {credentials_enc_path}")
                return False
                
        except json.JSONDecodeError as e:
            self.logger.error(f"Invalid JSON in credentials file: {e}")
            return False
        except Exception as e:
            self.logger.error(f"Credential setup failed: {e}", exc_info=True)
            return False

    def on_lifecycle_stopped(self, lifecycle_stopped_data: mqtt5.LifecycleStoppedData):
        self.logger.info("MQTT lifecycle stopped event received")
        self.logger.debug(f"Lifecycle stopped data: {lifecycle_stopped_data}")
        
        if self.future_stopped and not self.future_stopped.done():
            self.future_stopped.set_result(lifecycle_stopped_data)
            self.logger.debug("Set future_stopped result")
        
        self.connected.clear()
        self.logger.info("AWSIoT Connection Stopped - cleared connected event")

    def on_lifecycle_connection_success(self, lifecycle_connect_success_data: mqtt5.LifecycleConnectSuccessData):
        self.logger.info("MQTT lifecycle connection success event received")
        self.logger.debug(f"Connection success data: {lifecycle_connect_success_data}")
        
        if hasattr(lifecycle_connect_success_data, 'connack_packet'):
            connack = lifecycle_connect_success_data.connack_packet
            self.logger.debug(f"CONNACK reason code: {connack.reason_code}")
            if hasattr(connack, 'session_expiry_interval'):
                self.logger.debug(f"Session expiry interval: {connack.session_expiry_interval}")
        
        if self.future_connection_success and not self.future_connection_success.done():
            self.future_connection_success.set_result(lifecycle_connect_success_data)
            self.logger.debug("Set future_connection_success result")
        
        if self.connected.is_set():
            self.logger.warning("Auto-reconnection detected! Re-subscribing in background...")
            self.executor.submit(self.subscribe_topics)

        self.connected.set()
        self.logger.info("AWSIoT Connection Success - set connected event")

    def on_lifecycle_connection_failure(self, lifecycle_connection_failure: mqtt5.LifecycleConnectFailureData):
        self.logger.error("MQTT lifecycle connection failure event received")
        self.logger.error(f"Connection failure exception: {lifecycle_connection_failure.exception}")
        self.logger.debug(f"Connection failure data: {lifecycle_connection_failure}")
        
        if hasattr(lifecycle_connection_failure, 'connack_packet') and lifecycle_connection_failure.connack_packet:
            connack = lifecycle_connection_failure.connack_packet
            self.logger.error(f"CONNACK reason code: {connack.reason_code}")
        
        self.connected.clear()
        self.logger.info("Connection failure - cleared connected event")

    def connect(self) -> bool:
        self.logger.info("Starting MQTT connection process")
        try:
            # Reset futures before connection attempt
            self.future_connection_success = Future()
            self.future_stopped = Future()
            self.logger.debug("Reset connection futures")
            
            self.logger.debug("Creating MQTT5 client with mTLS")
            self.logger.debug(f"Endpoint: {self.endpoint}")
            self.logger.debug(f"Client ID: {self.serial_number}")
            self.logger.debug(f"Certificate length: {len(self.credentials['certificate_pem'])} chars")
            self.logger.debug(f"Private key length: {len(self.credentials['private_key'])} chars")
            self.logger.debug(f"CA bytes length: {len(self.credentials['ca_bytes'])} chars")
            
            self.client = mqtt5_client_builder.mtls_from_bytes(
                endpoint=self.endpoint,
                cert_bytes=self.credentials["certificate_pem"].encode('utf-8'),
                pri_key_bytes=self.credentials["private_key"].encode('utf-8'),
                ca_bytes=self.credentials["ca_bytes"].encode('utf-8'),
                on_lifecycle_stopped=self.on_lifecycle_stopped,
                on_publish_received=self.on_publish_received, 
                on_lifecycle_connection_success=self.on_lifecycle_connection_success,
                on_lifecycle_connection_failure=self.on_lifecycle_connection_failure,
                client_id=self.serial_number
            )
            
            self.logger.debug("MQTT5 client created successfully")
            self.logger.info("Starting MQTT client connection")
            self.client.start()
            
            self.logger.debug(f"Waiting for connection success (timeout: {self.TIMEOUT}s)")
            lifecycle_connect_success_data = self.future_connection_success.result(self.TIMEOUT)
            connack_packet = lifecycle_connect_success_data.connack_packet

            self.logger.debug(f"Connection attempt completed with reason code: {connack_packet.reason_code}")

            if connack_packet.reason_code != mqtt5.ConnectReasonCode.SUCCESS:
                self.logger.error(f"Connection failed with reason code: {connack_packet.reason_code}")
                self.logger.debug("Stopping client due to connection failure")
                self.client.stop()
                self.future_stopped.result(self.TIMEOUT)
                return False

            self.logger.info("MQTT connection established successfully")
            
            # Subscribe to topics after successful connection
            self.logger.debug("Subscribing to topics after successful connection")
            if not self.subscribe_topics():
                self.logger.error("Failed to subscribe to topics")
                return False
            
            self.logger.info("MQTT connection and subscription completed successfully")
            return True

        except concurrent.futures.TimeoutError as e:
            self.logger.error(f"Connection timeout after {self.TIMEOUT}s: {e}")
            return False
        except Exception as e:
            self.logger.error(f"Connection error: {e}", exc_info=True)
            return False

    def subscribe_topics(self):
        self.logger.info(f"Subscribing to {len(self.topic_filter)} topics")
        try:
            for i, topic in enumerate(self.topic_filter):
                self.logger.debug(f"Subscribing to topic {i+1}/{len(self.topic_filter)}: {topic}")
                
                subscribe_future = self.client.subscribe(
                    subscribe_packet=mqtt5.SubscribePacket(
                        subscriptions=[mqtt5.Subscription(
                            topic_filter=topic,
                            qos=mqtt5.QoS.AT_LEAST_ONCE
                        )]
                    )
                )
                
                self.logger.debug(f"Waiting for subscription result (timeout: {self.TIMEOUT}s)")
                suback = subscribe_future.result(self.TIMEOUT)
                self.logger.debug(f"Subscription response for {topic}: {suback.reason_codes}")
                
                if len(suback.reason_codes) > 0 and suback.reason_codes[0] != mqtt5.SubackReasonCode.GRANTED_QOS_1:
                    self.logger.error(f"Subscription failed for {topic}: {suback.reason_codes[0]}")
                    return False
                else:
                    self.logger.debug(f"Successfully subscribed to: {topic}")
            
            self.logger.info("Successfully subscribed to all topics")
            
            # Request settings shadow after all subscriptions are complete
            settings_topic = f"$aws/things/{self.serial_number}/shadow/name/settings/get"
            self.logger.debug(f"Requesting settings shadow: {settings_topic}")
            
            self.publish_message(topic=settings_topic, message="", no_prefix=True)
            self.logger.info("Settings shadow request sent")
            
            return True
            
        except concurrent.futures.TimeoutError as e:
            self.logger.error(f"Subscription timeout after {self.TIMEOUT}s: {e}")
            return False
        except Exception as e:
            self.logger.error(f"Subscription error: {e}", exc_info=True)
            return False

    def on_publish_received(self, publish_packet_data: mqtt5.PublishReceivedData):
        """Global callback for MQTT message reception"""
        try:
            packet = publish_packet_data.publish_packet
            topic_mapping = self.topic_handler.topic_mapping
            
            payload_str = packet.payload.decode('utf-8') if packet.payload else ""
            self.logger.info(f"Received MQTT message on topic: {packet.topic}")
            self.logger.debug(f"Message payload length: {len(payload_str)} characters")
            self.logger.debug(f"Message QoS: {packet.qos}")
            self.logger.debug(f"Message retain: {packet.retain}")
            
            if len(payload_str) < 500:  # Log full payload if it's short
                self.logger.debug(f"Message payload: {payload_str}")
            else:  # Log truncated payload if it's long
                self.logger.debug(f"Message payload (truncated): {payload_str[:500]}...")
            
            if packet.topic in topic_mapping:
                self.logger.info(f"Found handler for topic: {packet.topic}")
                handler_name = topic_mapping[packet.topic].__name__
                self.logger.debug(f"Calling handler: {handler_name}")
                
                topic_mapping[packet.topic](payload_str)
                self.logger.debug(f"Handler {handler_name} completed successfully")
            else:
                self.logger.warning(f"No handler found for topic: {packet.topic}")
                self.logger.debug(f"Available topics: {list(topic_mapping.keys())}")
                
        except UnicodeDecodeError as e:
            self.logger.error(f"Failed to decode message payload as UTF-8: {e}")
        except Exception as e:
            self.logger.error(f"Message processing error for topic {packet.topic if 'packet' in locals() else 'unknown'}: {e}", exc_info=True)

    def tv_power_get(self) -> str:
        self.logger.debug("Getting TV Power via CEC")
        self.logger.debug("cloud connect tv_power_get(), opening lock file...")
        with open(CEC_LOCK_FILE, 'w') as lockfile:
            try:
                # Try to acquire the lock (blocking)
                self.logger.debug("cloud connect tv_power_get(), attempting to acquire lock...")
                fcntl.flock(lockfile, fcntl.LOCK_EX)
                self.logger.debug("cloud connect tv_power_get(), lock acquired.")
                return self._cec_query_power()
            except Exception as e:
                # An error means we could not determine the state -- report UNKNOWN,
                # NOT OFF, so a failed query can't masquerade as a powered-off TV.
                self.logger.error(f"Error getting TV power status: {e}")
                return TV_POWER_UNKNOWN
            finally:
                self.logger.debug("cloud connect tv_power_get(), attempting to release lock...")
                fcntl.flock(lockfile, fcntl.LOCK_UN)
                self.logger.debug("cloud connect tv_power_get(), lock released.")

    def _cec_query_power(self, poll_interval: float = 0.7, deadline_s: float = 6.0) -> str:
        """Read the TV's live power state via CEC as a tri-state string.

        Returns TV_POWER_ON, TV_POWER_OFF, or TV_POWER_UNKNOWN. UNKNOWN means we could
        NOT determine the state -- the TV answered "unknown" or gave no reply before the
        deadline (e.g. the degraded null-EDID state where the CEC adapter never gets a
        physical/logical address). Reporting UNKNOWN (rather than OFF) keeps a can't-tell
        result distinct from a genuine powered-off TV instead of collapsing both to OFF.

        The TV answers "Give Device Power Status" only after cec-client has finished
        registering (allocating a logical address), which takes ~1-2s cold. Rather than a
        fixed warm-up sleep, we re-send `pow 0` every ~0.7s and terminate cec-client the
        instant a "power status:" line lands -- so a warm bus returns in ~1.5s and only a
        cold bus pays the full registration cost (~4.5s observed). `deadline_s` is a hard
        cap so a dead/unresponsive bus can't hang the call. Without -s, cec-client never
        exits on stdin EOF, so we always terminate() it in the finally block -- otherwise it
        would be orphaned holding /dev/cec0 and every subsequent poll would fail with EBUSY.
        """
        proc = subprocess.Popen(
            ["cec-client", "-d", "1", "-o", "ekadevice"],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL, text=True, bufsize=1,
        )
        deadline = time.monotonic() + deadline_s
        next_poll = 0.0
        try:
            while time.monotonic() < deadline:
                now = time.monotonic()
                if now >= next_poll:
                    try:
                        proc.stdin.write("pow 0\n")
                        proc.stdin.flush()
                    except (BrokenPipeError, ValueError):
                        break
                    next_poll = now + poll_interval
                # Block for a line, but wake at poll_interval to re-poll / re-check the deadline.
                rlist, _, _ = select.select([proc.stdout], [], [], poll_interval)
                if not rlist:
                    continue
                line = proc.stdout.readline()
                if not line:
                    break
                low = line.lower()
                if "power status:" in low:
                    self.logger.debug(f"cec pow -> {line.strip()!r}")
                    status = low.split("power status:", 1)[1].strip()
                    # Map cec-client's exact statuses to ON / OFF / UNKNOWN.
                    if status in ("on", "in transition from standby to on"):
                        return TV_POWER_ON
                    if status in ("standby", "in transition from on to standby"):
                        return TV_POWER_OFF
                    # "unknown" (or anything unexpected) -> can't tell.
                    return TV_POWER_UNKNOWN
            # No "power status:" line arrived before the deadline: can't tell, not off.
            self.logger.warning("CEC power query: no reply before deadline")
            return TV_POWER_UNKNOWN
        finally:
            proc.terminate()
            try:
                proc.wait(timeout=2)
            except subprocess.TimeoutExpired:
                proc.kill()
        
            
    def tv_display_get(self) -> str:
        env = os.environ.copy()
        if 'XDG_RUNTIME_DIR' not in env:
            env['XDG_RUNTIME_DIR'] = f'/run/user/1000' #ekausers uid is 1000

        # Use wlr-randr to get the display configuration
        try:
            result = subprocess.run(["wlr-randr"], capture_output=True, text=True, env=env)
            return result.stdout.strip()
        except Exception as e:
            self.logger.error(f"Error getting TV display status: {e}")
            return ""

    def get_system_data(self) -> Dict:
        # Get IP addresses for all interfaces
        ip_addresses = {}
        for interface, addrs in psutil.net_if_addrs().items():
            ip_addresses[interface] = []
            for addr in addrs:
                if addr.family == socket.AF_INET:  # IPv4
                    ip_addresses[interface].append({
                        'address': addr.address,
                        'netmask': addr.netmask,
                        'broadcast': addr.broadcast
                    })
                elif addr.family == socket.AF_INET6:  # IPv6
                    ip_addresses[interface].append({
                        'address': addr.address,
                        'netmask': addr.netmask
                    })
        # fetch wifi SSID and signal strength of currently connected network using nmcli
        try:
            result = subprocess.run("nmcli -t -f active,ssid,signal dev wifi | egrep '^yes' | cut -d: -f2,3", 
                                    shell=True, 
                                    capture_output=True, 
                                    text=True)
            ssid, signal_strength = result.stdout.strip().split(':')
        except Exception as e:
            ssid = ""
            signal_strength = ""
            self.logger.error(f"Error getting wifi info: {e}")

        return {
            "timestamp": datetime.now().isoformat(),
            "timezone": datetime.now().astimezone().tzname(),           
            "system": {
                "platform": platform.platform(),
                "processor": platform.processor(),
                "python_version": platform.python_version(),
                "cloudconnect_version": self.version,
                "local_server_version": requests.get(f"{self.local_server_url}/version").text.strip('"'),
                "uptime": int(psutil.boot_time())
            },
            "memory": psutil.virtual_memory()._asdict(),
            "cpu": {
                "percent": psutil.cpu_percent(interval=1, percpu=True),
                "freq": psutil.cpu_freq()._asdict() if psutil.cpu_freq() else {},
                "stats": psutil.cpu_stats()._asdict()
            },
            "disk": {
                "usage": {p.mountpoint: psutil.disk_usage(p.mountpoint)._asdict() 
                         for p in psutil.disk_partitions()}
            },
            "network": {
                "interfaces": psutil.net_if_stats(),
                "io_counters": psutil.net_io_counters()._asdict(),
                "ip_addresses": ip_addresses,
                "wifi":{
                    "ssid": ssid,
                    "signal_strength": signal_strength
                }
            },
            "tv": {
                "power": self.tv_power_get(),
                "display": self.tv_display_get()
            }
        }

    def publish_message(self, topic: str, message: str, no_prefix=False) -> bool:
        """Publish a message to a specific topic with device prefix"""
        if not self.connected.is_set():
            self.logger.error("Cannot publish - device not connected")
            return False
        
        MAX_RETRIES = 1 # update for future retries
        RETRY_DELAY = 1  # seconds
        
        # Determine the actual topic to publish to
        if no_prefix:
            pub_topic = topic
        else:
            pub_topic = f"eka-device/{self.serial_number}/{topic}"
        
        # Convert message to JSON string if it's not already a string
        message_str = json.dumps(message) if isinstance(message, (dict, list)) else str(message)
        
        self.logger.debug(f"Publishing message to topic: {pub_topic}")
        self.logger.debug(f"Message length: {len(message_str)} characters")
        
        if len(message_str) < 500:  # Log full message if it's short
            self.logger.debug(f"Message content: {message_str}")
        else:  # Log truncated message if it's long
            self.logger.debug(f"Message content (truncated): {message_str[:500]}...")
        
        for attempt in range(MAX_RETRIES):
            try:
                self.logger.debug(f"Publish attempt {attempt + 1}/{MAX_RETRIES}")
                
                # Create MQTT5 publish packet
                publish_packet = mqtt5.PublishPacket(
                    topic=pub_topic,
                    payload=message_str.encode('utf-8'),
                    qos=mqtt5.QoS.AT_MOST_ONCE,
                    retain=False
                )
                
                self.logger.debug(f"Created publish packet - QoS: {publish_packet.qos}, Retain: {publish_packet.retain}")
                
                # Publish using MQTT5 client
                publish_future = self.client.publish(publish_packet)
                
                # Wait for publish to complete with timeout. failure results in retry.
                try:
                    result = publish_future.result(timeout=3)
                    self.logger.debug(f"Publish completed successfully")
                    if hasattr(result, 'puback'):
                        self.logger.debug(f"PUBACK reason code: {result.puback.reason_code}")
                except concurrent.futures.TimeoutError:
                    self.logger.warning(f"Publish timeout on attempt {attempt + 1} - continuing anyway")
                    pass
                
                self.logger.info(f"Published message to {pub_topic}")
                return True
                
            except Exception as e:
                self.logger.error(f"Publish attempt {attempt + 1} failed. Topic: {pub_topic}: {str(e)}")
                if attempt < MAX_RETRIES - 1:
                    self.logger.info(f"Retrying in {RETRY_DELAY} seconds...")
                    time.sleep(RETRY_DELAY)
                else:
                    self.logger.error("Max retries reached. Publish failed.")
                    return False

    def publish_system_data(self,once=False):
        while not self.shutdown.is_set():
            if self.connected.wait(self.TIMEOUT):
                try:
                    sys_data = {
                        "state": {
                            "reported": self.get_system_data()
                    }
                    }
        
                    sysdata_shadow_topic = f"$aws/things/{self.serial_number}/shadow/name/sysdata/update"
                    
                    if self.publish_message(sysdata_shadow_topic, sys_data, no_prefix=True):
                        self.logger.info("System data published successfully")
                    else:
                        self.logger.error("Failed to publish system data")
                except Exception as e:
                    self.logger.error(f"Error publishing system data: {e}")
                    
            if once:
                break
            threading.Event().wait(self.sys_data_period)
    
    def send_status(self):
        # send connection status to the local server by posting to the /status endpoint.
        # {"registration": True/False}
        while not self.shutdown.is_set():
            try:
                status = {
                    "registration": self.connected.is_set()
                }
                r = requests.post(f"{self.local_server_url}/status", json=status)
                if r.status_code != 200:
                    self.logger.error(f"Error sending status: {r.text}")
            except Exception as e:
                self.logger.error(f"Error sending status: {e}")
            # if status is false, wait for 1 second before trying again. else wait for 10 seconds
            
            wait_time=10 if self.connected.is_set() else 1
            threading.Event().wait(wait_time)

    # def monitor_connection_status(self):
    #     """Monitor connection status and reboot if disconnected for too long"""
    #     disconnect_start = None
    #     DISCONNECT_THRESHOLD = 600  # 10 minutes in seconds
        
    #     while not self.shutdown.is_set():
    #         if not self.connected.is_set():
    #             if disconnect_start is None:
    #                 disconnect_start = time.time()
    #                 self.logger.warning("Device disconnected - starting disconnect timer")
                
    #             elif time.time() - disconnect_start > DISCONNECT_THRESHOLD:
    #                 self.logger.error("Device disconnected for more than 10 minutes - initiating reboot")
    #                 os.system("systemctl reboot")
    #         else:
    #             disconnect_start = None
                
    #         time.sleep(30)  # Check every 30 seconds
    
    def process_settings(self):
        try:
            with open(self.settings_file_path, 'r') as f:
                settings = json.load(f)
                if settings.get("sys_data_period"):
                    self.sys_data_period = settings["sys_data_period"]
            
            # do a local update since the settings change was called.
            response = requests.get(f"{self.local_server_url}/update_settings",timeout=10)
            self.logger.info(f"Settings updated in local_server with response: {response.text}")    

        except Exception as e:
            self.logger.error(f"Error processing settings: {e}")

    
    def cleanup(self):
        """Fast cleanup implementation"""
        if self.cleaning_up:
            return
        
        self.cleaning_up = True
        print("Starting cleanup...")
        
        try:
            if self.client:
                #unsubscribe
                try:
                    unsubscribe_future=self.client.unsubscribe(
                        mqtt5.UnsubscribePacket(
                            topic_filters=[self.topic_filter]
                        )
                    )
                except Exception as e:
                    print("Unsubscribe failed with error: ", e)
                    pass
                unsuback = unsubscribe_future.result(self.TIMEOUT)
                print("Unsubscribed with {}".format(unsuback.reason_codes))
                
                try:
                    self.future_stopped = Future()
                    self.client.stop()
                    self.future_stopped.result(self.CLEANUP_TIMEOUT)
                except:
                    print("Client stop failed")
                    pass
                
            if self.connection_future:
                try:
                    self.connection_future.cancel()
                except:
                    print("Connection future cancel failed")
                    pass
            self.executor.shutdown(wait=False, cancel_futures=True)
            concurrent.futures.thread._threads_queues.clear()
            
        except Exception as e:
            print(f"Cleanup error: {e}")
            
        finally:
            self.shutdown.set()
            self.client = None
            print("Cleanup complete")
            os.system("pkill -9 -f eka-cloudconnect.py")
            exit(0)

    def run(self):
        self.start_time = time.time()
        self.logger.info("="*80)
        self.logger.info("STARTING EKA CLOUD CONNECT SERVICE")
        self.logger.info("="*80)
        
        # Start API server thread
        self.logger.info("Starting FastAPI server thread")
        api_thread = threading.Thread(target=self.run_api, name="FastAPI-Server")
        api_thread.daemon = True
        api_thread.start()
        self.logger.debug("FastAPI server thread started")
        
        # Start status reporting thread
        self.logger.info("Starting status reporting thread")
        send_status_thread = threading.Thread(target=self.send_status, name="Status-Reporter")
        send_status_thread.daemon = True
        send_status_thread.start()
        self.logger.debug("Status reporting thread started")
        
        # Start connection monitor thread
        # self.logger.info("Starting connection monitoring thread")
        # connection_monitor_thread = threading.Thread(target=self.monitor_connection_status, name="Connection-Monitor")
        # connection_monitor_thread.daemon = True
        # connection_monitor_thread.start()
        # self.logger.debug("Connection monitoring thread started")

        # Initial credential setup (without CA download)
        self.logger.info("Setting up initial credentials")
        if not self.setup_credentials(root_ca_download=False):
            self.logger.warning("Initial credential setup failed - will retry with CA download after connectivity")
        else:
            self.logger.info("Initial credential setup completed")

        # Wait for internet connectivity
        self.logger.info("Waiting for internet connectivity")
        connectivity_check_count = 0
        while not self.wait_for_connectivity():
            connectivity_check_count += 1
            self.logger.debug(f"Connectivity check {connectivity_check_count} - waiting 5 seconds before retry")
            time.sleep(5)

        self.logger.info("Internet connectivity established")

        # Setup credentials with CA download
        self.logger.info("Setting up credentials with CA download")
        if not self.setup_credentials(root_ca_download=True):
            self.logger.error("Failed to setup credentials with CA download")
            self.credential_status = False
            self.logger.warning("Waiting indefinitely for restart via API due to credential failure")
            # wait to be restarted by API
            while True:
                time.sleep(1)
        else:
            self.logger.info("Credentials setup completed successfully")
            self.credential_status = True

        # Setup signal handlers
        self.logger.debug("Setting up signal handlers")
        signal.signal(signal.SIGINT, lambda s, f: self.cleanup())
        signal.signal(signal.SIGTERM, lambda s, f: self.cleanup())
        self.logger.debug("Signal handlers configured")

        self.logger.info("Entering main connection loop")
        connection_attempt = 0
        
        try:
            while not self.shutdown.is_set():
                connection_attempt += 1
                self.logger.info(f"Connection attempt #{connection_attempt}")
                self.logger.debug(f"Current reconnect delay: {self.current_reconnect_delay}s")
                
                self.connection_future = self.executor.submit(self.connect)
                self.logger.debug("Submitted connection task to executor")
                
                try:
                    connection_result = self.connection_future.result(self.TIMEOUT)
                    self.logger.debug(f"Connection attempt result: {connection_result}")
                except concurrent.futures.TimeoutError:
                    self.logger.error(f"Connection attempt timed out after {self.TIMEOUT}s")
                    connection_result = False
                except Exception as e:
                    self.logger.error(f"Connection attempt failed with exception: {e}", exc_info=True)
                    connection_result = False
                
                if connection_result:
                    self.logger.info("Connection established successfully")
                    self.current_reconnect_delay = self.min_reconnect_delay
                    self.logger.debug("Reset reconnect delay to minimum")
                    
                    # Start system data publishing
                    self.logger.debug("Starting system data publishing task")
                    sys_data_future = self.executor.submit(self.publish_system_data)
                    self.logger.debug("System data publishing task submitted")
                    
                    # Wait while connected
                    self.logger.info("Entering connected state - monitoring connection")
                    while not self.shutdown.is_set() and self.connected.is_set():
                        threading.Event().wait(1)
                    
                    if not self.shutdown.is_set():
                        self.logger.warning("Connection lost - detected disconnection")
                    else:
                        self.logger.info("Shutdown requested while connected")
                
                if not self.shutdown.is_set():
                    self.logger.warning(f"Connection lost, retrying in {self.current_reconnect_delay}s...")
                    time.sleep(min(self.max_reconnect_delay, self.current_reconnect_delay))
                    self.current_reconnect_delay = min(self.max_reconnect_delay, self.current_reconnect_delay * 2)
                    self.logger.debug(f"Updated reconnect delay to: {self.current_reconnect_delay}s")
                    
        except KeyboardInterrupt:
            self.logger.info("Keyboard interrupt received")
        except Exception as e:
            self.logger.error(f"Runtime error in main loop: {e}", exc_info=True)
        finally:
            self.logger.info("Exiting main loop - starting cleanup")
            self.cleanup()

    def run_api(self):
        uvicorn.run(self.api, host="127.0.0.1", port=8001)

if __name__ == "__main__":
    device = EkaDevice()
    device.run()
