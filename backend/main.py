from bigquery_functions import GLOBAL_LOG_STORE
from gemini_tools import (
    banking_tool,
    getBalance,
    getTransactionHistory,
    initiateFundTransfer,
    executeFundTransfer,
    getBillDetails,
    payBill,
    registerBiller,
    updateBillerDetails,
    removeBiller,
    listRegisteredBillers,
    search_faq
)
from gcs_utils import (
    upload_bytes_to_gcs,
    file_exists_in_gcs,
    get_file_from_gcs
)
from datetime import datetime, timezone  # For timestamping raw stdout logs
import asyncio
import os
import traceback
import uuid  # Added for generating unique IDs
import sys  # Added for stdout redirection
import io  # Added for stdout redirection
import json  # Added for parsing log strings
from PIL import Image  # For image processing
from quart import Quart, websocket, jsonify, request, Response
from quart_cors import cors
import google.genai as genai
import extcolors
from google.genai import types
from dotenv import load_dotenv
load_dotenv()


# --- Log Capturing Setup ---
CAPTURED_STDOUT_LOGS = []
_original_stdout = sys.stdout


class StdoutTee(io.TextIOBase):
    def __init__(self, original_stdout, log_list):
        self._original_stdout = original_stdout
        self._log_list = log_list

    def write(self, s):
        self._original_stdout.write(s)  # Write to original stdout (console)
        s_stripped = s.strip()
        if s_stripped:  # Avoid empty lines
            try:
                # Attempt to parse as JSON, assuming logs from gemini_tools are JSON strings
                log_entry = json.loads(s_stripped)
                # Ensure it has the expected structure for frontend if it's a TOOL_EVENT
                if isinstance(log_entry, dict) and log_entry.get("log_type") == "TOOL_EVENT":
                    self._log_list.append(log_entry)
                else:  # Not a TOOL_EVENT or not a dict, store as raw with context
                    self._log_list.append({
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                        "log_type": "RAW_STDOUT",
                        "message": s_stripped,
                        "parsed_json": log_entry if isinstance(log_entry, dict) else None
                    })
            except json.JSONDecodeError:
                # If it's not JSON, store it as a raw string entry
                self._log_list.append({
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "log_type": "RAW_STDOUT",
                    "message": s_stripped
                })
        return len(s)

    def flush(self):
        self._original_stdout.flush()


sys.stdout = StdoutTee(_original_stdout, CAPTURED_STDOUT_LOGS)
# --- End Log Capturing Setup ---


gemini_client = genai.Client(
    vertexai=True, project="account-pocs", location="us-central1")
# print("Using Google AI SDK with genai.Client.")

GEMINI_MODEL_NAME = "gemini-live-2.5-flash"
INPUT_SAMPLE_RATE = 16000

app = Quart(__name__)
app = cors(app, allow_origin="*")

UPLOAD_FOLDER = 'uploads'
os.makedirs(UPLOAD_FOLDER, exist_ok=True)


@app.route("/api/upload-logo", methods=["POST"])
async def upload_logo():
    """Handles logo file uploads and stores in GCS."""
    try:
        files = await request.files
        if 'logo' not in files:
            return jsonify({"error": "No logo file provided"}), 400

        file = files['logo']

        if file.filename == '':
            return jsonify({"error": "No selected file"}), 400

        if file:
            # Handle both async and sync file.read() implementations
            try:
                # First try the async approach
                file_content = await file.read()
                print(
                    f"Debug: Successfully read file using async approach. Data size: {len(file_content)} bytes")
                # Check first few bytes to verify it's an image
                if len(file_content) > 20:
                    first_bytes = ', '.join(
                        f'{b:02x}' for b in file_content[:20])
                    print(f"Debug: First 20 bytes (async): {first_bytes}")
            except TypeError as e:
                if "can't be used in 'await'" in str(e):
                    # If it fails with our specific error, use the sync approach
                    print(
                        "Falling back to sync file.read() due to Cloud Run compatibility")
                    file.seek(0)
                    file_content = file.read()
                    print(
                        f"Debug: Read file using sync approach. Data size: {len(file_content)} bytes")
                    # Check first few bytes to verify it's an image
                    if len(file_content) > 20:
                        first_bytes = ', '.join(
                            f'{b:02x}' for b in file_content[:20])
                        print(f"Debug: First 20 bytes (sync): {first_bytes}")
                else:
                    # If it's a different TypeError, re-raise it
                    print(f"Debug: Unexpected TypeError: {e}")
                    raise

            # Validate image format and content
            try:
                # Create PIL Image object from binary data
                print(
                    f"Debug: Creating BytesIO object for image validation. Data size: {len(file_content)} bytes")
                img_io = io.BytesIO(file_content)
                print(
                    f"Debug: BytesIO created: {img_io}, position: {img_io.tell()}")

                # Reset position to beginning to ensure proper reading
                img_io.seek(0)
                print(
                    f"Debug: BytesIO position after seek(0): {img_io.tell()}")

                try:
                    img = Image.open(img_io)
                    print(
                        f"Debug: Image opened successfully. Format: {img.format}, Mode: {img.mode}, Size: {img.size}")

                    # Validate image format
                    if img.format not in ['PNG', 'JPEG', 'JPG']:
                        print(
                            f"Warning: Unsupported image format: {img.format}. Converting to PNG.")

                    # Convert to PNG if needed and get bytes
                    if img.format != 'PNG':
                        print(
                            f"Debug: Converting image from {img.format} to PNG")
                        img_byte_arr = io.BytesIO()
                        img.save(img_byte_arr, format='PNG')
                        file_content = img_byte_arr.getvalue()
                        print(
                            f"Debug: Converted to PNG. New data size: {len(file_content)} bytes")
                except Exception as img_open_err:
                    print(
                        f"Debug: Error opening image with PIL: {type(img_open_err).__name__}: {img_open_err}")
                    # Try to get more diagnostic information
                    img_io.seek(0)
                    header_bytes = img_io.read(20)
                    hex_bytes = ' '.join(f'{b:02x}' for b in header_bytes)
                    print(f"Debug: First 20 bytes of image data: {hex_bytes}")
                    raise

                # Extract dominant color directly from PIL Image
                hex_color = "#282c34"  # Default color
                try:
                    # Use extcolors.extract with PIL Image instead of file path
                    colors, _ = extcolors.extract_from_image(img)
                    if colors:
                        dominant_color = colors[0][0]
                        hex_color = '#%02x%02x%02x' % dominant_color
                        print(
                            f"Successfully extracted dominant color: {hex_color}")
                    else:
                        print("No colors found in image, using default color.")
                except IndexError as idx_err:
                    print(
                        f"Could not find a dominant color, using default. Error: {idx_err}")
                except (ValueError, TypeError) as color_exc:
                    print(f"Error during color extraction: {color_exc}")
                except Exception as ext_err:
                    print(
                        f"Unexpected error during color extraction: {ext_err}")
                    traceback.print_exc()

            except (IOError, SyntaxError) as img_err:
                print(f"Error processing image: {img_err}")
                return jsonify({"error": "Invalid image format or corrupted image"}), 400

            # Upload logo directly to GCS from memory
            gcs_logo_name = "logo.png"
            try:
                upload_result = upload_bytes_to_gcs(
                    file_content, gcs_logo_name, 'image/png')
                print(f"Logo successfully uploaded to GCS: {upload_result}")
            except Exception as gcs_err:
                print(f"Error uploading logo to GCS: {gcs_err}")
                return jsonify({"error": "Failed to store logo"}), 500

            # Save style info to GCS
            style_data = {
                "dominantColor": hex_color,
                "logoUrl": "/api/logo"
            }

            # Convert style data to JSON string
            style_json = json.dumps(style_data)

            # Upload style JSON to GCS
            gcs_style_name = "header_style.json"
            try:
                style_upload_result = upload_bytes_to_gcs(
                    style_json.encode('utf-8'), gcs_style_name, 'application/json')
                print(
                    f"Style JSON successfully uploaded to GCS: {style_upload_result}")
            except Exception as style_err:
                print(f"Error uploading style JSON to GCS: {style_err}")
                return jsonify({"error": "Failed to store style information"}), 500

            return jsonify({
                "message": "Logo uploaded and style generated",
                "dominantColor": hex_color
            }), 200
    except Exception as e:
        print(f"Unhandled error during logo upload: {e}")
        traceback.print_exc()  # Print full stack trace for debugging
        return jsonify({"error": "An internal error occurred"}), 500


@app.route("/api/header-style", methods=["GET"])
async def get_header_style():
    """Serves the header style JSON from GCS."""
    try:
        # Check if style file exists in GCS
        if file_exists_in_gcs("header_style.json"):
            try:
                # Get style data from GCS
                style_data = get_file_from_gcs("header_style.json")
                print("Successfully retrieved header style from GCS")
                return Response(style_data, mimetype='application/json')
            except Exception as fetch_err:
                print(f"Error fetching header style from GCS: {fetch_err}")
                # Fall back to default style on error
                print("Falling back to default header style")
        else:
            print("Header style not found in GCS, using default style")

        # Return a default style if the file doesn't exist or there was an error
        return jsonify({
            "dominantColor": "#282c34",
            "logoUrl": "/api/logo"
        }), 200
    except Exception as e:
        print(f"Unhandled error in get_header_style: {e}")
        traceback.print_exc()
        return jsonify({"error": "An internal error occurred"}), 500


@app.route("/api/logo", methods=["GET"])
async def get_logo():
    """Serves the uploaded logo from GCS."""
    try:
        # Check if logo exists in GCS
        if file_exists_in_gcs("logo.png"):
            try:
                # Get logo data from GCS
                logo_data = get_file_from_gcs("logo.png")
                print("Successfully retrieved logo from GCS")
                print(
                    f"Debug: Logo data type: {type(logo_data)}, size: {len(logo_data)} bytes")

                # Validate the image data
                try:
                    img_io = io.BytesIO(logo_data)
                    print(
                        f"Debug: BytesIO created: {img_io}, tell position: {img_io.tell()}")

                    # Reset position to beginning just to be safe
                    img_io.seek(0)
                    print(
                        f"Debug: BytesIO position after seek(0): {img_io.tell()}")

                    try:
                        img = Image.open(img_io)
                        print(
                            f"Debug: Image opened successfully. Format: {img.format}, Mode: {img.mode}, Size: {img.size}")
                    except Exception as detailed_err:
                        print(
                            f"Debug: Detailed error opening image: {type(detailed_err).__name__}: {detailed_err}")
                        # Try to get more diagnostic information
                        img_io.seek(0)
                        header_bytes = img_io.read(20)
                        hex_bytes = ' '.join(f'{b:02x}' for b in header_bytes)
                        print(
                            f"Debug: First 20 bytes of image data: {hex_bytes}")
                        raise
                except (IOError, SyntaxError) as img_validate_err:
                    print(
                        f"Retrieved logo data is not a valid image: {img_validate_err}")
                    return jsonify({"error": "Retrieved logo is not a valid image"}), 500

                return Response(logo_data, mimetype='image/png')
            except Exception as fetch_err:
                print(f"Error fetching logo from GCS: {fetch_err}")
                traceback.print_exc()
                return jsonify({"error": "Failed to retrieve logo"}), 500
        else:
            # If logo is not found, return a 404
            print("Logo not found in GCS")
            return jsonify({"error": "Logo not found"}), 404
    except Exception as e:
        print(f"Unhandled error in get_logo: {e}")
        traceback.print_exc()
        return jsonify({"error": "An internal error occurred"}), 500


@app.websocket("/listen")
async def websocket_endpoint():
    # print("Quart WebSocket: Connection accepted from client.")
    current_session_handle = None  # Initialize session handle

    language_code_to_use = "en-US"

    gemini_live_config = types.LiveConnectConfig(
        response_modalities=["AUDIO"],
        system_instruction="You are helpful assistant for banking services. You are currently interacting with a user who is using a voice-based interface to interact with you. You should respond to the user's voice commands in a natural and conversational manner.You should not use any emojis or special characters.\
            Your `user_id` is `Alex` and your bill provider is `City Power`. You will detect the language of the user and respond in the same language. ",
        speech_config=types.SpeechConfig(
            language_code=language_code_to_use
        ),
        input_audio_transcription={},
        output_audio_transcription={},
        session_resumption=types.SessionResumptionConfig(
            handle=current_session_handle),  # Added from reference
        context_window_compression=types.ContextWindowCompressionConfig(  # Added from reference
            sliding_window=types.SlidingWindow(),
        ),
        tools=[banking_tool],
    )

    try:
        async with gemini_client.aio.live.connect(
            model=GEMINI_MODEL_NAME,
            config=gemini_live_config
        ) as session:
            # print(f"Quart Backend: Gemini session connected for model {GEMINI_MODEL_NAME} with tools.")
            active_processing = True

            async def handle_client_input_and_forward():
                nonlocal active_processing
                # print("Quart Backend: Starting handle_client_input_and_forward task.")
                try:
                    while active_processing:
                        try:
                            client_data = await asyncio.wait_for(websocket.receive(), timeout=0.2)

                            if isinstance(client_data, str):
                                message_text = client_data
                                # print(f"Quart Backend: Received text from client: '{message_text}'")
                                prompt_for_gemini = message_text
                                if message_text == "SEND_TEST_AUDIO_PLEASE":
                                    prompt_for_gemini = "Hello Gemini, please say 'testing one two three'."

                                # print(f"Quart Backend: Sending text prompt to Gemini: '{prompt_for_gemini}'")
                                user_content_for_text = types.Content(
                                    role="user",
                                    parts=[
                                        types.Part(text=prompt_for_gemini)]
                                )
                                await session.send_client_content(turns=user_content_for_text)
                                # print(f"Quart Backend: Prompt '{prompt_for_gemini}' sent to Gemini.")

                            elif isinstance(client_data, bytes):
                                audio_chunk = client_data
                                if audio_chunk:
                                    # print(f"Quart Backend: Received mic audio chunk: {len(audio_chunk)} bytes")
                                    # print(f"Quart Backend: Sending audio chunk ({len(audio_chunk)} bytes) to Gemini via send_realtime_input...")
                                    await session.send_realtime_input(
                                        audio=types.Blob(
                                            mime_type=f"audio/pcm;rate={INPUT_SAMPLE_RATE}",
                                            data=audio_chunk
                                        )
                                    )
                                    # print(f"Quart Backend: Successfully sent mic audio to Gemini via send_realtime_input.")
                            else:
                                print(
                                    f"Quart Backend: Received unexpected data type from client: {type(client_data)}, content: {client_data[:100] if isinstance(client_data, bytes) else client_data}")

                        except asyncio.TimeoutError:
                            if not active_processing:
                                break
                            continue  # Normal timeout, continue listening
                        except Exception as e_fwd_inner:
                            print(
                                f"Quart Backend: Error during client data handling/sending to Gemini: {type(e_fwd_inner).__name__}: {e_fwd_inner}")
                            traceback.print_exc()
                            active_processing = False
                            break  # Exit while loop on error
                except Exception as e_fwd_outer:
                    print(
                        f"Quart Backend: Outer error in handle_client_input_and_forward: {type(e_fwd_outer).__name__}: {e_fwd_outer}")
                    traceback.print_exc()
                    # Ensure outer errors also stop processing
                    active_processing = False
                finally:
                    # print("Quart Backend: Stopped handling client input.")
                    active_processing = False  # Ensure graceful shutdown of the other task

            async def receive_from_gemini_and_forward_to_client():
                nonlocal active_processing, current_session_handle
                # print("Quart Backend: Starting receive_from_gemini_and_forward_to_client task.")

                available_functions = {
                    "getBalance": getBalance,
                    "getTransactionHistory": getTransactionHistory,
                    "initiateFundTransfer": initiateFundTransfer,
                    "executeFundTransfer": executeFundTransfer,
                    "getBillDetails": getBillDetails,
                    "payBill": payBill,
                    "registerBiller": registerBiller,
                    "updateBillerDetails": updateBillerDetails,
                    "removeBiller": removeBiller,
                    "listRegisteredBillers": listRegisteredBillers,
                    "search_faq": search_faq
                }
                current_user_utterance_id = None
                # Renamed from latest_user_speech_text and initialized
                accumulated_user_speech_text = ""
                current_model_utterance_id = None
                accumulated_model_speech_text = ""

                try:
                    while active_processing:
                        had_gemini_activity_in_this_iteration = False
                        async for response in session.receive():
                            had_gemini_activity_in_this_iteration = True
                            if not active_processing:
                                break

                            if response.session_resumption_update:
                                update = response.session_resumption_update
                                if update.resumable and update.new_handle:
                                    current_session_handle = update.new_handle
                                    # print(f"Quart Backend: Received session resumption update. New handle: {current_session_handle}")

                            if hasattr(response, 'session_handle') and response.session_handle:
                                new_handle = response.session_handle
                                if new_handle != current_session_handle:
                                    current_session_handle = new_handle
                                    # print(f"Quart Backend: Updated session handle from direct response.session_handle: {current_session_handle}")

                            if response.data is not None:
                                try:
                                    await websocket.send(response.data)
                                except Exception as send_exc:
                                    print(
                                        f"Quart Backend: Error sending audio data to client WebSocket: {type(send_exc).__name__}: {send_exc}")
                                    active_processing = False
                                    break

                            elif response.server_content:
                                if response.server_content.interrupted:
                                    print(
                                        "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
                                    print(
                                        "Quart Backend: Gemini server sent INTERRUPTED signal.")
                                    print(
                                        "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
                                    try:
                                        await websocket.send_json({"type": "interrupt_playback"})
                                        # print("Quart Backend: Sent interrupt_playback signal to client.")
                                    except Exception as send_exc:
                                        print(
                                            f"Quart Backend: Error sending interrupt_playback signal to client: {type(send_exc).__name__}: {send_exc}")
                                        active_processing = False
                                        break

                                # User Input Processing
                                if response.server_content and hasattr(response.server_content, 'input_transcription') and \
                                   response.server_content.input_transcription and \
                                   hasattr(response.server_content.input_transcription, 'text') and \
                                   response.server_content.input_transcription.text:  # Ensure text is not empty

                                    user_speech_chunk = response.server_content.input_transcription.text

                                    if current_user_utterance_id is None:  # Start of a new user utterance
                                        current_user_utterance_id = str(
                                            uuid.uuid4())
                                        # Reset accumulator for new utterance
                                        accumulated_user_speech_text = ""

                                    accumulated_user_speech_text += user_speech_chunk

                                    if accumulated_user_speech_text:  # Only send if there's actual accumulated text
                                        payload = {
                                            'id': current_user_utterance_id,
                                            # Send accumulated text
                                            'text': accumulated_user_speech_text,
                                            'sender': 'user',
                                            'type': 'user_transcription_update',
                                            'is_final': False
                                        }
                                        try:
                                            await websocket.send_json(payload)
                                            print(
                                                f"Backend - Streaming User Input (accumulated): \033[92m{accumulated_user_speech_text}\033[0m")
                                        except Exception as send_exc:
                                            print(
                                                f"Quart Backend: Error sending user transcription update to client: {type(send_exc).__name__}: {send_exc}")
                                            active_processing = False
                                            break

                                # Model Output Processing
                                if response.server_content and hasattr(response.server_content, 'output_transcription') and \
                                   response.server_content.output_transcription and \
                                   hasattr(response.server_content.output_transcription, 'text') and \
                                   response.server_content.output_transcription.text:

                                    if current_model_utterance_id is None:
                                        current_model_utterance_id = str(
                                            uuid.uuid4())
                                        # Ensure accumulator is clear
                                        accumulated_model_speech_text = ""

                                    chunk = response.server_content.output_transcription.text
                                    if chunk:  # Only process if chunk has content
                                        accumulated_model_speech_text += chunk
                                        payload = {
                                            'id': current_model_utterance_id,
                                            # Send accumulated text
                                            'text': accumulated_model_speech_text,
                                            'sender': 'model',
                                            'type': 'model_response_update',
                                            'is_final': False
                                        }
                                        try:
                                            await websocket.send_json(payload)
                                            print(
                                                f"Backend - Streaming Model Output (accumulated): \033[92m{accumulated_model_speech_text}\033[0m")
                                        except Exception as send_exc:
                                            print(
                                                f"Quart Backend: Error sending model response update to client: {type(send_exc).__name__}: {send_exc}")
                                            active_processing = False
                                            break

                                # Handling Model Generation Completion
                                if response.server_content and hasattr(response.server_content, 'generation_complete') and \
                                   response.server_content.generation_complete == True:
                                    if current_model_utterance_id and accumulated_model_speech_text:  # Ensure there was a model utterance
                                        payload = {
                                            'id': current_model_utterance_id,
                                            'text': accumulated_model_speech_text,
                                            'sender': 'model',
                                            'type': 'model_response_update',
                                            'is_final': True
                                        }
                                        try:
                                            await websocket.send_json(payload)
                                            print(
                                                f"Backend - Final Model Output Sent: \033[92m{accumulated_model_speech_text}\033[0m")
                                        except Exception as send_exc:
                                            print(
                                                f"Quart Backend: Error sending final model response to client: {type(send_exc).__name__}: {send_exc}")
                                            active_processing = False
                                            break
                                    # Reset for next model utterance
                                    current_model_utterance_id = None
                                    accumulated_model_speech_text = ""

                                # Handling Turn Completion (Finalizing User Speech)
                                if response.server_content and hasattr(response.server_content, 'turn_complete') and \
                                   response.server_content.turn_complete == True:
                                    if current_user_utterance_id and accumulated_user_speech_text:  # Ensure there was a user utterance
                                        payload = {
                                            'id': current_user_utterance_id,
                                            # Send final accumulated text
                                            'text': accumulated_user_speech_text,
                                            'sender': 'user',
                                            'type': 'user_transcription_update',
                                            'is_final': True
                                        }
                                        try:
                                            await websocket.send_json(payload)
                                            print(
                                                f"Backend - Final User Input Sent: \033[92m{accumulated_user_speech_text}\033[0m")
                                        except Exception as send_exc:
                                            print(
                                                f"Quart Backend: Error sending final user transcription to client: {type(send_exc).__name__}: {send_exc}")
                                            active_processing = False
                                            break
                                    # Reset for next user utterance
                                    current_user_utterance_id = None
                                    accumulated_user_speech_text = ""  # Reset accumulator
                                    # Also reset model states
                                    current_model_utterance_id = None
                                    accumulated_model_speech_text = ""
                                    print(
                                        "Backend - Turn complete. User speech states reset.")

                                # Fallback for other potential text or error structures (simplified)
                                is_transcription_related = (hasattr(response.server_content, 'input_transcription') and response.server_content.input_transcription) or \
                                    (hasattr(response.server_content, 'output_transcription')
                                     and response.server_content.output_transcription)
                                is_control_signal = (hasattr(response.server_content, 'generation_complete') and response.server_content.generation_complete) or \
                                    (hasattr(response.server_content, 'turn_complete') and response.server_content.turn_complete) or \
                                    (hasattr(response.server_content, 'interrupted')
                                     and response.server_content.interrupted)

                                if not response.data and not is_transcription_related and not is_control_signal:
                                    unhandled_text = None
                                    if response.text:
                                        unhandled_text = response.text
                                    elif hasattr(response.server_content, 'model_turn') and response.server_content.model_turn and \
                                            hasattr(response.server_content.model_turn, 'parts'):
                                        for part in response.server_content.model_turn.parts:
                                            if part.text:
                                                unhandled_text = (
                                                    unhandled_text + " " if unhandled_text else "") + part.text
                                    elif hasattr(response.server_content, 'output_text') and response.server_content.output_text:
                                        unhandled_text = response.server_content.output_text

                                    if unhandled_text:
                                        print(
                                            f"Quart Backend: Received unhandled server_content text: {unhandled_text}")
                                    elif not response.tool_call:
                                        print(
                                            f"Quart Backend: Received server_content without primary data or known text parts: {response.server_content}")

                            elif response.tool_call:
                                print(
                                    f"\033[92mQuart Backend: Received tool_call from Gemini: {response.tool_call}\033[0m")
                                function_responses = []
                                for fc in response.tool_call.function_calls:
                                    print(
                                        f"\033[92mQuart Backend: Gemini requests function call: {fc.name} with args: {dict(fc.args)}\033[0m")

                                    function_to_call = available_functions.get(
                                        fc.name)
                                    function_response_content = None

                                    if function_to_call:
                                        try:
                                            # Execute the actual local function
                                            function_args = dict(fc.args)
                                            print(
                                                f"\033[92mQuart Backend: Calling function {fc.name} with args: {function_args}\033[0m")
                                            # Await the async function call
                                            result = await function_to_call(**function_args)
                                            if isinstance(result, str):
                                                function_response_content = {
                                                    "content": result}
                                            else:
                                                # Assumes result is already a dict if not a string
                                                function_response_content = result
                                            print(
                                                f"\033[92mQuart Backend: Function {fc.name} executed. Result: {result}\033[0m")
                                        except Exception as e:
                                            print(
                                                f"Quart Backend: Error executing function {fc.name}: {e}")
                                            traceback.print_exc()  # Add if not already there
                                            function_response_content = {
                                                "status": "error", "message": str(e)}
                                    else:
                                        print(
                                            f"Quart Backend: Function {fc.name} not found.")
                                        function_response_content = {"status": "error",
                                                                     "message": f"Function {fc.name} not implemented or available."}

                                    function_response = types.FunctionResponse(
                                        id=fc.id,
                                        name=fc.name,
                                        response=function_response_content
                                    )
                                    function_responses.append(
                                        function_response)

                                if function_responses:
                                    print(
                                        f"\033[92mQuart Backend: Sending {len(function_responses)} function response(s) to Gemini.\033[0m")
                                    await session.send_tool_response(function_responses=function_responses)
                                else:
                                    print(
                                        "Quart Backend: No function responses generated for tool_call.")

                            elif hasattr(response, 'error') and response.error:
                                error_details = response.error
                                if hasattr(response.error, 'message'):
                                    error_details = response.error.message
                                print(
                                    f"Quart Backend: Gemini Error in response: {error_details}")
                                try:
                                    await websocket.send(f"[ERROR_FROM_GEMINI]: {str(error_details)}")
                                except Exception as send_exc:
                                    print(
                                        f"Quart Backend: Error sending Gemini error to client WebSocket: {type(send_exc).__name__}: {send_exc}")
                                active_processing = False
                                break

                            # Removed the separate turn_complete log here as it's handled above with user speech sending.

                        if not active_processing:
                            break

                        if not had_gemini_activity_in_this_iteration and active_processing:
                            await asyncio.sleep(0.1)
                        elif had_gemini_activity_in_this_iteration and active_processing:
                            pass

                except Exception as e_rcv:
                    print(
                        f"Quart Backend: Error in Gemini receive processing task: {type(e_rcv).__name__}: {e_rcv}")
                    traceback.print_exc()
                    active_processing = False
                finally:
                    # print("Quart Backend: Stopped receiving from Gemini.")
                    active_processing = False  # Ensure graceful shutdown of the other task

            forward_task = asyncio.create_task(
                handle_client_input_and_forward(), name="ClientInputForwarder")
            receive_task = asyncio.create_task(
                receive_from_gemini_and_forward_to_client(), name="GeminiReceiver")

            try:
                await asyncio.gather(forward_task, receive_task)
            except Exception as e_gather:
                print(
                    f"Quart Backend: Exception during asyncio.gather: {type(e_gather).__name__}: {e_gather}")
                traceback.print_exc()  # Added traceback
            finally:
                active_processing = False
                if not forward_task.done():
                    forward_task.cancel()
                if not receive_task.done():
                    receive_task.cancel()
                try:
                    await forward_task
                except asyncio.CancelledError:
                    # print(f"Quart Backend: Task {forward_task.get_name()} was cancelled during cleanup.")
                    pass  # Task cancellation is an expected part of shutdown
                except Exception as e_fwd_cleanup:
                    print(
                        f"Quart Backend: Error during forward_task cleanup: {e_fwd_cleanup}")
                    traceback.print_exc()
                try:
                    await receive_task
                except asyncio.CancelledError:
                    # print(f"Quart Backend: Task {receive_task.get_name()} was cancelled during cleanup.")
                    pass  # Task cancellation is an expected part of shutdown
                except Exception as e_rcv_cleanup:
                    print(
                        f"Quart Backend: Error during receive_task cleanup: {e_rcv_cleanup}")
                    traceback.print_exc()  # Added traceback

            # print("Quart Backend: Gemini interaction tasks finished.")
    except asyncio.CancelledError:
        # print("Quart Backend: WebSocket connection cancelled (client likely disconnected or main task cancelled).")
        # Quart handles WebSocket closure. If Gemini session needs explicit closing, ensure it happens.
        # The 'async with session:' should handle session cleanup.
        pass  # Expected on client disconnect
    except Exception as e_ws_main:
        print(
            f"Quart Backend: UNHANDLED error in WebSocket connection main try-block: {type(e_ws_main).__name__}: {e_ws_main}")
        traceback.print_exc()
    finally:
        # The WebSocket is implicitly closed when the handler returns,
        # but a race condition can occur if the client disconnects simultaneously,
        # leading to an attempt to close an already-closed connection.
        # This block attempts to handle that gracefully.
        try:
            # The act of closing also serves as a check. If it's already
            # closed, it will raise a RuntimeError.
            await websocket.close(1000)
        except RuntimeError as e:
            # We expect a RuntimeError if the socket is already in the process
            # of closing. We'll log this as a warning rather than crashing.
            if "after sending 'websocket.close'" in str(e):
                print(
                    f"WebSocket connection already closing, ignoring expected error: {e}")
            else:
                # If it's a different RuntimeError, we should see it.
                print(
                    f"An unexpected runtime error occurred during WebSocket close: {e}")
                traceback.print_exc()
        except Exception as e:
            # Catch any other exceptions during close for completeness.
            print(
                f"An unexpected error occurred during WebSocket cleanup: {e}")
            traceback.print_exc()


@app.route("/api/logs", methods=["GET"])
async def get_logs():
    """API endpoint to fetch captured logs."""
    # Combine logs from BQ's global store and our captured stdout logs
    # Return copies to avoid issues if the lists are modified during serialization
    combined_logs = list(GLOBAL_LOG_STORE) + list(CAPTURED_STDOUT_LOGS)

    # Optional: Sort by timestamp if all logs have a compatible timestamp field
    # For now, just concatenating. Assuming GLOBAL_LOG_STORE entries also have a timestamp
    # or can be ordered meaningfully with the new TOOL_EVENT logs.
    # If sorting is needed:
    # combined_logs.sort(key=lambda x: x.get("timestamp", ""), reverse=True) # Example sort

    return jsonify(combined_logs)


@app.route("/api/clear-logs", methods=["POST"])
async def clear_logs():
    """API endpoint to clear captured logs."""
    GLOBAL_LOG_STORE.clear()
    CAPTURED_STDOUT_LOGS.clear()
    return jsonify({"message": "Logs cleared successfully"}), 200
