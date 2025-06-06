"""
Google Cloud Storage utility functions for the HDFC Live Assistant application.
"""
import os
from google.cloud import storage, exceptions

# Define constants
BUCKET_NAME = "bank-demo-sachin"
LOGO_FOLDER = "logos/"

# Initialize GCS client with appropriate credentials
def get_storage_client():
    """
    Get a GCS client with appropriate credentials.
    In Cloud Run, this will use the default service account.
    """
    # Check if running in Cloud Run
    if os.environ.get('K_SERVICE'):
        # In Cloud Run, use the default service account
        return storage.Client()
    else:
        # Locally, try to use explicit credentials if available
        try:
            # First try to use GOOGLE_APPLICATION_CREDENTIALS environment variable
            return storage.Client()
        except (exceptions.GoogleCloudError, PermissionError) as e:
            print(f"Warning: Failed to initialize GCS client with default credentials: {e}")
            print("Falling back to local file system for storage operations.")
            return None

# Get storage client
storage_client = get_storage_client()

# Local file system fallback paths
UPLOAD_FOLDER = 'uploads'
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

def upload_file_to_gcs(local_file_path, gcs_file_name):
    """Upload a file to GCS or local file system if GCS is unavailable."""
    if storage_client:
        try:
            bucket = storage_client.bucket(BUCKET_NAME)
            blob = bucket.blob(f"{LOGO_FOLDER}{gcs_file_name}")
            blob.upload_from_filename(local_file_path)
            
            # Verify upload was successful
            if blob.exists():
                print(f"File successfully uploaded to GCS: {blob.name}")
                return blob.name
            else:
                print("File upload to GCS failed verification check")
                raise exceptions.GoogleCloudError("File upload verification failed")
        except (exceptions.GoogleCloudError, PermissionError) as e:
            print(f"GCS upload failed: {e}. Falling back to local file system.")
        except Exception as e:
            print(f"Unexpected error during GCS upload: {e}. Falling back to local file system.")
            import traceback
            traceback.print_exc()
    
    # Fallback to local file system
    local_dest = os.path.join(UPLOAD_FOLDER, gcs_file_name)
    import shutil
    shutil.copy2(local_file_path, local_dest)
    print(f"File saved to local filesystem: {local_dest}")
    return local_dest

def upload_bytes_to_gcs(file_bytes, gcs_file_name, content_type=None):
    """Upload bytes data to GCS or local file system if GCS is unavailable."""
    if storage_client:
        try:
            bucket = storage_client.bucket(BUCKET_NAME)
            blob = bucket.blob(f"{LOGO_FOLDER}{gcs_file_name}")
            blob.upload_from_string(file_bytes, content_type=content_type)
            
            # Verify upload was successful
            if blob.exists():
                print(f"Bytes successfully uploaded to GCS: {blob.name}")
                return blob.name
            else:
                print("Bytes upload to GCS failed verification check")
                raise exceptions.GoogleCloudError("Bytes upload verification failed")
        except (exceptions.GoogleCloudError, PermissionError) as e:
            print(f"GCS upload failed: {e}. Falling back to local file system.")
        except Exception as e:
            print(f"Unexpected error during GCS upload: {e}. Falling back to local file system.")
            import traceback
            traceback.print_exc()
    
    # Fallback to local file system
    local_dest = os.path.join(UPLOAD_FOLDER, gcs_file_name)
    with open(local_dest, 'wb') as f:
        f.write(file_bytes)
    print(f"Bytes saved to local filesystem: {local_dest}")
    return local_dest

def file_exists_in_gcs(gcs_file_name):
    """Check if a file exists in GCS or local file system if GCS is unavailable."""
    if storage_client:
        try:
            bucket = storage_client.bucket(BUCKET_NAME)
            blob = bucket.blob(f"{LOGO_FOLDER}{gcs_file_name}")
            exists = blob.exists()
            print(f"GCS file existence check for {gcs_file_name}: {exists}")
            return exists
        except (exceptions.GoogleCloudError, PermissionError) as e:
            print(f"GCS existence check failed: {e}. Falling back to local file system.")
        except Exception as e:
            print(f"Unexpected error during GCS existence check: {e}. Falling back to local file system.")
            import traceback
            traceback.print_exc()
    
    # Fallback to local file system
    local_path = os.path.join(UPLOAD_FOLDER, gcs_file_name)
    exists = os.path.exists(local_path)
    print(f"Local file existence check for {gcs_file_name}: {exists}")
    return exists

def get_file_from_gcs(gcs_file_name):
    """Get a file from GCS or local file system if GCS is unavailable."""
    if storage_client:
        try:
            bucket = storage_client.bucket(BUCKET_NAME)
            blob = bucket.blob(f"{LOGO_FOLDER}{gcs_file_name}")
            
            if not blob.exists():
                print(f"File {gcs_file_name} does not exist in GCS")
                raise FileNotFoundError(f"File {gcs_file_name} not found in GCS")
            
            # Get blob metadata before downloading
            blob.reload()
            print(f"GCS Debug: Blob metadata - Content-Type: {blob.content_type}, Size: {blob.size}, Updated: {blob.updated}")
                
            file_data = blob.download_as_bytes()
            print(f"Successfully downloaded {len(file_data)} bytes from GCS: {gcs_file_name}")
            
            # Debug: Check first few bytes to verify image header
            if len(file_data) > 20:
                first_bytes = ', '.join(f'{b:02x}' for b in file_data[:20])
                print(f"GCS Debug: First 20 bytes of downloaded data: {first_bytes}")
                
                # Check for common image headers
                png_signature = b'\x89PNG\r\n\x1a\n'
                jpg_signature = b'\xff\xd8\xff'
                if file_data.startswith(png_signature):
                    print("GCS Debug: Data has valid PNG signature")
                elif file_data.startswith(jpg_signature):
                    print("GCS Debug: Data has valid JPEG signature")
                else:
                    print("GCS Debug: Data does not start with standard PNG or JPEG signatures")
            
            return file_data
        except (exceptions.GoogleCloudError, PermissionError) as e:
            print(f"GCS download failed: {e}. Falling back to local file system.")
        except FileNotFoundError:
            print(f"File {gcs_file_name} not found in GCS. Falling back to local file system.")
        except Exception as e:
            print(f"Unexpected error during GCS download: {e}. Falling back to local file system.")
            import traceback
            traceback.print_exc()
    
    # Fallback to local file system
    local_path = os.path.join(UPLOAD_FOLDER, gcs_file_name)
    if os.path.exists(local_path):
        with open(local_path, 'rb') as f:
            file_data = f.read()
            print(f"Successfully read {len(file_data)} bytes from local file: {local_path}")
            return file_data
    
    print(f"File {gcs_file_name} not found in local storage")
    raise FileNotFoundError(f"File {gcs_file_name} not found in GCS or local storage")