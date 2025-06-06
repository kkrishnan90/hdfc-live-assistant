import React, { useState, useRef, useEffect } from 'react';

function LogoUploader({ onUploadSuccess }) {
  const [file, setFile] = useState(null);
  const [preview, setPreview] = useState(null);
  const [feedback, setFeedback] = useState({ message: '', type: '' });
  const [uploading, setUploading] = useState(false);
  const [uploadProgress, setUploadProgress] = useState(0);
  const fileInputRef = useRef(null);

  // Create a preview when file is selected
  useEffect(() => {
    if (!file) {
      setPreview(null);
      return;
    }

    const objectUrl = URL.createObjectURL(file);
    setPreview(objectUrl);

    // Free memory when component unmounts
    return () => URL.revokeObjectURL(objectUrl);
  }, [file]);

  const handleFileChange = (e) => {
    const selectedFile = e.target.files[0];
    if (selectedFile) {
      setFile(selectedFile);
      setFeedback({ message: '', type: '' });
      setUploadProgress(0);
    }
  };

  const handleUpload = async () => {
    if (!file) {
      setFeedback({ message: 'Please select a file first!', type: 'error' });
      return;
    }

    setUploading(true);
    setUploadProgress(10); // Start progress animation
    
    const formData = new FormData();
    formData.append('logo', file);

    const PRODUCTION_HOST = 'hdfc-assistant-backend-1018963165306.us-central1.run.app';
    const LOCAL_HOST = 'localhost:8000';
    const isProduction = window.location.hostname !== 'localhost' && window.location.hostname !== '127.0.0.1';
    const BACKEND_HOST = isProduction ? PRODUCTION_HOST : LOCAL_HOST;
    const HTTP_PROTOCOL = isProduction ? 'https' : 'http';
    const uploadUrl = `${HTTP_PROTOCOL}://${BACKEND_HOST}/api/upload-logo`;

    try {
      // Simulate progress for better UX
      const progressInterval = setInterval(() => {
        setUploadProgress(prev => {
          const newProgress = prev + Math.random() * 15;
          return newProgress >= 90 ? 90 : newProgress; // Cap at 90% until complete
        });
      }, 300);

      const response = await fetch(uploadUrl, {
        method: 'POST',
        body: formData,
      });

      clearInterval(progressInterval);
      setUploadProgress(100);
      
      if (response.ok) {
        setFeedback({ message: '✓ Logo uploaded successfully!', type: 'success' });
        if (onUploadSuccess) {
          onUploadSuccess();
        }
      } else {
        setFeedback({ message: '✗ Logo upload failed. Please try again.', type: 'error' });
      }
    } catch (error) {
      console.error('Error uploading logo:', error);
      setFeedback({ message: '✗ An error occurred while uploading the logo.', type: 'error' });
    } finally {
      setUploading(false);
      // Reset progress after a delay
      setTimeout(() => setUploadProgress(0), 1000);
    }
  };


  return (
    <div className="logo-uploader">
      <div className="file-input-wrapper">
        <input
          type="file"
          id="file-upload"
          onChange={handleFileChange}
          ref={fileInputRef}
          accept="image/*"
          style={{ display: 'none' }}
        />
        <label htmlFor="file-upload" className="file-input-label">
          Choose Logo File
        </label>
        {file && <p className="file-name">{file.name}</p>}
      </div>
      
      {/* Logo Preview Area */}
      <div className="logo-preview-container">
        <div className="logo-preview-label">Logo Preview</div>
        <div className="logo-preview">
          {preview ? (
            <img src={preview} alt="Logo preview" />
          ) : (
            <div className="logo-preview-placeholder">
              No image selected
            </div>
          )}
        </div>
      </div>
      
      <button
        onClick={handleUpload}
        disabled={!file || uploading}
        className="upload-btn"
      >
        {uploading ? 'Uploading...' : 'Upload Logo'}
      </button>
      
      {uploadProgress > 0 && (
        <div className="upload-progress">
          <div
            className="upload-progress-bar"
            style={{ width: `${uploadProgress}%` }}
          ></div>
        </div>
      )}
      
      {feedback.message && (
        <div className={`feedback-message ${feedback.type}`}>
          {feedback.message}
        </div>
      )}
    </div>
  );
}

export default LogoUploader;