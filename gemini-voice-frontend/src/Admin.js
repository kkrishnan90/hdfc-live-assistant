import React, { useContext } from 'react';
import LogoUploader from './LogoUploader';
import { LogoContext } from './LogoContext';
import './Admin.css';

function Admin() {
  const { refreshLogo, dominantColor } = useContext(LogoContext);

  return (
    <div className="admin-page">
      <div className="admin-header" style={{ backgroundColor: dominantColor }}>
        <h1>Admin Dashboard</h1>
      </div>
      <div className="admin-card">
        <h1>Manage Logo</h1>
        <p className="admin-description">
          Upload a new logo to customize the application header. The system will automatically
          extract colors from your logo to create a cohesive theme.
        </p>
        <LogoUploader onUploadSuccess={refreshLogo} />
      </div>
    </div>
  );
}

export default Admin;