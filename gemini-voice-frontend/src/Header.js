import React, { useContext } from 'react';
import { LogoContext } from './LogoContext';
import './App.css';

function Header() {
  const { logoUrl, dominantColor } = useContext(LogoContext);

  return (
    <header className="App-header" style={{ backgroundColor: dominantColor }}>
      {logoUrl && <img src={logoUrl} className="App-logo" alt="logo" />}
    </header>
  );
}

export default Header;