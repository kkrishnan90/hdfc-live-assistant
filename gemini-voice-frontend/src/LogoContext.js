import React, { createContext, useState, useEffect, useCallback } from 'react';

export const LogoContext = createContext();

// Convert hex color to HSL
const hexToHSL = (hex) => {
  // Remove # if present
  hex = hex.replace(/^#/, '');
  
  // Parse the hex values
  let r = parseInt(hex.substring(0, 2), 16) / 255;
  let g = parseInt(hex.substring(2, 4), 16) / 255;
  let b = parseInt(hex.substring(4, 6), 16) / 255;
  
  // Find greatest and smallest channel values
  let cmin = Math.min(r, g, b);
  let cmax = Math.max(r, g, b);
  let delta = cmax - cmin;
  let h = 0;
  let s = 0;
  let l = 0;
  
  // Calculate hue
  if (delta === 0) h = 0;
  else if (cmax === r) h = ((g - b) / delta) % 6;
  else if (cmax === g) h = (b - r) / delta + 2;
  else h = (r - g) / delta + 4;
  
  h = Math.round(h * 60);
  if (h < 0) h += 360;
  
  // Calculate lightness
  l = (cmax + cmin) / 2;
  
  // Calculate saturation
  s = delta === 0 ? 0 : delta / (1 - Math.abs(2 * l - 1));
  
  // Convert to percentages
  s = +(s * 100).toFixed(1);
  l = +(l * 100).toFixed(1);
  
  return { h, s, l };
};

// Convert HSL to hex
const hslToHex = (h, s, l) => {
  // Convert to decimal
  s /= 100;
  l /= 100;
  
  let c = (1 - Math.abs(2 * l - 1)) * s;
  let x = c * (1 - Math.abs((h / 60) % 2 - 1));
  let m = l - c / 2;
  let r = 0;
  let g = 0;
  let b = 0;
  
  if (0 <= h && h < 60) {
    r = c; g = x; b = 0;
  } else if (60 <= h && h < 120) {
    r = x; g = c; b = 0;
  } else if (120 <= h && h < 180) {
    r = 0; g = c; b = x;
  } else if (180 <= h && h < 240) {
    r = 0; g = x; b = c;
  } else if (240 <= h && h < 300) {
    r = x; g = 0; b = c;
  } else {
    r = c; g = 0; b = x;
  }
  
  // Convert to hex
  r = Math.round((r + m) * 255).toString(16).padStart(2, '0');
  g = Math.round((g + m) * 255).toString(16).padStart(2, '0');
  b = Math.round((b + m) * 255).toString(16).padStart(2, '0');
  
  return `#${r}${g}${b}`;
};

// Calculate complementary color (180° hue shift with adjusted saturation and lightness)
const calculateComplementaryColor = (hexColor) => {
  if (!hexColor) return '#6495ED'; // Default blue if no color provided
  
  const hsl = hexToHSL(hexColor);
  // Shift hue by 180 degrees
  let newHue = (hsl.h + 180) % 360;
  
  // Adjust saturation and lightness for better contrast
  // Ensure saturation is high enough for a vibrant complementary color
  let newSaturation = Math.min(Math.max(hsl.s, 60), 90);
  
  // Adjust lightness based on original lightness
  // If original is dark, make complementary lighter, and vice versa
  let newLightness = hsl.l < 50 ? Math.min(hsl.l + 30, 80) : Math.max(hsl.l - 30, 30);
  
  return hslToHex(newHue, newSaturation, newLightness);
};

export const LogoProvider = ({ children }) => {
  const [logoUrl, setLogoUrl] = useState('');
  const [dominantColor, setDominantColor] = useState('#282c34'); // Default color
  const [complementaryColor, setComplementaryColor] = useState('#ED7D31'); // Default complementary color

  const fetchHeaderStyle = useCallback(async () => {
    try {
      const PRODUCTION_HOST = 'hdfc-assistant-backend-1018963165306.us-central1.run.app';
      const LOCAL_HOST = 'localhost:8000';
      const isProduction = window.location.hostname !== 'localhost' && window.location.hostname !== '127.0.0.1';
      const BACKEND_HOST = isProduction ? PRODUCTION_HOST : LOCAL_HOST;
      const HTTP_PROTOCOL = isProduction ? 'https' : 'http';
      const styleUrl = `${HTTP_PROTOCOL}://${BACKEND_HOST}/api/header-style?t=${new Date().getTime()}`;
      const response = await fetch(styleUrl);
      if (response.ok) {
        const data = await response.json();
        const fullLogoUrl = `${HTTP_PROTOCOL}://${BACKEND_HOST}${data.logoUrl}?t=${new Date().getTime()}`;
        setLogoUrl(fullLogoUrl);
        setDominantColor(data.dominantColor);
      }
    } catch (error) {
      console.error('Error fetching header style:', error);
    }
  }, []);

  // Calculate complementary color whenever dominant color changes
  useEffect(() => {
    const complementary = calculateComplementaryColor(dominantColor);
    setComplementaryColor(complementary);
  }, [dominantColor]);

  useEffect(() => {
    fetchHeaderStyle();
  }, [fetchHeaderStyle]);

  const refreshLogo = () => {
    fetchHeaderStyle();
  };

  return (
    <LogoContext.Provider value={{ logoUrl, dominantColor, complementaryColor, refreshLogo }}>
      {children}
    </LogoContext.Provider>
  );
};
