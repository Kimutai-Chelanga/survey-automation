// src/scripts/extraction/persistent_session.js
/**
 * FIXED: Better cookie restoration for EditThisCookie format
 * Added proper handling of sameSite and expiration
 */

import fs from 'fs';
import path from 'path';

function getSessionFilePath() {
  const profileDir = process.env.CHROME_PROFILE_DIR;
  
  if (!profileDir) {
    throw new Error(
      '❌ FATAL: CHROME_PROFILE_DIR environment variable not set'
    );
  }
  
  if (!fs.existsSync(profileDir)) {
    throw new Error(
      `❌ FATAL: Chrome profile directory does not exist: ${profileDir}`
    );
  }
  
  const sessionFile = path.join(profileDir, 'session_data.json');
  
  console.log('═'.repeat(80));
  console.log('SESSION FILE PATH CONFIGURATION');
  console.log('═'.repeat(80));
  console.log(`  Profile directory: ${profileDir}`);
  console.log(`  Session file: ${sessionFile}`);
  console.log('═'.repeat(80));
  console.log('');
  
  return sessionFile;
}

let SESSION_FILE;
try {
  SESSION_FILE = getSessionFilePath();
} catch (error) {
  console.error(error.message);
  process.exit(1);
}

/**
 * Convert EditThisCookie format to Puppeteer cookie format
 */
function convertToPuppeteerCookie(editThisCookie) {
  const cookie = {
    name: editThisCookie.name,
    value: editThisCookie.value,
    domain: editThisCookie.domain,
    path: editThisCookie.path || '/',
    secure: editThisCookie.secure === true,
    httpOnly: editThisCookie.httpOnly === true,
  };

  if (editThisCookie.expirationDate) {
    cookie.expires = editThisCookie.expirationDate;
  } else if (editThisCookie.expires) {
    cookie.expires = editThisCookie.expires;
  }

  if (editThisCookie.sameSite) {
    const sameSite = editThisCookie.sameSite.toLowerCase();
    if (sameSite === 'no_restriction') {
      cookie.sameSite = 'None';
      cookie.secure = true;
    } else if (sameSite === 'lax') {
      cookie.sameSite = 'Lax';
    } else if (sameSite === 'strict') {
      cookie.sameSite = 'Strict';
    }
  }

  return cookie;
}

/**
 * Load session data with ENHANCED cookie restoration
 */
export async function loadSession(page) {
  console.log('');
  console.log('█'.repeat(80));
  console.log('LOADING PERSISTENT SESSION');
  console.log('█'.repeat(80));
  console.log(`Profile: ${process.env.CHROME_PROFILE_DIR}`);
  console.log(`Session file: ${SESSION_FILE}`);
  console.log('');
  
  if (!fs.existsSync(SESSION_FILE)) {
    console.log('📝 No saved session found');
    console.log('   Starting with fresh session...');
    console.log('█'.repeat(80));
    console.log('');
    return false;
  }

  try {
    const rawData = fs.readFileSync(SESSION_FILE, 'utf8');
    const data = JSON.parse(rawData);
    
    console.log('📄 SESSION FILE FOUND');
    console.log('─'.repeat(80));
    console.log(`  Saved: ${data.timestamp || 'Unknown'}`);
    console.log(`  Account ID: ${data.accountId || 'Unknown'}`);
    console.log(`  Profile: ${data.profileDir || 'Unknown'}`);
    console.log('─'.repeat(80));
    console.log('');

    if (data.cookies && Array.isArray(data.cookies) && data.cookies.length > 0) {
      console.log(`📦 Found ${data.cookies.length} cookies to restore`);
      
      console.log('🌐 Navigating to x.com to set cookie context...');
      await page.goto('https://x.com/home', { waitUntil: 'domcontentloaded', timeout: 30000 }).catch(() => {
        console.log('  ⚠️ Initial navigation failed, continuing anyway...');
      });
      await new Promise(resolve => setTimeout(resolve, 2000));
      
      console.log('');
      console.log('🔄 Applying cookies to page...');
      console.log('─'.repeat(80));
      
      let successCount = 0;
      const criticalCookies = ['auth_token', 'ct0', 'kdt'];
      
      for (const cookie of data.cookies) {
        try {
          const puppeteerCookie = convertToPuppeteerCookie(cookie);
          
          if (criticalCookies.includes(cookie.name)) {
            console.log(`  → Setting ${cookie.name}: ${cookie.value.substring(0, 20)}...`);
          }
          
          await page.setCookie(puppeteerCookie);
          successCount++;
          
          if (criticalCookies.includes(cookie.name)) {
            console.log(`  ✓ ${cookie.name} set successfully`);
          }
        } catch (error) {
          console.log(`  ✗ Failed to set ${cookie.name}: ${error.message}`);
        }
      }
      
      console.log('─'.repeat(80));
      console.log(`✓ Successfully set ${successCount}/${data.cookies.length} cookies`);
      
      // Verify cookies
      console.log('');
      console.log('🔍 Verifying cookies in browser...');
      
      const currentCookies = await page.cookies();
      const verifiedAuthToken = currentCookies.find(c => c.name === 'auth_token');
      const verifiedCt0 = currentCookies.find(c => c.name === 'ct0');
      
      if (verifiedAuthToken) {
        console.log(`  ✓ auth_token present: ${verifiedAuthToken.value.substring(0, 20)}...`);
      } else {
        console.log(`  ❌ auth_token MISSING after setting!`);
        
        // Try alternative method for auth_token
        const authCookie = data.cookies.find(c => c.name === 'auth_token');
        if (authCookie) {
          console.log('  🔄 Attempting alternative method for auth_token...');
          await page.setCookie({
            name: 'auth_token',
            value: authCookie.value,
            domain: '.x.com',
            path: '/',
            secure: true,
            httpOnly: true,
            sameSite: 'None'
          });
        }
      }
      
      if (verifiedCt0) {
        console.log(`  ✓ ct0 present: ${verifiedCt0.value.substring(0, 20)}...`);
      }
      
      console.log(`  📊 Total cookies now: ${currentCookies.length}`);
      console.log('');

    } else {
      console.log('⚠ No cookies found in session');
      console.log('');
    }

    // Restore localStorage
    if (data.localStorage && typeof data.localStorage === 'object') {
      try {
        await page.evaluate(storage => {
          if (typeof localStorage !== 'undefined') {
            Object.entries(storage).forEach(([key, value]) => {
              try {
                localStorage.setItem(key, value);
              } catch (e) {}
            });
          }
        }, data.localStorage);
        
        const itemCount = Object.keys(data.localStorage).length;
        console.log(`✓ Restored ${itemCount} localStorage items`);
        console.log('');
      } catch (e) {
        console.log('⚠ localStorage blocked or unavailable');
        console.log('');
      }
    }

    // Refresh the page
    console.log('🔄 Refreshing page to apply cookies...');
    await page.reload({ waitUntil: 'domcontentloaded', timeout: 30000 }).catch(() => {
      console.log('  ⚠️ Refresh failed, continuing...');
    });
    await new Promise(resolve => setTimeout(resolve, 3000));

    console.log('█'.repeat(80));
    console.log('✓ SESSION LOADED SUCCESSFULLY');
    console.log('█'.repeat(80));
    console.log('');
    
    return true;

  } catch (error) {
    console.error('');
    console.error('█'.repeat(80));
    console.error('❌ FAILED TO LOAD SESSION');
    console.error('█'.repeat(80));
    console.error(`Error: ${error.message}`);
    console.error('Continuing with fresh session...');
    console.error('█'.repeat(80));
    console.error('');
    return false;
  }
}

/**
 * Save session data with proper cookie format
 */
export async function saveSession(page) {
  console.log('');
  console.log('█'.repeat(80));
  console.log('SAVING PERSISTENT SESSION');
  console.log('█'.repeat(80));
  console.log(`Session file: ${SESSION_FILE}`);
  console.log('');
  
  try {
    const cookies = await page.cookies();
    console.log(`📦 Captured ${cookies.length} cookies`);
    
    if (cookies.length > 0) {
      const domains = [...new Set(cookies.map(c => c.domain))];
      console.log(`   Domains: ${domains.join(', ')}`);
      
      const criticalCookies = cookies.filter(c => 
        ['auth_token', 'ct0', 'kdt'].includes(c.name)
      );
      
      if (criticalCookies.length > 0) {
        console.log('');
        console.log('🔑 Critical cookies captured:');
        criticalCookies.forEach(c => {
          console.log(`   - ${c.name}: ${c.value.substring(0, 20)}... (domain: ${c.domain})`);
        });
      }
    }
    console.log('');

    let localStorage = {};
    try {
      localStorage = await page.evaluate(() => {
        const data = {};
        if (typeof window.localStorage !== 'undefined') {
          for (let i = 0; i < window.localStorage.length; i++) {
            const key = window.localStorage.key(i);
            if (key) {
              data[key] = window.localStorage.getItem(key);
            }
          }
        }
        return data;
      });
      
      const itemCount = Object.keys(localStorage).length;
      console.log(`📦 Captured ${itemCount} localStorage items`);
    } catch (e) {
      console.log('⚠ Could not capture localStorage');
      localStorage = {};
    }
    console.log('');

    const sessionData = {
      timestamp: new Date().toISOString(),
      accountId: 1,
      profileDir: process.env.CHROME_PROFILE_DIR,
      cookies,
      localStorage,
      metadata: {
        cookieCount: cookies.length,
        localStorageCount: Object.keys(localStorage).length,
        savedBy: 'extract_links_weekly',
        nodeVersion: process.version,
        hasAuthToken: cookies.some(c => c.name === 'auth_token'),
        hasCt0: cookies.some(c => c.name === 'ct0')
      }
    };

    const sessionDir = path.dirname(SESSION_FILE);
    if (!fs.existsSync(sessionDir)) {
      fs.mkdirSync(sessionDir, { recursive: true, mode: 0o755 });
      console.log(`✓ Created directory: ${sessionDir}`);
    }

    const tempFile = `${SESSION_FILE}.tmp`;
    fs.writeFileSync(tempFile, JSON.stringify(sessionData, null, 2), { mode: 0o644 });
    fs.renameSync(tempFile, SESSION_FILE);
    
    const stats = fs.statSync(SESSION_FILE);
    const sizeMB = (stats.size / 1024 / 1024).toFixed(2);
    
    console.log('█'.repeat(80));
    console.log('✓ SESSION SAVED SUCCESSFULLY');
    console.log('█'.repeat(80));
    console.log(`  File: ${SESSION_FILE}`);
    console.log(`  Size: ${sizeMB} MB`);
    console.log(`  Cookies: ${cookies.length}`);
    console.log(`  Has auth_token: ${sessionData.metadata.hasAuthToken ? 'Yes' : 'No'}`);
    console.log(`  Has ct0: ${sessionData.metadata.hasCt0 ? 'Yes' : 'No'}`);
    console.log('█'.repeat(80));
    console.log('');

  } catch (error) {
    console.error('');
    console.error('█'.repeat(80));
    console.error('❌ FAILED TO SAVE SESSION');
    console.error('█'.repeat(80));
    console.error(`Error: ${error.message}`);
    console.error('█'.repeat(80));
    console.error('');
  }
}

/**
 * Clear saved session data
 */
export async function clearSession() {
  console.log('');
  console.log('█'.repeat(80));
  console.log('CLEARING PERSISTENT SESSION');
  console.log('█'.repeat(80));
  console.log(`Session file: ${SESSION_FILE}`);
  console.log('');
  
  try {
    if (fs.existsSync(SESSION_FILE)) {
      fs.unlinkSync(SESSION_FILE);
      console.log('✓ Session file deleted');
    } else {
      console.log('⚠ No session file to delete');
    }
    console.log('█'.repeat(80));
    console.log('');
  } catch (error) {
    console.error('❌ Failed to clear session:', error.message);
    console.log('█'.repeat(80));
    console.log('');
    throw error;
  }
}

/**
 * Check if a saved session exists
 */
export function sessionExists() {
  return fs.existsSync(SESSION_FILE);
}

/**
 * Get session info without loading it
 */
export function getSessionInfo() {
  if (!fs.existsSync(SESSION_FILE)) {
    return null;
  }
  
  try {
    const data = JSON.parse(fs.readFileSync(SESSION_FILE, 'utf8'));
    const stats = fs.statSync(SESSION_FILE);
    
    return {
      exists: true,
      path: SESSION_FILE,
      timestamp: data.timestamp,
      accountId: data.accountId,
      profileDir: data.profileDir,
      cookieCount: data.cookies?.length || 0,
      localStorageCount: Object.keys(data.localStorage || {}).length,
      fileSizeBytes: stats.size,
      fileSizeMB: (stats.size / 1024 / 1024).toFixed(2),
      lastModified: stats.mtime.toISOString(),
      hasAuthToken: data.metadata?.hasAuthToken || data.cookies?.some(c => c.name === 'auth_token') || false,
      hasCt0: data.metadata?.hasCt0 || data.cookies?.some(c => c.name === 'ct0') || false
    };
  } catch (error) {
    return {
      exists: true,
      path: SESSION_FILE,
      error: error.message
    };
  }
}

/**
 * Validate session file integrity
 */
export function validateSession() {
  const result = {
    valid: false,
    exists: false,
    readable: false,
    hasCookies: false,
    hasAuthToken: false,
    hasCt0: false,
    hasLocalStorage: false,
    errors: []
  };
  
  if (!fs.existsSync(SESSION_FILE)) {
    result.errors.push('Session file does not exist');
    return result;
  }
  result.exists = true;
  
  try {
    const data = JSON.parse(fs.readFileSync(SESSION_FILE, 'utf8'));
    result.readable = true;
    
    if (!data.timestamp) {
      result.errors.push('Missing timestamp');
    }
    
    if (!data.accountId) {
      result.errors.push('Missing accountId');
    }
    
    if (Array.isArray(data.cookies) && data.cookies.length > 0) {
      result.hasCookies = true;
      
      result.hasAuthToken = data.cookies.some(c => c.name === 'auth_token');
      result.hasCt0 = data.cookies.some(c => c.name === 'ct0');
      
      if (!result.hasAuthToken) {
        result.errors.push('Missing auth_token cookie - authentication will fail');
      }
      if (!result.hasCt0) {
        result.errors.push('Missing ct0 cookie - CSRF protection will fail');
      }
    } else {
      result.errors.push('No cookies found');
    }
    
    if (data.localStorage && typeof data.localStorage === 'object') {
      result.hasLocalStorage = true;
    }
    
    result.valid = result.errors.length === 0 && result.hasCookies && result.hasAuthToken && result.hasCt0;
    
  } catch (error) {
    result.errors.push(`Parse error: ${error.message}`);
  }
  
  return result;
}
