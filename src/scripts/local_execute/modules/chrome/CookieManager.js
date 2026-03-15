// modules/chrome/CookieManager.js
import fs from 'fs';
import path from 'path';

/**
 * Manages cookie loading from session files
 */
export class CookieManager {
    /**
     * Load cookies from session_data.json into a Puppeteer page
     */
    async loadCookies(page, profilePath) {
        const sessionFile = path.join(profilePath, 'session_data.json');
        
        console.log('');
        console.log('█'.repeat(80));
        console.log('LOADING COOKIES FROM SESSION FILE');
        console.log('█'.repeat(80));
        console.log(`Profile: ${profilePath}`);
        console.log(`Session file: ${sessionFile}`);
        console.log('');
        
        if (!fs.existsSync(sessionFile)) {
            console.log('⚠️ No session_data.json found');
            console.log('  User may need to log in manually');
            console.log('█'.repeat(80));
            console.log('');
            return { success: false, reason: 'no_session_file' };
        }
        
        try {
            const rawData = fs.readFileSync(sessionFile, 'utf8');
            const data = JSON.parse(rawData);
            
            console.log('📄 SESSION FILE FOUND');
            console.log('─'.repeat(80));
            console.log(`  Saved: ${data.timestamp || 'Unknown'}`);
            console.log(`  Cookie count: ${data.cookies?.length || 0}`);
            console.log('─'.repeat(80));
            console.log('');
            
            if (!data.cookies || !Array.isArray(data.cookies) || data.cookies.length === 0) {
                console.log('⚠️ No cookies found in session file');
                console.log('█'.repeat(80));
                return { success: false, reason: 'no_cookies_in_file' };
            }
            
            // Apply cookies
            console.log('🔄 Applying cookies to page...');
            
            let successCount = 0;
            let failCount = 0;
            const failedCookies = [];
            
            for (const cookie of data.cookies) {
                try {
                    const puppeteerCookie = this.convertCookie(cookie);
                    await page.setCookie(puppeteerCookie);
                    successCount++;
                    
                    // Log critical cookies
                    if (['auth_token', 'ct0', 'kdt'].includes(cookie.name)) {
                        console.log(`  ✓ ${cookie.name}: ${cookie.value.substring(0, 20)}...`);
                    }
                } catch (error) {
                    failCount++;
                    failedCookies.push({
                        name: cookie.name,
                        error: error.message
                    });
                }
            }
            
            console.log('─'.repeat(80));
            console.log(`✓ Successfully loaded ${successCount}/${data.cookies.length} cookies`);
            
            if (failCount > 0) {
                console.log(`⚠️ Failed to load ${failCount} cookies:`);
                failedCookies.forEach(f => {
                    console.log(`  - ${f.name}: ${f.error}`);
                });
            }
            
            // Verify critical cookies
            const criticalCookies = ['auth_token', 'ct0', 'kdt'];
            const loadedCookieNames = data.cookies.map(c => c.name);
            const missingCritical = criticalCookies.filter(name => !loadedCookieNames.includes(name));
            
            if (missingCritical.length === 0) {
                console.log('✓ All critical authentication cookies loaded');
            } else {
                console.log('⚠️ WARNING: Missing critical cookies:');
                missingCritical.forEach(name => {
                    console.log(`  - ${name}`);
                });
            }
            
            console.log('');
            console.log('█'.repeat(80));
            console.log('✓ COOKIES LOADED SUCCESSFULLY');
            console.log('█'.repeat(80));
            console.log('');
            
            return {
                success: true,
                cookiesLoaded: successCount,
                cookiesFailed: failCount,
                hasCriticalCookies: missingCritical.length === 0
            };
            
        } catch (error) {
            console.error('');
            console.error('█'.repeat(80));
            console.error('❌ FAILED TO LOAD COOKIES');
            console.error('█'.repeat(80));
            console.error(`Error: ${error.message}`);
            console.error('█'.repeat(80));
            console.error('');
            return { success: false, error: error.message };
        }
    }

    /**
     * Convert EditThisCookie format to Puppeteer format
     */
    convertCookie(cookie) {
        const puppeteerCookie = {
            name: cookie.name,
            value: cookie.value,
            domain: cookie.domain,
            path: cookie.path || '/',
            secure: cookie.secure !== undefined ? cookie.secure : false,
            httpOnly: cookie.httpOnly !== undefined ? cookie.httpOnly : false,
        };
        
        if (cookie.expirationDate) {
            puppeteerCookie.expires = cookie.expirationDate;
        }
        
        if (cookie.sameSite) {
            const sameSite = cookie.sameSite.toLowerCase();
            if (['strict', 'lax', 'none'].includes(sameSite)) {
                puppeteerCookie.sameSite = sameSite.charAt(0).toUpperCase() + sameSite.slice(1);
            } else if (sameSite === 'no_restriction') {
                puppeteerCookie.sameSite = 'None';
                puppeteerCookie.secure = true;
            }
        }
        
        return puppeteerCookie;
    }
}