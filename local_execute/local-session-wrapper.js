/**
 * Wrapper for LocalChromeSessionManager that properly separates logs from JSON output
 * Save this as: /app/local_execute/local-session-wrapper.js
 * ES Module version for projects with "type": "module" in package.json
 */

import { LocalChromeSessionManager } from './local-session-manager.js';

// Redirect console.log to stderr to keep stdout clean for JSON
const originalLog = console.log;
console.log = (...args) => {
    console.error(...args);
};

// Restore for final JSON output
const outputJSON = (data) => {
    // Use process.stdout directly to ensure clean output
    process.stdout.write(JSON.stringify(data) + '\n');
};

async function createProfile(accountId, username) {
    const manager = new LocalChromeSessionManager();
    
    try {
        await manager.connect();
        
        const result = await manager.createLocalProfile(
            accountId,
            username
        );
        
        await manager.disconnect();
        
        outputJSON(result);
        process.exit(0);
        
    } catch (error) {
        await manager.disconnect();
        
        outputJSON({
            success: false,
            error: error.message,
            stack: error.stack
        });
        process.exit(1);
    }
}

async function startSession(profileId, accountId, sessionConfig) {
    const manager = new LocalChromeSessionManager();
    
    try {
        await manager.connect();
        
        const result = await manager.startSession(
            profileId,
            accountId,
            sessionConfig
        );
        
        await manager.disconnect();
        
        outputJSON(result);
        process.exit(0);
        
    } catch (error) {
        await manager.disconnect();
        
        outputJSON({
            success: false,
            error: error.message,
            stack: error.stack
        });
        process.exit(1);
    }
}

async function stopSession(sessionId) {
    const manager = new LocalChromeSessionManager();
    
    try {
        await manager.connect();
        
        const result = await manager.stopSession(sessionId);
        
        await manager.disconnect();
        
        outputJSON(result);
        process.exit(0);
        
    } catch (error) {
        await manager.disconnect();
        
        outputJSON({
            success: false,
            error: error.message,
            stack: error.stack
        });
        process.exit(1);
    }
}

// Parse command line arguments
const command = process.argv[2];

switch (command) {
    case 'createProfile':
        createProfile(process.argv[3], process.argv[4]);
        break;
    
    case 'startSession':
        startSession(
            process.argv[3],
            process.argv[4],
            JSON.parse(process.argv[5])
        );
        break;
    
    case 'stopSession':
        stopSession(process.argv[3]);
        break;
    
    default:
        outputJSON({
            success: false,
            error: `Unknown command: ${command}`
        });
        process.exit(1);
}