#!/usr/bin/env node

/**
 * HyperBrowser.ai Script - Navigate to Facebook
 * This script creates a HyperBrowser session and navigates to Facebook
 */

import fs from 'fs';
import path from 'path';
import { Hyperbrowser } from '@hyperbrowser/sdk';
import { connect } from 'puppeteer-core';
import { fileURLToPath } from 'url';
import 'dotenv/config';

// Get __dirname equivalent for ES modules
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// HyperBrowser.ai Configuration
const API_KEY = process.env.HYPERBROWSER_API_KEY; // Set this in your .env file
const WORKFLOW_DIRS = [
    path.join(__dirname, 'gui-scripts/workflows'),
    "/workspace/gui-scripts/workflows"
];
const LOG_FILE = "hyperbrowser_trigger_log.txt";

let client = null;
let currentSession = null;
let browser = null;
let page = null;

// Initialize HyperBrowser client
function initializeClient() {
    if (!API_KEY) {
        throw new Error('HYPERBROWSER_API_KEY environment variable not set!');
    }
    client = new Hyperbrowser({
        apiKey: API_KEY
    });
}

// Load workflows from JSON files (kept for compatibility)
function loadWorkflows() {
    const workflows = [];
    
    for (const workflowDir of WORKFLOW_DIRS) {
        if (!fs.existsSync(workflowDir)) {
            console.log(`⚠️ Skipping missing directory: ${workflowDir}`);
            continue;
        }
        
        const files = fs.readdirSync(workflowDir)
            .filter(file => file.endsWith('.json'));
            
        if (files.length === 0) {
            console.log(`⚠️ No JSON files in: ${workflowDir}`);
            continue;
        }
        
        const dirname = path.basename(workflowDir);
        
        for (const file of files) {
            try {
                const filePath = path.join(workflowDir, file);
                const data = JSON.parse(fs.readFileSync(filePath, 'utf8'));
                const fname = path.basename(file, '.json');
                
                data.id = data.id || `${dirname}_${fname}`;
                data.name = data.name || `${dirname.charAt(0).toUpperCase() + dirname.slice(1)} - ${fname}`;
                
                const ts = Date.now();
                data.createdAt = data.createdAt || ts;
                data.updatedAt = data.updatedAt || ts;
                data.isDisabled = data.isDisabled || false;
                data.description = data.description || `Imported ${dirname} workflow: ${fname}`;
                
                workflows.push(data);
                console.log(`  ✅ Loaded: ${data.name}`);
            } catch (e) {
                console.error(`  ❌ Failed parsing ${file}: ${e.message}`);
            }
        }
    }
    
    console.log(`📊 Total workflows loaded: ${workflows.length}`);
    return workflows;
}

// Create HyperBrowser session and connect Puppeteer
async function createBrowserSession() {
    console.log('🌐 Creating HyperBrowser session...');
    
    try {
        currentSession = await client.sessions.create({
            browser: 'chrome',
            headless: false, // Set to false to see what's happening
            timeout: 300000, // 5 minutes
            stealth: true
        });
        
        console.log(`✅ Created HyperBrowser session: ${currentSession.id}`);
        
        // Connect Puppeteer to the session
        browser = await connect({ 
            browserWSEndpoint: currentSession.wsEndpoint,
            defaultViewport: null
        });
        
        // Get or create a page
        const pages = await browser.pages();
        page = pages.length > 0 ? pages[0] : await browser.newPage();
        
        console.log('✅ Connected Puppeteer to HyperBrowser session');
        
        return { browser, page, sessionId: currentSession.id };
        
    } catch (error) {
        console.error(`❌ Failed to create browser session: ${error.message}`);
        throw error;
    }
}

// Navigate to Facebook and stop there
async function setupAutomaEnvironment(workflows, variablesMap = null) {
    try {
        console.log('🔍 Navigating to Facebook...');
        
        // Simply navigate to Facebook
        await page.goto('https://www.facebook.com', {
            waitUntil: 'networkidle2',
            timeout: 60000
        });
        
        console.log('✅ Successfully navigated to Facebook');
        console.log('🛑 Stopping here as requested - no workflow execution');
        
    } catch (error) {
        console.error(`Error navigating to Facebook: ${error.message}`);
        throw error;
    }
}

// Clean up resources
async function cleanup() {
    console.log('🧹 Cleaning up resources...');
    
    try {
        if (browser) {
            await browser.close();
            console.log('✅ Browser closed');
        }
        
        if (currentSession && client) {
            await client.sessions.stop(currentSession.id);
            console.log(`✅ HyperBrowser session stopped: ${currentSession.id}`);
        }
    } catch (error) {
        console.error(`⚠️ Cleanup error: ${error.message}`);
    }
}

// Check API status
async function checkHyperBrowserStatus() {
    console.log("🔍 Checking HyperBrowser API status...");
    
    try {
        initializeClient();
        
        // Try to list sessions to verify API connectivity
        const sessions = await client.sessions.list();
        console.log("✅ HyperBrowser API is accessible");
        console.log(`📊 Active sessions: ${sessions.length || 0}`);
        
        return true;
    } catch (error) {
        console.error("❌ HyperBrowser API is not accessible!");
        console.error(`💡 Error: ${error.message}`);
        if (error.message.includes('API key')) {
            console.error("💡 Check your HYPERBROWSER_API_KEY environment variable");
            console.error("💡 Get your API key from: https://app.hyperbrowser.ai/dashboard");
        }
        return false;
    }
}

// Check workflow directories (optional now)
function checkWorkflowDirectory() {
    console.log('📂 Checking workflow directories...');
    
    for (const workflowPath of WORKFLOW_DIRS) {
        if (fs.existsSync(workflowPath)) {
            console.log(`✅ Found workflow directory: ${workflowPath}`);
            
            const files = fs.readdirSync(workflowPath)
                .filter(file => file.endsWith('.json'));
            
            console.log(`📊 Found ${files.length} JSON workflow files in ${workflowPath}`);
            
            if (files.length === 0) {
                console.log('⚠️ No .json files found in workflow directory');
            } else {
                console.log('📋 Workflow files found:');
                files.forEach(file => console.log(`  - ${file}`));
            }
            
            return workflowPath;
        }
    }
    
    console.log('⚠️ No workflow directory found (not required for Facebook navigation)');
    return null;
}

// Main function
async function main() {
    console.log("🚀 HyperBrowser Facebook Navigator");
    console.log("========================================");
    console.log("");
    
    // Set up cleanup handlers
    process.on('SIGINT', async () => {
        console.log('');
        console.log('👋 Shutting down...');
        await cleanup();
        process.exit(0);
    });
    
    process.on('uncaughtException', async (error) => {
        console.error('Uncaught Exception:', error);
        await cleanup();
        process.exit(1);
    });
    
    try {
        // Step 1: Check HyperBrowser status
        const apiWorking = await checkHyperBrowserStatus();
        if (!apiWorking) {
            process.exit(1);
        }
        console.log("");
        
        // Step 2: Check workflow directory (optional now)
        checkWorkflowDirectory();
        console.log("");
        
        // Step 3: Load workflows (optional now)
        console.log("📥 Loading workflows (optional)...");
        const workflows = loadWorkflows();
        console.log("");
        
        // Step 4: Create browser session
        const sessionInfo = await createBrowserSession();
        console.log("");
        
        // Step 5: Navigate to Facebook and stop
        console.log("🔄 Navigating to Facebook...");
        console.log("==================================================");
        console.log("");
        
        await setupAutomaEnvironment(workflows);
        console.log("");
        
        console.log("");
        console.log("==================================================");
        console.log("🎉 Navigation completed! Browser is on Facebook.");
        console.log("");
        console.log("📖 Session information:");
        console.log(`  - Session ID: ${currentSession.id}`);
        console.log("  - Current page: https://www.facebook.com");
        console.log("  - Browser is running in HyperBrowser cloud");
        console.log("");
        console.log("🔧 Session info:");
        console.log(`  - WebSocket Endpoint: ${currentSession.wsEndpoint}`);
        console.log("  - Browser will stay open for debugging");
        console.log("  - Press Ctrl+C to clean up and exit");
        console.log("==================================================");
        
        // Keep the session alive
        console.log('💡 Session is now running. Press Ctrl+C to exit and cleanup.');
        
        // Wait indefinitely until user interrupts
        await new Promise(() => {});
        
    } catch (error) {
        console.error("");
        console.error("==================================================");
        console.error("❌ Process failed!");
        console.error(`Error: ${error.message}`);
        console.error("");
        console.error("🔧 Debugging steps:");
        console.error("  1. Check your HYPERBROWSER_API_KEY environment variable");
        console.error("  2. Verify your API key at https://app.hyperbrowser.ai/dashboard");
        console.error("  3. Check your internet connection");
        console.error("==================================================");
        
        await cleanup();
        process.exit(1);
    }
}

// Run the main function
if (import.meta.url === `file://${process.argv[1]}`) {
    main().catch(async (error) => {
        console.error(error);
        await cleanup();
    });
}