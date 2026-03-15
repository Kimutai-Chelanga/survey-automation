#!/bin/bash
set -e

echo "🔧 Setting up Automa extension..."

# Check if automa source exists
if [ ! -d "automa" ]; then
    echo "📥 Cloning Automa repository..."
    git clone https://github.com/AutomaApp/automa.git
fi

cd automa

# Check if pnpm is installed
if ! command -v pnpm &> /dev/null; then
    echo "📦 Installing pnpm..."
    npm install -g pnpm
fi

# Install dependencies
echo "📦 Installing dependencies..."
pnpm install

# Build the extension
echo "🏗️  Building Automa extension..."
pnpm build

# Check if build was successful
BUILD_DIR=""
if [ -d "dist" ]; then
    BUILD_DIR="dist"
elif [ -d "build" ]; then
    BUILD_DIR="build"
fi

if [ -n "$BUILD_DIR" ] && [ -f "$BUILD_DIR/manifest.json" ]; then
    echo "✅ Build successful! manifest.json found in $BUILD_DIR/"
    echo "📋 Extension contents:"
    ls -la "$BUILD_DIR/"
    
    # Check if zip command is available, install if needed
    if ! command -v zip &> /dev/null; then
        echo "📦 Installing zip utility..."
        if command -v sudo &> /dev/null; then
            sudo apt-get update -qq && sudo apt-get install -y zip
        else
            apt-get update -qq && apt-get install -y zip
        fi
    fi

    # Create the target directory at the root
    echo "📂 Creating target directory for the extension..."
    mkdir -p ../src/automa-extension
    
    # Create the zip file
    echo "🗜️  Zipping built extension..."
    EXTENSION_ZIP_NAME="automa-extension.zip"
    
    # Change into the build directory to zip its contents directly
    (cd "$BUILD_DIR" && zip -r "../../src/automa-extension/$EXTENSION_ZIP_NAME" .)
    
    # Also create a copy in workspace root for easy access
    cp "../src/automa-extension/$EXTENSION_ZIP_NAME" "../$EXTENSION_ZIP_NAME"
    
    echo ""
    echo "✅ Extension packaged successfully!"
    echo "📍 Locations:"
    echo "   - /workspace/src/automa-extension/$EXTENSION_ZIP_NAME"
    echo "   - /workspace/$EXTENSION_ZIP_NAME"
    
    # Show file size
    ls -lh "../$EXTENSION_ZIP_NAME"
    
    echo ""
    echo "📝 To load the extension in Chrome:"
    echo "   1. Open Chrome and go to chrome://extensions/"
    echo "   2. Enable 'Developer mode' (toggle in top right)"
    echo "   3. Click 'Load unpacked'"
    echo "   4. Select the /workspace/automa/$BUILD_DIR directory"
    echo ""
    echo "💡 Or upload automa-extension.zip to Hyperbrowser Extensions API"
    
else
    echo "❌ Build failed or manifest.json not found"
    exit 1
fi

cd ..
echo "🎉 Setup complete!"