#!/bin/bash

# ===================================================================
# Verify Safe to Commit - Pre-commit Security Check
# ===================================================================
# Ensures no sensitive files are about to be committed
# Usage: ./verify-before-commit.sh
# ===================================================================

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${BLUE}╔════════════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║           Pre-Commit Security Verification                     ║${NC}"
echo -e "${BLUE}╚════════════════════════════════════════════════════════════════╝${NC}"
echo ""

# Files that should NEVER be committed
SENSITIVE_FILES=(
    ".env.production"
    ".production-credentials.txt"
    "*.pem"
    "*.key"
    "id_rsa"
    "id_ed25519"
    "*.sql.gz"
    "backups/"
)

ISSUES_FOUND=0

echo -e "${BLUE}🔍 Checking for sensitive files in Git staging area...${NC}"
echo ""

# Check each sensitive file pattern
for pattern in "${SENSITIVE_FILES[@]}"; do
    # Check if any files matching the pattern are staged
    if git diff --cached --name-only | grep -q "$pattern"; then
        echo -e "${RED}❌ DANGER: Found staged file matching: $pattern${NC}"
        git diff --cached --name-only | grep "$pattern" | while read file; do
            echo -e "   ${YELLOW}→ $file${NC}"
        done
        ISSUES_FOUND=$((ISSUES_FOUND + 1))
        echo ""
    fi
done

# Check for potential password/key strings in staged files
echo -e "${BLUE}🔍 Scanning staged files for potential secrets...${NC}"
echo ""

STAGED_FILES=$(git diff --cached --name-only)
if [ -n "$STAGED_FILES" ]; then
    for file in $STAGED_FILES; do
        # Skip binary files
        if file "$file" 2>/dev/null | grep -q "text"; then
            # Look for common secret patterns
            if grep -qE "(password|secret|key|token).*=.*[A-Za-z0-9]{20,}" "$file" 2>/dev/null; then
                if [[ "$file" != *.md ]] && [[ "$file" != *.example ]] && [[ "$file" != *.template ]]; then
                    echo -e "${YELLOW}⚠️  Potential secret found in: $file${NC}"
                    echo -e "   ${YELLOW}(This might be OK if it's a template or example)${NC}"
                    echo ""
                fi
            fi
        fi
    done
fi

# Check if nginx.conf has a real IP (not placeholder)
echo -e "${BLUE}🔍 Checking nginx.conf configuration...${NC}"
if [ -f "nginx/nginx.conf" ]; then
    if grep -q "YOUR_IP_ADDRESS_HERE" nginx/nginx.conf; then
        echo -e "${RED}❌ nginx.conf still has placeholder IP address${NC}"
        echo -e "   ${YELLOW}Update it with your real IP before committing${NC}"
        ISSUES_FOUND=$((ISSUES_FOUND + 1))
    else
        echo -e "${GREEN}✓${NC} nginx.conf has been updated with IP address"
    fi
else
    echo -e "${YELLOW}⚠️  nginx/nginx.conf not found${NC}"
fi
echo ""

# Check .gitignore
echo -e "${BLUE}🔍 Verifying .gitignore protection...${NC}"
if [ -f ".gitignore" ]; then
    MISSING_PATTERNS=()
    
    if ! grep -q ".env.production" .gitignore; then
        MISSING_PATTERNS+=(".env.production")
    fi
    
    if ! grep -q ".production-credentials.txt" .gitignore; then
        MISSING_PATTERNS+=(".production-credentials.txt")
    fi
    
    if [ ${#MISSING_PATTERNS[@]} -gt 0 ]; then
        echo -e "${YELLOW}⚠️  Missing patterns in .gitignore:${NC}"
        for pattern in "${MISSING_PATTERNS[@]}"; do
            echo -e "   ${YELLOW}→ $pattern${NC}"
        done
        echo -e "${YELLOW}   Add these patterns to .gitignore!${NC}"
        ISSUES_FOUND=$((ISSUES_FOUND + 1))
    else
        echo -e "${GREEN}✓${NC} .gitignore has proper protection"
    fi
else
    echo -e "${RED}❌ .gitignore file not found!${NC}"
    ISSUES_FOUND=$((ISSUES_FOUND + 1))
fi
echo ""

# Check current IP
echo -e "${BLUE}🌐 Current IP Address Information:${NC}"
CURRENT_IP=$(curl -s ifconfig.me 2>/dev/null || echo "Unable to fetch")
echo -e "   Your current IP: ${GREEN}$CURRENT_IP${NC}"
if [ -f "nginx/nginx.conf" ]; then
    if grep -q "$CURRENT_IP" nginx/nginx.conf 2>/dev/null; then
        echo -e "   ${GREEN}✓${NC} This IP is whitelisted in nginx.conf"
    else
        echo -e "   ${YELLOW}⚠️  This IP is NOT in nginx.conf whitelist${NC}"
        echo -e "   ${YELLOW}   (OK if you'll access from a different IP)${NC}"
    fi
fi
echo ""

# Check what's being committed
echo -e "${BLUE}📋 Files staged for commit:${NC}"
if [ -z "$STAGED_FILES" ]; then
    echo -e "${YELLOW}   No files staged${NC}"
else
    echo "$STAGED_FILES" | while read file; do
        echo -e "   ${GREEN}→${NC} $file"
    done
fi
echo ""

# Final verdict
echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
if [ $ISSUES_FOUND -eq 0 ]; then
    echo -e "${GREEN}╔════════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${GREEN}║                  ✅ SAFE TO COMMIT                             ║${NC}"
    echo -e "${GREEN}╚════════════════════════════════════════════════════════════════╝${NC}"
    echo ""
    echo -e "${GREEN}✓${NC} No sensitive files detected"
    echo -e "${GREEN}✓${NC} No security issues found"
    echo ""
    echo -e "${BLUE}You can safely commit and push:${NC}"
    echo -e "   ${YELLOW}git commit -m 'Your commit message'${NC}"
    echo -e "   ${YELLOW}git push origin main${NC}"
    exit 0
else
    echo -e "${RED}╔════════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${RED}║                  ❌ NOT SAFE TO COMMIT                         ║${NC}"
    echo -e "${RED}╚════════════════════════════════════════════════════════════════╝${NC}"
    echo ""
    echo -e "${RED}Found $ISSUES_FOUND security issue(s)${NC}"
    echo ""
    echo -e "${YELLOW}Fix these issues before committing:${NC}"
    echo -e "1. Remove sensitive files from staging:"
    echo -e "   ${YELLOW}git reset HEAD .env.production${NC}"
    echo -e "   ${YELLOW}git reset HEAD .production-credentials.txt${NC}"
    echo ""
    echo -e "2. Update .gitignore if needed"
    echo ""
    echo -e "3. Update nginx.conf if needed"
    echo ""
    echo -e "4. Run this script again to verify"
    echo ""
    exit 1
fi