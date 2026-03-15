#!/bin/bash
# Session script for local_brian_1765899227
# Profile: /workspace/chrome_profiles/account_brian

echo "Session: local_brian_1765899227"
echo "Profile: /workspace/chrome_profiles/account_brian"
echo ""
echo "Available commands:"
echo "  cookie-scripts     - Go to cookie scripts directory"
echo "  list-cookies       - List available cookie scripts"
echo "  session-info       - Show session information"

alias cookie-scripts='cd /app/cookie_scripts && ls -la'
alias list-cookies='ls -la /app/cookie_scripts/*.sh'
alias session-info='echo "Session: local_brian_1765899227" && echo "Profile: /workspace/chrome_profiles/account_brian"'

# Open bash shell
bash
