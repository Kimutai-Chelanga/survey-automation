#!/bin/bash
# Session script for local_mark_1765973849
# Profile: /workspace/chrome_profiles/account_mark

echo "Session: local_mark_1765973849"
echo "Profile: /workspace/chrome_profiles/account_mark"
echo ""
echo "Available commands:"
echo "  cookie-scripts     - Go to cookie scripts directory"
echo "  list-cookies       - List available cookie scripts"
echo "  session-info       - Show session information"

alias cookie-scripts='cd /app/cookie_scripts && ls -la'
alias list-cookies='ls -la /app/cookie_scripts/*.sh'
alias session-info='echo "Session: local_mark_1765973849" && echo "Profile: /workspace/chrome_profiles/account_mark"'

# Open bash shell
bash
