#!/usr/bin/env bash

echo "🔍 Testing Gemini API..."

docker exec -it streamlit_app python -c "
import os, sys
from google import genai

api_key = os.environ.get('GEMINI_API_KEY')

if not api_key:
    print('❌ GEMINI_API_KEY not set')
    sys.exit(1)

try:
    client = genai.Client(api_key=api_key)

    response = client.models.generate_content(
        model='gemini-2.5-flash',  # ✅ FREE + WORKING MODEL
        contents='Say hello in one short sentence.'
    )

    text = getattr(response, 'text', None)

    if text:
        print(f'✅ Success! Response: {text}')
    else:
        print('⚠️ No text response returned')
        print(response)

except Exception as e:
    print(f'❌ Error: {e}')
    sys.exit(1)
"