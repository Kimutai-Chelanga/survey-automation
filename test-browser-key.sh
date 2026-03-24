docker exec -it streamlit_app python -c "
import asyncio, websockets
async def test():
    token = 'bu_wU7AOU8d5BuxquQQe2VFb74B5FTYv9GZQnaZXdQM1hk'
    url = f'wss://production-sfo.browserless.io?token={token}'
    try:
        async with websockets.connect(url) as ws:
            print('Connected!')
    except Exception as e:
        print(f'Error: {e}')
asyncio.run(test())
"