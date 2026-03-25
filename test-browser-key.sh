docker exec -it streamlit_app python -c "
import asyncio, websockets
async def test():
    token = '2UDHTet8etjhZ2M0cc02f659e8affc140ca5839c91454f501'
    url = f'wss://production-sfo.browserless.io?token={token}'
    try:
        async with websockets.connect(url) as ws:
            print('Connected!')
    except Exception as e:
        print(f'Error: {e}')
asyncio.run(test())
"