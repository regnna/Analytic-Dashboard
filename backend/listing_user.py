# In your backend folder with venv activated
import asyncio
import asyncpg

async def list_users():
    conn = await asyncpg.connect('postgresql://postgres:postgres@localhost:5432/analytics')
    users = await conn.fetch('SELECT id, email FROM users LIMIT 5')
    for u in users:
        print(f'{u["id"]} - {u["email"]}')
    await conn.close()

asyncio.run(list_users())