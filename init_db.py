# backend/init_db.py
import asyncio
import asyncpg
import os

async def init_db():
    conn = await asyncpg.connect('postgresql://postgres:postgres@localhost:5432/analytics')
    
    # Read and execute schema.sql
    with open('backend/schema.sql', 'r') as f:
        schema = f.read()
        await conn.execute(schema)
        print("Schema created successfully!")
    
    await conn.close()

if __name__ == "__main__":
    asyncio.run(init_db())