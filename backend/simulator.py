"""
Event simulator to generate realistic e-commerce data
Run this separately to feed data into the system: python simulator.py
"""
import asyncio
import aiohttp
import random
from datetime import datetime, timedelta
from uuid import uuid4
import json

API_BASE = "http://localhost:8000"

class EventSimulator:
    def __init__(self):
        self.users = []
        self.sessions = {}
        self.products = [f"prod_{i}" for i in range(1, 101)]
        
    async def generate_user(self):
        """Generate a new user"""
        sources = ["organic", "paid_search", "social", "referral", "email"]
        devices = ["desktop", "mobile", "tablet"]
        
        user_data = {
            "email": f"user_{uuid4().hex[:8]}@example.com",
            "acquisition_source": random.choice(sources),
            "country_code": random.choice(["US", "CA", "GB", "DE", "FR"]),
            "device_type": random.choice(devices)
        }
        # Note: In real implementation, you'd have a user creation endpoint
        # This is simplified for the simulator
        return uuid4()
    
    async def simulate_session(self, session: aiohttp.ClientSession):
        """Simulate a complete user session with funnel progression"""
        user_id = random.choice(self.users) if self.users and random.random() > 0.3 else await self.generate_user()
        if user_id not in self.users:
            self.users.append(user_id)
            
        session_id = uuid4()
        self.sessions[session_id] = {"user_id": user_id, "events": []}
        
        # Simulate funnel: Page View -> Add to Cart -> Checkout -> Purchase (with drop-offs)
        try:
            # Step 1: Page View (100%)
            await self.send_event(session, {
                "user_id": user_id,
                "session_id": session_id,
                "event_type": "page_view",
                "page_path": f"/product/{random.choice(self.products)}",
                "metadata": {"referrer": random.choice(["google", "direct", "facebook"])}
            })
            await asyncio.sleep(random.uniform(1, 5))
            
            # Step 2: Add to Cart (30% conversion)
            if random.random() < 0.3:
                await self.send_event(session, {
                    "user_id": user_id,
                    "session_id": session_id,
                    "event_type": "add_to_cart",
                    "metadata": {"product_id": random.choice(self.products), "price": random.randint(10, 500)}
                })
                await asyncio.sleep(random.uniform(2, 8))
                
                # Step 3: Checkout Start (60% of cart users)
                if random.random() < 0.6:
                    await self.send_event(session, {
                        "user_id": user_id,
                        "session_id": session_id,
                        "event_type": "checkout_start",
                        "metadata": {}
                    })
                    await asyncio.sleep(random.uniform(3, 10))
                    
                    # Step 4: Purchase (40% of checkout users)
                    if random.random() < 0.4:
                        amount = random.randint(20, 1000)
                        await self.send_order(session, {
                            "user_id": user_id,
                            "order_number": f"ORD-{uuid4().hex[:8].upper()}",
                            "amount": amount,
                            "currency": "USD",
                            "items_count": random.randint(1, 5),
                            "metadata": {"products": random.sample(self.products, random.randint(1, 3))}
                        })
                        
            # Random additional events (scrolls, clicks, etc)
            for _ in range(random.randint(0, 5)):
                await self.send_event(session, {
                    "user_id": user_id,
                    "session_id": session_id,
                    "event_type": random.choice(["click", "scroll", "hover"]),
                    "metadata": {"element": random.choice(["button", "image", "link"])}
                })
                await asyncio.sleep(random.uniform(0.5, 2))
                
        except Exception as e:
            print(f"Error in session simulation: {e}")
    
    async def send_event(self, session: aiohttp.ClientSession, event_data: dict):
        """Send event to API"""
        try:
            async with session.post(f"{API_BASE}/events", json=event_data) as resp:
                if resp.status != 201:
                    print(f"Error sending event: {await resp.text()}")
        except Exception as e:
            print(f"Connection error: {e}")
    
    async def send_order(self, session: aiohttp.ClientSession, order_data: dict):
        """Send order to API"""
        try:
            async with session.post(f"{API_BASE}/orders", json=order_data) as resp:
                if resp.status != 201:
                    print(f"Error sending order: {await resp.text()}")
        except Exception as e:
            print(f"Connection error: {e}")
    
    async def run(self, events_per_second: float = 10.0, duration_minutes: int = 60):
        """Run simulation"""
        print(f"Starting simulation: {events_per_second} events/sec for {duration_minutes} minutes...")
        
        async with aiohttp.ClientSession() as session:
            start_time = datetime.now()
            end_time = start_time + timedelta(minutes=duration_minutes)
            
            while datetime.now() < end_time:
                # Create multiple concurrent sessions
                tasks = []
                for _ in range(int(events_per_second)):
                    tasks.append(asyncio.create_task(self.simulate_session(session)))
                
                await asyncio.gather(*tasks)
                await asyncio.sleep(1)  # Wait 1 second before next batch
                
                elapsed = (datetime.now() - start_time).seconds
                if elapsed % 60 == 0:
                    print(f"Running... {elapsed//60} minutes elapsed")

if __name__ == "__main__":
    simulator = EventSimulator()
    # Run at 50 events/second for 10 minutes
    asyncio.run(simulator.run(events_per_second=50, duration_minutes=10))