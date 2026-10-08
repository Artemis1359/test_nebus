import asyncio
import random

from app.enums.payment import PaymentStatus


async def process_payment() -> PaymentStatus:
    await asyncio.sleep(random.uniform(2, 5))
    return PaymentStatus.SUCCEEDED if random.random() < 0.9 else PaymentStatus.FAILED
