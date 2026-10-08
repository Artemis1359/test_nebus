import logging

from faststream import AckPolicy
from faststream.rabbit import Channel, RabbitBroker, RabbitExchange, RabbitQueue

from app.core.config import get_settings

exchange = RabbitExchange("payments", durable=True)
dead_exchange = RabbitExchange("payments.dlx", durable=True)
queue = RabbitQueue(
    "payments.new",
    durable=True,
    arguments={
        "x-dead-letter-exchange": "payments.dlx",
        "x-dead-letter-routing-key": "payments.dlq",
        "x-single-active-consumer": True,
    },
)
dead_queue = RabbitQueue("payments.dlq", durable=True)
broker = RabbitBroker(
    get_settings().rabbitmq_url,
    logger=logging.getLogger("app.messaging"),
    ack_policy=AckPolicy.NACK_ON_ERROR,
    graceful_timeout=20,  # Оставляем время на шлюз и webhook до SIGKILL от Docker через 30 секунд.
    default_channel=Channel(publisher_confirms=True, on_return_raises=True, prefetch_count=1),
)


async def declare_topology() -> None:
    main = await broker.declare_exchange(exchange)
    dead = await broker.declare_exchange(dead_exchange)
    main_queue = await broker.declare_queue(queue)
    dlq = await broker.declare_queue(dead_queue)
    await main_queue.bind(main, routing_key=queue.name)
    await dlq.bind(dead, routing_key=dead_queue.name)
