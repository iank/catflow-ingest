import aio_pika


class Producer:
    @classmethod
    async def create(cls, url, exchange_name, routing_keys):
        self = Producer()
        self.connection = await aio_pika.connect_robust(url)
        self.channel = await self.connection.channel()
        self.exchange = await self.channel.declare_exchange(exchange_name, "direct")

        for routing_key in routing_keys:
            queue = await self.channel.declare_queue(f"{routing_key}_queue")
            await queue.bind(self.exchange, routing_key=routing_key)

        return self

    async def send_to_rabbitmq(self, routing_key, message_str):
        message = aio_pika.Message(body=message_str.encode())
        return await self.exchange.publish(message, routing_key=routing_key)

    async def close(self):
        return await self.connection.close()
