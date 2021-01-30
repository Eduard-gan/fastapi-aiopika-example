import uvicorn
from fastapi import FastAPI, responses
from aio_pika import connect_robust, Message


RABBIT_USER = ""
RABBIT_PASSWORD = ""
RABBIT_IP = "127.0.0.1"
RABBIT_QUEUE = "echo"
RABBIT_CHANNEL = None


app = FastAPI()


async def consumer(message):
    print(f"GOT MESSAGE: {message.body.decode()}")
    message.ack()


@app.on_event("startup")
async def startup_event():
    global RABBIT_CHANNEL
    connection = await connect_robust(f"amqp://{RABBIT_USER}:{RABBIT_PASSWORD}@{RABBIT_IP}:5672/")
    RABBIT_CHANNEL = await connection.channel()
    queue = await RABBIT_CHANNEL.declare_queue(RABBIT_QUEUE, durable=True)

    await queue.consume(consumer)


@app.get("/echo")
async def echo(body: str):
    await RABBIT_CHANNEL.default_exchange.publish(Message(body.encode()), routing_key=RABBIT_QUEUE)
    return responses.PlainTextResponse(f"Message with body {body} placed in queue {RABBIT_QUEUE}")


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000, log_level="debug")


""" TEST WITH:

http://127.0.0.1:8000/echo?body=hello%20world

"""