# Producător 3
# Trimite un singur mesaj către coada RabbitMQ la pornirea aplicației.

import json
import logging
import os
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pika


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s"
)

GAZDA_RABBITMQ = os.getenv("RABBITMQ_HOST", "localhost")
NUME_COADA = "hello"

CALE_FISIER = Path(__file__).with_name("Aplicatia_3.txt")


def trimite_mesaj():
    """Citește fișierul și publică mesajul în coada RabbitMQ."""

    continut = CALE_FISIER.read_text(encoding="utf-8").strip()

    mesaj = {
        "id_mesaj": str(uuid.uuid4()),
        "timestamp_trimis": datetime.now(timezone.utc).isoformat(),
        "continut": continut,
        "expeditor": 3
    }

    corp_mesaj = json.dumps(mesaj, ensure_ascii=False).encode("utf-8")

    parametri = pika.ConnectionParameters(
        host=GAZDA_RABBITMQ,
        heartbeat=600,
        blocked_connection_timeout=300
    )

    conexiune = pika.BlockingConnection(parametri)

    try:
        canal = conexiune.channel()
        canal.queue_declare(queue=NUME_COADA, durable=True)
        canal.confirm_delivery()

        canal.basic_publish(
            exchange="",
            routing_key=NUME_COADA,
            body=corp_mesaj,
            properties=pika.BasicProperties(
                content_type="application/json",
                content_encoding="utf-8",
                delivery_mode=2
            ),
            mandatory=True
        )

        logging.info(
            "Mesaj trimis. ID=%s, expeditor=%s",
            mesaj["id_mesaj"],
            mesaj["expeditor"]
        )

    finally:
        if conexiune.is_open:
            conexiune.close()


def main():
    """Încearcă publicarea mesajului până când conexiunea reușește."""

    logging.info("Producătorul 3 a pornit.")

    while True:
        try:
            trimite_mesaj()
            break

        except (pika.exceptions.AMQPError, OSError):
            logging.exception(
                "RabbitMQ nu este disponibil. Reîncercare în 5 secunde."
            )
            time.sleep(5)

        except KeyboardInterrupt:
            logging.info("Producătorul 3 a fost oprit.")
            break


if __name__ == "__main__":
    main()
