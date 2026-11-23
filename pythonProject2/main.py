# Producător 2
# Citește mesajul din Aplicatia_2.txt și îl trimite la fiecare 60 de secunde.

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
INTERVAL_TRIMITERE = 60

CALE_FISIER = Path(__file__).with_name("Aplicatia_2.txt")


def trimite_mesaj():
    """Citește fișierul și publică mesajul în coada RabbitMQ."""

    continut = CALE_FISIER.read_text(encoding="utf-8").strip()

    mesaj = {
        "id_mesaj": str(uuid.uuid4()),
        "timestamp_trimis": datetime.now(timezone.utc).isoformat(),
        "continut": continut,
        "expeditor": 2
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
    """Execută publicarea periodică a mesajelor."""

    logging.info("Producătorul 2 a pornit.")

    while True:
        try:
            trimite_mesaj()

        except FileNotFoundError:
            logging.exception("Fișierul Aplicatia_2.txt nu a fost găsit.")
            time.sleep(5)

        except (pika.exceptions.AMQPError, OSError):
            logging.exception("Eroare la conectarea sau publicarea în RabbitMQ.")
            time.sleep(5)

        except KeyboardInterrupt:
            logging.info("Producătorul 2 a fost oprit.")
            break

        else:
            time.sleep(INTERVAL_TRIMITERE)


if __name__ == "__main__":
    main()
