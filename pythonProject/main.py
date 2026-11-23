# Producător 1
# Citește mesajul din Aplicatia_1.txt și îl trimite la fiecare 120 de secunde.

import json
import logging
import os
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pika


# Configurarea jurnalizării
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s"
)

# Configurarea conexiunii RabbitMQ
GAZDA_RABBITMQ = os.getenv("RABBITMQ_HOST", "localhost")
NUME_COADA = "hello"
INTERVAL_TRIMITERE = 120

# Fișierul cu mesajul se află în același director cu acest script.
CALE_FISIER = Path(__file__).with_name("Aplicatia_1.txt")


def trimite_mesaj():
    """Citește fișierul și publică mesajul în coada RabbitMQ."""

    # Citirea conținutului în format UTF-8
    continut = CALE_FISIER.read_text(encoding="utf-8").strip()

    # Construirea mesajului cu identificator unic și marcaj temporal UTC
    mesaj = {
        "id_mesaj": str(uuid.uuid4()),
        "timestamp_trimis": datetime.now(timezone.utc).isoformat(),
        "continut": continut,
        "expeditor": 1
    }

    # Serializarea structurii în JSON
    corp_mesaj = json.dumps(mesaj, ensure_ascii=False).encode("utf-8")

    # Deschiderea conexiunii către RabbitMQ
    parametri = pika.ConnectionParameters(
        host=GAZDA_RABBITMQ,
        heartbeat=600,
        blocked_connection_timeout=300
    )

    conexiune = pika.BlockingConnection(parametri)

    try:
        canal = conexiune.channel()

        # Coada persistentă: nu se șterge la închiderea brokerului
        canal.queue_declare(queue=NUME_COADA, durable=True)

        # Confirmarea publicării de către broker
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
        # Închiderea conexiunii chiar dacă apare o eroare
        if conexiune.is_open:
            conexiune.close()


def main():
    """Execută publicarea periodică a mesajelor."""

    logging.info("Producătorul 1 a pornit.")

    while True:
        try:
            trimite_mesaj()

        except FileNotFoundError:
            logging.exception("Fișierul Aplicatia_1.txt nu a fost găsit.")
            time.sleep(5)

        except (pika.exceptions.AMQPError, OSError):
            logging.exception("Eroare la conectarea sau publicarea în RabbitMQ.")
            time.sleep(5)

        except KeyboardInterrupt:
            logging.info("Producătorul 1 a fost oprit.")
            break

        else:
            # Așteaptă 120 de secunde după o trimitere reușită.
            time.sleep(INTERVAL_TRIMITERE)


if __name__ == "__main__":
    main()
