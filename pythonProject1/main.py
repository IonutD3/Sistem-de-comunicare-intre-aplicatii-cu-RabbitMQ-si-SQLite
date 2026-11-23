# Consumator - Aplicația 4
# Primește mesajele din RabbitMQ și le salvează în SQLite.

import json
import logging
import os
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

import pika


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s"
)

GAZDA_RABBITMQ = os.getenv("RABBITMQ_HOST", "localhost")
NUME_COADA = "hello"

# Baza de date este creată automat în directorul aplicației consumator.
CALE_BAZA_DATE = Path(__file__).with_name("mesaje.db")


def initializare_baza_date():
    """Creează tabelul în care vor fi păstrate mesajele primite."""

    with sqlite3.connect(CALE_BAZA_DATE) as conexiune:
        cursor = conexiune.cursor()

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS mesaje (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                id_mesaj TEXT NOT NULL UNIQUE,
                expeditor INTEGER NOT NULL,
                continut TEXT NOT NULL,
                timestamp_trimis TEXT NOT NULL,
                timestamp_primit TEXT NOT NULL
            )
        """)

        # Index pentru căutarea rapidă după expeditor.
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_mesaje_expeditor
            ON mesaje(expeditor)
        """)

        conexiune.commit()

    logging.info("Baza de date a fost inițializată: %s", CALE_BAZA_DATE)


def salveaza_mesaj(mesaj):
    """Salvează mesajul primit în baza de date SQLite."""

    timestamp_primit = datetime.now(timezone.utc).isoformat()

    with sqlite3.connect(CALE_BAZA_DATE, timeout=10) as conexiune:
        cursor = conexiune.cursor()

        cursor.execute("""
            INSERT OR IGNORE INTO mesaje (
                id_mesaj,
                expeditor,
                continut,
                timestamp_trimis,
                timestamp_primit
            )
            VALUES (?, ?, ?, ?, ?)
        """, (
            mesaj["id_mesaj"],
            mesaj["expeditor"],
            mesaj["continut"],
            mesaj["timestamp_trimis"],
            timestamp_primit
        ))

        conexiune.commit()

        return cursor.rowcount == 1


def proceseaza_mesaj(canal, metoda, proprietati, corp):
    """Procesează mesajul și confirmă recepția după salvare."""

    try:
        # Decodificarea corpului JSON primit de la RabbitMQ.
        mesaj = json.loads(corp.decode("utf-8"))

        # Verificarea câmpurilor obligatorii.
        campuri_obligatorii = {
            "id_mesaj",
            "expeditor",
            "continut",
            "timestamp_trimis"
        }

        if not campuri_obligatorii.issubset(mesaj):
            raise ValueError("Mesajul nu conține toate câmpurile obligatorii.")

        # Salvarea în SQLite.
        salvat = salveaza_mesaj(mesaj)

        if salvat:
            logging.info(
                "Mesaj salvat: ID=%s, expeditor=%s",
                mesaj["id_mesaj"],
                mesaj["expeditor"]
            )
        else:
            logging.info(
                "Mesaj duplicat ignorat: ID=%s",
                mesaj["id_mesaj"]
            )

        # Confirmarea se face numai după salvarea cu succes.
        canal.basic_ack(delivery_tag=metoda.delivery_tag)

    except (ValueError, UnicodeDecodeError, json.JSONDecodeError):
        logging.exception("Mesaj invalid. Mesajul va fi eliminat din coadă.")

        # Mesajul invalid nu va fi retrimis la nesfârșit.
        canal.basic_nack(
            delivery_tag=metoda.delivery_tag,
            requeue=False
        )

    except sqlite3.Error:
        logging.exception("Eroare SQLite. Mesajul va fi retrimis.")

        # Dacă baza de date nu poate salva mesajul, RabbitMQ îl păstrează.
        canal.basic_nack(
            delivery_tag=metoda.delivery_tag,
            requeue=True
        )


def main():
    """Inițializează baza de date și pornește consumatorul RabbitMQ."""

    initializare_baza_date()

    parametri = pika.ConnectionParameters(
        host=GAZDA_RABBITMQ,
        heartbeat=600,
        blocked_connection_timeout=300
    )

    conexiune = pika.BlockingConnection(parametri)

    try:
        canal = conexiune.channel()

        # Trebuie să aibă aceleași proprietăți ca la producători.
        canal.queue_declare(queue=NUME_COADA, durable=True)

        # Consumatorul primește câte un mesaj înainte de confirmare.
        canal.basic_qos(prefetch_count=1)

        canal.basic_consume(
            queue=NUME_COADA,
            on_message_callback=proceseaza_mesaj,
            auto_ack=False
        )

        logging.info("Consumatorul a pornit. Se așteaptă mesaje...")

        canal.start_consuming()

    finally:
        if conexiune.is_open:
            conexiune.close()


if __name__ == "__main__":
    try:
        main()

    except KeyboardInterrupt:
        logging.info("Consumatorul a fost oprit de utilizator.")
        sys.exit(0)

    except pika.exceptions.AMQPError:
        logging.exception("Eroare de comunicare cu RabbitMQ.")
        sys.exit(1)
