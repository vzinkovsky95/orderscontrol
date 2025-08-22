import os
import requests
import mysql.connector
from pathlib import Path
import logging
import logging.handlers
from datetime import datetime
from urllib.parse import urlparse
import time
import threading
import hashlib
from dotenv import load_dotenv

load_dotenv()

LOG_FILE = os.getenv("LOG_FILE2")
log_level = logging.INFO
log_max_size = 10 * 1024 * 1024
log_backup_count = 5

logger = logging.getLogger(__name__)
logger.setLevel(log_level)

rotating_handler = logging.handlers.RotatingFileHandler(
    LOG_FILE,
    maxBytes=log_max_size,
    backupCount=log_backup_count
)

log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(threadName)s - %(message)s')
rotating_handler.setFormatter(log_formatter)

logger.addHandler(rotating_handler)

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_DATABASE"),
    "port": int(os.getenv("DB_PORT"))
}

BASE_DIR = Path(os.getenv("BASE_DIR"))

failed_downloads = set()
failed_downloads_lock = threading.Lock()

def calculate_md5(filepath):
    """Вычисляет MD5 хэш файла."""
    hash_md5 = hashlib.md5()
    try:
        with open(filepath, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()
    except FileNotFoundError:
        error_message = f"Файл не найден: {filepath}"
        logger.error(error_message)
        print(error_message)
        logger.error(error_message)
        return None
    except Exception as e:
        error_message = f"Ошибка при чтении файла {filepath}: {e}"
        print(error_message)
        logger.error(error_message)
        return None

def get_file_extension(url):
    """Определяет расширение файла из URL."""
    parsed_url = urlparse(url)
    path = parsed_url.path
    _, ext = os.path.splitext(path)
    return ext if ext else ".dat"


def download_kvit(transaction_id, partner_order_id, customer_code, document_url, cnx):
    user_folder = BASE_DIR / f"{customer_code}_receipts"
    user_folder.mkdir(exist_ok=True)

    file_extension = get_file_extension(document_url)
    filename = f"{partner_order_id}_receipt{file_extension}"
    filepath = user_folder / filename

    try:
        response = requests.get(document_url, timeout=30)
        response.raise_for_status()

        with open(filepath, 'wb') as f:
            f.write(response.content)

        added_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        local_link = f"{filepath.resolve()}"


        md5_hash = calculate_md5(filepath)
        if not md5_hash:
            error_message = f"Не удалось вычислить MD5 хэш для {filename}"
            logger.error(error_message)
            print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - {threading.current_thread().name} - {error_message}. Подробнее в логе.")
            md5_hash = None

        cursor_kv = cnx.cursor()
        add_query = """
        INSERT INTO orderdocstable (partner_order_id, transaction_id, customer_code, receipt_path, added_at, md5_hash)
        VALUES (%s, %s, %s, %s, %s, %s)
        """
        cursor_kv.execute(add_query, (partner_order_id, transaction_id, customer_code, local_link, added_at, md5_hash))
        cnx.commit()
        cursor_kv.close()

        success_message = f"Квитанция {filename} успешно скачана, добавлен MD5 и добавлена в БД."
        print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - {threading.current_thread().name} - {success_message}")

        with failed_downloads_lock:
            if partner_order_id in failed_downloads:
                failed_downloads.remove(partner_order_id)
                logger.info(f"partner_order_id={partner_order_id} успешно загружен.  Удаляем из списка ошибок.")

        return True

    except (requests.RequestException, Exception) as e:
        with failed_downloads_lock:
            if partner_order_id not in failed_downloads:
                error_message = f"Не удалось скачать квитанцию для partner_order_id={partner_order_id}: {e}"
                logger.error(error_message)
                print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - {threading.current_thread().name} - Ошибка скачивания квитанции для {partner_order_id}. Подробнее в логе.")
                failed_downloads.add(partner_order_id)
            else:
                debug_message = f"Повторная ошибка скачивания квитанции для {partner_order_id}, не логируем повторно."
                print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - {threading.current_thread().name} - {debug_message}")
                logger.debug(debug_message) # Логируем повторные ошибки на уровне DEBUG

        return False

def process_new_transactions():
    try:
        cnx = mysql.connector.connect(**DB_CONFIG)
        cursor = cnx.cursor(dictionary=True)

        query = """
        SELECT t.partner_order_id, t.transaction_id, t.customer_code, t.document_url
        FROM orderstable t
        LEFT JOIN orderdocstable k ON t.partner_order_id = k.partner_order_id
        WHERE k.partner_order_id IS NULL
        """

        cursor.execute(query)
        rows = cursor.fetchall()
        BASE_DIR.mkdir(exist_ok=True)

        for row in rows:
            download_kvit(row['partner_order_id'], row['transaction_id'], row['customer_code'], row['document_url'], cnx)

        cursor.close()
        cnx.close()
        print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - {threading.current_thread().name} - Проверка новых транзакций завершена.")
    except mysql.connector.Error as err:
        error_message = f"Ошибка работы с БД (новых транзакций): {err}"
        logger.error(error_message)
        print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - {threading.current_thread().name} - Ошибка подключения или запроса к БД (новых транзакций): {err}")


def process_failed_downloads():
    try:
        cnx = mysql.connector.connect(**DB_CONFIG)
        cursor = cnx.cursor(dictionary=True)

        with failed_downloads_lock:
            ids_to_retry = set(failed_downloads)

        query = """
            SELECT t.partner_order_id, t.transaction_id, t.customer_code, t.document_url
            FROM orderstable t
            WHERE t.partner_order_id IN ({})
            """.format(','.join(['%s'] * len(ids_to_retry))) if ids_to_retry else ""

        if query:
            cursor.execute(query, tuple(ids_to_retry))
            rows = cursor.fetchall()

            for row in rows:
                download_kvit(row['partner_order_id'], row['transaction_id'], row['customer_code'], row['document_url'], cnx)

        cursor.close()
        cnx.close()
        print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - {threading.current_thread().name} - Повторная проверка завершена.")
    except mysql.connector.Error as err:
        error_message = f"Ошибка работы с БД (повторные загрузки): {err}"
        logger.error(error_message)
        print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - {threading.current_thread().name} - Ошибка подключения или запроса к БД (повторные загрузки): {err}")


if __name__ == "__main__":

    while True:

        thread_new = threading.Thread(target=process_new_transactions, name="НовыеТранзакции")
        thread_failed = threading.Thread(target=process_failed_downloads, name="ПовторныеЗагрузки")

        thread_new.start()
        thread_failed.start()

        thread_new.join()
        thread_failed.join()

        print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - Главный поток - Ожидание перед следующей проверкой...")
        time.sleep(20)
