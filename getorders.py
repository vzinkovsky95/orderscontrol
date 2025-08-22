import requests
import time
import json
import logging
import threading
import mysql.connector
import os
import sys
from dotenv import load_dotenv

load_dotenv()

LOG_FILE = os.getenv("LOG_FILE1")
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s:%(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)

TOKEN_FILE = os.getenv("TOKEN_FILE")
LOGIN_URL = os.getenv("LOGIN_URL")
API_URL = os.getenv("API_URL")

LOGIN_DATA = {
    "user_login": os.getenv("USER_LOGIN"),
    "user_password": os.getenv("USER_PASSWORD")
}

REQUEST_BODIES = [
    {"filter": {}, "sort": {}, "limit": {"last_id": 0, "max_results": 512, "descending": True}},
    {"filter": {"payment_method_id": ""}, "sort": {"is_valid": True}, "limit": {"last_id": 0, "max_results": 512, "descending": True}},
    {"filter": {"payment_method_id": ""}, "sort": {"is_valid": True}, "limit": {"last_id": 0, "max_results": 512, "descending": True}},
    {"filter": {"payment_method_id": ""}, "sort": {"is_valid": True}, "limit": {"last_id": 0, "max_results": 512, "descending": True}}
]

def save_token(token):
    try:
        with open(TOKEN_FILE, "w") as f:
            json.dump({"token": token}, f)
        logging.info("Токен сохранен в файл.")
    except Exception as e:
        logging.error(f"Ошибка сохранения токена: {e}")

def load_token():
    if os.path.exists(TOKEN_FILE):
        try:
            with open(TOKEN_FILE, "r") as f:
                data = json.load(f)
                token = data.get("token")
                if token:
                    logging.info("Токен загружен из файла.")
                    return token
        except Exception as e:
            logging.error(f"Ошибка загрузки токена из файла: {e}")
    return None

def get_new_token():
    logging.info("Запрос нового токена...")
    try:
        response = requests.post(LOGIN_URL, json=LOGIN_DATA)
        response.raise_for_status()
        resp_json = response.json()
        token = resp_json.get("access_token")
        if token:
            save_token(token)
            return token
        else:
            logging.error("Токен не найден в ответе сервера.")
    except requests.RequestException as e:
        logging.error(f"Ошибка при получении токена: {e}")
    except Exception as e:
        logging.error(f"Неожиданная ошибка при получении токена: {e}")
    return None

def get_data(output_file="data.json"):
    headers = {
        "Content-Type": "application/json;charset=UTF-8",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36"
    }

    body_index = 0

    while True:
        try:
            token = load_token()
            if not token:
                token = get_new_token()
                if not token:
                    logging.error("Не удалось получить токен, пропускаем итерацию.")
                    time.sleep(60)
                    continue

            headers["Authorization"] = f"Bearer {token}"
            payload = REQUEST_BODIES[body_index]

            response = requests.post(API_URL, headers=headers, json=payload)

            if response.status_code == 401:
                logging.warning("Токен недействителен, получаем новый...")
                token = get_new_token()
                if not token:
                    logging.error("Не удалось обновить токен, пропускаем итерацию.")
                    time.sleep(60)
                    continue
                headers["Authorization"] = f"Bearer {token}"
                response = requests.post(API_URL, headers=headers, json=payload)

            response.raise_for_status()

            data = response.json()

            with open(output_file, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)

            logging.info(f"Данные успешно обновлены. Получено объектов: {len(data)}")
            body_index = (body_index + 1) % len(REQUEST_BODIES)

        except requests.RequestException as e:
            logging.error(f"Ошибка запроса: {e}")
        except json.JSONDecodeError as e:
            logging.error(f"Ошибка декодирования JSON, пробуем следующее тело запроса.")
            body_index = (body_index + 1) % len(REQUEST_BODIES)
        except Exception as e:
            logging.error(f"Неожиданная ошибка: {e}")

        time.sleep(20)

def parse_and_insert(output_file="data.json", interval=20):
    db_config = {
        "host": os.getenv("DB_HOST"),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "database": os.getenv("DB_DATABASE"),
        "port": int(os.getenv("DB_PORT")),
    }

    if not all(db_config.values()):
        logging.error("Не все параметры базы данных заданы в переменных окружения.")
        return

    def parse_datetime(dt_str):
        if not dt_str:
            return None
        return dt_str[:19].replace("T", " ")

    while True:
        try:
            with open(output_file, "r", encoding="utf-8") as f:
                data = json.load(f)
        except FileNotFoundError:
            logging.warning(f"Файл {output_file} не найден. Ожидаю его появления...")
            time.sleep(interval)
            continue
        except json.JSONDecodeError:
            logging.warning(f"Файл {output_file} содержит некорректный JSON. Жду следующего обновления...")
            time.sleep(interval)
            continue
        except Exception as e:
            logging.error(f"Ошибка при чтении {output_file}: {e}")
            time.sleep(interval)
            continue

        try:
            with mysql.connector.connect(**db_config) as conn:
                with conn.cursor() as cursor:
                    added = 0
                    updated = 0

                    for record in data:
                        partner_order_id = record.get("order_id")
                        if partner_order_id is None:
                            continue

                        transaction_id = record.get("internal_id")
                        status = record.get("order_status")

                        partner = record.get("partner", {})
                        partner_id = partner.get("internal_id")
                        aboutpartner_info1 = partner.get("aboutpartner_info1")
                        aboutpartner_info2 = partner.get("aboutpartner_info2")
                        aboutpartner_info3 = partner.get("aboutpartner_info3")

                        payment_details = record.get("payment_details", {})
                        payment_method_id = payment_details.get("internal_id")
                        aboutpayment_info1 = payment_details.get("aboutpayment_info1")
                        aboutpayment_info2 = payment_details.get("aboutpayment_info2")

                        customer_code = record.get("customer_code")
                        aboutorder_info1 = record.get("aboutorder_info1")
                        aboutorder_info2 = record.get("aboutorder_info2")
                        aboutorder_info3 = record.get("aboutorder_info3")
                        aboutorder_info4 = record.get("aboutorder_info4")
                        aboutorder_info5 = record.get("aboutorder_info5")
                        document_url = record.get("additional_info", {}).get("document_url")
                        aboutorder_info6 = record.get("additional_info", {}).get("aboutorder_info6")
                        aboutorder_info7 = record.get("additional_info", {}).get("aboutorder_info7")

                        created_at = parse_datetime(record.get("created_at"))
                        updated_at = parse_datetime(record.get("updated_at"))
                        payment_amount_str = record.get("payment_amount")
                        payment_amount = float(payment_amount_str) if payment_amount_str else None

                        responsible_user_username = None
                        if "responsible_user" in record and isinstance(record["responsible_user"], dict) and "username" in record["responsible_user"]:
                            responsible_user_username = record["responsible_user"]["username"]

                        cursor.execute("SELECT COUNT(*) FROM orderstable WHERE partner_order_id = %s", (partner_order_id,))
                        exists = cursor.fetchone()[0] > 0

                        if exists:
                            update_query = """
                            UPDATE orderstable SET
                                transaction_id = %s,
                                status = %s,
                                customer_code = %s,
                                aboutorder_info1 = %s,  
                                aboutorder_info2 = %s,
                                aboutorder_info3 = %s,
                                aboutorder_info4 = %s,
                                aboutorder_info5 = %s,
                                document_url = %s,
                                aboutorder_info6 = %s,
                                aboutorder_info7 = %s,
                                partner_id = %s,
                                aboutpartner_info1 = %s,
                                aboutpartner_info2 = %s,
                                aboutpartner_info3 = %s,
                                payment_method_id = %s,
                                aboutpayment_info1 = %s,
                                aboutpayment_info2 = %s,
                                created_at = %s,
                                updated_at = %s,
                                payment_amount = %s,
                                responsible_user_username = %s
                            WHERE partner_order_id = %s
                            """
                            params = (
                                transaction_id,
                                status,
                                customer_code,
                                aboutorder_info1,
                                aboutorder_info2,
                                aboutorder_info3,
                                aboutorder_info4,
                                aboutorder_info5,
                                document_url,
                                aboutorder_info6,
                                aboutorder_info7,
                                partner_id,
                                aboutpartner_info1,
                                aboutpartner_info2,
                                aboutpartner_info3,
                                payment_method_id,
                                aboutpayment_info1,
                                aboutpayment_info2,
                                created_at,
                                updated_at,
                                payment_amount,
                                responsible_user_username,
                                partner_order_id
                            )
                            cursor.execute(update_query, params)
                            updated += 1
                        else:
                            insert_query = """
                            INSERT INTO orderstable (
                                partner_order_id,
                                transaction_id,
                                status,
                                customer_code,
                                aboutorder_info1,
                                aboutorder_info2,
                                aboutorder_info3,
                                aboutorder_info4,
                                aboutorder_info5,
                                document_url,
                                aboutorder_info6,
                                aboutorder_info7,
                                partner_id,
                                aboutpartner_info1,
                                aboutpartner_info2,
                                aboutpartner_info3,
                                payment_method_id,
                                aboutpayment_info1,
                                aboutpayment_info2,
                                created_at,
                                updated_at,
                                payment_amount,
                                responsible_user_username
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            """

                            params = (
                                partner_order_id,
                                transaction_id,
                                status,
                                customer_code,
                                aboutorder_info1,
                                aboutorder_info2,
                                aboutorder_info3,
                                aboutorder_info4,
                                aboutorder_info5,
                                document_url,
                                aboutorder_info6,
                                aboutorder_info7,
                                partner_id,
                                aboutpartner_info1,
                                aboutpartner_info2,
                                aboutpartner_info3,
                                payment_method_id,
                                aboutpayment_info1,
                                aboutpayment_info2,
                                created_at,
                                updated_at,
                                payment_amount,
                                responsible_user_username
                            )

                            cursor.execute(insert_query, params)
                            added += 1

                    conn.commit()
                    logging.info(f"Добавлено записей: {added}, Обновлено записей: {updated}")

        except mysql.connector.Error as err:
            logging.error(f"Ошибка при работе с базой данных: {err}")
        except Exception as e:
            logging.exception(f"Ошибка в parse_and_insert: {e}")

        time.sleep(interval)

if __name__ == "__main__":

    output_file = "data.json"

    thread_get_data = threading.Thread(target=get_data, args=(output_file,), daemon=True)
    thread_parser = threading.Thread(target=parse_and_insert, args=(output_file, 20), daemon=True)

    thread_get_data.start()
    thread_parser.start()

    logging.info("Скрипт запущен. Оба потока работают.")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logging.info("Завершение работы по сигналу пользователя.")