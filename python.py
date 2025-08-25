import telegram
from telegram.ext import (
    Updater, CommandHandler, MessageHandler,
    ConversationHandler, Filters, CallbackQueryHandler
)
import requests
import json
import datetime
import pytz
from apscheduler.schedulers.background import BackgroundScheduler
import logging
import sqlite3
import os
from dotenv import load_dotenv

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

load_dotenv()
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")

TIMEZONE = 'UTC'
DATABASE_FILE = 'scheduled_tasks.db'

def init_db():
    conn = sqlite3.connect(DATABASE_FILE)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS tasks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            api_url TEXT NOT NULL,
            http_method TEXT NOT NULL,
            headers_json TEXT,
            body_json TEXT,
            scheduled_time TEXT NOT NULL,
            description TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    conn.commit()
    conn.close()
    logger.info("Database initialized.")

def send_api_request(task_id, api_url, http_method, headers_json, body_json):
    try:
        headers = json.loads(headers_json) if headers_json else {}
        body = json.loads(body_json) if body_json else None

        logger.info(f"Executing task {task_id}: {http_method} {api_url}")

        if body and 'Content-Type' not in headers:
            headers['Content-Type'] = 'application/json'

        if http_method.upper() == 'POST':
            response = requests.post(api_url, headers=headers, json=body, timeout=20)
        elif http_method.upper() == 'GET':
            response = requests.get(api_url, headers=headers, params=body, timeout=20)
        elif http_method.upper() == 'PUT':
            response = requests.put(api_url, headers=headers, json=body, timeout=20)
        elif http_method.upper() == 'DELETE':
            response = requests.delete(api_url, headers=headers, json=body, timeout=20)
        else:
            logger.warning(f"Unsupported HTTP method: {http_method}")
            return None, f"Unsupported HTTP method: {http_method}"

        response.raise_for_status()
        logger.info(f"Task {task_id} successful. Status Code: {response.status_code}")

        response_text = response.text
        if len(response_text) > 500:
            response_text = response_text[:500] + "... (truncated)"

        return response.status_code, response_text

    except requests.exceptions.RequestException as e:
        logger.error(f"Error sending API request for task {task_id}: {e}")
        return None, str(e)
    except json.JSONDecodeError as e:
        logger.error(f"Error decoding JSON for task {task_id}: {e}")
        return None, f"Invalid JSON format: {e}"
    except Exception as e:
        logger.error(f"An unexpected error occurred during API request for task {task_id}: {e}")
        return None, f"An unexpected error: {e}"

def schedule_task(task_id, scheduled_time_str, user_id):
    try:
        tz_scheduler = pytz.timezone(TIMEZONE)
        scheduled_time = datetime.datetime.strptime(scheduled_time_str, '%Y-%m-%d %H:%M:%S')
        scheduled_time = tz_scheduler.localize(scheduled_time)

        if scheduled_time < datetime.datetime.now(tz_scheduler):
            logger.warning(f"Task {task_id} scheduled time is in the past. Will not re-schedule.")
            return False

        scheduler.add_job(
            func=run_scheduled_task,
            trigger='date',
            run_date=scheduled_time,
            args=[task_id, scheduled_time_str, user_id],
            id=f"task_{task_id}"
        )
        logger.info(f"Task {task_id} scheduled for {scheduled_time.strftime('%Y-%m-%d %H:%M:%S %Z')}")
        return True
    except Exception as e:
        logger.error(f"Error scheduling task {task_id}: {e}")
        return False

def run_scheduled_task(task_id, scheduled_time_str, user_id):
    conn = sqlite3.connect(DATABASE_FILE)
    cursor = conn.cursor()
    cursor.execute("SELECT api_url, http_method, headers_json, body_json FROM tasks WHERE id = ?", (task_id,))
    task_data = cursor.fetchone()
    conn.close()

    if task_data:
        api_url, http_method, headers_json, body_json = task_data
        status_code, result_text = send_api_request(task_id, api_url, http_method, headers_json, body_json)

        message_text = f"--- Task {task_id} ({scheduled_time_str}) ---\n"
        if status_code is not None:
            message_text += f"Status: {status_code}\nResult: {result_text}"
        else:
            message_text += f"Status: Failed\nError: {result_text}"

        try:
            bot_instance.send_message(chat_id=user_id, text=message_text)
            conn = sqlite3.connect(DATABASE_FILE)
            cursor = conn.cursor()
            cursor.execute("DELETE FROM tasks WHERE id = ?", (task_id,))
            conn.commit()
            conn.close()
            logger.info(f"Task {task_id} executed and removed from DB.")
        except Exception as e:
            logger.error(f"Failed to send result message for task {task_id} or remove from DB: {e}")
    else:
        logger.warning(f"Task {task_id} not found in database when scheduled to run.")

def start(update, context):
    user_id = update.message.from_user.id
    update.message.reply_text(
        'Welcome! I am your API request scheduler bot.\n'
        'Use /new_request to schedule a new API POST request.\n'
        'Use /list to see your scheduled requests.\n'
        'Use /delete <ID> to cancel a scheduled request.\n'
        'Use /help for more details.'
    )

def help_command(update, context):
    help_text = (
        "Here are the available commands:\n\n"
        "/new_request - Start the conversation to schedule a new API request.\n"
        "/list - View all your currently scheduled requests.\n"
        "/delete <ID> - Cancel a scheduled request using its ID (e.g., /delete 12).\n"
        "/cancel - Cancel the current scheduling process if you change your mind.\n\n"
        "When scheduling:\n"
        "- You will be asked for the API URL.\n"
        "- The HTTP Method (POST is the default, but you can specify GET, PUT, DELETE).\n"
        "- The Headers in JSON format (e.g., `{\"Authorization\": \"Bearer YOUR_KEY\", \"Content-Type\": \"application/json\"}`).\n"
        "- The Body in JSON format (e.g., `{\"product\": \"XYZ\", \"quantity\": 2}`).\n"
        "- The Date and Time for execution (YYYY-MM-DD HH:MM:SS).\n"
        "- An optional Description for the task."
    )
    update.message.reply_text(help_text)

def new_request(update, context):
    update.message.reply_text('Please enter the API endpoint URL:')
    return 'GET_URL'

def get_url(update, context):
    context.user_data['api_url'] = update.message.text
    update.message.reply_text('Enter the HTTP Method (POST, GET, PUT, DELETE, or leave blank for POST):')
    return 'GET_METHOD'

def get_method(update, context):
    method = update.message.text.strip().upper()
    if not method:
        method = 'POST'
    elif method not in ['POST', 'GET', 'PUT', 'DELETE']:
        update.message.reply_text('Invalid method. Please enter POST, GET, PUT, or DELETE. Or leave blank for POST.')
        return 'GET_METHOD'
    context.user_data['http_method'] = method
    update.message.reply_text('Enter the Headers in JSON format (e.g., `{\"Authorization\": \"Bearer YOUR_KEY\", \"Content-Type\": \"application/json\"}`). Press Enter if no headers:')
    return 'GET_HEADERS'

def get_headers(update, context):
    headers_text = update.message.text.strip()
    if headers_text:
        try:
            json.loads(headers_text)
            context.user_data['headers_json'] = headers_text
        except json.JSONDecodeError:
            update.message.reply_text('Invalid JSON format for headers. Please try again or press Enter if no headers.')
            return 'GET_HEADERS'
    else:
        context.user_data['headers_json'] = None

    update.message.reply_text('Enter the Body in JSON format (e.g., `{\"product\": \"XYZ\", \"quantity\": 2}`). Press Enter if no body:')
    return 'GET_BODY'

def get_body(update, context):
    body_text = update.message.text.strip()
    if body_text:
        try:
            json.loads(body_text)
            context.user_data['body_json'] = body_text
        except json.JSONDecodeError:
            update.message.reply_text('Invalid JSON format for body. Please try again or press Enter if no body:')
            return 'GET_BODY'
    else:
        context.user_data['body_json'] = None

    update.message.reply_text('Enter the Date and Time for execution in YYYY-MM-DD HH:MM:SS format (using your local time):')
    return 'GET_DATETIME'

def get_datetime(update, context):
    datetime_str = update.message.text.strip()
    try:
        tz_user = pytz.timezone(TIMEZONE)
        scheduled_time = datetime.datetime.strptime(datetime_str, '%Y-%m-%d %H:%M:%S')

        now = datetime.datetime.now(tz_user)
        scheduled_time_localized = tz_user.localize(scheduled_time)

        if scheduled_time_localized <= now:
            update.message.reply_text('The date and time must be in the future. Please enter a valid future date and time (YYYY-MM-DD HH:MM:SS).')
            return 'GET_DATETIME'

        context.user_data['scheduled_time'] = datetime_str
        update.message.reply_text('Enter an optional description for this task (or press Enter to skip):')
        return 'GET_DESCRIPTION'
    except ValueError:
        update.message.reply_text('Invalid date/time format. Please use YYYY-MM-DD HH:MM:SS.')
        return 'GET_DATETIME'

def get_description(update, context):
    context.user_data['description'] = update.message.text.strip()
    user_id = update.message.from_user.id
    data = context.user_data

    conn = sqlite3.connect(DATABASE_FILE)
    cursor = conn.cursor()
    try:
        cursor.execute(
            "INSERT INTO tasks (user_id, api_url, http_method, headers_json, body_json, scheduled_time, description) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (user_id, data['api_url'], data['http_method'], data.get('headers_json'), data.get('body_json'), data['scheduled_time'], data.get('description'))
        )
        task_id = cursor.lastrowid
        conn.commit()
        logger.info(f"Task saved to DB with ID: {task_id}")

        if schedule_task(task_id, data['scheduled_time'], user_id):
            update.message.reply_text(
                f'✅ API request scheduled successfully!\n\n'
                f'ID: {task_id}\n'
                f'URL: {data["api_url"]}\n'
                f'Method: {data["http_method"]}\n'
                f'Scheduled for: {data["scheduled_time"]} ({TIMEZONE})\n'
                f'Description: {data.get("description", "N/A")}'
            )
        else:
            update.message.reply_text(
                f'❌ API request saved, but failed to schedule. Please check bot logs or try again.\n'
                f'ID: {task_id}\n'
                f'URL: {data["api_url"]}\n'
                f'Method: {data["http_method"]}\n'
                f'Scheduled for: {data["scheduled_time"]} ({TIMEZONE})\n'
                f'Description: {data.get("description", "N/A")}'
            )
            cursor.execute("DELETE FROM tasks WHERE id = ?", (task_id,))
            conn.commit()

    except Exception as e:
        logger.error(f"Failed to save or schedule task: {e}")
        update.message.reply_text('An error occurred while saving or scheduling the task. Please try again.')
    finally:
        conn.close()

    context.user_data.clear()
    return ConversationHandler.END

def list_tasks(update, context):
    user_id = update.message.from_user.id
    conn = sqlite3.connect(DATABASE_FILE)
    cursor = conn.cursor()
    cursor.execute("SELECT id, api_url, http_method, headers_json, body_json, scheduled_time, description FROM tasks WHERE user_id = ? ORDER BY scheduled_time ASC", (user_id,))
    tasks = cursor.fetchall()
    conn.close()

    if not tasks:
        update.message.reply_text('You have no scheduled tasks at the moment.')
        return

    message = 'Your scheduled tasks:\n\n'
    tz_display = pytz.timezone(TIMEZONE)
    for task in tasks:
        task_id, url, method, headers_json, body_json, schedule_time_str, description = task

        try:
            scheduled_dt = datetime.datetime.strptime(schedule_time_str, '%Y-%m-%d %H:%M:%S')
            scheduled_dt_localized = tz_display.localize(scheduled_dt)
            readable_time = scheduled_dt_localized.strftime('%Y-%m-%d %H:%M:%S %Z')
        except:
            readable_time = schedule_time_str

        short_headers = (headers_json[:50] + '...') if headers_json and len(headers_json) > 50 else headers_json
        short_body = (body_json[:50] + '...') if body_json and len(body_json) > 50 else body_json

        message += (f"ID: {task_id}\n"
                    f"URL: {url}\n"
                    f"Method: {method}\n"
                    f"Scheduled: {readable_time}\n"
                    f"Headers: {short_headers or 'N/A'}\n"
                    f"Body: {short_body or 'N/A'}\n"
                    f"Description: {description or 'N/A'}\n"
                    f"--------------------\n")
    update.message.reply_text(message)

def delete_task(update, context):
    user_id = update.message.from_user.id
    try:
        task_id_to_cancel = int(context.args[0])

        conn = sqlite3.connect(DATABASE_FILE)
        cursor = conn.cursor()

        cursor.execute("SELECT id FROM tasks WHERE id = ? AND user_id = ?", (task_id_to_cancel, user_id))
        task_exists_for_user = cursor.fetchone()

        if not task_exists_for_user:
            update.message.reply_text(f'Task with ID {task_id_to_cancel} not found or does not belong to you.')
            conn.close()
            return

        cursor.execute("DELETE FROM tasks WHERE id = ?", (task_id_to_cancel,))
        conn.commit()

        try:
            scheduler.remove_job(job_id=f"task_{task_id_to_cancel}")
            update.message.reply_text(f'✅ Task {task_id_to_cancel} cancelled successfully.')
            logger.info(f"Task {task_id_to_cancel} removed from DB and scheduler.")
        except Exception as e:
            update.message.reply_text(f'Task {task_id_to_cancel} removed from database, but could not be removed from scheduler (it might have already run or there was an error).')
            logger.error(f"Error removing job {task_id_to_cancel} from scheduler: {e}")

    except (IndexError, ValueError):
        update.message.reply_text('Please use the format: /delete <task_id>')
    except Exception as e:
        logger.error(f"Error in delete_task command: {e}")
        update.message.reply_text('An error occurred while trying to delete the task.')
    finally:
        conn.close()

def cancel_conversation(update, context):
    context.user_data.clear()
    update.message.reply_text('Operation cancelled. You can start again with /new_request.')
    return ConversationHandler.END

def setup_bot():
    global updater, dp, scheduler, bot_instance

    if not TELEGRAM_BOT_TOKEN:
        logger.error("TELEGRAM_BOT_TOKEN not found. Please set it in your .env file.")
        exit()

    updater = Updater(TELEGRAM_BOT_TOKEN, use_context=True)
    dp = updater.dispatcher
    bot_instance = telegram.Bot(token=TELEGRAM_BOT_TOKEN)

    scheduler = BackgroundScheduler(timezone=pytz.timezone(TIMEZONE))

    init_db()
    conn = sqlite3.connect(DATABASE_FILE)
    cursor = conn.cursor()
    cursor.execute("SELECT id, scheduled_time, user_id FROM tasks")
    old_tasks = cursor.fetchall()
    conn.close()

    tz_scheduler = pytz.timezone(TIMEZONE)
    now = datetime.datetime.now(tz_scheduler)

    for task_id, schedule_time_str, user_id in old_tasks:
        try:
            scheduled_time = datetime.datetime.strptime(schedule_time_str, '%Y-%m-%d %H:%M:%S')
            scheduled_time_localized = tz_scheduler.localize(scheduled_time)
            if scheduled_time_localized > now:
                schedule_task(task_id, schedule_time_str, user_id)
        except Exception as e:
            logger.error(f"Failed to re-schedule old task {task_id}: {e}")

    scheduler.start()
    logger.info("Scheduler started and loaded existing tasks.")

    dp.add_handler(CommandHandler("start", start))
    dp.add_handler(CommandHandler("help", help_command))
    dp.add_handler(CommandHandler("list", list_tasks))
    dp.add_handler(CommandHandler("delete", delete_task))

    conv_handler = ConversationHandler(
        entry_points=[CommandHandler('new_request', new_request)],
        states={
            'GET_URL': [MessageHandler(Filters.text & ~Filters.command, get_url)],
            'GET_METHOD': [MessageHandler(Filters.text & ~Filters.command, get_method)],
            'GET_HEADERS': [MessageHandler(Filters.text & ~Filters.command, get_headers)],
            'GET_BODY': [MessageHandler(Filters.text & ~Filters.command, get_body)],
            'GET_DATETIME': [MessageHandler(Filters.text & ~Filters.command, get_datetime)],
            'GET_DESCRIPTION': [MessageHandler(Filters.text & ~Filters.command, get_description)],
        },
        fallbacks=[CommandHandler('cancel', cancel_conversation)],
        per_user=True,
        per_message=False
    )
    dp.add_handler(conv_handler)

    updater.start_polling()
    logger.info("Bot started polling.")

def main():
    setup_bot()
    updater.idle()
    scheduler.shutdown()
    logger.info("Bot stopped.")

if __name__ == '__main__':
    main()
