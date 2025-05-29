import asyncio
import asyncio
import os
import time
import multiprocessing
from telethon import TelegramClient
from telethon import errors
import sys
import subprocess
import logging
from bot11afirst import* #schedule_message_transfer, app1  # استيراد صريح للدالة
from bot2a import *
from app import server
import gunicorn
# إعداد السجل
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('mainbot.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# التوكن الخاص بالبوت الأول
bot_token = "8014178533:AAHVQ6aMU4zO1Kb46stFCNqk0DbPZPuB9SM"

@app2.on_message(filters.command("reload"))
async def reload_bots(_: Client, message: Message):
    logger.info("Received reload command")
    await message.reply("جاري إعادة تشغيل البرنامج...")
    os.execl(sys.executable, sys.executable, *sys.argv)

def restart_program():
    logger.info("Restarting program")
    os.execl(sys.executable, sys.executable, *sys.argv)

Tests = 0    
async def run_bot1():
    global Tests
    try:
        # بدء البوت باستخدام التوكن مباشرة
        await app1.start(bot_token=bot_token)
        sent_alert = False  # متغير لتتبع ما إذا تم إرسال التنبيه بالفعل أم لا
        try:
 # التحقق من الجلسة
            me = await app1.get_me()
            logger.info(f"Bot 1 is running as {me.username}")
            await schedule_message_transfer()  # استدعاء الجدولة
            logger.info("Bot 1 started scheduler")
            while True:
                await asyncio.sleep(3600)  # الانتظار للحفاظ على المجدول
        except errors.AuthKeyError:
            if not sent_alert:
                logger.error("Bot 1 session has an authentication key error")
                await app2.send_message(1596661941, "هناك مشكلة في جلسة تليجرام. جرب التسجيل مرة أخرى وأعد تشغيل البوت.")
                sent_alert = True
        except Exception as e:
            logger.error(f"Error in run_bot1: {str(e)}")
            await app2.send_message(1596661941, f"حدث خطأ أثناء عمل البوت: {str(e)}")
            Tests += 1
            if Tests >= 3:
                Tests = 0
                restart_program()
                time.sleep(350)
            time.sleep(45)
    except Exception as e:
        logger.error(f"Error starting bot1: {str(e)}")
        pass

async def run_bot2():
    session_file1 = "SessionsExcutor.session"
    session_file2 = "path_to_bot2_session_file_2"
    try:
        if app1.is_connected():
            logger.info("Bot 1 is connected, stopping bot 2")
            await app2.stop()
        await app2.start()
        logger.info("Bot 2 started")
        while True:
            await asyncio.sleep(50)
    except errors.AuthKeyDuplicatedError:
        logger.error("Bot 2 session is duplicated. Deleting session files and restarting.")
        time.sleep(20)
        if os.path.exists(session_file1):
            os.remove(session_file1)
        if os.path.exists(session_file2):
            os.remove(session_file2)
        restart_program()
    except Exception as e:
        logger.error(f"Error in bot2: {str(e)}")
        pass

def main():
    server
    loop = asyncio.get_event_loop()
    task1 = asyncio.ensure_future(run_bot1())
    task2 = asyncio.ensure_future(run_bot2())
    try:
        loop.run_until_complete(asyncio.gather(task1, task2))
    except KeyboardInterrupt:
        logger.info("Program stopped by user")
    except Exception as e:
        logger.error(f"Error in main: {str(e)}")
    finally:
        loop.stop()

if __name__ == "__main__":
    main()
