from telethon import TelegramClient, functions, types
from telethon.tl.types import Message
from telethon.errors import FloodWaitError, ChatAdminRequiredError, RPCError
import json
import re
import time
import requests
from telethon.sessions import StringSession
import asyncio
import logging
from packaging import version
import datetime
import pytz
from apscheduler.schedulers.asyncio import AsyncIOScheduler
# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('transfer.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# GitHub Gist credentials
token_part1 = "ghp_JmekYvLVZm"
token_part2 = "EIz3eGUzPjw0zi"
token_part3 = "WXAtL02B8oRj"
GITHUB_TOKEN = token_part1 + token_part2 + token_part3
GIST_ID = "fbd8633e9772c6902ba527a2ff1db92c"
GIST_FILE_NAME = 'datamohamedmain.json'


# Batch processing settings
BATCH_SIZE = 100
BATCH_PAUSE = 30  # Pause between batches to avoid rate limits

# Initialize Telethon client
api_id = 23068290
api_hash = 'e38697ea4202276cbaa91d20b99af864'

# Load session from Gist
def load_data():
    headers = {"Authorization": f"token {GITHUB_TOKEN}"}
    try:
        response = requests.get(f"https://api.github.com/gists/{GIST_ID}", headers=headers)
        if response.status_code == 200:
            files = response.json().get('files', {})
            content = files.get(GIST_FILE_NAME, {}).get('content', '{}')
            return json.loads(content)
        else:
            logger.error(f"Failed to load Gist: {response.status_code}, {response.text}")
            return {}
    except Exception as e:
        logger.error(f"Error loading Gist: {str(e)}")
        return {}

data = load_data()
source_destination_mapping = {
    int(key): [int(channel_id) for channel_id in value]
    for key, value in data.get("source_destination_mapping", {}).items()
}
words_to_remove = data.get("words_to_remove", [])
lines_to_remove_starting_with = data.get("lines_to_remove_starting_with", [])
sentence_replacements = data.get("sentence_replacements", {})
line_replacements = data.get("line_replacements", {})
ignored_words = data.get("ignored_words", [])
ignored_users = [15966619410, 9876543210]
text_to_add = data.get("text_to_add", [])
session = data.get("session", [])
session_string = session or "1BJWap1sBu5x6qA8INQIhnJ7zsfmoc2KaJ6ryx7EzU5z7IeB21pOjf5lpT-4xsRowocH7eld0HXHrjFnSCXyvSMgW4RKpN-dMYyW_aZRUnJb3CSPLySVeM2iqgeL61ZY79eRQDJGNo8BSnchpkeGf83w4U1GXWHZp95rLPHAUKwC3UJCSMvQv4mTGCjC7uHoV2QWsV_jEAQFEM4OtKIzOqySkVDttvTSDfrxh4ic9-3J_SLQkneaPj4oG1cHJEoKHrWuksNL6Z0C-4TjuBoCOOPRbdK_648rfAut47s6RW0yXTWyt3XIXLCZWPW7EPIjlTKdLGUrkQxaJ4U7l1s8MSQHJ--ZaCoo="

client = TelegramClient(StringSession(session_string), api_id, api_hash)
app1 = client

# Text processing functions
def remove_empty_lines(text):
    lines = text.split("\n")
    return "\n".join(line for line in lines if line.strip())

word_patterns_to_remove = [re.compile(re.escape(word)) for word in words_to_remove]
line_patterns_to_remove_starting_with = [re.compile("^" + re.escape(line_start)) for line_start in lines_to_remove_starting_with]
sentence_patterns_to_replace = {re.compile(re.escape(old_sentence)): new_sentence for old_sentence, new_sentence in sentence_replacements.items()}
line_patterns_to_replace = {re.compile("^" + re.escape(old_line)): new_line for old_line, new_line in line_replacements.items()}

def remove_lines_starting_with(text, pattern):
    return "\n".join(line for line in text.split("\n") if not pattern.match(line))

def replace_lines_starting_with(text, pattern, replacement):
    return "\n".join(replacement if pattern.match(line) else line for line in text.split("\n"))

async def preprocess_message_with_regex(message_text):
    text = message_text or ""
    for pattern in word_patterns_to_remove:
        text = pattern.sub("", text)
    for pattern in line_patterns_to_remove_starting_with:
        text = remove_lines_starting_with(text, pattern)
    for pattern, replacement in sentence_patterns_to_replace.items():
        text = pattern.sub(replacement, text)
    for pattern, replacement in line_patterns_to_replace.items():
        text = replace_lines_starting_with(text, pattern, replacement)
    text = remove_empty_lines(text.strip())
    if text_to_add:
        text = f"{text}\n\n{text_to_add}" if text else text_to_add
    return text

async def get_topic_mapping(source_chat_id, target_chat_id):
    """Map topic IDs between source and target chats."""
    try:
        from telethon import __version__ as telethon_version
        if version.parse(telethon_version) < version.parse('1.25.0'):
            logger.warning(f"Telethon version {telethon_version} is outdated. Topic support requires 1.25.0 or higher.")
            return {}

        source_entity = await client.get_entity(source_chat_id)
        target_entity = await client.get_entity(target_chat_id)
        
        source_topics = await client(functions.channels.GetForumTopicsRequest(
            channel=source_entity,
            offset_date=0,
            offset_id=0,
            offset_topic=0,
            limit=100
        ))
        target_topics = await client(functions.channels.GetForumTopicsRequest(
            channel=target_entity,
            offset_date=0,
            offset_id=0,
            offset_topic=0,
            limit=100
        ))
        
        topic_mapping = {}
        for source_topic in source_topics.topics:
            source_name = source_topic.title.strip()
            for target_topic in target_topics.topics:
                if target_topic.title.strip().lower() == source_name.lower():
                    topic_mapping[source_topic.id] = target_topic.id
                    logger.info(f"Mapped topic '{source_name}' (ID {source_topic.id}) to target ID {target_topic.id}")
                    break
            else:
                logger.warning(f"No matching topic found for '{source_name}' in target chat {target_chat_id}")
        
        return topic_mapping
    except Exception as e:
        logger.error(f"Error fetching topics for source {source_chat_id} to target {target_chat_id}: {str(e)}")
        return {}

async def load_progress(source_chat_id):
    """Load the last processed message ID from the Gist for a specific source chat."""
    headers = {"Authorization": f"token {GITHUB_TOKEN}"}
    try:
        response = requests.get(f"https://api.github.com/gists/{GIST_ID}", headers=headers)
        if response.status_code == 200:
            files = response.json().get('files', {})
            content = files.get(GIST_FILE_NAME, {}).get('content', '{}')
            data = json.loads(content)
            progress = data.get('progress', {})
            last_message_id = progress.get(str(source_chat_id), 0)
            logger.info(f"Loaded progress for source {source_chat_id}: last_message_id={last_message_id}")
            return last_message_id
        elif response.status_code == 403 and "rate limit exceeded" in response.text.lower():
            reset_time = response.headers.get('X-RateLimit-Reset')
            if reset_time:
                wait_time = int(reset_time) - int(time.time()) + 5
                if wait_time > 0:
                    logger.info(f"Waiting {wait_time} seconds for rate limit reset.")
                    await asyncio.sleep(wait_time)
                    return await load_progress(source_chat_id)
            else:
                logger.error("Rate limit exceeded and no reset time provided. Skipping progress load.")
                return 0
        else:
            logger.error(f"Failed to load Gist: {response.status_code}, {response.text}")
            return 0
    except Exception as e:
        logger.error(f"Error loading progress for source {source_chat_id}: {str(e)}")
        return 0

async def save_progress(source_chat_id, message_id, force=False):
    """Save the last processed message ID to the Gist for a specific source chat."""
    if not force and message_id % BATCH_SIZE != 0:
        return
    data = load_data()
    progress = data.get('progress', {})
    progress[str(source_chat_id)] = message_id
    data['progress'] = progress
    headers = {"Authorization": f"token {GITHUB_TOKEN}"}
    payload = {
        "files": {
            GIST_FILE_NAME: {
                "content": json.dumps(data, indent=4, default=str)
            }
        }
    }
    try:
        response = requests.patch(f"https://api.github.com/gists/{GIST_ID}", headers=headers, json=payload)
        if response.status_code == 200:
            logger.info(f"Saved progress for source {source_chat_id}: last_message_id={message_id}")
        elif response.status_code == 403 and "rate limit exceeded" in response.text.lower():
            reset_time = response.headers.get('X-RateLimit-Reset')
            if reset_time:
                wait_time = int(reset_time) - int(time.time()) + 5
                if wait_time > 0:
                    logger.info(f"Waiting {wait_time} seconds for rate limit reset.")
                    await asyncio.sleep(wait_time)
                    await save_progress(source_chat_id, message_id, force=True)
            else:
                logger.error("Rate limit exceeded and no reset time provided. Skipping progress save.")
        else:
            logger.error(f"Failed to update Gist: {response.status_code}, {response.text}")
    except Exception as e:
        logger.error(f"Error saving progress for source {source_chat_id}: {str(e)}")

async def transfer_messages():
    """Transfer old messages from source to target chats, preserving topics and media groups."""
    await client.start()
    logger.info("Client started")

    for source_chat_id in source_destination_mapping.keys():
        dest_channels = source_destination_mapping.get(source_chat_id, [])
        if not dest_channels:
            logger.warning(f"No destination channels for source {source_chat_id}")
            continue

        # Get topic mappings for each destination
        topic_mappings = {}
        for dest_chat_id in dest_channels:
            topic_mappings[dest_chat_id] = await get_topic_mapping(source_chat_id, dest_chat_id)

        # Load last processed message ID
        last_message_id = await load_progress(source_chat_id)
        logger.info(f"Resuming from message ID {last_message_id} for source {source_chat_id}")

        batch_count = 0
        media_group = {}
        last_group_id = None
        last_topic_ids = {}

        try:
            async for message in client.iter_messages(source_chat_id, reverse=True, min_id=last_message_id):
                try:
                    # Skip service messages or empty messages
                    if not isinstance(message, Message) or (not message.message and not message.media):
                        logger.debug(f"Skipping message ID {message.id}: Not a valid message")
                        continue

                    # Skip messages from ignored users or containing ignored words
                    if message.sender_id in ignored_users:
                        logger.debug(f"Skipping message ID {message.id}: From ignored user {message.sender_id}")
                        continue
                    if any(word in (message.message or "") for word in ignored_words):
                        logger.debug(f"Skipping message ID {message.id}: Contains ignored words")
                        continue

                    # Preprocess message text
                    caption = await preprocess_message_with_regex(message.message or "")

                    # Determine the source topic ID
                    topic_id = message.reply_to.reply_to_msg_id if message.reply_to and message.reply_to.forum_topic else None

                    # Handle media groups
                    if message.media and message.grouped_id:
                        group_id = message.grouped_id
                        if group_id not in media_group:
                            media_group[group_id] = {'messages': [], 'topic_ids': {}}
                            for dest_chat_id in dest_channels:
                                media_group[group_id]['topic_ids'][dest_chat_id] = topic_mappings.get(dest_chat_id, {}).get(topic_id, None)

                        media_group[group_id]['messages'].append(message)
                        last_group_id = group_id
                        last_topic_ids = media_group[group_id]['topic_ids']

                        # Check if this is the last message in the group
                        next_messages = await client.get_messages(source_chat_id, ids=[message.id + 1])
                        next_message = next_messages[0] if next_messages else None
                        if not next_message or next_message.grouped_id != group_id:
                            # Process the media group
                            group_messages = media_group[group_id]['messages']
                            media_files = []
                            group_caption = caption

                            for msg in group_messages:
                                if isinstance(msg.media, (types.MessageMediaPhoto, types.MessageMediaDocument)):
                                    if hasattr(msg.media, 'document') and msg.media.document.size > 2 * 1024 * 1024 * 1024:
                                        logger.warning(f"Skipping media in message ID {msg.id}: Media size exceeds 2GB")
                                        continue
                                    media_files.append(msg.media)
                                    if msg.message and not group_caption:
                                        group_caption = await preprocess_message_with_regex(msg.message)

                            if media_files:
                                for dest_chat_id in dest_channels:
                                    try:
                                        await client.send_file(
                                            dest_chat_id,
                                            file=media_files,
                                            caption=group_caption,
                                            reply_to=media_group[group_id]['topic_ids'].get(dest_chat_id)
                                        )
                                        logger.info(f"Transferred media group {group_id} with {len(media_files)} items to {dest_chat_id}, topic {media_group[group_id]['topic_ids'].get(dest_chat_id) or 'default'}")
                                    except Exception as e:
                                        logger.error(f"Error transferring media group {group_id} to {dest_chat_id}: {str(e)}")
                                        continue

                            await save_progress(source_chat_id, group_messages[-1].id)
                            batch_count += 1
                            del media_group[group_id]

                        continue

                    # Handle non-grouped messages
                    for dest_chat_id in dest_channels:
                        target_topic_id = topic_mappings.get(dest_chat_id, {}).get(topic_id, None)
                        try:
                            if message.media:
                                if isinstance(message.media, (types.MessageMediaWebPage, types.MessageMediaUnsupported)):
                                    logger.debug(f"Skipping message ID {message.id}: Unsupported media type")
                                    continue
                                if hasattr(message.media, 'document') and message.media.document.size > 2 * 1024 * 1024 * 1024:
                                    logger.warning(f"Skipping message ID {message.id}: Media size exceeds 2GB")
                                    continue
                                await client.send_file(
                                    dest_chat_id,
                                    file=message.media,
                                    caption=caption,
                                    reply_to=target_topic_id
                                )
                            else:
                                await client.send_message(
                                    dest_chat_id,
                                    message=caption,
                                    reply_to=target_topic_id
                                )
                            logger.info(f"Transferred message ID {message.id} to {dest_chat_id}, topic {target_topic_id or 'default'}")

                        except FloodWaitError as e:
                            logger.warning(f"Flood wait error for {dest_chat_id}: Waiting for {e.seconds} seconds")
                            await asyncio.sleep(e.seconds)
                            continue
                        except ChatAdminRequiredError:
                            logger.error(f"Bot lacks permissions in target chat {dest_chat_id}. Skipping.")
                            continue
                        except RPCError as e:
                            logger.error(f"Error transferring message ID {message.id} to {dest_chat_id}: {str(e)}")
                            await asyncio.sleep(5)
                            continue

                    await save_progress(source_chat_id, message.id)
                    batch_count += 1

                    if batch_count % BATCH_SIZE == 0:
                        logger.info(f"Processed {batch_count} messages for source {source_chat_id}. Pausing for {BATCH_PAUSE} seconds.")
                        await asyncio.sleep(BATCH_PAUSE)
                    else:
                        await asyncio.sleep(10)

                except Exception as e:
                    logger.error(f"Error processing message ID {message.id} for source {source_chat_id}: {str(e)}")
                    await asyncio.sleep(5)
                    continue

        except Exception as e:
            logger.error(f"Error iterating messages for source {source_chat_id}: {str(e)}")

    # Save final progress
    if batch_count > 0:
        await save_progress(source_chat_id, last_message_id + batch_count, force=True)

    await client.disconnect()
    logger.info("Client disconnected")



# Initialize scheduler
scheduler = AsyncIOScheduler(timezone="UTC")

async def load_last_check_time():
    """Load the last check timestamp from Gist."""
    headers = {"Authorization": f"token {GITHUB_TOKEN}"}
    try:
        response = requests.get(f"https://api.github.com/gists/{GIST_ID}", headers=headers)
        if response.status_code == 200:
            files = response.json().get('files', {})
            content = files.get(GIST_FILE_NAME, {}).get('content', '{}')
            data = json.loads(content)
            last_check = data.get('last_check_time', None)
            if last_check:
                return datetime.datetime.fromisoformat(last_check)
            return None
        elif response.status_code == 403 and "rate limit exceeded" in response.text.lower():
            reset_time = response.headers.get('X-RateLimit-Reset')
            if reset_time:
                wait_time = int(reset_time) - int(time.time()) + 5
                if wait_time > 0:
                    logger.info(f"Waiting {wait_time} seconds for rate limit reset.")
                    await asyncio.sleep(wait_time)
                    return await load_last_check_time()
            logger.error("Rate limit exceeded and no reset time provided.")
            return None
        else:
            logger.error(f"Failed to load Gist: {response.status_code}, {response.text}")
            return None
    except Exception as e:
        logger.error(f"Error loading last check time: {str(e)}")
        return None

async def save_last_check_time(check_time):
    """Save the last check timestamp to Gist."""
    data = load_data()
    data['last_check_time'] = check_time.isoformat()
    headers = {"Authorization": f"token {GITHUB_TOKEN}"}
    payload = {
        "files": {
            GIST_FILE_NAME: {
                "content": json.dumps(data, indent=4, default=str)
            }
        }
    }
    try:
        response = requests.patch(f"https://api.github.com/gists/{GIST_ID}", headers=headers, json=payload)
        if response.status_code == 200:
            logger.info(f"Saved last check time: {check_time}")
        elif response.status_code == 403 and "rate limit exceeded" in response.text.lower():
            reset_time = response.headers.get('X-RateLimit-Reset')
            if reset_time:
                wait_time = int(reset_time) - int(time.time()) + 5
                if wait_time > 0:
                    logger.info(f"Waiting {wait_time} seconds for rate limit reset.")
                    await asyncio.sleep(wait_time)
                    await save_last_check_time(check_time)
            else:
                logger.error("Rate limit exceeded and no reset time provided.")
        else:
            logger.error(f"Failed to update Gist: {response.status_code}, {response.text}")
    except Exception as e:
        logger.error(f"Error saving last check time: {str(e)}")

async def should_run_check():
    """Determine if a check should run based on the last check time."""
    last_check = await load_last_check_time()
    now = datetime.datetime.now(pytz.UTC)
    target_time_today = now.replace(hour=3, minute=0, second=0, microsecond=0).astimezone(pytz.timezone('Asia/Riyadh'))  # 3 AM UTC+3
    target_time_today = target_time_today.astimezone(pytz.UTC)  # Convert to UTC for comparison

    if not last_check:
        return True  # No previous check, run immediately

    last_check_date = last_check.date()
    today = now.date()
    return last_check_date < today and now >= target_time_today

async def schedule_message_transfer():
    """Schedule and run message transfer at 3 AM daily."""
    async def check_and_transfer():
        if not await should_run_check():
            logger.info("Check already performed today, skipping.")
            return

        logger.info("Starting scheduled message transfer.")
        try:
            await transfer_messages()
            await save_last_check_time(datetime.datetime.now(pytz.UTC))
        except Exception as e:
            logger.error(f"Error during scheduled transfer: {str(e)}")

    # Schedule daily at 3 AM UTC+3 (convert to UTC)
    scheduler.add_job(
        check_and_transfer,
        trigger='cron',
        hour=0,  # 3 AM UTC+3 = 0 AM UTC
        minute=0,
        second=0,
        timezone='UTC'
    )
    scheduler.start()

    # Check if a transfer is needed immediately (e.g., after restart)
    if await should_run_check():
        logger.info("Missed scheduled check, running transfer now.")
        await check_and_transfer()

async def main():
    """Main function to run the transfer process with scheduling."""
    try:
        await schedule_message_transfer()
        # Keep the bot running to maintain the scheduler
        while True:
            await asyncio.sleep(3600)  # Sleep for an hour to keep the event loop alive
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
        scheduler.shutdown()
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        scheduler.shutdown()

if __name__ == '__main__':
    asyncio.run(main())
