"""Ordered channel mirror: download a chat's videos, files AND text messages
in their original order, then re-post them to another channel in that same
order.

Why a separate tool: tbmd_fixed.py fetches media with Telegram's server-side
search filters (InputMessagesFilterVideo etc.), so plain text posts never
reach it, and it downloads in parallel batches, so the original sequence is
lost. Here the whole chat is scanned unfiltered in chronological order into
a manifest.json that is the single source of truth for ordering; downloads
may run in parallel (files are numbered), but sending is strictly
sequential and stops on the first error instead of skipping, so the target
channel can never end up out of order.

Self-healing: transient failures (network drops, timeouts, server errors,
stalled transfers) are retried in-process with backoff, the answers given at
startup are saved to mirrors/run_state.json, and a small supervisor
(auto_restart.py) starts the program again with those answers if the process
itself dies. Run with --no-restart to disable the supervisor.
"""
import os
import sys

# Must run before the heavy imports below: those read credentials and can
# prompt, and the supervisor only needs the standard library.
if __name__ == "__main__":
    import auto_restart

    if not auto_restart.is_child() and "--no-restart" not in sys.argv:
        sys.exit(auto_restart.supervise(os.path.abspath(__file__), sys.argv[1:]))

import asyncio
import base64
import contextlib
import errno
import json
import re
import time
from datetime import datetime, timezone
from pathlib import Path

from colorama import Fore, Style
from tqdm.asyncio import tqdm
from telethon import TelegramClient, functions, utils
from telethon import errors as tg_errors
from telethon.errors import (
    ChatAdminRequiredError,
    ChatForwardsRestrictedError,
    ChatWriteForbiddenError,
    FileReferenceExpiredError,
    FloodWaitError,
    MediaCaptionTooLongError,
    RPCError,
)
from telethon.tl import types
from telethon.tl.tlobject import TLObject

# Reuses config (.env / API credential prompt), the login session and helpers
# of the original downloader, so both tools share one Telegram login.
from tbmd_fixed import (
    BASE_DIR,
    FAST_DOWNLOAD_MAX_CONNECTIONS,
    FAST_DOWNLOAD_MIN_MB,
    MAX_RETRY,
    USE_FAST_DOWNLOAD,
    api_hash,
    api_id,
    ask_yes_no,
    batch_size,
    guess_extension,
    resolve_channel,
    safe_filename,
    session_name,
)
from FastTelethon import download_file as fast_parallel_download
from FastTelethon import upload_file as fast_parallel_upload

MIRRORS_DIR = BASE_DIR / "mirrors"
MANIFEST_NAME = "manifest.json"
ORDER_LIST_NAME = "SIRA.txt"
# Pause between posts to the target channel. Posting hundreds of messages
# back to back triggers FloodWait quickly; a small steady delay is faster
# overall than repeatedly hitting multi-minute waits.
SEND_DELAY = float(os.getenv("SEND_DELAY", "2"))
RESUME_CHUNK = 512 * 1024  # partial downloads resume on this boundary
FORWARD_CHUNK = 100  # Telegram's max message IDs per ForwardMessages call

# --- self-healing settings (all overridable from .env) ---
# Seconds a transfer may go without any progress before it is aborted and
# retried; a silently hung connection otherwise freezes the program forever.
STALL_TIMEOUT = float(os.getenv("STALL_TIMEOUT", "180"))
# Hard limit for a single send/forward request (no progress callbacks there).
REQUEST_TIMEOUT = float(os.getenv("REQUEST_TIMEOUT", "300"))
# Non-network failures (a rejected message, a missing file...) that happen this
# many times in a row WITHOUT any progress in between stop the run for good,
# because skipping would break the order. Network failures never count: the
# program waits for the connection as long as it takes.
AUTO_RESTART_LIMIT = int(os.getenv("AUTO_RESTART_LIMIT", "15"))
RETRY_DELAY_MAX = float(os.getenv("RETRY_DELAY_MAX", "120"))
AUTO_MODE = "--auto" in sys.argv  # set by the supervisor on restarts
STATE_NAME = "run_state.json"

EXIT_DONE = 0
EXIT_CRASH = 1        # supervisor restarts the program
EXIT_NEEDS_HUMAN = 2  # supervisor does NOT restart

# Errors where retrying cannot help: the account/session or the chats
# themselves are the problem.
FATAL_ERRORS = tuple(
    getattr(tg_errors, name)
    for name in (
        "AuthKeyUnregisteredError", "AuthKeyDuplicatedError", "UserDeactivatedError",
        "UserDeactivatedBanError", "SessionRevokedError", "SessionExpiredError",
        "PhoneNumberBannedError", "SessionPasswordNeededError", "ChannelPrivateError",
        "ChannelInvalidError", "ChatForwardsRestrictedError", "UserBannedInChannelError",
        "ChatRestrictedError",
    )
    if hasattr(tg_errors, name)
)
NETWORK_ERRORS = tuple(
    [ConnectionError, TimeoutError, asyncio.TimeoutError, EOFError, asyncio.IncompleteReadError]
    + [getattr(tg_errors, n) for n in ("ServerError", "TimedOutError", "InvalidBufferError") if hasattr(tg_errors, n)]
)
LOCAL_FILE_ERRORS = (PermissionError, FileNotFoundError, FileExistsError, IsADirectoryError, NotADirectoryError)


def is_network_error(error: BaseException) -> bool:
    """True for connection-type failures, which are waited out indefinitely."""
    if isinstance(error, NETWORK_ERRORS):
        return True
    # Sockets/SSL/DNS errors are plain OSErrors; local file problems are not.
    return isinstance(error, OSError) and not isinstance(error, LOCAL_FILE_ERRORS) \
        and error.errno != errno.ENOSPC

_progress = [0]


def tick():
    """Record that real work got done (a file downloaded, an item sent...).
    The recovery loop uses it to tell 'flaky connection' from 'stuck for good'."""
    _progress[0] += 1


class StallError(TimeoutError):
    """No progress for STALL_TIMEOUT seconds."""


class Activity:
    def __init__(self):
        self.last = time.monotonic()

    def touch(self):
        self.last = time.monotonic()


async def stall_guard(awaitable, activity: Activity, what: str):
    """Await `awaitable`, but abort it if `activity` shows no progress for
    STALL_TIMEOUT seconds (the progress callback keeps touching it)."""
    task = asyncio.ensure_future(awaitable)
    activity.touch()
    try:
        while True:
            done, _ = await asyncio.wait({task}, timeout=5)
            if done:
                return task.result()
            if time.monotonic() - activity.last > STALL_TIMEOUT:
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await task
                raise StallError(f"{what}: no progress for {STALL_TIMEOUT:.0f}s")
    except asyncio.CancelledError:
        task.cancel()
        raise


async def with_timeout(awaitable, what: str):
    try:
        return await asyncio.wait_for(awaitable, REQUEST_TIMEOUT)
    except asyncio.TimeoutError:
        raise StallError(f"{what}: no answer from Telegram for {REQUEST_TIMEOUT:.0f}s") from None

# Attributes that reference other server objects (sticker sets, custom emoji)
# can't be recreated by re-uploading, so they're dropped on send.
UNSENDABLE_ATTRIBUTES = {
    "DocumentAttributeSticker",
    "DocumentAttributeCustomEmoji",
    "DocumentAttributeHasStickers",
}

TYPE_LABELS = {
    "text": "METIN",
    "photo": "FOTO",
    "video": "VIDEO",
    "round": "YUVARLAK",
    "gif": "GIF",
    "voice": "SES",
    "audio": "MUZIK",
    "document": "DOSYA",
}


def info(en: str, tr: str, color=Fore.CYAN):
    print(f"{color}{en}{Style.RESET_ALL} ({tr})")


# ---------------------------------------------------------------------------
# TL <-> JSON (message entities and document attributes are stored verbatim,
# so bold/italic/links/spoilers and video duration/size survive the round trip)
# ---------------------------------------------------------------------------
def tl_to_json(value):
    if isinstance(value, TLObject):
        value = value.to_dict()
    if isinstance(value, dict):
        return {key: tl_to_json(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [tl_to_json(item) for item in value]
    if isinstance(value, bytes):
        return {"__bytes__": base64.b64encode(value).decode("ascii")}
    if isinstance(value, datetime):
        return value.isoformat()
    return value


def tl_from_json(value):
    if isinstance(value, list):
        return [tl_from_json(item) for item in value]
    if not isinstance(value, dict):
        return value
    if "__bytes__" in value:
        return base64.b64decode(value["__bytes__"])
    if "_" in value:
        cls = getattr(types, value["_"])
        return cls(**{k: tl_from_json(v) for k, v in value.items() if k != "_"})
    return value


def build_entities(raw_entities):
    entities = []
    for raw in raw_entities or []:
        try:
            entity = tl_from_json(raw)
        except (AttributeError, TypeError):
            continue
        # A received mention carries a bare user_id; sending one requires an
        # InputUser we may not have access to, so degrade it to a user link.
        if isinstance(entity, types.MessageEntityMentionName):
            entity = types.MessageEntityTextUrl(
                entity.offset, entity.length, f"tg://user?id={entity.user_id}"
            )
        entities.append(entity)
    return entities


def build_attributes(file_info):
    attributes = []
    for raw in file_info.get("attributes") or []:
        if raw.get("_") in UNSENDABLE_ATTRIBUTES:
            continue
        try:
            attributes.append(tl_from_json(raw))
        except (AttributeError, TypeError):
            continue
    if not any(isinstance(a, types.DocumentAttributeFilename) for a in attributes):
        attributes.append(types.DocumentAttributeFilename(file_info["orig_name"]))
    return attributes


# ---------------------------------------------------------------------------
# Manifest
# ---------------------------------------------------------------------------
def load_manifest(folder: Path):
    path = folder / MANIFEST_NAME
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def save_manifest(folder: Path, manifest):
    # Write-then-rename so a crash mid-write never corrupts the order data.
    path = folder / MANIFEST_NAME
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(manifest, ensure_ascii=False, indent=1), encoding="utf-8")
    os.replace(tmp, path)


def get_progress(manifest, target, create=False):
    """Find (or create) the per-target progress record.

    Older runs / helper scripts stored the key as "2974652532" and others as
    "-1002974652532". If the two disagree, progress looks empty and EVERYTHING
    is downloaded and sent again. Both spellings are accepted here and merged
    into one canonical key (keeping the furthest progress)."""
    sent = manifest.setdefault("sent", {})
    key = str(target.id)
    alt = str(utils.get_peer_id(target))
    found = [sent[k] for k in dict.fromkeys((key, alt)) if k in sent]
    if found:
        best = max(found, key=lambda r: r.get("last_n", 0))
        sent.pop(alt, None)
        sent[key] = best
        return best
    if create:
        sent[key] = {"title": getattr(target, "title", key), "last_n": 0}
        return sent[key]
    return None


def write_order_list(folder: Path, manifest):
    lines = []
    for item in manifest["items"]:
        for part in item["parts"]:
            if part.get("skip"):
                kind, name = "ATLANDI", part["skip"]
            elif part.get("file"):
                kind, name = TYPE_LABELS[part["file"]["type"]], part["file"]["name"]
            else:
                kind, name = TYPE_LABELS["text"], ""
            text = (part.get("text") or "").replace("\n", " ")[:120]
            album = " [ALBUM]" if len(item["parts"]) > 1 else ""
            lines.append(
                f"{item['n']:05d}{album}  {part['date'][:16]}  {kind:<8} {name}  {text}".rstrip()
            )
    (folder / ORDER_LIST_NAME).write_text("\n".join(lines) + "\n", encoding="utf-8")


def classify(message):
    """Return (file_type, skip_reason); (None, None) means a plain text post."""
    media = message.media
    if media is None or isinstance(media, types.MessageMediaWebPage):
        return None, None
    if isinstance(media, types.MessageMediaPhoto):
        return ("photo", None) if media.photo else (None, "expired photo")
    if isinstance(media, types.MessageMediaDocument) and message.document:
        if message.sticker:
            return None, "sticker"
        if message.video_note:
            return "round", None
        if message.voice:
            return "voice", None
        if message.gif:
            return "gif", None
        if message.video:
            return "video", None
        if message.audio:
            return "audio", None
        return "document", None
    return None, type(media).__name__.replace("MessageMedia", "").lower()


def build_part(message, n: int, index_in_album):
    file_type, skip = classify(message)
    part = {
        "msg_id": message.id,
        "date": message.date.astimezone(timezone.utc).isoformat(),
        "text": message.message or "",
        "entities": tl_to_json(message.entities or []),
        "file": None,
        "skip": skip,
    }
    if file_type:
        orig_name = getattr(message.file, "name", None)
        if not orig_name:
            orig_name = safe_filename(file_type, guess_extension(message))
        prefix = f"{n:05d}" if index_in_album is None else f"{n:05d}-{index_in_album + 1}"
        name = f"{prefix}_{safe_filename(orig_name, guess_extension(message))}"
        document = message.document
        part["file"] = {
            "type": file_type,
            "name": name,
            "orig_name": orig_name,
            "size": message.file.size or 0,
            "mime": getattr(document, "mime_type", None) if document else "image/jpeg",
            "attributes": tl_to_json(document.attributes) if document else [],
            "thumb": (Path(name).stem + ".thumb.jpg")
            if document and getattr(document, "thumbs", None)
            else None,
        }
    return part


async def scan_chat(client, channel, manifest, message_cache, folder=None):
    """Append every message newer than the last scanned one, in order.

    Consecutive messages sharing a grouped_id form one album item, exactly
    as Telegram displays them.
    """
    last_id = manifest.get("last_scanned_id", 0)
    min_id = max(last_id, manifest.get("min_id") or 0)
    max_id = manifest.get("max_id") or 0
    items = manifest["items"]
    n = items[-1]["n"] if items else 0
    added = 0

    info("Scanning chat in chronological order...", "Sohbet tarih sirasina gore taraniyor...")
    async for message in client.iter_messages(
        channel,
        reverse=True,
        min_id=min_id,
        max_id=max_id,
        reply_to=manifest.get("topic_id"),
    ):
        if isinstance(message, types.MessageService):
            continue
        if not message.message and message.media is None:
            continue

        message_cache[message.id] = message
        last = items[-1] if items else None
        same_album = (
            message.grouped_id
            and last is not None
            and last.get("grouped_id") == message.grouped_id
        )

        if same_album:
            last["parts"].append(build_part(message, last["n"], len(last["parts"])))
        else:
            n += 1
            items.append(
                {
                    "n": n,
                    "grouped_id": message.grouped_id,
                    "link_preview": isinstance(message.media, types.MessageMediaWebPage),
                    "parts": [build_part(message, n, 0 if message.grouped_id else None)],
                }
            )
        manifest["last_scanned_id"] = message.id
        added += 1
        if added % 500 == 0:
            print(f"  {added} messages scanned... ({added} mesaj tarandi...)")
            if folder:
                # A dropped connection halfway through a huge chat resumes
                # from here instead of rescanning everything.
                save_manifest(folder, manifest)
                tick()

    info(f"{added} new message(s) scanned.", f"{added} yeni mesaj tarandi.", Fore.GREEN)
    return added


# ---------------------------------------------------------------------------
# Download
# ---------------------------------------------------------------------------
def file_is_complete(folder: Path, file_info) -> bool:
    path = folder / file_info["name"]
    if not path.exists():
        return False
    # Photo "size" is the largest PhotoSize's estimate, which does not always
    # match the saved JPEG byte-for-byte, so only require a non-empty file.
    if file_info["type"] == "photo" or not file_info["size"]:
        return path.stat().st_size > 0
    return path.stat().st_size == file_info["size"]


async def download_part(client, channel, message, part, folder, semaphore):
    file_info = part["file"]
    path = folder / file_info["name"]
    tmp = path.with_name(path.name + ".part")

    async with semaphore:
        bar = tqdm(
            total=file_info["size"] or None,
            desc=file_info["name"][:40],
            unit="B",
            unit_scale=True,
            ncols=100,
            leave=False,
        )
        activity = Activity()

        def callback(current, total):
            activity.touch()
            bar.update(current - bar.n)

        last_error = None

        for attempt in range(1, MAX_RETRY + 2):
            try:
                bar.reset(total=file_info["size"] or None)
                use_fast = (
                    USE_FAST_DOWNLOAD
                    and message.document
                    and file_info["size"] >= FAST_DOWNLOAD_MIN_MB * 1024 * 1024
                )
                resume_from = 0
                if message.document and file_info["size"] and tmp.exists():
                    resume_from = (tmp.stat().st_size // RESUME_CHUNK) * RESUME_CHUNK
                    if resume_from >= file_info["size"]:
                        resume_from = 0
                if resume_from:
                    # Continue the interrupted .part file instead of starting over.
                    async def resume_download(offset=resume_from):
                        with open(tmp, "r+b") as out:
                            out.truncate(offset)
                            out.seek(offset)
                            written = offset
                            callback(written, file_info["size"])
                            async for chunk in client.iter_download(
                                message.document,
                                offset=offset,
                                request_size=RESUME_CHUNK,
                                file_size=file_info["size"],
                            ):
                                out.write(chunk)
                                written += len(chunk)
                                callback(written, file_info["size"])

                    await stall_guard(resume_download(), activity, f"download {file_info['name']}")
                elif use_fast:
                    with open(tmp, "wb") as out:
                        await stall_guard(
                            fast_parallel_download(
                                client,
                                message.document,
                                out,
                                progress_callback=callback,
                                connection_count=FAST_DOWNLOAD_MAX_CONNECTIONS,
                            ),
                            activity,
                            f"download {file_info['name']}",
                        )
                else:
                    result = await stall_guard(
                        client.download_media(message, file=str(tmp), progress_callback=callback),
                        activity,
                        f"download {file_info['name']}",
                    )
                    if not result:
                        raise RuntimeError("Telegram returned no file")

                size = tmp.stat().st_size
                if file_info["type"] != "photo" and file_info["size"] and size != file_info["size"]:
                    if size > file_info["size"]:
                        tmp.unlink(missing_ok=True)  # corrupt; start clean next time
                    raise RuntimeError(f"size mismatch {size} != {file_info['size']}")
                os.replace(tmp, path)
                break
            except FileReferenceExpiredError as error:
                # Long runs outlive Telegram's file references; refetching
                # the message yields a fresh one.
                last_error = error
                try:
                    message = await client.get_messages(channel, ids=message.id) or message
                except RPCError:
                    await asyncio.sleep(2 * attempt)
            except FloodWaitError as error:
                last_error = error
                await asyncio.sleep(error.seconds + 1)
            except (RPCError, RuntimeError, OSError, ConnectionError) as error:
                if isinstance(error, OSError) and error.errno == errno.ENOSPC:
                    bar.close()
                    tmp.unlink(missing_ok=True)
                    raise OrderStop(
                        "Disk is full. Free some space and run again. (Disk dolu, yer acip tekrar calistirin.)",
                        fatal=True,
                    )
                last_error = error
                await asyncio.sleep(2 * attempt)
        else:
            bar.close()
            # The .part file is kept so the next run resumes from it.
            print(
                f"{Fore.RED}FAILED #{part['msg_id']} {file_info['name']}: {last_error}"
                f"{Style.RESET_ALL} (indirilemedi)"
            )
            return False

        bar.close()

        if file_info.get("thumb") and not (folder / file_info["thumb"]).exists():
            try:
                await client.download_media(message, file=str(folder / file_info["thumb"]), thumb=-1)
            except (RPCError, OSError, TypeError, ValueError):
                file_info["thumb"] = None  # optional; Telegram generates one if missing

        tick()
        print(f"{Fore.GREEN}OK{Style.RESET_ALL} {file_info['name']}")
        return True


async def download_all(client, channel, folder: Path, manifest, message_cache, items=None):
    if items is None:
        # Items already sent to any target were probably deleted locally
        # (delete-after-send); do not download them again.
        sent_upto = max((v.get("last_n", 0) for v in manifest.get("sent", {}).values()), default=0)
        source_items = [i for i in manifest["items"] if i["n"] > sent_upto]
        if sent_upto:
            info(
                f"Skipping items up to #{sent_upto:05d} (already sent).",
                f"#{sent_upto:05d} numaraya kadar olanlar zaten gonderilmis, atlaniyor.",
                Fore.YELLOW,
            )
    else:
        source_items = items
    pending = [
        part
        for item in source_items
        for part in item["parts"]
        if part.get("file") and not part.get("skip") and not file_is_complete(folder, part["file"])
    ]
    if not pending:
        info("All files are already downloaded.", "Tum dosyalar zaten indirilmis.", Fore.GREEN)
        return 0

    info(f"{len(pending)} file(s) to download.", f"{len(pending)} dosya indirilecek.")

    # Messages from an earlier session are not cached; fetch them in bulk.
    missing_ids = [p["msg_id"] for p in pending if p["msg_id"] not in message_cache]
    for start in range(0, len(missing_ids), 100):
        chunk = missing_ids[start:start + 100]
        for message in await client.get_messages(channel, ids=chunk):
            if message:
                message_cache[message.id] = message

    semaphore = asyncio.Semaphore(max(1, batch_size))
    tasks = []
    failed = 0
    for part in pending:
        message = message_cache.get(part["msg_id"])
        if message is None:
            print(f"{Fore.RED}Message {part['msg_id']} no longer exists.{Style.RESET_ALL} (Mesaj silinmis.)")
            failed += 1
            continue
        tasks.append(download_part(client, channel, message, part, folder, semaphore))

    results = await asyncio.gather(*tasks, return_exceptions=True)
    save_manifest(folder, manifest)  # thumbs may have been cleared
    for result in results:
        if isinstance(result, OrderStop) and result.fatal:
            raise result
    for result in results:
        if isinstance(result, BaseException):
            print(f"{Fore.RED}Unexpected download error: {result}{Style.RESET_ALL}")
    failed += sum(1 for result in results if result is not True)
    if failed:
        info(
            f"{failed} file(s) failed. Run again to resume; nothing already downloaded is repeated.",
            f"{failed} dosya indirilemedi. Tekrar calistirinca kaldigi yerden devam eder.",
            Fore.YELLOW,
        )
    else:
        info("All files downloaded.", "Tum dosyalar indirildi.", Fore.GREEN)
    return failed


# ---------------------------------------------------------------------------
# Re-upload to the target channel, strictly in order
# ---------------------------------------------------------------------------
class OrderStop(Exception):
    """Raised to halt sending; skipping an item would break the order.

    fatal=True means retrying cannot help (no permission, protected source...)
    so the recovery loop stops instead of trying again."""

    def __init__(self, message, fatal=False):
        super().__init__(message)
        self.fatal = fatal


async def tg_send_file(client, *args, **kwargs):
    return await with_timeout(client.send_file(*args, **kwargs), "send file")


async def tg_send_message(client, *args, **kwargs):
    return await with_timeout(client.send_message(*args, **kwargs), "send message")


async def upload_path(client, path: Path, desc: str):
    size = path.stat().st_size
    bar = tqdm(total=size, desc=desc[:40], unit="B", unit_scale=True, ncols=100, leave=False)
    activity = Activity()

    def callback(current, total):
        activity.touch()
        bar.update(current - bar.n)

    try:
        if USE_FAST_DOWNLOAD and size >= FAST_DOWNLOAD_MIN_MB * 1024 * 1024:
            with open(path, "rb") as handle:
                return await stall_guard(
                    fast_parallel_upload(client, handle, progress_callback=callback),
                    activity,
                    f"upload {desc}",
                )
        return await stall_guard(
            client.upload_file(str(path), progress_callback=callback), activity, f"upload {desc}"
        )
    finally:
        bar.close()


async def build_media(client, folder: Path, file_info):
    path = folder / file_info["name"]
    if not file_is_complete(folder, file_info):
        raise MissingFileStop(
            f"File missing or incomplete: {file_info['name']} - run the download step first. "
            f"(Dosya eksik: once indirme adimini calistirin.)"
        )
    uploaded = await upload_path(client, path, file_info["name"])
    if file_info["type"] == "photo":
        return types.InputMediaUploadedPhoto(file=uploaded)

    thumb = None
    if file_info.get("thumb") and (folder / file_info["thumb"]).exists():
        thumb = await with_timeout(client.upload_file(str(folder / file_info["thumb"])), "upload thumbnail")

    return types.InputMediaUploadedDocument(
        file=uploaded,
        mime_type=file_info.get("mime") or "application/octet-stream",
        attributes=build_attributes(file_info),
        force_file=file_info["type"] == "document",
        nosound_video=True if file_info["type"] == "gif" else None,
        thumb=thumb,
    )


async def send_item(client, target, folder: Path, item, silent: bool, medias, topic_id=None):
    parts = [p for p in item["parts"] if not p.get("skip")]
    media_parts = [p for p in parts if p.get("file")]
    texts = [p["text"] for p in parts]
    entities = [build_entities(p["entities"]) for p in parts]

    if not media_parts:
        if not texts[0].strip():
            return
        await tg_send_message(client,
            target,
            texts[0],
            formatting_entities=entities[0] or None,
            parse_mode=None,
            link_preview=item.get("link_preview", False),
            silent=silent,
            reply_to=topic_id,
        )
        return

    if not medias:
        for part in media_parts:
            medias.append(await build_media(client, folder, part["file"]))

    try:
        if len(medias) == 1:
            await tg_send_file(client,
                target,
                medias[0],
                caption=texts[0],
                formatting_entities=entities[0] or None,
                parse_mode=None,
                silent=silent,
                reply_to=topic_id,
            )
        else:
            await tg_send_file(client,
                target,
                medias,
                caption=texts,
                formatting_entities=entities,
                parse_mode=None,
                silent=silent,
                reply_to=topic_id,
            )
    except MediaCaptionTooLongError:
        # The source was posted by a Premium account with a longer caption
        # limit than ours: post the media bare, then the text right after it.
        # Re-upload, since the failed request may have consumed the file parts.
        medias[:] = [await build_media(client, folder, p["file"]) for p in media_parts]
        await tg_send_file(client,
            target, medias if len(medias) > 1 else medias[0], silent=silent, reply_to=topic_id,
        )
        for text, ents in zip(texts, entities):
            if text.strip():
                await tg_send_message(client,
                    target, text, formatting_entities=ents or None, parse_mode=None,
                    link_preview=False, silent=silent, reply_to=topic_id,
                )


class MissingFileStop(OrderStop):
    """A file to send isn't on disk (yet)."""


def delete_item_files(folder: Path, item):
    for part in item["parts"]:
        file_info = part.get("file")
        if file_info:
            (folder / file_info["name"]).unlink(missing_ok=True)
            if file_info.get("thumb"):
                (folder / file_info["thumb"]).unlink(missing_ok=True)


async def send_all(client, target, folder: Path, manifest, silent: bool, upto_n=None, delete_after=False, topic_id=None):
    progress = get_progress(manifest, target, create=True)
    todo = [
        item
        for item in manifest["items"]
        if item["n"] > progress["last_n"] and (upto_n is None or item["n"] <= upto_n)
    ]
    if not todo:
        info("Everything has already been sent.", "Hepsi zaten gonderilmis.", Fore.GREEN)
        return

    for item in todo:
        if all(p.get("skip") for p in item["parts"]):
            reasons = ", ".join(p["skip"] for p in item["parts"])
            info(f"#{item['n']:05d} skipped ({reasons})", "atlandi - yeniden yuklenemeyen tur", Fore.YELLOW)
        else:
            medias = []  # reused across FloodWait retries so files upload once
            attempt = 0
            while True:
                try:
                    await send_item(client, target, folder, item, silent, medias, topic_id)
                    break
                except FloodWaitError as error:
                    info(f"Flood wait {error.seconds}s...", f"Telegram {error.seconds}sn bekletiyor...", Fore.YELLOW)
                    await asyncio.sleep(error.seconds + 1)
                except (ChatWriteForbiddenError, ChatAdminRequiredError) as error:
                    raise OrderStop(
                        f"No permission to post in the target: {error} "
                        f"(Hedef kanalda paylasim yetkiniz yok; yonetici olup mesaj gonderme izni gerekir.)",
                        fatal=True,
                    )
                except FATAL_ERRORS as error:
                    raise OrderStop(f"Cannot continue: {error!r}", fatal=True)
                except (RPCError, OSError, ConnectionError) as error:
                    attempt += 1
                    medias.clear()  # uploaded parts may have been discarded server-side
                    if attempt > MAX_RETRY:
                        raise OrderStop(f"#{item['n']:05d} could not be sent: {error}")
                    await asyncio.sleep(3 * attempt)
            kinds = "+".join(
                TYPE_LABELS[p["file"]["type"]] if p.get("file") else TYPE_LABELS["text"]
                for p in item["parts"]
                if not p.get("skip")
            )
            print(f"{Fore.GREEN}SENT{Style.RESET_ALL} #{item['n']:05d}/{manifest['items'][-1]['n']:05d} {kinds}")
            await asyncio.sleep(SEND_DELAY)

        progress["last_n"] = item["n"]
        save_manifest(folder, manifest)
        tick()
        # Only after progress is saved: a crash in between leaves the file
        # on disk rather than losing an unsent one.
        if delete_after:
            delete_item_files(folder, item)

    if upto_n is None:
        info("All items sent in the original order.", "Tum ogeler orijinal sirayla gonderildi.", Fore.GREEN)


async def forward_all(client, channel, target, folder: Path, manifest, silent: bool, topic_id=None):
    """Copy without downloading: forward in order with the 'Forwarded from'
    header removed. Albums are never split across requests."""
    progress = get_progress(manifest, target, create=True)
    todo = [item for item in manifest["items"] if item["n"] > progress["last_n"]]
    chunk_items = []

    async def flush():
        ids = [p["msg_id"] for item in chunk_items for p in item["parts"]]
        while True:
            try:
                if topic_id:
                    # The high-level forward_messages() has no way to target a
                    # specific topic, so the raw request is used directly.
                    await with_timeout(
                        client(
                            functions.messages.ForwardMessagesRequest(
                                from_peer=channel,
                                id=ids,
                                to_peer=target,
                                drop_author=True,
                                silent=silent,
                                top_msg_id=topic_id,
                            )
                        ),
                        "forward",
                    )
                else:
                    await with_timeout(
                        client.forward_messages(
                            target, ids, from_peer=channel, drop_author=True, silent=silent
                        ),
                        "forward",
                    )
                break
            except FloodWaitError as error:
                info(f"Flood wait {error.seconds}s...", f"Telegram {error.seconds}sn bekletiyor...", Fore.YELLOW)
                await asyncio.sleep(error.seconds + 1)
        progress["last_n"] = chunk_items[-1]["n"]
        save_manifest(folder, manifest)
        tick()
        print(f"{Fore.GREEN}COPIED{Style.RESET_ALL} up to #{progress['last_n']:05d}")
        chunk_items.clear()
        await asyncio.sleep(SEND_DELAY)

    try:
        for item in todo:
            count = sum(len(i["parts"]) for i in chunk_items)
            if chunk_items and count + len(item["parts"]) > FORWARD_CHUNK:
                await flush()
            chunk_items.append(item)
        if chunk_items:
            await flush()
    except ChatForwardsRestrictedError:
        raise OrderStop(
            "Source chat has content protection; direct copy is blocked. Use mode 3 "
            "(download + re-upload) instead. (Kaynak kanal korumali; 3 numarali "
            "'indir + gonder' modunu kullanin.)",
            fatal=True,
        )
    except (ChatWriteForbiddenError, ChatAdminRequiredError) as error:
        raise OrderStop(
            f"No permission to post in the target: {error} (Hedef kanalda yetkiniz yok.)", fatal=True
        )
    except FATAL_ERRORS as error:
        raise OrderStop(f"Cannot continue: {error!r}", fatal=True)
    except (RPCError, OSError, ConnectionError) as error:
        raise OrderStop(f"Copy failed after #{progress['last_n']:05d}: {error}")

    info("Copy finished in the original order.", "Kopyalama orijinal sirayla tamamlandi.", Fore.GREEN)


# ---------------------------------------------------------------------------
# Interactive flow
# ---------------------------------------------------------------------------
def parse_message_id(text: str):
    text = text.strip()
    if not text:
        return 0
    match = re.search(r"(\d+)\s*$", text)  # accepts a bare ID or a message link
    if not match:
        raise ValueError(text)
    return int(match.group(1))


async def resolve_any(client, text: str):
    """Accepts @username, t.me links (public or /c/ private), numeric IDs and
    dialog names."""
    text = text.strip()
    await client.get_dialogs()  # fills the entity cache so numeric IDs resolve
    try:
        private = re.match(r"(?:https?://)?t\.me/c/(\d+)", text)
        if private:
            return await client.get_entity(int("-100" + private.group(1)))
        public = re.match(r"(?:https?://)?t\.me/([A-Za-z0-9_+]+)", text)
        if public:
            return await client.get_entity(public.group(1) if not public.group(1).startswith("+") else text)
        if text.startswith("@"):
            return await client.get_entity(text)
        if re.fullmatch(r"-?\d+", text):
            return await client.get_entity(int(text))
    except (ValueError, RPCError):
        pass
    return await resolve_channel(client, text)


async def ask_chat(client, en: str, tr: str):
    while True:
        text = input(f"{Fore.CYAN}{en} ({tr}): {Style.RESET_ALL}").strip()
        if not text:
            continue
        try:
            entity = await resolve_any(client, text)
            title = getattr(entity, "title", None) or getattr(entity, "first_name", "")
            info(f"Selected: {title} (ID {entity.id})", "Secildi", Fore.GREEN)
            return entity
        except ValueError:
            info("Chat not found, try again.", "Sohbet bulunamadi, tekrar deneyin.", Fore.RED)


async def load_topics(client, channel):
    topics, seen = [], set()
    offset_date = offset_id = offset_topic = 0
    while True:
        result = await client(
            functions.messages.GetForumTopicsRequest(
                peer=channel,
                offset_date=offset_date,
                offset_id=offset_id,
                offset_topic=offset_topic,
                limit=100,
            )
        )
        new = [t for t in result.topics if t.id not in seen and hasattr(t, "title")]
        if not new:
            break
        seen.update(t.id for t in new)
        topics.extend(new)
        if len(result.topics) < 100:
            break
        last = result.topics[-1]
        last_message = next((m for m in result.messages if m.id == last.top_message), None)
        offset_topic, offset_id = last.id, last.top_message
        offset_date = int(last_message.date.timestamp()) if last_message else 0
    return topics


async def pick_topic(client, channel):
    """List the forum's topics and return (topic_id, topic_title); (None, None)
    means the whole chat."""
    info("Loading topics...", "Alt basliklar yukleniyor...")
    try:
        topics = await load_topics(client, channel)
    except RPCError as error:
        info(f"Could not load topics: {error}", "Alt basliklar yuklenemedi, tum sohbet kullanilacak.", Fore.RED)
        return None, None
    if not topics:
        return None, None

    print(f"\n{Fore.CYAN}Topics / Alt basliklar ({len(topics)}):{Style.RESET_ALL}")
    print("  0. Whole chat (Tum sohbet)")
    for index, topic in enumerate(topics, 1):
        print(f"  {index}. {topic.title}")
    while True:
        raw = input(f"{Fore.CYAN}Select topic (Alt baslik secin) (0-{len(topics)}): {Style.RESET_ALL}").strip()
        if raw.isdigit() and 0 <= int(raw) <= len(topics):
            break
    if raw == "0":
        return None, None
    topic = topics[int(raw) - 1]
    info(f"Selected topic: {topic.title} (ID {topic.id})", "Alt baslik secildi", Fore.GREEN)
    return topic.id, topic.title


async def resolve_target_topic(client, target, topic_title):
    """Mirror the source topic on the target: if the source item came from a
    named topic and the target is itself a forum, reuse a same-titled topic
    there or create one automatically, no questions asked.

    If the source had NO topic (private chat, or a plain group/channel) but
    the target IS a forum, ask whether to create a new topic there instead of
    just dropping everything into General.

    Returns None when the target isn't a forum at all - in which case the
    item is sent straight into the chat with no topic."""
    if not getattr(target, "forum", False):
        return None

    if not topic_title:
        if not ask_yes_no(
            "Target is a forum group but the source has no topic. Create a new topic for this content?",
            "Hedef forum grubu ama kaynakta alt baslik yok. Bu icerik icin hedefte yeni bir alt baslik olusturulsun mu? (Hayir = Genel'e gonderilir)",
        ):
            return None
        while True:
            topic_title = input(f"{Fore.CYAN}New topic title (Yeni alt baslik adi): {Style.RESET_ALL}").strip()
            if topic_title:
                break

    try:
        topics = await load_topics(client, target)
    except RPCError as error:
        info(
            f"Could not load target topics, sending to General instead: {error}",
            "Hedefteki alt basliklar yuklenemedi, Genel'e gonderilecek.",
            Fore.RED,
        )
        return None
    for topic in topics:
        if topic.title == topic_title:
            info(
                f"Using matching target topic: {topic.title} (ID {topic.id})",
                "Hedefte ayni isimli alt baslik bulundu, oraya gonderilecek",
                Fore.GREEN,
            )
            return topic.id

    info(
        f"Creating target topic '{topic_title}'",
        "Hedefte alt baslik olusturuluyor",
        Fore.CYAN,
    )
    result = await client(
        functions.messages.CreateForumTopicRequest(peer=target, title=topic_title)
    )
    for update in result.updates:
        if isinstance(update, types.UpdateNewChannelMessage) and isinstance(
            update.message.action, types.MessageActionTopicCreate
        ):
            info(f"Target topic created (ID {update.message.id})", "Hedef alt basligi olusturuldu", Fore.GREEN)
            return update.message.id
    raise OrderStop(
        "Target topic was created but its ID could not be determined. "
        "(Hedefte alt baslik olusturuldu ama ID'si tespit edilemedi.)"
    )


async def choose_source(client, channel=None, topic_override=None, auto_resume=False):
    """Pick the source chat/topic/range and create (or continue) its mirror
    folder. Nothing is scanned here; execute_plan does that, so a restart goes
    through exactly the same code path. Returns (channel, folder)."""
    if channel is None:
        channel = await ask_chat(
            client, "Source channel/group (link, @username, ID or name)", "Kaynak kanal/grup"
        )
    topic_id, topic_title = (
        topic_override if topic_override is not None else
        (await pick_topic(client, channel) if getattr(channel, "forum", False) else (None, None))
    )

    title = safe_filename(getattr(channel, "title", "chat"))[:60]
    base_name = f"{title}_{abs(channel.id)}" + (f"_t{topic_id}" if topic_id else "")

    existing = [
        p
        for p in MIRRORS_DIR.iterdir()
        # Only this exact chat/topic, optionally with a range suffix; a
        # whole-chat run must not pick up a topic folder (or vice versa).
        if re.fullmatch(re.escape(base_name) + r"(_(\d+|bas)-(\d+|son))?", p.name)
        and (p / MANIFEST_NAME).exists()
    ]
    folder = None
    if auto_resume and existing:
        folder = next((p for p in existing if p.name == base_name), None)
    if not auto_resume:
        for candidate in existing:
            if ask_yes_no(
                f"Continue the existing mirror '{candidate.name}' (only new messages are scanned)?",
                f"Mevcut '{candidate.name}' klasorunden devam edilsin mi (sadece yeni mesajlar taranir)?",
            ):
                folder = candidate
                break

    if not folder:
        if auto_resume:
            min_id = max_id = 0
        else:
            info(
                "Optional range: paste a message link or ID; leave empty for the whole chat.",
                "Istege bagli aralik: mesaj linki veya ID yapistirin; bos = tum sohbet.",
            )
            while True:
                try:
                    min_id = parse_message_id(input("  Start / Baslangic (dahil): "))
                    max_id = parse_message_id(input("  End / Bitis (dahil): "))
                    break
                except ValueError:
                    info("Invalid ID or link.", "Gecersiz ID/link.", Fore.RED)
        suffix = f"_{min_id or 'bas'}-{max_id or 'son'}" if (min_id or max_id) else ""
        folder = MIRRORS_DIR / (base_name + suffix)
        folder.mkdir(parents=True, exist_ok=True)
        manifest = {
            "version": 1,
            "source": {"id": channel.id, "title": getattr(channel, "title", "")},
            "topic_id": topic_id,
            "topic_title": topic_title,
            # iter_messages bounds are exclusive, the user's are inclusive.
            "min_id": max(min_id - 1, 0) if min_id else 0,
            "max_id": max_id + 1 if max_id else 0,
            "created": datetime.now(timezone.utc).isoformat(),
            "items": [],
            "sent": {},
        }
        save_manifest(folder, manifest)

    return channel, folder


async def scan_and_report(client, channel, folder: Path, manifest, message_cache):
    await scan_chat(client, channel, manifest, message_cache, folder)
    save_manifest(folder, manifest)
    write_order_list(folder, manifest)

    total_files = sum(1 for i in manifest["items"] for p in i["parts"] if p.get("file"))
    total_text = sum(1 for i in manifest["items"] for p in i["parts"] if not p.get("file") and not p.get("skip"))
    info(
        f"{len(manifest['items'])} items in order ({total_files} files, {total_text} texts). "
        f"Order list: {folder / ORDER_LIST_NAME}",
        f"{len(manifest['items'])} oge sirali ({total_files} dosya, {total_text} metin). "
        f"Sira listesi: {ORDER_LIST_NAME}",
        Fore.GREEN,
    )


async def prepare_source(client, message_cache, channel=None, topic_override=None, auto_resume=False):
    """choose_source + incremental scan; used by the all-topics mode."""
    channel, folder = await choose_source(client, channel, topic_override, auto_resume)
    manifest = load_manifest(folder)
    await scan_and_report(client, channel, folder, manifest, message_cache)
    return channel, folder, manifest


def pick_existing_folder():
    folders = sorted(
        (p for p in MIRRORS_DIR.glob("*") if (p / MANIFEST_NAME).exists()),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    ) if MIRRORS_DIR.exists() else []
    if not folders:
        info("No downloaded mirror found. Use mode 1 first.", "Indirilmis klasor yok; once 1. modu kullanin.", Fore.RED)
        return None
    for index, folder in enumerate(folders, 1):
        print(f"  {index}. {folder.name}")
    while True:
        choice = input(f"{Fore.CYAN}Select folder (Klasor secin): {Style.RESET_ALL}").strip()
        if choice.isdigit() and 1 <= int(choice) <= len(folders):
            return folders[int(choice) - 1]


async def ask_target_and_options(client, manifest):
    target = await ask_chat(client, "Target channel to post into", "Gonderilecek hedef kanal")
    target_topic_id = await resolve_target_topic(client, target, manifest.get("topic_title"))
    progress = get_progress(manifest, target)
    if progress and progress["last_n"]:
        if not ask_yes_no(
            f"Resume after item #{progress['last_n']:05d}?",
            f"#{progress['last_n']:05d} numarali ogeden sonra devam edilsin mi? (Hayir = bastan)",
        ):
            progress["last_n"] = 0
    silent = ask_yes_no(
        "Post silently (no notification for each message)?",
        "Sessiz gonderilsin mi (her mesaj icin bildirim gitmesin)?",
    )
    return target, silent, target_topic_id


async def setup_all_topics(client):
    """Mode 5 questions (asked once; the answers are saved in the run state)."""
    channel = await ask_chat(
        client, "Source forum group (link, @username, ID or name)", "Kaynak forum grubu"
    )
    if not getattr(channel, "forum", False):
        info("Source is not a forum.", "Kaynak bir forum grubu degil.", Fore.RED)
        return None
    target = await ask_chat(client, "Target forum group", "Hedef forum grubu")
    if not getattr(target, "forum", False):
        info("Target must be a forum group.", "Hedef bir forum grubu olmali.", Fore.RED)
        return None
    if target.id == channel.id:
        info("Source and target must differ.", "Kaynak ve hedef farkli olmali.", Fore.RED)
        return None
    silent = ask_yes_no(
        "Post silently (no notification for each message)?",
        "Sessiz gonderilsin mi (her mesaj icin bildirim gitmesin)?",
    )
    delete_after = ask_yes_no(
        "Delete each local file after sending?",
        "Gonderilen her dosya yerelden silinsin mi?",
    )
    return {
        "mode": "5",
        "folder": None,
        "source_peer": utils.get_peer_id(channel),
        "target_peer": utils.get_peer_id(target),
        "silent": silent,
        "delete_after": delete_after,
        "created": datetime.now(timezone.utc).isoformat(),
        "finished": False,
    }


async def run_all_topics(client, plan, message_cache):
    """Transfer all source forum topics, resuming each topic independently.
    Safe to call again after a failure: finished topics are skipped quickly."""
    channel = await resolve_peer(client, plan["source_peer"], "Source")
    target = await resolve_peer(client, plan["target_peer"], "Target")
    silent = plan.get("silent", False)
    delete_after = plan.get("delete_after", False)
    topics = await load_topics(client, channel)
    if not topics:
        raise OrderStop("No topics found. (Alt baslik bulunamadi.)", fatal=True)

    info(f"Transferring {len(topics)} topics.", f"{len(topics)} alt baslik aktarilacak.")
    for index, topic in enumerate(topics, 1):
        info(f"Topic {index}/{len(topics)}: {topic.title}",
             f"Alt baslik {index}/{len(topics)}: {topic.title}")
        _, folder, manifest = await prepare_source(
            client, message_cache, channel=channel,
            topic_override=(topic.id, topic.title), auto_resume=True,
        )
        progress = get_progress(manifest, target) or {}
        todo = [item for item in manifest["items"] if item["n"] > progress.get("last_n", 0)]
        if not todo:
            continue
        target_topic_id = await resolve_target_topic(client, target, topic.title)
        if not target_topic_id:
            raise OrderStop(f"Could not resolve target topic: {topic.title}")
        if not delete_after:
            failed = await download_all(client, channel, folder, manifest, message_cache, todo)
            if failed:
                raise OrderStop(f"Download failed in topic {topic.title}; will retry.")
            await send_all(client, target, folder, manifest, silent, topic_id=target_topic_id)
            continue
        window = max(1, batch_size) * 2
        for start in range(0, len(todo), window):
            chunk = todo[start:start + window]
            failed = await download_all(client, channel, folder, manifest, message_cache, chunk)
            if failed:
                raise OrderStop(f"Download failed in topic {topic.title}; will retry.")
            await send_all(
                client, target, folder, manifest, silent,
                upto_n=chunk[-1]["n"], delete_after=True, topic_id=target_topic_id,
            )
    info("All topics transferred.", "Tum alt basliklar aktarildi.", Fore.GREEN)


# ---------------------------------------------------------------------------
# Saved run state, execution and automatic recovery
# ---------------------------------------------------------------------------
def save_state(plan):
    path = MIRRORS_DIR / STATE_NAME
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(plan, ensure_ascii=False, indent=1), encoding="utf-8")
    os.replace(tmp, path)


def load_state():
    try:
        return json.loads((MIRRORS_DIR / STATE_NAME).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None


def keep_system_awake():
    """Windows suspends the PC (and with it the connection) after a while;
    ask it not to for as long as this program runs. The screen may still turn off."""
    if sys.platform == "win32":
        try:
            import ctypes

            ctypes.windll.kernel32.SetThreadExecutionState(0x80000000 | 0x00000001)
        except Exception:
            pass


async def resolve_peer(client, peer_id, label: str):
    """Find a chat again from the ID saved in the run state."""
    try:
        return await client.get_entity(peer_id)
    except ValueError:
        pass
    await client.get_dialogs()  # refresh the entity cache, then try once more
    try:
        return await client.get_entity(peer_id)
    except ValueError:
        raise OrderStop(
            f"{label} chat {peer_id} was not found anymore (left the chat or it was deleted). "
            f"({label} sohbet artik bulunamiyor.)",
            fatal=True,
        )


async def ensure_connected(client):
    if not client.is_connected():
        await client.connect()  # connection_retries=-1: waits until the network is back
    if not await client.is_user_authorized():
        raise OrderStop(
            "Telegram login is missing or was revoked. Delete the .session file and run the program "
            "once by hand to log in again. (Telegram oturumu gecersiz; .session dosyasini silip "
            "programi bir kez elle calistirarak tekrar giris yapin.)",
            fatal=True,
        )


async def execute_plan(client, plan, message_cache):
    """Run the saved plan from wherever it currently stands. Everything here
    is resumable, so the recovery loop simply calls it again after a failure."""
    mode = plan["mode"]
    if mode == "5":
        await run_all_topics(client, plan, message_cache)
        return
    folder = MIRRORS_DIR / plan["folder"]
    manifest = load_manifest(folder)  # fresh from disk: it is the source of truth
    if manifest is None:
        raise OrderStop(f"{folder / MANIFEST_NAME} is missing. (manifest.json bulunamadi.)", fatal=True)

    channel = None
    if mode != "2":
        channel = await resolve_peer(client, plan["source_peer"], "Source")
        await scan_and_report(client, channel, folder, manifest, message_cache)

    if mode == "1":
        failed = await download_all(client, channel, folder, manifest, message_cache)
        if failed:
            raise OrderStop(f"{failed} file(s) could not be downloaded")
        return

    target = await resolve_peer(client, plan["target_peer"], "Target")
    topic_id = plan.get("target_topic_id")
    silent = plan.get("silent", False)

    if mode == "4":
        await forward_all(client, channel, target, folder, manifest, silent, topic_id=topic_id)
    elif mode == "2":
        try:
            await send_all(client, target, folder, manifest, silent, topic_id=topic_id)
        except MissingFileStop as stop:
            # Send-only mode never downloads, so retrying can't fix this.
            raise OrderStop(str(stop), fatal=True)
    else:  # mode 3: download + send
        progress = get_progress(manifest, target) or {}
        todo = [i for i in manifest["items"] if i["n"] > progress.get("last_n", 0)]

        if not plan.get("delete_after"):
            # Failed downloads don't block sending: send_all stops at the first
            # missing file and the next round retries just those downloads.
            await download_all(client, channel, folder, manifest, message_cache, todo)
            await send_all(client, target, folder, manifest, silent, topic_id=topic_id)
        else:
            # Download and send in small windows so disk usage stays at a
            # few files instead of the whole chat.
            window = max(1, batch_size) * 2
            for start in range(0, len(todo), window):
                chunk = todo[start:start + window]
                await download_all(client, channel, folder, manifest, message_cache, chunk)
                await send_all(
                    client, target, folder, manifest, silent,
                    upto_n=chunk[-1]["n"], delete_after=True, topic_id=topic_id,
                )
            info(
                "All items sent in the original order; files deleted.",
                "Tum ogeler sirayla gonderildi, dosyalar silindi.",
                Fore.GREEN,
            )
    return


def report_stop(stop_text: str, resumable=True):
    print(f"\n{Fore.RED}STOPPED: {stop_text}{Style.RESET_ALL}")
    if resumable:
        info(
            "Progress is saved; start the program again to resume from this exact item.",
            "Ilerleme kaydedildi; programi tekrar calistirinca tam bu ogeden devam eder.",
            Fore.YELLOW,
        )


async def run_plan(client, plan) -> int:
    """Run the plan, retrying with backoff until it finishes or hits something
    only a human can fix. Returns the process exit code."""
    message_cache = {}
    failures = 0          # consecutive failed rounds without progress in between
    hard_failures = 0     # same, but only counting non-network failures
    seen_progress = _progress[0]

    while True:
        try:
            await ensure_connected(client)
            await execute_plan(client, plan, message_cache)
            return EXIT_DONE
        except OrderStop as stop:
            if stop.fatal:
                report_stop(str(stop))
                return EXIT_NEEDS_HUMAN
            last_error, network = stop, False
        except FATAL_ERRORS as fatal:
            report_stop(f"{fatal!r}")
            return EXIT_NEEDS_HUMAN
        except (KeyboardInterrupt, SystemExit, asyncio.CancelledError):
            raise
        except Exception as caught:
            if isinstance(caught, OSError) and caught.errno == errno.ENOSPC:
                report_stop("Disk is full. Free some space and start again. (Disk dolu.)")
                return EXIT_NEEDS_HUMAN
            # (`except ... as name` unbinds the name afterwards, hence the copy)
            last_error, network = caught, is_network_error(caught)

        if _progress[0] != seen_progress:  # something got done since the last failure
            seen_progress = _progress[0]
            failures = hard_failures = 0
        failures += 1
        if not network:
            hard_failures += 1
            if hard_failures > AUTO_RESTART_LIMIT:
                report_stop(
                    f"Same problem {hard_failures} times in a row with no progress: {last_error!r}"
                )
                return EXIT_NEEDS_HUMAN

        delay = min(3 * 2 ** min(failures - 1, 6), RETRY_DELAY_MAX)
        kind = "connection problem" if network else "error"
        print(
            f"\n{Fore.YELLOW}{kind}: {last_error!r}{Style.RESET_ALL}\n"
            f"Retrying automatically in {delay:.0f}s, resuming where it stopped "
            f"(attempt {failures}). ({delay:.0f} sn sonra otomatik olarak kaldigi yerden devam edilecek.)"
        )
        message_cache.clear()  # file references in cached messages may be stale by now
        await asyncio.sleep(delay)


async def interactive_setup(client):
    """Ask everything once, up front, and return the plan (or None). The plan is
    saved to run_state.json so a restart never has to ask again."""
    print(
        f"\n{Fore.CYAN}=== Ordered Mirror / Sirali Kanal Aktarimi ==={Style.RESET_ALL}\n"
        "1. Download in order (videos + files + texts)   / Sirali indir\n"
        "2. Send a downloaded folder to a channel        / Indirilen klasoru kanala sirayla gonder\n"
        "3. Download + send                              / Indir + gonder\n"
        "4. Direct copy without download (no 'forwarded' tag; not for protected channels)\n"
        "                                                / Indirmeden dogrudan kopyala\n"
        "5. Mirror all forum topics automatically        / Tum alt basliklari otomatik aktar\n"
    )
    mode = input(f"{Fore.CYAN}Choice (1-5): {Style.RESET_ALL}").strip()
    if mode not in ("1", "2", "3", "4", "5"):
        info("Invalid choice.", "Gecersiz secim.", Fore.RED)
        return None

    if mode == "5":
        return await setup_all_topics(client)

    if mode == "2":
        folder = pick_existing_folder()
        if not folder:
            return None
        channel = None
    else:
        channel, folder = await choose_source(client)
        if mode == "4" and getattr(channel, "noforwards", False):
            info(
                "This chat has content protection; direct copy will fail. Use mode 3.",
                "Bu kanal korumali; dogrudan kopyalama calismaz, 3. modu kullanin.",
                Fore.RED,
            )
            return None

    plan = {
        "mode": mode,
        "folder": folder.name,
        "source_peer": utils.get_peer_id(channel) if channel is not None else None,
        "created": datetime.now(timezone.utc).isoformat(),
        "finished": False,
    }
    if mode == "1":
        return plan

    # Every remaining question is asked up front so the run can be left unattended.
    manifest = load_manifest(folder)
    target, silent, target_topic_id = await ask_target_and_options(client, manifest)
    save_manifest(folder, manifest)  # "start over" answer resets the saved position
    plan.update(
        target_peer=utils.get_peer_id(target),
        target_topic_id=target_topic_id,
        silent=silent,
        delete_after=ask_yes_no(
            "Delete each file from this computer right after it is sent?",
            "Her dosya gonderildikten hemen sonra bu bilgisayardan silinsin mi?",
        ) if mode == "3" else False,
    )
    return plan


async def main() -> int:
    keep_system_awake()
    MIRRORS_DIR.mkdir(parents=True, exist_ok=True)

    plan = None
    state = load_state()
    if state and not state.get("finished"):
        if AUTO_MODE or ask_yes_no(
            f"An earlier run (mode {state.get('mode')}, '{state.get('folder') or 'all topics'}') did not finish. "
            f"Resume it where it stopped?",
            "Onceki islem tamamlanmamis. Kaldigi yerden devam edilsin mi? (Hayir = yeni islem)",
        ):
            plan = state

    # connection_retries=-1 / auto_reconnect: Telethon itself keeps trying to
    # reconnect forever instead of giving up after 5 attempts.
    async with TelegramClient(
        str(BASE_DIR / session_name), api_id, api_hash,
        connection_retries=-1, retry_delay=3, request_retries=8, auto_reconnect=True,
    ) as client:
        try:
            if plan is None:
                plan = await interactive_setup(client)
                if plan is None:
                    return EXIT_NEEDS_HUMAN  # nothing to run; don't let the supervisor restart
                save_state(plan)
            code = await run_plan(client, plan)
        except OrderStop as stop:  # raised while asking (e.g. creating the target topic)
            report_stop(str(stop), resumable=False)
            return EXIT_NEEDS_HUMAN

    if code == EXIT_DONE:
        plan["finished"] = True
        save_state(plan)
        info("Done.", "Tamamlandi.", Fore.GREEN)
    return code


if __name__ == "__main__":
    try:
        exit_code = asyncio.run(main())
    except KeyboardInterrupt:
        print("\nStopped by user; progress is saved. (Kullanici durdurdu; ilerleme kaydedildi.)")
        exit_code = EXIT_NEEDS_HUMAN
    sys.exit(exit_code)
