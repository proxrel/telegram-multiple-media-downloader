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
"""
import asyncio
import base64
import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path

from colorama import Fore, Style
from tqdm.asyncio import tqdm
from telethon import TelegramClient
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
FORWARD_CHUNK = 100  # Telegram's max message IDs per ForwardMessages call

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


async def scan_chat(client, channel, manifest, message_cache):
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
        callback = lambda current, total: bar.update(current - bar.n)
        last_error = None

        for attempt in range(1, MAX_RETRY + 2):
            try:
                bar.reset(total=file_info["size"] or None)
                use_fast = (
                    USE_FAST_DOWNLOAD
                    and message.document
                    and file_info["size"] >= FAST_DOWNLOAD_MIN_MB * 1024 * 1024
                )
                if use_fast:
                    with open(tmp, "wb") as out:
                        await fast_parallel_download(
                            client,
                            message.document,
                            out,
                            progress_callback=callback,
                            connection_count=FAST_DOWNLOAD_MAX_CONNECTIONS,
                        )
                else:
                    result = await client.download_media(
                        message, file=str(tmp), progress_callback=callback
                    )
                    if not result:
                        raise RuntimeError("Telegram returned no file")

                size = tmp.stat().st_size
                if file_info["type"] != "photo" and file_info["size"] and size != file_info["size"]:
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
                last_error = error
                await asyncio.sleep(2 * attempt)
        else:
            bar.close()
            tmp.unlink(missing_ok=True)
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

        print(f"{Fore.GREEN}OK{Style.RESET_ALL} {file_info['name']}")
        return True


async def download_all(client, channel, folder: Path, manifest, message_cache, items=None):
    pending = [
        part
        for item in (manifest["items"] if items is None else items)
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
    """Raised to halt sending; skipping an item would break the order."""


async def upload_path(client, path: Path, desc: str):
    size = path.stat().st_size
    bar = tqdm(total=size, desc=desc[:40], unit="B", unit_scale=True, ncols=100, leave=False)
    callback = lambda current, total: bar.update(current - bar.n)
    try:
        if USE_FAST_DOWNLOAD and size >= FAST_DOWNLOAD_MIN_MB * 1024 * 1024:
            with open(path, "rb") as handle:
                return await fast_parallel_upload(client, handle, progress_callback=callback)
        return await client.upload_file(str(path), progress_callback=callback)
    finally:
        bar.close()


async def build_media(client, folder: Path, file_info):
    path = folder / file_info["name"]
    if not file_is_complete(folder, file_info):
        raise OrderStop(
            f"File missing or incomplete: {file_info['name']} - run the download step first. "
            f"(Dosya eksik: once indirme adimini calistirin.)"
        )
    uploaded = await upload_path(client, path, file_info["name"])
    if file_info["type"] == "photo":
        return types.InputMediaUploadedPhoto(file=uploaded)

    thumb = None
    if file_info.get("thumb") and (folder / file_info["thumb"]).exists():
        thumb = await client.upload_file(str(folder / file_info["thumb"]))

    return types.InputMediaUploadedDocument(
        file=uploaded,
        mime_type=file_info.get("mime") or "application/octet-stream",
        attributes=build_attributes(file_info),
        force_file=file_info["type"] == "document",
        nosound_video=True if file_info["type"] == "gif" else None,
        thumb=thumb,
    )


async def send_item(client, target, folder: Path, item, silent: bool, medias):
    parts = [p for p in item["parts"] if not p.get("skip")]
    media_parts = [p for p in parts if p.get("file")]
    texts = [p["text"] for p in parts]
    entities = [build_entities(p["entities"]) for p in parts]

    if not media_parts:
        if not texts[0].strip():
            return
        await client.send_message(
            target,
            texts[0],
            formatting_entities=entities[0] or None,
            parse_mode=None,
            link_preview=item.get("link_preview", False),
            silent=silent,
        )
        return

    if not medias:
        for part in media_parts:
            medias.append(await build_media(client, folder, part["file"]))

    try:
        if len(medias) == 1:
            await client.send_file(
                target,
                medias[0],
                caption=texts[0],
                formatting_entities=entities[0] or None,
                parse_mode=None,
                silent=silent,
            )
        else:
            await client.send_file(
                target,
                medias,
                caption=texts,
                formatting_entities=entities,
                parse_mode=None,
                silent=silent,
            )
    except MediaCaptionTooLongError:
        # The source was posted by a Premium account with a longer caption
        # limit than ours: post the media bare, then the text right after it.
        # Re-upload, since the failed request may have consumed the file parts.
        medias[:] = [await build_media(client, folder, p["file"]) for p in media_parts]
        await client.send_file(target, medias if len(medias) > 1 else medias[0], silent=silent)
        for text, ents in zip(texts, entities):
            if text.strip():
                await client.send_message(
                    target, text, formatting_entities=ents or None, parse_mode=None,
                    link_preview=False, silent=silent,
                )


def delete_item_files(folder: Path, item):
    for part in item["parts"]:
        file_info = part.get("file")
        if file_info:
            (folder / file_info["name"]).unlink(missing_ok=True)
            if file_info.get("thumb"):
                (folder / file_info["thumb"]).unlink(missing_ok=True)


async def send_all(client, target, folder: Path, manifest, silent: bool, upto_n=None, delete_after=False):
    key = str(target.id)
    progress = manifest.setdefault("sent", {}).setdefault(
        key, {"title": getattr(target, "title", str(target.id)), "last_n": 0}
    )
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
                    await send_item(client, target, folder, item, silent, medias)
                    break
                except FloodWaitError as error:
                    info(f"Flood wait {error.seconds}s...", f"Telegram {error.seconds}sn bekletiyor...", Fore.YELLOW)
                    await asyncio.sleep(error.seconds + 1)
                except (ChatWriteForbiddenError, ChatAdminRequiredError) as error:
                    raise OrderStop(
                        f"No permission to post in the target: {error} "
                        f"(Hedef kanalda paylasim yetkiniz yok; yonetici olup mesaj gonderme izni gerekir.)"
                    )
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
        # Only after progress is saved: a crash in between leaves the file
        # on disk rather than losing an unsent one.
        if delete_after:
            delete_item_files(folder, item)

    if upto_n is None:
        info("All items sent in the original order.", "Tum ogeler orijinal sirayla gonderildi.", Fore.GREEN)


async def forward_all(client, channel, target, folder: Path, manifest, silent: bool):
    """Copy without downloading: forward in order with the 'Forwarded from'
    header removed. Albums are never split across requests."""
    key = str(target.id)
    progress = manifest.setdefault("sent", {}).setdefault(
        key, {"title": getattr(target, "title", str(target.id)), "last_n": 0}
    )
    todo = [item for item in manifest["items"] if item["n"] > progress["last_n"]]
    chunk_items = []

    async def flush():
        ids = [p["msg_id"] for item in chunk_items for p in item["parts"]]
        while True:
            try:
                await client.forward_messages(
                    target, ids, from_peer=channel, drop_author=True, silent=silent
                )
                break
            except FloodWaitError as error:
                info(f"Flood wait {error.seconds}s...", f"Telegram {error.seconds}sn bekletiyor...", Fore.YELLOW)
                await asyncio.sleep(error.seconds + 1)
        progress["last_n"] = chunk_items[-1]["n"]
        save_manifest(folder, manifest)
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
            "'indir + gonder' modunu kullanin.)"
        )
    except (ChatWriteForbiddenError, ChatAdminRequiredError) as error:
        raise OrderStop(f"No permission to post in the target: {error} (Hedef kanalda yetkiniz yok.)")
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


async def prepare_source(client, message_cache):
    channel = await ask_chat(
        client, "Source channel/group (link, @username, ID or name)", "Kaynak kanal/grup"
    )
    topic_id = None
    if getattr(channel, "forum", False):
        raw = input(
            f"{Fore.CYAN}Forum topic ID or link (empty = whole chat) "
            f"(Topic ID/linki, bos = tum sohbet): {Style.RESET_ALL}"
        ).strip()
        topic_id = parse_message_id(raw) or None

    title = safe_filename(getattr(channel, "title", "chat"))[:60]
    base_name = f"{title}_{abs(channel.id)}" + (f"_t{topic_id}" if topic_id else "")

    existing = [
        p
        for p in MIRRORS_DIR.iterdir()
        if (p.name == base_name or p.name.startswith(base_name + "_"))
        and (p / MANIFEST_NAME).exists()
    ]
    folder = None
    for candidate in existing:
        if ask_yes_no(
            f"Continue the existing mirror '{candidate.name}' (only new messages are scanned)?",
            f"Mevcut '{candidate.name}' klasorunden devam edilsin mi (sadece yeni mesajlar taranir)?",
        ):
            folder = candidate
            break

    if folder:
        manifest = load_manifest(folder)
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
            # iter_messages bounds are exclusive, the user's are inclusive.
            "min_id": max(min_id - 1, 0) if min_id else 0,
            "max_id": max_id + 1 if max_id else 0,
            "created": datetime.now(timezone.utc).isoformat(),
            "items": [],
            "sent": {},
        }

    await scan_chat(client, channel, manifest, message_cache)
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
    progress = manifest.get("sent", {}).get(str(target.id))
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
    return target, silent


async def main():
    print(
        f"\n{Fore.CYAN}=== Ordered Mirror / Sirali Kanal Aktarimi ==={Style.RESET_ALL}\n"
        "1. Download in order (videos + files + texts)   / Sirali indir\n"
        "2. Send a downloaded folder to a channel        / Indirilen klasoru kanala sirayla gonder\n"
        "3. Download + send                              / Indir + gonder\n"
        "4. Direct copy without download (no 'forwarded' tag; not for protected channels)\n"
        "                                                / Indirmeden dogrudan kopyala\n"
    )
    mode = input(f"{Fore.CYAN}Choice (1-4): {Style.RESET_ALL}").strip()
    if mode not in ("1", "2", "3", "4"):
        info("Invalid choice.", "Gecersiz secim.", Fore.RED)
        return

    MIRRORS_DIR.mkdir(parents=True, exist_ok=True)
    message_cache = {}

    async with TelegramClient(str(BASE_DIR / session_name), api_id, api_hash) as client:
        try:
            if mode == "2":
                folder = pick_existing_folder()
                if not folder:
                    return
                manifest = load_manifest(folder)
                target, silent = await ask_target_and_options(client, manifest)
                await send_all(client, target, folder, manifest, silent)
                return

            channel, folder, manifest = await prepare_source(client, message_cache)

            if mode == "4":
                if getattr(channel, "noforwards", False):
                    info(
                        "This chat has content protection; direct copy will fail. Use mode 3.",
                        "Bu kanal korumali; dogrudan kopyalama calismaz, 3. modu kullanin.",
                        Fore.RED,
                    )
                    return
                target, silent = await ask_target_and_options(client, manifest)
                await forward_all(client, channel, target, folder, manifest, silent)
                return

            if mode == "1":
                await download_all(client, channel, folder, manifest, message_cache)
                return

            # Mode 3: every question is asked up front so the run can be
            # left unattended.
            target, silent = await ask_target_and_options(client, manifest)
            delete_after = ask_yes_no(
                "Delete each file from this computer right after it is sent?",
                "Her dosya gonderildikten hemen sonra bu bilgisayardan silinsin mi?",
            )
            progress = manifest.get("sent", {}).get(str(target.id), {})
            todo = [i for i in manifest["items"] if i["n"] > progress.get("last_n", 0)]

            if not delete_after:
                failed = await download_all(client, channel, folder, manifest, message_cache, todo)
                if failed and not ask_yes_no(
                    "Some files failed. Send anyway? Sending will stop at the first missing file.",
                    "Bazi dosyalar inmedi. Yine de gonderilsin mi? Ilk eksik dosyada durur.",
                ):
                    return
                await send_all(client, target, folder, manifest, silent)
                return

            # Download and send in small windows so disk usage stays at a
            # few files instead of the whole chat.
            window = max(1, batch_size) * 2
            for start in range(0, len(todo), window):
                chunk = todo[start:start + window]
                await download_all(client, channel, folder, manifest, message_cache, chunk)
                await send_all(
                    client, target, folder, manifest, silent,
                    upto_n=chunk[-1]["n"], delete_after=True,
                )
            info(
                "All items sent in the original order; files deleted.",
                "Tum ogeler sirayla gonderildi, dosyalar silindi.",
                Fore.GREEN,
            )
        except OrderStop as stop:
            print(f"\n{Fore.RED}STOPPED: {stop}{Style.RESET_ALL}")
            info(
                "Progress is saved; run again to resume from this exact item.",
                "Ilerleme kaydedildi; tekrar calistirinca tam bu ogeden devam eder.",
                Fore.YELLOW,
            )


if __name__ == "__main__":
    asyncio.run(main())
