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
import difflib
import errno
import hashlib
import json
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

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

# --- automatic pacing ---
# SEND_DELAY is only the starting value. After every FloodWait the pause is
# recalculated so the long-run speed fits what Telegram allowed (work time
# vs. ban time, plus a safety margin); after a long stretch without bans it
# is lowered a little again. The learned value is saved and reused next run.
SEND_DELAY_MIN = float(os.getenv("SEND_DELAY_MIN", "1"))
SEND_DELAY_MAX = float(os.getenv("SEND_DELAY_MAX", "60"))
PACING_SAFETY = 1.3          # aim 30% below the speed that got banned
PACING_RELAX_AFTER = 1800    # seconds without a ban before speeding up again
PACING_RELAX_FACTOR = 0.9    # each relax step shortens the pause by 10%
PACING_NAME = "pacing.json"


class Pacer:
    def __init__(self):
        self.delay = SEND_DELAY
        self.loaded = False
        now = time.monotonic()
        self.work_since = now      # start of the current ban-free stretch
        self.last_relax = now
        self.last_step = None      # end of the previous pause
        self.cycles = []           # recent real time per posting step
        self.relax_after = PACING_RELAX_AFTER

    def _path(self):
        return MIRRORS_DIR / PACING_NAME

    def load(self):
        if self.loaded:
            return
        self.loaded = True
        try:
            saved = json.loads(self._path().read_text(encoding="utf-8"))
            self.delay = min(max(float(saved["delay"]), SEND_DELAY_MIN), SEND_DELAY_MAX)
            info(
                f"Learned send delay: {self.delay:.1f}s",
                f"Ogrenilmis bekleme suresi: {self.delay:.1f} sn",
            )
        except (OSError, ValueError, KeyError, TypeError):
            pass

    def save(self):
        try:
            MIRRORS_DIR.mkdir(parents=True, exist_ok=True)
            self._path().write_text(
                json.dumps({"delay": round(self.delay, 2),
                            "updated": datetime.now(timezone.utc).isoformat()}),
                encoding="utf-8",
            )
        except OSError:
            pass

    async def pause(self):
        """Wait between two posts; also measures how long a step really takes."""
        self.load()
        now = time.monotonic()
        if self.last_step is not None:
            self.cycles.append(now - self.last_step + self.delay)
            del self.cycles[:-50]
        if (now - self.work_since >= self.relax_after
                and now - self.last_relax >= self.relax_after
                and self.delay > SEND_DELAY_MIN):
            self.last_relax = now
            self.delay = max(SEND_DELAY_MIN, self.delay * PACING_RELAX_FACTOR)
            self.save()
            info(
                f"No ban for a while; send delay lowered to {self.delay:.1f}s",
                f"Uzun suredir ban yok; bekleme {self.delay:.1f} sn'ye dusuruldu",
                Fore.GREEN,
            )
        await asyncio.sleep(self.delay)
        self.last_step = time.monotonic()

    async def flood_wait(self, seconds: int):
        """Recalculate the pause from this ban, then sit the ban out."""
        self.load()
        now = time.monotonic()
        work = max(now - self.work_since, 1.0)
        if self.cycles:
            ordered = sorted(self.cycles)
            cycle = ordered[len(ordered) // 2]
        else:
            cycle = self.delay + 1.5
        overhead = max(cycle - self.delay, 0.0)
        needed = cycle * (work + seconds) / work * PACING_SAFETY
        old = self.delay
        self.delay = min(SEND_DELAY_MAX, max(SEND_DELAY_MIN, needed - overhead, old * 1.1))
        # Only speed up again after staying ban-free for twice as long as it
        # took to get banned this time.
        self.relax_after = max(PACING_RELAX_AFTER, 2 * work)
        self.save()
        minutes = (seconds + 59) // 60
        info(
            f"Flood wait {seconds}s (~{minutes} min). Worked {work / 60:.0f} min before it; "
            f"send delay {old:.1f}s -> {self.delay:.1f}s",
            f"Telegram {seconds} sn (~{minutes} dk) bekletiyor. Oncesinde {work / 60:.0f} dk calisti; "
            f"bekleme {old:.1f} sn -> {self.delay:.1f} sn",
            Fore.YELLOW,
        )
        await asyncio.sleep(seconds + 1)
        now = time.monotonic()
        self.work_since = self.last_relax = now
        self.last_step = None


pacer = Pacer()

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
# Re-using an earlier mirror: when set (REUSE_REQUIRE_SAME_MEDIA_ID=1), two files
# only count as the same if Telegram's media ids are equal too (strictest
# check; a chat that was re-created by RE-UPLOADING gets new ids, so with this
# on nothing would be reused in that case).
REUSE_REQUIRE_SAME_MEDIA_ID = os.getenv("REUSE_REQUIRE_SAME_MEDIA_ID", "0") == "1"

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
# Local file safety
# ---------------------------------------------------------------------------
class UnsafePathError(ValueError):
    """A manifest file name that would leave the mirror folder."""


def safe_path(folder: Path, name) -> Path:
    """folder / name, but only when `name` is a plain file name that really
    lives directly inside `folder` (symlinks resolved). A damaged or edited
    manifest such as "../outside.bin" can therefore never make the program
    read, overwrite or delete anything outside its own mirror folder."""
    if (
        not isinstance(name, str)
        or not name
        or name in (".", "..")
        or "/" in name
        or "\\" in name
        or "\x00" in name
    ):
        raise UnsafePathError(f"unsafe file name in manifest: {name!r}")
    base = folder.resolve()
    path = base / name
    try:
        resolved = path.resolve()
    except (OSError, RuntimeError):
        raise UnsafePathError(f"unsafe file name in manifest: {name!r}") from None
    if resolved.parent != base:
        raise UnsafePathError(f"file name leaves the mirror folder: {name!r}")
    return folder / name


def check_manifest_paths(folder: Path, manifest):
    """Refuse to run on a manifest whose file names point outside the folder."""
    for item in manifest["items"]:
        for part in item["parts"]:
            file_info = part.get("file")
            if not file_info:
                continue
            for key in ("name", "thumb"):
                if file_info.get(key):
                    try:
                        safe_path(folder, file_info[key])
                    except UnsafePathError as error:
                        raise OrderStop(
                            f"{error} (item #{item['n']:05d}). The manifest looks damaged or edited. "
                            f"(manifest.json bozuk veya degistirilmis gorunuyor.)",
                            fatal=True,
                        ) from None


def sha256_of(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def looks_like_image(path: Path) -> bool:
    """Cheap sanity check for downloaded photos: known image signature and,
    for JPEG, the end-of-image marker (i.e. not cut off)."""
    try:
        with open(path, "rb") as handle:
            head = handle.read(12)
            if head.startswith(b"\xff\xd8\xff"):
                handle.seek(0, os.SEEK_END)
                size = handle.tell()
                handle.seek(max(0, size - 1024))
                return b"\xff\xd9" in handle.read()
            return (
                head.startswith((b"\x89PNG\r\n\x1a\n", b"GIF8"))
                or (head[:4] == b"RIFF" and head[8:12] == b"WEBP")
            )
    except OSError:
        return False


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


def canonical_sent_key(key) -> str:
    """"-1002974652532", "2974652532" and "2974652532:7" style keys all name the
    same target; the canonical form is the bare id plus an optional ":topic"."""
    match = re.fullmatch(r"(?:-100)?(-?\d+)(?::(\d+))?", str(key))
    if not match:
        return str(key)
    return match.group(1) + (f":{match.group(2)}" if match.group(2) else "")


def get_progress(manifest, target, create=False, topic_id=None):
    """Find (or create) the per-target progress record.

    One record per (target chat, target topic): posting the same source into a
    different topic of the same chat must not inherit another topic's
    progress. The key is "<chat id>" without a topic and "<chat id>:<topic id>"
    with one, and the record stores `target_topic_id`.

    Older runs / helper scripts stored the chat id as "2974652532" and others
    as "-1002974652532"; both spellings are merged into the canonical key
    (keeping the furthest progress). A legacy record that has no
    `target_topic_id` at all (written before topics were tracked) is adopted
    by the first topic that asks for it. A record that belongs to a DIFFERENT
    topic is never merged."""
    sent = manifest.setdefault("sent", {})
    ids = list(dict.fromkeys((str(target.id), str(utils.get_peer_id(target)))))
    canonical = ids[0] if topic_id is None else f"{ids[0]}:{topic_id}"

    candidates = []
    for ident in ids:
        key = ident if topic_id is None else f"{ident}:{topic_id}"
        if key in sent:
            candidates.append((key, sent[key]))
    if topic_id is not None:
        for ident in ids:  # legacy plain-key record, never tied to a topic
            record = sent.get(ident)
            if record is not None and record.get("target_topic_id") in (None, topic_id):
                candidates.append((ident, record))
    else:
        candidates = [(k, r) for k, r in candidates if r.get("target_topic_id") is None]

    if candidates:
        best = max((r for _, r in candidates), key=lambda r: r.get("last_n", 0))
        for key, _ in candidates:
            sent.pop(key, None)
        if topic_id is not None:
            best["target_topic_id"] = topic_id
        sent[canonical] = best
        return best
    if create:
        record = {"title": getattr(target, "title", canonical), "last_n": 0}
        if topic_id is not None:
            record["target_topic_id"] = topic_id
        sent[canonical] = record
        return record
    return None


def target_floor(manifest, target=None) -> int:
    """Highest item number that EVERY relevant target already received.

    With a target: the least advanced record of that chat (its topics are kept
    apart). Without one: the least advanced record of all targets, because an
    item is still needed as long as any of them has not got it."""
    values = []
    for key, record in (manifest.get("sent") or {}).items():
        if target is not None and canonical_sent_key(key).split(":")[0] != str(target.id):
            continue
        values.append(record.get("last_n", 0))
    return min(values) if values else 0


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


def photo_dims(message):
    """Width/height of the largest size of a photo (None if unknown)."""
    best = None
    for size in getattr(getattr(message, "photo", None), "sizes", None) or []:
        w, h = getattr(size, "w", None), getattr(size, "h", None)
        if w and h and (best is None or w * h > best[0] * best[1]):
            best = (w, h)
    return list(best) if best else None


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
            # Telegram's own id of the media: equal ids = the very same file.
            "media_id": getattr(document, "id", None) if document else getattr(getattr(message, "photo", None), "id", None),
            "dims": None if document else photo_dims(message),
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
    # Items up to this number were already posted to some target. A part of an
    # album that shows up AFTER that must not be glued into the already-sent
    # item (it would be treated as sent and silently dropped): it becomes an
    # item of its own and is posted right after.
    sent_boundary = max((r.get("last_n", 0) for r in (manifest.get("sent") or {}).values()), default=0)

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
        continues_album = bool(
            message.grouped_id
            and last is not None
            and last.get("grouped_id") == message.grouped_id
        )
        same_album = continues_album and last["n"] > sent_boundary

        if same_album:
            last["parts"].append(build_part(message, last["n"], len(last["parts"])))
        else:
            n += 1
            new_item = {
                "n": n,
                "grouped_id": message.grouped_id,
                "link_preview": isinstance(message.media, types.MessageMediaWebPage),
                "parts": [build_part(message, n, 0 if message.grouped_id else None)],
            }
            if continues_album:
                new_item["late_album_part"] = True
                info(
                    f"Album part #{message.id} arrived after its album was already sent; it becomes item #{n:05d}.",
                    f"Albume sonradan eklenen parca #{n:05d} numarali ayri oge oldu.",
                    Fore.YELLOW,
                )
            items.append(new_item)
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
    try:
        path = safe_path(folder, file_info["name"])
    except UnsafePathError:
        return False
    if not path.is_file():  # a directory (or anything else) is not a finished download
        return False
    size = path.stat().st_size
    if size <= 0:
        return False
    # Photo "size" is the largest PhotoSize's estimate, which does not always
    # match the saved JPEG byte-for-byte, so a photo is checked by content
    # (image signature, JPEG not cut off) instead of by size.
    if file_info["type"] == "photo":
        return looks_like_image(path)
    if not file_info.get("size"):
        return True
    return size == file_info["size"]


async def download_part(client, channel, message, part, folder, semaphore):
    file_info = part["file"]
    try:
        path = safe_path(folder, file_info["name"])
    except UnsafePathError as error:
        raise OrderStop(f"{error} (manifest.json bozuk.)", fatal=True) from None
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
                if file_info["type"] == "photo" and not looks_like_image(tmp):
                    tmp.unlink(missing_ok=True)  # an invalid image must not count as downloaded
                    raise RuntimeError("downloaded photo is not a valid image")
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

        try:
            # Content fingerprint: lets a later run verify a file before re-using it.
            file_info["sha256"] = await asyncio.get_running_loop().run_in_executor(None, sha256_of, path)
        except OSError:
            file_info.pop("sha256", None)

        if file_info.get("thumb"):
            try:
                thumb_path = safe_path(folder, file_info["thumb"])
                if not thumb_path.exists():
                    await client.download_media(message, file=str(thumb_path), thumb=-1)
            except (RPCError, OSError, TypeError, ValueError):
                file_info["thumb"] = None  # optional; Telegram generates one if missing

        tick()
        print(f"{Fore.GREEN}OK{Style.RESET_ALL} {file_info['name']}")
        return True


async def download_all(client, channel, folder: Path, manifest, message_cache, items=None):
    if items is None:
        # Items already sent to EVERY known target were probably deleted
        # locally (delete-after-send); do not download them again. One target
        # having received an item says nothing about the others.
        sent_upto = target_floor(manifest)
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
    try:
        path = safe_path(folder, file_info["name"])
    except UnsafePathError as error:
        raise OrderStop(f"{error} (manifest.json bozuk.)", fatal=True) from None
    if not file_is_complete(folder, file_info):
        raise MissingFileStop(
            f"File missing or incomplete: {file_info['name']} - run the download step first. "
            f"(Dosya eksik: once indirme adimini calistirin.)"
        )
    uploaded = await upload_path(client, path, file_info["name"])
    if file_info["type"] == "photo":
        return types.InputMediaUploadedPhoto(file=uploaded)

    thumb = None
    if file_info.get("thumb"):
        try:
            thumb_path = safe_path(folder, file_info["thumb"])
        except UnsafePathError:
            thumb_path = None
        if thumb_path is not None and thumb_path.is_file():
            thumb = await with_timeout(client.upload_file(str(thumb_path)), "upload thumbnail")

    return types.InputMediaUploadedDocument(
        file=uploaded,
        mime_type=file_info.get("mime") or "application/octet-stream",
        attributes=build_attributes(file_info),
        force_file=file_info["type"] == "document",
        nosound_video=True if file_info["type"] == "gif" else None,
        thumb=thumb,
    )


def _created_ids(result):
    """Message ids out of whatever send_message/send_file returned."""
    if result is None:
        return []
    if isinstance(result, (list, tuple)):
        return [m.id for m in result if getattr(m, "id", None)]
    return [result.id] if getattr(result, "id", None) else []


async def send_item(client, target, folder: Path, item, silent: bool, medias, topic_id=None):
    """Post one item; returns the highest id of the messages it created (0 if none)."""
    parts = [p for p in item["parts"] if not p.get("skip")]
    media_parts = [p for p in parts if p.get("file")]
    texts = [p["text"] for p in parts]
    entities = [build_entities(p["entities"]) for p in parts]
    created = []

    if not media_parts:
        if not texts[0].strip():
            return 0
        created += _created_ids(await tg_send_message(client,
            target,
            texts[0],
            formatting_entities=entities[0] or None,
            parse_mode=None,
            link_preview=item.get("link_preview", False),
            silent=silent,
            reply_to=topic_id,
        ))
        return max(created, default=0)

    if not medias:
        for part in media_parts:
            medias.append(await build_media(client, folder, part["file"]))

    try:
        if len(medias) == 1:
            created += _created_ids(await tg_send_file(client,
                target,
                medias[0],
                caption=texts[0],
                formatting_entities=entities[0] or None,
                parse_mode=None,
                silent=silent,
                reply_to=topic_id,
            ))
        else:
            created += _created_ids(await tg_send_file(client,
                target,
                medias,
                caption=texts,
                formatting_entities=entities,
                parse_mode=None,
                silent=silent,
                reply_to=topic_id,
            ))
    except MediaCaptionTooLongError:
        # The source was posted by a Premium account with a longer caption
        # limit than ours: post the media bare, then the text right after it.
        # Re-upload, since the failed request may have consumed the file parts.
        medias[:] = [await build_media(client, folder, p["file"]) for p in media_parts]
        created += _created_ids(await tg_send_file(client,
            target, medias if len(medias) > 1 else medias[0], silent=silent, reply_to=topic_id,
        ))
        for text, ents in zip(texts, entities):
            if text.strip():
                created += _created_ids(await tg_send_message(client,
                    target, text, formatting_entities=ents or None, parse_mode=None,
                    link_preview=False, silent=silent, reply_to=topic_id,
                ))
    return max(created, default=0)


# ---- Delivery check -------------------------------------------------------
# A send can end with a lost connection AFTER Telegram accepted it. Sending
# again would post the item twice, so before any retry the target is asked
# whether the item already arrived.
def delivery_unclear(error: BaseException) -> bool:
    """True when the request may have reached Telegram although we saw an error."""
    return is_network_error(error) or isinstance(error, (TimeoutError, asyncio.TimeoutError))


async def newest_message_id(client, target, topic_id=None) -> int:
    async for message in client.iter_messages(target, limit=1, reply_to=topic_id):
        return message.id
    return 0


async def outgoing_after(client, target, topic_id, after_id):
    """Messages this account posted to the target after message `after_id`."""
    found = []
    async for message in client.iter_messages(target, min_id=after_id, reply_to=topic_id, limit=300):
        if isinstance(message, types.MessageService):
            continue
        if getattr(message, "out", False):
            found.append(message)
    found.sort(key=lambda m: m.id)
    return found


def _has_media(message) -> bool:
    return message.media is not None and not isinstance(message.media, types.MessageMediaWebPage)


def judge_delivery(item, sent) -> str:
    """'delivered', 'missing' or 'partial' for `item`, given the messages we
    posted after the item was first attempted."""
    parts = [p for p in item["parts"] if not p.get("skip")]
    media_parts = [p for p in parts if p.get("file")]
    texts = [(p.get("text") or "").strip() for p in parts]
    if not media_parts:
        want = texts[0] if texts else ""
        return "delivered" if any((m.message or "").strip() == want for m in sent) else "missing"
    media_msgs = [m for m in sent if _has_media(m)]
    if not media_msgs:
        return "missing"
    if len(media_msgs) < len(media_parts):
        return "partial"
    wanted = [t for t in texts if t]
    if wanted and not any((m.message or "").strip() for m in media_msgs):
        # Bare media + separate texts (the caption-too-long path): check the texts arrived too.
        extra = [m for m in sent if not _has_media(m) and (m.message or "").strip()]
        if len(extra) < len(wanted):
            return "partial"
    return "delivered"


async def settle_delivery(client, target, topic_id, after_id, judge, label):
    """Wait for the connection and find out whether `label` already arrived.
    Returns (state, messages); raises OrderStop when it cannot be decided."""
    for attempt in range(1, MAX_RETRY + 2):
        try:
            sent = await with_timeout(outgoing_after(client, target, topic_id, after_id), "check delivery")
        except FloodWaitError as error:
            await asyncio.sleep(error.seconds + 1)
            continue
        except (RPCError, OSError, ConnectionError) as error:
            if not delivery_unclear(error):
                raise
            await asyncio.sleep(min(3 * attempt, RETRY_DELAY_MAX))
            continue
        state = judge(sent)
        if state == "partial":
            raise OrderStop(
                f"{label} was only partly delivered to the target. Check the target channel, fix it by hand, "
                f"then remove the \"inflight\" entry (or advance last_n) in manifest.json. "
                f"(Oge hedefe yarim ulasmis; hedefi elle kontrol edip manifest.json icindeki \"inflight\" "
                f"kaydini silin veya last_n degerini ilerletin.)",
                fatal=True,
            )
        return state, sent
    raise OrderStop(f"Could not verify whether {label} arrived; will check again before sending it.")


class MissingFileStop(OrderStop):
    """A file to send isn't on disk (yet)."""


def delete_item_files(folder: Path, item):
    for part in item["parts"]:
        file_info = part.get("file")
        if not file_info:
            continue
        for key in ("name", "thumb"):
            if not file_info.get(key):
                continue
            try:
                path = safe_path(folder, file_info[key])
            except UnsafePathError as error:
                print(f"{Fore.RED}Not deleting: {error}{Style.RESET_ALL}")
                continue
            if path.is_file() or path.is_symlink():  # never touch a directory
                path.unlink(missing_ok=True)


async def send_all(client, target, folder: Path, manifest, silent: bool, upto_n=None, delete_after=False, topic_id=None):
    progress = get_progress(manifest, target, create=True, topic_id=topic_id)
    todo = [
        item
        for item in manifest["items"]
        if item["n"] > progress["last_n"] and (upto_n is None or item["n"] <= upto_n)
    ]
    if not todo:
        info("Everything has already been sent.", "Hepsi zaten gonderilmis.", Fore.GREEN)
        return

    # Highest message id in the target before we post anything; a delivery
    # check later only has to look at what came after it.
    last_id = await with_timeout(newest_message_id(client, target, topic_id), "read target")

    # An earlier run died while an item's delivery was unclear: find out first.
    inflight = progress.get("inflight")
    if inflight:
        first = todo[0]
        if inflight.get("n") == first["n"] and "after_id" in inflight:
            info(
                f"Checking whether #{first['n']:05d} arrived before the interruption...",
                f"#{first['n']:05d} kesintiden once ulasmis mi kontrol ediliyor...",
                Fore.YELLOW,
            )
            state, sent = await settle_delivery(
                client, target, topic_id, inflight["after_id"],
                lambda messages: judge_delivery(first, messages), f"#{first['n']:05d}",
            )
            progress.pop("inflight", None)
            if state == "delivered":
                progress["last_n"] = first["n"]
                tick()
                if delete_after:
                    delete_item_files(folder, first)
                todo.pop(0)
                if sent:
                    last_id = max(last_id, max(m.id for m in sent))
                info(
                    f"#{first['n']:05d} had already arrived; not sending it again.",
                    f"#{first['n']:05d} zaten ulasmis; tekrar gonderilmiyor.",
                    Fore.GREEN,
                )
        else:
            progress.pop("inflight", None)  # stale
        save_manifest(folder, manifest)

    for item in todo:
        posted = False
        if all(p.get("skip") for p in item["parts"]):
            reasons = ", ".join(p["skip"] for p in item["parts"])
            info(f"#{item['n']:05d} skipped ({reasons})", "atlandi - yeniden yuklenemeyen tur", Fore.YELLOW)
        else:
            medias = []  # reused across FloodWait retries so files upload once
            attempt = 0
            while True:
                try:
                    new_id = await send_item(client, target, folder, item, silent, medias, topic_id)
                    last_id = max(last_id, new_id or 0)
                    break
                except FloodWaitError as error:
                    await pacer.flood_wait(error.seconds)
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
                    if delivery_unclear(error):
                        # Telegram may have accepted the post although the answer
                        # got lost: never post again before looking. The marker is
                        # saved so even a restart checks first.
                        progress["inflight"] = {"n": item["n"], "after_id": last_id}
                        save_manifest(folder, manifest)
                        state, sent = await settle_delivery(
                            client, target, topic_id, last_id,
                            lambda messages, it=item: judge_delivery(it, messages), f"#{item['n']:05d}",
                        )
                        if state == "delivered":
                            if sent:
                                last_id = max(last_id, max(m.id for m in sent))
                            info(
                                f"#{item['n']:05d} had already arrived; not sending it again.",
                                f"#{item['n']:05d} zaten ulasmis; tekrar gonderilmiyor.",
                                Fore.GREEN,
                            )
                            break
                    if attempt > MAX_RETRY:
                        raise OrderStop(f"#{item['n']:05d} could not be sent: {error}")
                    await asyncio.sleep(3 * attempt)
            kinds = "+".join(
                TYPE_LABELS[p["file"]["type"]] if p.get("file") else TYPE_LABELS["text"]
                for p in item["parts"]
                if not p.get("skip")
            )
            print(f"{Fore.GREEN}SENT{Style.RESET_ALL} #{item['n']:05d}/{manifest['items'][-1]['n']:05d} {kinds}")
            posted = True

        # Progress is written the moment the post succeeded, BEFORE any pause,
        # so an interruption during the delay cannot cause a repeat.
        progress["last_n"] = item["n"]
        progress.pop("inflight", None)
        save_manifest(folder, manifest)
        tick()
        # Only after progress is saved: a crash in between leaves the file
        # on disk rather than losing an unsent one.
        if delete_after:
            delete_item_files(folder, item)
        if posted:
            await pacer.pause()

    if upto_n is None:
        info("All items sent in the original order.", "Tum ogeler orijinal sirayla gonderildi.", Fore.GREEN)


async def forward_all(client, channel, target, folder: Path, manifest, silent: bool, topic_id=None):
    """Copy without downloading: forward in order with the 'Forwarded from'
    header removed. Albums are never split across requests."""
    progress = get_progress(manifest, target, create=True, topic_id=topic_id)
    todo = [item for item in manifest["items"] if item["n"] > progress["last_n"]]
    chunk_items = []
    state = {"last_id": None}

    async def mark_done(last_n):
        progress["last_n"] = last_n
        progress.pop("inflight", None)
        save_manifest(folder, manifest)
        tick()

    def count_judge(expected):
        def judge(sent):
            if len(sent) >= expected:
                return "delivered"
            return "partial" if sent else "missing"
        return judge

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
                await pacer.flood_wait(error.seconds)
            except (RPCError, OSError, ConnectionError) as error:
                if not delivery_unclear(error):
                    raise
                # The forward may have gone through although the answer was lost.
                progress["inflight"] = {
                    "n": chunk_items[-1]["n"], "after_id": state["last_id"], "count": len(ids),
                }
                save_manifest(folder, manifest)
                result, _sent = await settle_delivery(
                    client, target, topic_id, state["last_id"], count_judge(len(ids)),
                    f"forward up to #{chunk_items[-1]['n']:05d}",
                )
                if result == "delivered":
                    break
                raise
        await mark_done(chunk_items[-1]["n"])
        state["last_id"] = await with_timeout(newest_message_id(client, target, topic_id), "read target")
        print(f"{Fore.GREEN}COPIED{Style.RESET_ALL} up to #{progress['last_n']:05d}")
        chunk_items.clear()
        await pacer.pause()

    try:
        if todo:
            state["last_id"] = await with_timeout(newest_message_id(client, target, topic_id), "read target")
            inflight = progress.get("inflight")
            if inflight and "count" in inflight and "after_id" in inflight:
                # An earlier run stopped while a forward's delivery was unclear.
                result, _sent = await settle_delivery(
                    client, target, topic_id, inflight["after_id"],
                    count_judge(inflight["count"]), f"forward up to #{inflight['n']:05d}",
                )
                if result == "delivered" and inflight["n"] > progress["last_n"]:
                    await mark_done(inflight["n"])
                    todo = [item for item in todo if item["n"] > progress["last_n"]]
                else:
                    progress.pop("inflight", None)
                    save_manifest(folder, manifest)
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


async def pick_topics(client, channel):
    """Like pick_topic, but several topics can be picked at once ("hepsi",
    "1,5,9-12", "10-"). Returns None for the whole chat, otherwise a list of
    (topic_id, topic_title) in the order typed."""
    info("Loading topics...", "Alt basliklar yukleniyor...")
    try:
        topics = await load_topics(client, channel)
    except RPCError as error:
        info(f"Could not load topics: {error}", "Alt basliklar yuklenemedi, tum sohbet kullanilacak.", Fore.RED)
        return None
    if not topics:
        return None

    print(f"\n{Fore.CYAN}Topics / Alt basliklar ({len(topics)}):{Style.RESET_ALL}")
    print("  0. Whole chat as one piece (Tum sohbet tek parca, alt basliklar karisik)")
    print("  hepsi. All topics, each kept separate (Tum alt basliklar, her biri ayri)")
    for index, topic in enumerate(topics, 1):
        print(f"  {index}. {topic.title}")
    print(
        "\nExamples / Ornekler:  hepsi  |  3-40  |  1,5,9-12  |  10-  (10'dan sona kadar)  |  0"
    )
    while True:
        raw = input(
            f"{Fore.CYAN}Select topic(s) (Alt baslik(lar) secin) [0]: {Style.RESET_ALL}"
        ).strip()
        if raw in ("", "0"):
            return None
        numbers = parse_topic_selection(raw, len(topics))
        if numbers is not None:
            break
        info(
            f"Invalid selection; use 0 or numbers 1-{len(topics)}, commas and dashes.",
            f"Gecersiz secim; 0 ya da 1-{len(topics)} arasi sayi, virgul ve tire kullanin.",
            Fore.RED,
        )
    chosen = [topics[number - 1] for number in numbers]
    info(f"{len(chosen)} topic(s) selected.", f"{len(chosen)} alt baslik secildi.", Fore.GREEN)
    return [(topic.id, topic.title) for topic in chosen]


async def resolve_target_topic(client, target, topic_title, known_topics=None):
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
        # known_topics: a list loaded once by the caller (all-topics mode asks
        # for every source topic, so loading it again each time is wasteful).
        topics = known_topics if known_topics is not None else await load_topics(client, target)
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
            if known_topics is not None:
                known_topics.append(SimpleNamespace(id=update.message.id, title=topic_title))
            return update.message.id
    raise OrderStop(
        "Target topic was created but its ID could not be determined. "
        "(Hedefte alt baslik olusturuldu ama ID'si tespit edilemedi.)"
    )


# ---------------------------------------------------------------------------
# Re-using an earlier mirror (the source channel was closed and re-opened
# under another name with the same content)
# ---------------------------------------------------------------------------
def find_old_mirrors(folder: Path, manifest):
    """Other mirror folders of the same topic title but a different chat."""
    found = []
    if not MIRRORS_DIR.exists():
        return found
    source_id = (manifest.get("source") or {}).get("id")
    for candidate in sorted(MIRRORS_DIR.iterdir()):
        if not candidate.is_dir() or candidate.resolve() == folder.resolve():
            continue
        try:
            old = load_manifest(candidate)
        except (OSError, ValueError):
            continue
        if not old or not old.get("items"):
            continue
        if (old.get("source") or {}).get("id") == source_id:
            continue
        if (old.get("topic_title") or None) != (manifest.get("topic_title") or None):
            continue
        found.append((candidate, old))
    return found


def other_mirrors_exist(source_id) -> bool:
    if not MIRRORS_DIR.exists():
        return False
    for candidate in MIRRORS_DIR.iterdir():
        try:
            old = load_manifest(candidate) if candidate.is_dir() else None
        except (OSError, ValueError):
            continue
        if old and (old.get("source") or {}).get("id") != source_id:
            return True
    return False


def link_or_copy(source: Path, destination: Path) -> bool:
    """Hard-link (no extra disk space) or copy; False if it failed."""
    import shutil

    try:
        tmp = destination.with_name(destination.name + ".part")
        tmp.unlink(missing_ok=True)
        try:
            os.link(source, tmp)
        except OSError:
            shutil.copy2(source, tmp)
        os.replace(tmp, destination)
        return True
    except OSError:
        return False


# Only these attribute fields describe the CONTENT of a media file; flags such
# as supports_streaming legitimately differ between two uploads of one file.
CORE_ATTRIBUTE_FIELDS = {
    "DocumentAttributeVideo": ("duration", "w", "h", "round_message"),
    "DocumentAttributeAudio": ("duration", "voice", "title", "performer"),
    "DocumentAttributeImageSize": ("w", "h"),
    "DocumentAttributeAnimated": (),
}


def core_attributes(file_info):
    result = []
    for raw in file_info.get("attributes") or []:
        fields = CORE_ATTRIBUTE_FIELDS.get(raw.get("_"))
        if fields is None:
            continue
        values = []
        for field in fields:
            value = raw.get(field)
            if isinstance(value, float):
                value = round(value, 1)
            values.append(value or None)
        result.append([raw["_"], values])
    return sorted(result, key=lambda entry: json.dumps(entry, sort_keys=True))


def same_file(new_file, old_file) -> bool:
    """Same type, size and original name AND the same content descriptors
    (mime type, duration/dimensions/tags, photo size). When both sides carry
    Telegram's media id and it is equal, it is the very same file. Ids that
    differ are normal for a re-created chat, so they only veto the match when
    REUSE_REQUIRE_SAME_MEDIA_ID is on; the old file's content hash is checked
    separately before the file is actually re-used."""
    if new_file["type"] != old_file["type"]:
        return False
    if (new_file.get("size") or 0) != (old_file.get("size") or 0):
        return False
    if (new_file.get("orig_name") or "") != (old_file.get("orig_name") or ""):
        return False
    new_mime, old_mime = new_file.get("mime"), old_file.get("mime")
    if new_mime and old_mime and new_mime != old_mime:
        return False
    if core_attributes(new_file) != core_attributes(old_file):
        return False
    new_dims, old_dims = new_file.get("dims"), old_file.get("dims")
    if new_dims and old_dims and new_dims != old_dims:
        return False
    new_id, old_id = new_file.get("media_id"), old_file.get("media_id")
    if REUSE_REQUIRE_SAME_MEDIA_ID and new_id and old_id and new_id != old_id:
        return False
    return True


def item_signature(item) -> str:
    """Fingerprint of everything that decides what gets posted: text, message
    entities (bold, links, spoilers...), link-preview flag and the files'
    content descriptors, per part."""
    parts = []
    for part in item["parts"]:
        file_info = part.get("file")
        parts.append([
            part.get("text") or "",
            json.dumps(part.get("entities") or [], sort_keys=True, ensure_ascii=False),
            part.get("skip") or "",
            [file_info["type"], file_info.get("size") or 0, file_info.get("orig_name") or "",
             core_attributes(file_info)] if file_info else None,
        ])
    payload = json.dumps([bool(item.get("link_preview")), parts], sort_keys=True, ensure_ascii=False)
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()


def _files_match(new_item, old_item) -> bool:
    if len(new_item["parts"]) != len(old_item["parts"]):
        return False
    for new_part, old_part in zip(new_item["parts"], old_item["parts"]):
        new_file, old_file = new_part.get("file"), old_part.get("file")
        if bool(new_file) != bool(old_file):
            return False
        if new_file and not same_file(new_file, old_file):
            return False
    return True


def items_match(new_item, old_item) -> bool:
    """Same post: identical text, entities, link-preview flag, part count and
    files (see same_file)."""
    if old_item is None:
        return False
    return item_signature(new_item) == item_signature(old_item) and _files_match(new_item, old_item)


def align_items(new_items, old_items):
    """Map new item number -> matching old item, by CONTENT instead of by
    position, so a message inserted into or deleted from the new chat does not
    shift every later item out of alignment."""
    new_keys = [item_signature(i) for i in new_items]
    old_keys = [item_signature(i) for i in old_items]
    matcher = difflib.SequenceMatcher(None, new_keys, old_keys, autojunk=False)
    mapping = {}
    for new_start, old_start, size in matcher.get_matching_blocks():
        for offset in range(size):
            new_item, old_item = new_items[new_start + offset], old_items[old_start + offset]
            if _files_match(new_item, old_item):
                mapping[new_item["n"]] = old_item
    return mapping


def old_file_intact(old_folder: Path, old_file) -> bool:
    """The old copy must still be what was downloaded (hash recorded at download time)."""
    expected = old_file.get("sha256")
    if not expected:
        return True  # downloaded by an older version; nothing to compare with
    try:
        return sha256_of(safe_path(old_folder, old_file["name"])) == expected
    except (UnsafePathError, OSError):
        return False


def reuse_old_mirrors(folder: Path, manifest, target=None):
    """The source chat was replaced by a new one (different chat ID) whose topic
    has the same title as an earlier mirror's. Items are paired with the old
    mirror's items by CONTENT (text, entities, link preview, file type, name,
    size, duration/dimensions), not by position.

    1. Sent progress: the leading run of items that match an old item that an
       earlier mirror already posted to a target is not posted again; that
       target continues after the first item that differs. This is redone
       whenever new items were scanned, not only the first time.
    2. Files: for matching items still to be sent, a complete, unmodified file
       from the old mirror folder is linked/copied here instead of being
       downloaded.
    Items that do not match are downloaded and sent normally.

    `target`, when known, limits step 2 to what that chat still needs; without
    it the least advanced target decides."""
    items = manifest["items"]
    done_upto = manifest.get("reuse_upto", 0)
    if not items or items[-1]["n"] <= done_upto:
        return
    manifest["reuse_upto"] = items[-1]["n"]
    olds = find_old_mirrors(folder, manifest)
    if not olds:
        info("No earlier mirror with the same topic title found.",
             "Ayni alt baslik adina sahip onceki bir aktarim bulunamadi.", Fore.YELLOW)
        return
    aligned = [(old_folder, old, align_items(items, old["items"])) for old_folder, old in olds]

    sent = manifest.setdefault("sent", {})
    for old_folder, old, mapping in aligned:
        for key, record in (old.get("sent") or {}).items():
            last_old = record.get("last_n", 0)
            if not last_old:
                continue
            canonical = canonical_sent_key(key)
            variants = [k for k in sent if canonical_sent_key(k) == canonical]
            if any(sent[k].get("no_inherit") for k in variants):
                continue  # the user chose "start over" for this target
            current = max((sent[k].get("last_n", 0) for k in variants), default=0)

            last_new, stopped = 0, None
            for index, item in enumerate(items):
                old_item = mapping.get(item["n"])
                if old_item is not None and old_item["n"] <= last_old:
                    last_new = item["n"]
                else:
                    stopped = index
                    break
            title = record.get("title", key)
            if last_new > current:
                merged = {"title": title, "last_n": last_new, "inherited_from": old_folder.name}
                if record.get("target_topic_id") is not None:
                    merged["target_topic_id"] = record["target_topic_id"]
                for k in variants:
                    sent.pop(k, None)
                sent[canonical] = merged
                info(
                    f"Items up to #{last_new:05d} were already posted to '{title}' "
                    f"by the earlier mirror '{old_folder.name}'; they will not be sent again.",
                    f"#{last_new:05d} numaraya kadar olan ogeler daha once '{title}' "
                    f"kanalina gonderilmis; tekrar gonderilmeyecek.",
                    Fore.GREEN,
                )
            if stopped is not None and any(
                mapping.get(later["n"]) is not None and mapping[later["n"]]["n"] <= last_old
                for later in items[stopped + 1:]
            ):
                info(
                    f"Item #{items[stopped]['n']:05d} differs from the earlier mirror (text/format/file/count); "
                    f"sending resumes from there.",
                    f"#{items[stopped]['n']:05d} numarali oge eskisinden farkli (metin/bicim/dosya/adet); "
                    f"gonderim oradan itibaren yapilacak.",
                    Fore.YELLOW,
                )

    floor = target_floor(manifest, target)
    reused = 0
    for item in items:
        if item["n"] <= max(floor, done_upto):
            continue  # already posted, or handled by an earlier call
        for index, part in enumerate(item["parts"]):
            file_info = part.get("file")
            if not file_info or part.get("skip") or file_is_complete(folder, file_info):
                continue
            try:
                destination = safe_path(folder, file_info["name"])
            except UnsafePathError:
                continue
            for old_folder, _old, mapping in aligned:
                old_item = mapping.get(item["n"])
                if old_item is None or index >= len(old_item["parts"]):
                    continue
                old_file = old_item["parts"][index].get("file")
                if not old_file or not file_is_complete(old_folder, old_file):
                    continue  # deleted after sending; nothing to reuse
                if not old_file_intact(old_folder, old_file):
                    continue  # the old copy changed since it was downloaded
                try:
                    source = safe_path(old_folder, old_file["name"])
                except UnsafePathError:
                    continue
                if link_or_copy(source, destination):
                    reused += 1
                    if old_file.get("sha256"):
                        file_info["sha256"] = old_file["sha256"]
                    thumb = old_file.get("thumb")
                    if thumb and file_info.get("thumb"):
                        try:
                            old_thumb = safe_path(old_folder, thumb)
                            new_thumb = safe_path(folder, file_info["thumb"])
                        except UnsafePathError:
                            old_thumb = new_thumb = None
                        if old_thumb is not None and old_thumb.is_file():
                            link_or_copy(old_thumb, new_thumb)
                    break
    if reused:
        info(f"{reused} file(s) reused from the earlier mirror; they will not be downloaded again.",
             f"{reused} dosya onceki aktarimdan alindi; tekrar indirilmeyecek.", Fore.GREEN)


def find_existing_mirrors(channel, topic_id, base_name):
    """Mirror folders of exactly this chat and topic, oldest first.

    Found by the source id stored in the manifest, NOT by folder name: the
    name contains the chat title, and a renamed chat would otherwise lose its
    progress. Manifests without a stored source id fall back to the name."""
    found = []
    if not MIRRORS_DIR.exists():
        return found
    legacy = re.compile(
        re.escape(base_name) + r"(_(\d+|bas)-(\d+|son))?(_v\d+)?"
    )
    for candidate in MIRRORS_DIR.iterdir():
        if not candidate.is_dir() or not (candidate / MANIFEST_NAME).exists():
            continue
        try:
            manifest = load_manifest(candidate)
        except (OSError, ValueError):
            continue
        if not manifest:
            continue
        source_id = (manifest.get("source") or {}).get("id")
        if source_id is not None:
            if source_id == channel.id and (manifest.get("topic_id") or None) == (topic_id or None):
                found.append((candidate, manifest))
        elif legacy.fullmatch(candidate.name):
            found.append((candidate, manifest))
    found.sort(key=lambda entry: (entry[1].get("created") or "", entry[0].name))
    return found


async def choose_source(client, channel=None, topic_override=None, auto_resume=False, reuse_old=None):
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

    existing = find_existing_mirrors(channel, topic_id, base_name)
    folder = None
    if auto_resume:
        # The whole-chat mirror (no range); if several exist, the newest one.
        whole = [p for p, m in existing if not m.get("min_id") and not m.get("max_id")]
        if whole:
            folder = whole[-1]
    else:
        for candidate, _manifest in reversed(existing):
            if ask_yes_no(
                f"Continue the existing mirror '{candidate.name}' (only new messages are scanned)?",
                f"Mevcut '{candidate.name}' klasorunden devam edilsin mi (sadece yeni mesajlar taranir)?",
            ):
                folder = candidate
                break

    if folder:
        manifest = load_manifest(folder)
        changed = False
        # An explicit choice (the all-topics plan) overrides what the saved
        # manifest says; None means "no opinion", keep the saved value.
        if reuse_old is not None and bool(manifest.get("reuse_old")) != bool(reuse_old):
            manifest["reuse_old"] = bool(reuse_old)
            changed = True
        source = manifest.setdefault("source", {"id": channel.id})
        if source.get("title") != getattr(channel, "title", ""):
            source["title"] = getattr(channel, "title", "")  # chat was renamed
            changed = True
        if changed:
            save_manifest(folder, manifest)

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
        # Never reuse the name of a folder that already holds a manifest (the
        # user declined to continue it): writing a fresh manifest there would
        # wipe the saved progress.
        folder = MIRRORS_DIR / (base_name + suffix)
        version = 2
        while (folder / MANIFEST_NAME).exists():
            folder = MIRRORS_DIR / f"{base_name}{suffix}_v{version}"
            version += 1
        folder.mkdir(parents=True, exist_ok=True)
        if reuse_old is None:
            reuse_old = (not auto_resume) and other_mirrors_exist(channel.id) and ask_yes_no(
                "Was this chat re-created from an earlier one (same topic title, file name and size = same content)? "
                "Skip what was already downloaded/sent?",
                "Bu sohbet eskisinin yeniden acilmis hali mi (ayni alt baslik adi = ayni icerik)? "
                "O alt basliklar icin daha once indirilen/gonderilenler atlansin mi?",
            )
        manifest = {
            "reuse_old": bool(reuse_old),
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


async def scan_and_report(client, channel, folder: Path, manifest, message_cache, target=None):
    await scan_chat(client, channel, manifest, message_cache, folder)
    if manifest.get("reuse_old"):
        reuse_old_mirrors(folder, manifest, target)
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


async def prepare_source(client, message_cache, channel=None, topic_override=None, auto_resume=False, reuse_old=None, target=None):
    """choose_source + incremental scan; used by the all-topics mode."""
    channel, folder = await choose_source(client, channel, topic_override, auto_resume, reuse_old)
    manifest = load_manifest(folder)
    await scan_and_report(client, channel, folder, manifest, message_cache, target)
    return channel, folder, manifest


def pick_existing_folders():
    """List the downloaded mirror folders and let the user pick one or more
    ("hepsi", "1,3", "2-5"). Returns a list of folders (or None)."""
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
    print("\nExamples / Ornekler:  2  |  all (hepsi)  |  1,3  |  2-5")
    while True:
        raw = input(f"{Fore.CYAN}Select folder(s) (Klasor(ler) secin): {Style.RESET_ALL}").strip()
        if not raw:
            continue
        numbers = parse_topic_selection(raw, len(folders))
        if numbers:
            return [folders[number - 1] for number in numbers]
        info(
            f"Invalid selection; use numbers 1-{len(folders)}, commas and dashes.",
            f"Gecersiz secim; 1-{len(folders)} arasi sayi, virgul ve tire kullanin.",
            Fore.RED,
        )


async def ask_target_and_options(client, manifest):
    target = await ask_chat(client, "Target channel to post into", "Gonderilecek hedef kanal")
    target_topic_id = await resolve_target_topic(client, target, manifest.get("topic_title"))
    progress = get_progress(manifest, target, topic_id=target_topic_id)
    if progress and progress["last_n"]:
        if not ask_yes_no(
            f"Resume after item #{progress['last_n']:05d}?",
            f"#{progress['last_n']:05d} numarali ogeden sonra devam edilsin mi? (Hayir = bastan)",
        ):
            progress["last_n"] = 0
            progress.pop("inflight", None)
            # Remember the choice, or an earlier mirror's progress would be
            # inherited again on the next scan.
            progress["no_inherit"] = True
    silent = ask_yes_no(
        "Post silently (no notification for each message)?",
        "Sessiz gonderilsin mi (her mesaj icin bildirim gitmesin)?",
    )
    return target, silent, target_topic_id


def parse_topic_selection(raw: str, total: int):
    """Turn input like "4-44", "1,5,9-12" or "10-" into 1-based topic numbers.

    Empty input, "all" or "hepsi" selects everything. Order is kept as typed
    and duplicates are dropped. Returns None if the text cannot be understood
    or a number is outside 1..total."""
    text = raw.strip().lower().replace(" ", "")
    if text in ("", "all", "hepsi", "tumu", "tümü", "*"):
        return list(range(1, total + 1))
    chosen, seen = [], set()
    for part in text.replace(";", ",").split(","):
        if not part:
            continue
        match = re.fullmatch(r"(\d+)(?:[-–—](\d*))?", part)
        if not match:
            return None
        start = int(match.group(1))
        if match.group(2) is None:
            end = start  # single number
        elif match.group(2) == "":
            end = total  # open range such as "10-"
        else:
            end = int(match.group(2))
        if start > end:
            start, end = end, start
        if start < 1 or end > total:
            return None
        for number in range(start, end + 1):
            if number not in seen:
                seen.add(number)
                chosen.append(number)
    return chosen or None


async def ask_topic_selection(client, channel):
    """List the source forum's topics and let the user pick some of them.
    Returns a list of topic IDs (stable even if the topic order changes)."""
    info("Loading topics...", "Alt basliklar yukleniyor...")
    topics = await load_topics(client, channel)
    if not topics:
        info("No topics found.", "Alt baslik bulunamadi.", Fore.RED)
        return None
    print(f"\n{Fore.CYAN}Topics / Alt basliklar ({len(topics)}):{Style.RESET_ALL}")
    for index, topic in enumerate(topics, 1):
        print(f"  {index}. {topic.title}")
    print(
        "\nExamples / Ornekler:  all (hepsi)  |  4-44  |  1,5,9-12  |  10-  (10'dan sona kadar)"
    )
    while True:
        raw = input(
            f"{Fore.CYAN}Topics to transfer (Aktarilacak alt basliklar) [all]: {Style.RESET_ALL}"
        )
        numbers = parse_topic_selection(raw, len(topics))
        if numbers is None:
            info(
                f"Invalid selection; use numbers 1-{len(topics)}, commas and dashes.",
                f"Gecersiz secim; 1-{len(topics)} arasi sayi, virgul ve tire kullanin.",
                Fore.RED,
            )
            continue
        break
    chosen = [topics[number - 1] for number in numbers]
    info(
        f"{len(chosen)} topic(s) selected.",
        f"{len(chosen)} alt baslik secildi.",
        Fore.GREEN,
    )
    return [topic.id for topic in chosen]


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
    topic_ids = await ask_topic_selection(client, channel)
    if not topic_ids:
        return None
    reuse_old = other_mirrors_exist(channel.id) and ask_yes_no(
        "Was this chat re-created from an earlier one (same topic titles = same content)? "
        "Skip what was already downloaded/sent for those topics?",
        "Bu sohbet eskisinin yeniden acilmis hali mi (ayni alt baslik adi = ayni icerik)? "
        "O alt basliklar icin daha once indirilen/gonderilenler atlansin mi?",
    )
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
        "topic_ids": topic_ids,
        "reuse_old": bool(reuse_old),
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

    # Only the topics picked at setup (older saved runs have no list = all topics).
    selected_ids = plan.get("topic_ids")
    if selected_ids:
        by_id = {topic.id: topic for topic in topics}
        topics = [by_id[topic_id] for topic_id in selected_ids if topic_id in by_id]
        if not topics:
            raise OrderStop(
                "None of the selected topics exist any more. (Secilen alt basliklar bulunamadi.)",
                fatal=True,
            )

    # The target's topic list is loaded once and reused for every source topic.
    try:
        target_topics = await load_topics(client, target)
    except RPCError:
        target_topics = None
    info(f"Transferring {len(topics)} topics.", f"{len(topics)} alt baslik aktarilacak.")
    for index, topic in enumerate(topics, 1):
        info(f"Topic {index}/{len(topics)}: {topic.title}",
             f"Alt baslik {index}/{len(topics)}: {topic.title}")
        _, folder, manifest = await prepare_source(
            client, message_cache, channel=channel,
            topic_override=(topic.id, topic.title), auto_resume=True,
            reuse_old=plan.get("reuse_old", False), target=target,
        )
        if not manifest["items"]:
            continue  # empty source topic: do not create an empty one on the target
        target_topic_id = await resolve_target_topic(client, target, topic.title, known_topics=target_topics)
        if not target_topic_id:
            raise OrderStop(f"Could not resolve target topic: {topic.title}")
        progress = get_progress(manifest, target, topic_id=target_topic_id) or {}
        todo = [item for item in manifest["items"] if item["n"] > progress.get("last_n", 0)]
        if not todo:
            save_manifest(folder, manifest)  # progress keys may have been normalised
            continue
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


async def download_and_send(client, channel, target, folder, manifest, message_cache,
                            silent, delete_after, topic_id, label):
    """Mode 3 work for one mirror folder (shared by the multi-topic runs)."""
    progress = get_progress(manifest, target, topic_id=topic_id) or {}
    todo = [item for item in manifest["items"] if item["n"] > progress.get("last_n", 0)]
    if not todo:
        save_manifest(folder, manifest)  # progress keys may have been normalised
        return
    if not delete_after:
        failed = await download_all(client, channel, folder, manifest, message_cache, todo)
        if failed:
            raise OrderStop(f"Download failed in {label}; will retry.")
        await send_all(client, target, folder, manifest, silent, topic_id=topic_id)
        return
    window = max(1, batch_size) * 2
    for start in range(0, len(todo), window):
        chunk = todo[start:start + window]
        failed = await download_all(client, channel, folder, manifest, message_cache, chunk)
        if failed:
            raise OrderStop(f"Download failed in {label}; will retry.")
        await send_all(
            client, target, folder, manifest, silent,
            upto_n=chunk[-1]["n"], delete_after=True, topic_id=topic_id,
        )


async def run_selected_topics(client, plan, message_cache):
    """Modes 1, 3 and 4 with several source topics picked: each topic gets its
    own mirror folder and is handled in turn, resuming independently. A forum
    target gets a same-titled topic per source topic; any other target
    receives the topics one after another."""
    mode = plan["mode"]
    channel = await resolve_peer(client, plan["source_peer"], "Source")
    target = None
    if plan.get("target_peer") is not None:
        target = await resolve_peer(client, plan["target_peer"], "Target")
    silent = plan.get("silent", False)
    topics = await load_topics(client, channel)
    by_id = {topic.id: topic for topic in topics}
    topics = [by_id[topic_id] for topic_id in plan["topic_ids"] if topic_id in by_id]
    if not topics:
        raise OrderStop(
            "None of the selected topics exist any more. (Secilen alt basliklar bulunamadi.)",
            fatal=True,
        )

    target_topics = None
    if target is not None and getattr(target, "forum", False):
        try:
            target_topics = await load_topics(client, target)
        except RPCError:
            target_topics = None
    info(f"{len(topics)} topics selected.", f"{len(topics)} alt baslik islenecek.")
    for index, topic in enumerate(topics, 1):
        info(f"Topic {index}/{len(topics)}: {topic.title}",
             f"Alt baslik {index}/{len(topics)}: {topic.title}")
        _, folder, manifest = await prepare_source(
            client, message_cache, channel=channel,
            topic_override=(topic.id, topic.title), auto_resume=True,
            reuse_old=plan.get("reuse_old", False), target=target,
        )
        if mode == "1":
            failed = await download_all(client, channel, folder, manifest, message_cache)
            if failed:
                raise OrderStop(f"{failed} file(s) could not be downloaded in topic {topic.title}")
            continue
        if not manifest["items"]:
            continue  # empty source topic: do not create an empty one on the target
        target_topic_id = await resolve_target_topic(client, target, topic.title, known_topics=target_topics)
        if getattr(target, "forum", False) and not target_topic_id:
            raise OrderStop(f"Could not resolve target topic: {topic.title}")
        if mode == "4":
            await forward_all(client, channel, target, folder, manifest, silent, topic_id=target_topic_id)
        else:
            await download_and_send(
                client, channel, target, folder, manifest, message_cache, silent,
                plan.get("delete_after", False), target_topic_id, f"topic {topic.title}",
            )
    info("All selected topics done.", "Secilen tum alt basliklar tamamlandi.", Fore.GREEN)


async def run_selected_folders(client, plan):
    """Mode 2 with several downloaded folders: send each one in turn. On a
    forum target a folder that came from a topic goes into the same-titled
    topic; a folder without a topic goes to General."""
    target = await resolve_peer(client, plan["target_peer"], "Target")
    silent = plan.get("silent", False)
    target_topics = None
    if getattr(target, "forum", False):
        try:
            target_topics = await load_topics(client, target)
        except RPCError:
            target_topics = None
    names = plan["folders"]
    for index, name in enumerate(names, 1):
        folder = MIRRORS_DIR / name
        manifest = load_manifest(folder)
        if manifest is None:
            raise OrderStop(f"{folder / MANIFEST_NAME} is missing. (manifest.json bulunamadi.)", fatal=True)
        check_manifest_paths(folder, manifest)
        info(f"Folder {index}/{len(names)}: {name}", f"Klasor {index}/{len(names)}: {name}")
        title = manifest.get("topic_title")
        topic_id = None
        if title and getattr(target, "forum", False):
            topic_id = await resolve_target_topic(client, target, title, known_topics=target_topics)
        try:
            await send_all(client, target, folder, manifest, silent, topic_id=topic_id)
        except MissingFileStop as stop:
            raise OrderStop(f"{name}: {stop}", fatal=True)
    info("All selected folders sent.", "Secilen tum klasorler gonderildi.", Fore.GREEN)


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
    if mode in ("1", "3", "4") and plan.get("topic_ids"):
        await run_selected_topics(client, plan, message_cache)
        return
    if mode == "2" and plan.get("folders"):
        await run_selected_folders(client, plan)
        return
    folder = MIRRORS_DIR / plan["folder"]
    manifest = load_manifest(folder)  # fresh from disk: it is the source of truth
    if manifest is None:
        raise OrderStop(f"{folder / MANIFEST_NAME} is missing. (manifest.json bulunamadi.)", fatal=True)

    check_manifest_paths(folder, manifest)

    channel = None
    target = None
    if plan.get("target_peer") is not None:
        target = await resolve_peer(client, plan["target_peer"], "Target")
    if mode != "2":
        channel = await resolve_peer(client, plan["source_peer"], "Source")
        await scan_and_report(client, channel, folder, manifest, message_cache, target)

    if mode == "1":
        failed = await download_all(client, channel, folder, manifest, message_cache)
        if failed:
            raise OrderStop(f"{failed} file(s) could not be downloaded")
        return

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
        progress = get_progress(manifest, target, topic_id=topic_id) or {}
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


async def setup_selected_topics(client, mode, channel, topic_ids):
    """Questions for modes 1, 3 and 4 when several source topics were picked."""
    plan = {
        "mode": mode,
        "folder": None,
        "source_peer": utils.get_peer_id(channel),
        "target_peer": None,
        "topic_ids": topic_ids,
        "created": datetime.now(timezone.utc).isoformat(),
        "finished": False,
    }
    if mode != "4":
        plan["reuse_old"] = other_mirrors_exist(channel.id) and ask_yes_no(
            "Was this chat re-created from an earlier one (same topic titles = same content)? "
            "Skip what was already downloaded/sent for those topics?",
            "Bu sohbet eskisinin yeniden acilmis hali mi (ayni alt baslik adi = ayni icerik)? "
            "O alt basliklar icin daha once indirilen/gonderilenler atlansin mi?",
        )
    if mode == "1":
        return plan
    target = await ask_target_chat(client, channel)
    if target is None:
        return None
    plan["target_peer"] = utils.get_peer_id(target)
    plan["silent"] = ask_yes_no(
        "Post silently (no notification for each message)?",
        "Sessiz gonderilsin mi (her mesaj icin bildirim gitmesin)?",
    )
    if mode == "3":
        plan["delete_after"] = ask_yes_no(
            "Delete each file from this computer right after it is sent?",
            "Her dosya gonderildikten hemen sonra bu bilgisayardan silinsin mi?",
        )
    return plan


async def ask_target_chat(client, source=None):
    """Ask for the target and explain where multi-topic content will land."""
    target = await ask_chat(client, "Target channel to post into", "Gonderilecek hedef kanal")
    if source is not None and target.id == source.id:
        info("Source and target must differ.", "Kaynak ve hedef farkli olmali.", Fore.RED)
        return None
    if getattr(target, "forum", False):
        info(
            "Each topic goes into a same-titled topic on the target (created if missing).",
            "Her alt baslik hedefte ayni isimli alt basliga gider (yoksa olusturulur).",
        )
    else:
        info(
            "Target is not a forum: the selected items are posted one after another.",
            "Hedef forum degil: secilenler arka arkaya gonderilecek.",
            Fore.YELLOW,
        )
    return target


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
        folders = pick_existing_folders()
        if not folders:
            return None
        if len(folders) > 1:
            target = await ask_target_chat(client)
            if target is None:
                return None
            return {
                "mode": "2",
                "folder": None,
                "folders": [folder.name for folder in folders],
                "source_peer": None,
                "target_peer": utils.get_peer_id(target),
                "silent": ask_yes_no(
                    "Post silently (no notification for each message)?",
                    "Sessiz gonderilsin mi (her mesaj icin bildirim gitmesin)?",
                ),
                "created": datetime.now(timezone.utc).isoformat(),
                "finished": False,
            }
        folder = folders[0]
        channel = None
    else:
        channel = await ask_chat(
            client, "Source channel/group (link, @username, ID or name)", "Kaynak kanal/grup"
        )
        if mode == "4" and getattr(channel, "noforwards", False):
            info(
                "This chat has content protection; direct copy will fail. Use mode 3.",
                "Bu kanal korumali; dogrudan kopyalama calismaz, 3. modu kullanin.",
                Fore.RED,
            )
            return None
        picks = await pick_topics(client, channel) if getattr(channel, "forum", False) else None
        if picks and len(picks) > 1:
            return await setup_selected_topics(client, mode, channel, [topic_id for topic_id, _ in picks])
        channel, folder = await choose_source(
            client, channel, topic_override=picks[0] if picks else (None, None)
        )

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
            f"An earlier run (mode {state.get('mode')}, '{state.get('folder') or 'secilenler / selected items'}') did not finish. "
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
