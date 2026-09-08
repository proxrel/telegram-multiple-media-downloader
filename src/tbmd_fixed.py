import asyncio
import os
import re
import json
import mimetypes
from pathlib import Path

from dotenv import load_dotenv
from colorama import Fore, Style
from tqdm.asyncio import tqdm
from telethon import TelegramClient, functions
from telethon.errors import FloodWaitError, RPCError
from telethon.tl.types import (
    InputMessagesFilterVideo,
    InputMessagesFilterPhotos,
    InputMessagesFilterDocument,
)

from FastTelethon import download_file as fast_parallel_download

BASE_DIR = Path(__file__).resolve().parent
PROJECT_DIR = BASE_DIR.parent

load_dotenv(PROJECT_DIR / ".env")
load_dotenv(BASE_DIR / ".env")

api_id_value = os.getenv("API_ID")
api_hash = os.getenv("API_HASH")
session_name = os.getenv("SESSION_NAME", "default_session")
batch_size = int(os.getenv("BATCH_SIZE", "5"))

LAST_CHANNEL_FILE = BASE_DIR / "last_channel.json"
FAILED_DOWNLOADS_FILE = BASE_DIR / "failed_downloads.json"
DOWNLOADS_DIR = BASE_DIR / "downloads"
SIZE_TOLERANCE_MB = 0.01
MAX_RETRY = int(os.getenv("MAX_RETRY", "3"))

# Parallel ("fast") download support - only kicks in for documents/videos at
# or above this size, since small files aren't worth the extra connections.
USE_FAST_DOWNLOAD = os.getenv("USE_FAST_DOWNLOAD", "1") == "1"
FAST_DOWNLOAD_MIN_MB = float(os.getenv("FAST_DOWNLOAD_MIN_MB", "5"))


def save_credentials_to_env(entered_api_id, entered_api_hash):
    """Persist the entered credentials to a .env file so the user is not asked again."""
    env_path = PROJECT_DIR / ".env"
    lines = []
    if env_path.exists():
        with env_path.open("r", encoding="utf-8") as file:
            lines = file.readlines()

    def upsert(lines, key, value):
        for index, line in enumerate(lines):
            if line.strip().startswith(f"{key}="):
                lines[index] = f"{key}={value}\n"
                return lines
        lines.append(f"{key}={value}\n")
        return lines

    lines = upsert(lines, "API_ID", entered_api_id)
    lines = upsert(lines, "API_HASH", entered_api_hash)
    if not any(line.strip().startswith("SESSION_NAME=") for line in lines):
        lines.append("SESSION_NAME=default_session\n")
    if not any(line.strip().startswith("BATCH_SIZE=") for line in lines):
        lines.append("BATCH_SIZE=5\n")

    with env_path.open("w", encoding="utf-8") as file:
        file.writelines(lines)


def prompt_for_credentials():
    """Ask the user for API_ID / API_HASH interactively when .env is missing them."""
    global api_id_value, api_hash

    print(f"{Fore.YELLOW}Telegram API configuration was not found.{Style.RESET_ALL}")
    print(
        "Telegram API ayarlari bulunamadi. Degerleri simdi girebilirsin "
        "(https://my.telegram.org adresinden alabilirsin).\n"
    )

    while True:
        entered_id = input("API_ID: ").strip()
        if entered_id.isdigit():
            break
        print(f"{Fore.RED}API_ID must be a number. / API_ID sayisal olmalidir.{Style.RESET_ALL}")

    entered_hash = input("API_HASH: ").strip()
    while not entered_hash:
        print(f"{Fore.RED}API_HASH cannot be empty. / API_HASH bos birakilamaz.{Style.RESET_ALL}")
        entered_hash = input("API_HASH: ").strip()

    api_id_value = entered_id
    api_hash = entered_hash
    save_credentials_to_env(entered_id, entered_hash)
    print(f"{Fore.GREEN}Saved to .env. / .env dosyasina kaydedildi.{Style.RESET_ALL}\n")


def validate_configuration():
    if not api_id_value or not api_hash:
        prompt_for_credentials()

    try:
        return int(api_id_value)
    except ValueError:
        print(f"{Fore.RED}API_ID must be a number.{Style.RESET_ALL}")
        print("API_ID sayisal bir deger olmalidir.")
        raise SystemExit(1)


api_id = validate_configuration()


# ---------------------------------------------------------------------------
# Ana sayfa ve son kanal hafizasi
# ---------------------------------------------------------------------------
def load_last_channel():
    if LAST_CHANNEL_FILE.exists():
        try:
            with LAST_CHANNEL_FILE.open("r", encoding="utf-8") as file:
                data = json.load(file)
                return data.get("last_input")
        except (OSError, json.JSONDecodeError):
            return None
    return None


def save_last_channel(chat_input: str):
    try:
        with LAST_CHANNEL_FILE.open("w", encoding="utf-8") as file:
            json.dump({"last_input": chat_input}, file, ensure_ascii=False)
    except OSError as error:
        print(
            f"{Fore.RED}Could not save the last channel: {error}"
            f"{Style.RESET_ALL}"
        )
        print(f"Son kanal bilgisi kaydedilemedi: {error}")


def load_failed_downloads():
    """Return the list of previously failed download records (may be empty)."""
    if FAILED_DOWNLOADS_FILE.exists():
        try:
            with FAILED_DOWNLOADS_FILE.open("r", encoding="utf-8") as file:
                data = json.load(file)
                if isinstance(data, list):
                    return data
        except (OSError, json.JSONDecodeError):
            return []
    return []


def save_failed_downloads(records):
    try:
        with FAILED_DOWNLOADS_FILE.open("w", encoding="utf-8") as file:
            json.dump(records, file, ensure_ascii=False, indent=2)
    except OSError as error:
        print(
            f"{Fore.RED}Could not save the failed downloads list: {error}"
            f"{Style.RESET_ALL}"
        )
        print(f"Hatali indirme listesi kaydedilemedi: {error}")


def update_failed_records(channel_id, folder_path: Path, attempted_ids, new_failed_records):
    """
    Refresh failed_downloads.json for one (channel, folder) batch: drop every
    previously-logged entry that belonged to this batch (it was just retried,
    either succeeding now or reappearing in new_failed_records), then add the
    fresh failures back in. This keeps the file in sync automatically after
    every normal run and every retry-only run, without needing a separate
    "remove on success" step.
    """
    existing = load_failed_downloads()
    folder_str = str(folder_path)

    remaining = [
        record
        for record in existing
        if not (
            record.get("channel_id") == channel_id
            and record.get("folder_path") == folder_str
            and record.get("message_id") in attempted_ids
        )
    ]

    remaining.extend(new_failed_records)
    save_failed_downloads(remaining)
    return remaining


def ask_yes_no(question_en: str, question_tr: str) -> bool:
    prompt = (
        f"{Fore.CYAN}{question_en} ({question_tr}) [Y/N]: "
        f"{Style.RESET_ALL}"
    )

    while True:
        answer = input(prompt).strip().lower()

        if answer in ("y", "yes"):
            return True

        if answer in ("n", "no"):
            return False

        print(
            f"{Fore.RED}Please enter Y (Yes) or N (No). "
            f"(Lutfen Y (Evet) veya N (Hayir) girin.){Style.RESET_ALL}"
        )


def show_home_page():
    divider = "=" * 76

    print(f"\n{Fore.CYAN}{divider}{Style.RESET_ALL}")
    print(
        f"{Fore.GREEN}TELEGRAM BATCH MEDIA DOWNLOADER - HOME PAGE"
        f"{Style.RESET_ALL}"
    )
    print(f"{Fore.CYAN}{divider}{Style.RESET_ALL}")

    print(
        "\nHow to start the application:"
        "\n(Uygulama nasil baslatilir:)"
    )
    print(
        "\n1. Open Git Bash, Command Prompt, or Terminal in the project's "
        "src folder."
        "\n   (Projenin src klasorunde Git Bash, Komut Istemi veya Terminal acin.)"
    )
    print(
        "\n2. Run the following command:"
        "\n   (Asagidaki komutu calistirin:)"
    )
    print("\n   python tbmd_fixed.py")

    print(
        "\nYou can also start the program by double-clicking "
        "run_downloader.bat, if that file exists."
        "\n(run_downloader.bat dosyasi varsa cift tiklayarak da "
        "programi baslatabilirsiniz.)"
    )

    print(
        "\nHow to find a channel or group ID:"
        "\n(Kanal veya grup ID'si nasil bulunur:)"
    )
    print(
        "\n1. Enter the exact channel/group name, a visible username, "
        "or a Chat ID."
        "\n   (Tam kanal/grup adini, gorunen kullanici adini veya Chat ID'yi girin.)"
    )
    print(
        "\n2. If the entered name cannot be found, the program prints "
        "available chats as: Chat name -> Chat ID."
        "\n   (Girilen ad bulunamazsa program sohbetleri "
        "Sohbet adi -> Chat ID biciminde listeler.)"
    )
    print(
        "\n3. Copy the number after the -> symbol and run the program "
        "again. Then enter that number as the Chat ID."
        "\n   (-> isaretinden sonraki numarayi kopyalayin, programi "
        "yeniden acin ve bu numarayi Chat ID olarak girin.)"
    )
    print(
        "\n4. Your Telegram account must already have access to the "
        "selected channel or group."
        "\n   (Telegram hesabinizin secilen kanal veya gruba erisim "
        "yetkisi olmalidir.)"
    )

    print(f"\n{Fore.CYAN}{divider}{Style.RESET_ALL}\n")


def get_new_channel_input():
    while True:
        chat_input = input(
            f"{Fore.CYAN}Enter the channel name, username, or Chat ID "
            f"(Kanal adi, kullanici adi veya Chat ID girin): "
            f"{Style.RESET_ALL}"
        ).strip()

        if chat_input:
            return chat_input

        print(
            f"{Fore.RED}Channel information cannot be empty. "
            f"(Kanal bilgisi bos birakilamaz.){Style.RESET_ALL}"
        )


# ---------------------------------------------------------------------------
# Dosya ve duplicate yardimcilari
# ---------------------------------------------------------------------------
def safe_filename(name: str, default_ext: str = ""):
    name = re.sub(r"[\r\n\t]+", " ", name)
    name = re.sub(r"[\u200b\u200c\u200d\ufeff]", "", name)
    name = "".join(char for char in name if char.isprintable())
    name = re.sub(r'[\\/*?:"<>|`]', "_", name)
    name = name.strip(" .")
    name = re.sub(r"\s+", " ", name).strip()

    if not name:
        name = "unnamed"

    if default_ext and not name.lower().endswith(default_ext.lower()):
        max_base_length = 150
        if len(name) > max_base_length:
            name = name[:max_base_length].rstrip(" .")
        name += default_ext

    elif len(name) > 180:
        base, dot, extension = name.rpartition(".")
        if dot and len(extension) <= 10:
            name = base[:170].rstrip(" .") + dot + extension
        else:
            name = name[:180].rstrip(" .")

    return name


def guess_extension(message):
    if message.document and getattr(message.document, "mime_type", None):
        extension = mimetypes.guess_extension(message.document.mime_type)
        if extension:
            return extension

    if message.video:
        return ".mp4"

    if message.photo:
        return ".jpg"

    return ""


def get_message_file_size(message) -> int:
    if message.video:
        return message.video.size

    if message.document:
        return message.document.size

    return 0


def scan_existing_sizes(folder_path: Path):
    sizes = []

    if folder_path.is_dir():
        for file_path in folder_path.iterdir():
            if file_path.is_file():
                try:
                    sizes.append(file_path.stat().st_size)
                except OSError:
                    pass

    return sizes


def size_already_exists(
    file_size_bytes: int,
    existing_sizes: list,
    tolerance_mb: float = SIZE_TOLERANCE_MB,
) -> bool:
    if file_size_bytes <= 0:
        return False

    tolerance_bytes = tolerance_mb * 1024 * 1024

    return any(
        abs(existing_size - file_size_bytes) <= tolerance_bytes
        for existing_size in existing_sizes
    )


# ---------------------------------------------------------------------------
# Indirme mantigi
# ---------------------------------------------------------------------------
async def download_file(message, folder_path: Path, progress_bars, existing_sizes):
    # Guard: a message can slip through the server-side filter without any
    # media Telethon actually knows how to download (e.g. a webpage preview,
    # a poll, or media that has since been removed). Attempting these always
    # returns None from download_media(), so skip them immediately instead
    # of burning retries on a request that can never succeed.
    if not (message.photo or message.video or message.document):
        print(
            f"{Fore.RED}Message {message.id} has no downloadable media and was skipped. "
            f"(Mesaj {message.id} indirilebilir medya icermiyor, atlandi.)"
            f"{Style.RESET_ALL}"
        )
        return False

    file_size = get_message_file_size(message)

    if size_already_exists(file_size, existing_sizes):
        size_mb = round(file_size / (1024 * 1024), 2)
        print(
            f"{Fore.YELLOW}Skipped: already downloaded (~{size_mb} MB) - "
            f"Message ID: {message.id}"
            f"{Style.RESET_ALL}"
        )
        print(f"Atlandi: zaten indirilmis (~{size_mb} MB) - Mesaj ID: {message.id}")
        return True

    progress_bar = tqdm(
        total=file_size,
        desc=f"Downloading {message.id}",
        ncols=100,
        unit="B",
        unit_scale=True,
        leave=True,
        bar_format=(
            "{l_bar}%s{bar}%s| {n_fmt}/{total_fmt} {unit} "
            "| Elapsed: {elapsed}/{remaining} | {rate_fmt}"
            % (Fore.BLUE, Style.RESET_ALL)
        ),
    )
    progress_bars.append(progress_bar)

    # Filename is always prefixed with the message ID so two messages that
    # share the same caption text can never resolve to the same destination
    # path. Concurrent downloads inside a batch (batch_size, default 5) used
    # to be able to collide on identical captions, so two coroutines wrote to
    # the exact same file at the same time - Telethon's own ".temp" file for
    # that path got overwritten mid-write by the other task, and the loser
    # of that race got back None instead of a path ("Telegram returned no
    # output file path"). Making every destination unique removes that race
    # entirely.
    if message.text:
        base_name = safe_filename(message.text.strip(), guess_extension(message))
    else:
        base_name = safe_filename("", guess_extension(message))

    custom_name = safe_filename(f"{message.id} - {base_name}", guess_extension(message))
    destination = folder_path / custom_name

    success = False
    last_error = None
    downloaded_path = None
    max_attempts = 1 + MAX_RETRY

    for attempt in range(1, max_attempts + 1):
        try:
            progress_bar.n = 0
            progress_bar.refresh()

            progress_callback = lambda current, total: (
                progress_bar.update(current - progress_bar.n)
                if total
                else None
            )

            # Large documents/videos go through FastTelethon's multi-connection
            # parallel downloader (github.com/painor's FastTelethon.py, MIT).
            # Small files and photos keep using the plain Telethon path -
            # opening several parallel connections for a 200 KB thumbnail
            # would only add overhead and flood-wait risk for no benefit.
            use_fast_download = (
                USE_FAST_DOWNLOAD
                and message.document
                and file_size >= FAST_DOWNLOAD_MIN_MB * 1024 * 1024
            )

            if use_fast_download:
                with open(destination, "wb") as out_file:
                    await fast_parallel_download(
                        message.client,
                        message.document,
                        out_file,
                        progress_callback=progress_callback,
                    )
                downloaded_path = str(destination)
            else:
                downloaded_path = await message.download_media(
                    file=str(destination),
                    progress_callback=progress_callback,
                )

            if not downloaded_path:
                raise RuntimeError("Telegram returned no output file path.")

            success = True
            break

        except FloodWaitError as error:
            last_error = error
            wait_seconds = getattr(error, "seconds", 5) + 1

            if attempt < max_attempts:
                print(
                    f"{Fore.YELLOW}Rate limited on message {message.id}. "
                    f"Waiting {wait_seconds}s before retrying... "
                    f"(Mesaj {message.id} icin hiz siniri; {wait_seconds}sn "
                    f"beklenip tekrar denenecek.){Style.RESET_ALL}"
                )
                await asyncio.sleep(wait_seconds)

        except (RPCError, RuntimeError, OSError, ConnectionError) as error:
            last_error = error

            if attempt < max_attempts:
                backoff_seconds = 2 * attempt
                print(
                    f"{Fore.YELLOW}Message {message.id} failed (attempt "
                    f"{attempt}/{max_attempts}). Retrying in {backoff_seconds}s... "
                    f"(Mesaj {message.id} indirilemedi, {backoff_seconds}sn "
                    f"sonra tekrar denenecek.) Error: {error}{Style.RESET_ALL}"
                )
                await asyncio.sleep(backoff_seconds)

    if success:
        progress_bar.bar_format = (
            "{l_bar}%s{bar}%s| {n_fmt}/{total_fmt} {unit} "
            "| Elapsed: {elapsed}/{rate_fmt}"
            % (Fore.GREEN, Style.RESET_ALL)
        )
        progress_bar.set_description(f"Finished {message.id}")
        progress_bar.n = progress_bar.total
        progress_bar.update(0)

        try:
            actual_size = Path(downloaded_path).stat().st_size
        except (OSError, TypeError):
            actual_size = file_size

        existing_sizes.append(actual_size)

    else:
        progress_bar.bar_format = (
            "{l_bar}%s{bar}%s| {n_fmt}/{total_fmt} {unit} "
            "| Elapsed: {elapsed}/{rate_fmt}"
            % (Fore.RED, Style.RESET_ALL)
        )
        progress_bar.set_description(f"Failed {message.id}")
        progress_bar.refresh()

        print(
            f"{Fore.RED}Message {message.id} could not be downloaded and was skipped. "
            f"(Mesaj {message.id} indirilemedi ve atlandi.) "
            f"Last error: {last_error}{Style.RESET_ALL}"
        )

    progress_bar.close()
    return success


async def download_in_batches(messages, folder_path: Path, batch_size: int, channel_id=None):
    """Download all messages in batches and return the list of failure records
    (each ``{"channel_id", "message_id", "folder_path"}``) so the caller can
    persist them for a later "retry only failed downloads" run."""
    tasks = []
    batch_messages = []
    progress_bars = []
    existing_sizes = scan_existing_sizes(folder_path)
    failed_records = []

    for index, message in enumerate(messages, 1):
        tasks.append(
            download_file(
                message,
                folder_path,
                progress_bars,
                existing_sizes,
            )
        )
        batch_messages.append(message)

        if len(tasks) == batch_size or index == len(messages):
            results = await asyncio.gather(*tasks, return_exceptions=True)

            for batch_message, result in zip(batch_messages, results):
                # result is True on success, False on a handled failure, or
                # an Exception instance if something unexpected escaped
                # download_file's own try/except entirely.
                if result is not True:
                    failed_records.append(
                        {
                            "channel_id": channel_id,
                            "message_id": batch_message.id,
                            "folder_path": str(folder_path),
                        }
                    )

            tasks.clear()
            batch_messages.clear()

    return failed_records


async def get_topic_messages(client, channel, topic_id, filter_type, limit=2000):
    all_messages = []
    offset_id = 0

    while True:
        messages = await client.get_messages(
            channel,
            filter=filter_type,
            limit=100,
            offset_id=offset_id,
            reply_to=topic_id,
        )

        if not messages:
            break

        all_messages.extend(messages)

        if len(messages) < 100 or len(all_messages) >= limit:
            break

        offset_id = messages[-1].id

    return all_messages[:limit]


# ---------------------------------------------------------------------------
# Topic, kanal ve program akisi
# ---------------------------------------------------------------------------
async def select_topics(client, channel):
    try:
        full_channel = await client.get_entity(channel)

        if not getattr(full_channel, "forum", False):
            print(
                f"{Fore.YELLOW}This channel does not use forum topics. "
                f"All channel media will be searched."
                f"{Style.RESET_ALL}"
            )
            print("Bu kanal forum/topic yapisina sahip degil. Tum kanal indirilecek.")
            return None

        print(f"{Fore.YELLOW}Loading forum topics...{Style.RESET_ALL}")

        all_topics = []
        seen_ids = set()
        offset_topic = 0
        offset_id = 0
        offset_date = 0

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

            if not result.topics:
                break

            new_topics = [
                topic
                for topic in result.topics
                if topic.id not in seen_ids
            ]

            if not new_topics:
                break

            seen_ids.update(topic.id for topic in new_topics)
            all_topics.extend(new_topics)

            if len(result.topics) < 100:
                break

            messages_by_id = {
                message.id: message
                for message in result.messages
            }

            last_topic = result.topics[-1]
            last_message = messages_by_id.get(last_topic.top_message)

            offset_topic = last_topic.id
            offset_id = last_topic.top_message
            offset_date = (
                int(last_message.date.timestamp())
                if last_message
                else 0
            )

        if not all_topics:
            print(f"{Fore.RED}No topics found. (Hic topic bulunamadi.){Style.RESET_ALL}")
            return None

        print(
            f"\n{Fore.CYAN}Available topics - Total: {len(all_topics)}"
            f"{Style.RESET_ALL}"
        )
        print(" 0. Download the entire channel without a topic filter")
        print("    (Topic filtresi olmadan tum kanali indir)")

        for index, topic in enumerate(all_topics, 1):
            print(f" {index}. {topic.title}")

        print(
            f"\n{Fore.YELLOW}Selection examples: 30 | 30,50,75 | 30-35 | 30,32-36,50"
            f"{Style.RESET_ALL}"
        )

        while True:
            raw = input(
                f"{Fore.CYAN}Select topic(s) (0-{len(all_topics)}): "
                f"{Style.RESET_ALL}"
            ).strip()

            if raw == "0":
                return None

            selected_indices = set()

            try:
                for part in raw.split(","):
                    part = part.strip()

                    if "-" in part:
                        start, end = part.split("-", 1)
                        start, end = int(start), int(end)

                        if start > end:
                            start, end = end, start

                        selected_indices.update(range(start, end + 1))

                    else:
                        selected_indices.add(int(part))

            except ValueError:
                print(
                    f"{Fore.RED}Invalid format. Example: 30,50 or 30-35. "
                    f"(Gecersiz format.){Style.RESET_ALL}"
                )
                continue

            if not selected_indices:
                print(f"{Fore.RED}Please select at least one topic.{Style.RESET_ALL}")
                continue

            if any(
                item < 1 or item > len(all_topics)
                for item in selected_indices
            ):
                print(
                    f"{Fore.RED}Choose values between 1 and {len(all_topics)}. "
                    f"(1-{len(all_topics)} arasinda bir deger girin.)"
                    f"{Style.RESET_ALL}"
                )
                continue

            break

        selected_topics = [
            all_topics[index - 1]
            for index in sorted(selected_indices)
        ]

        print(f"{Fore.GREEN}Selected topics:{Style.RESET_ALL}")
        for topic in selected_topics:
            print(f" - {topic.title} (ID: {topic.id})")

        return selected_topics

    except Exception as error:
        print(
            f"{Fore.RED}Could not load topic list: {error}"
            f"{Style.RESET_ALL}"
        )
        print("Topic listesi alinamadi. Tum kanal indirilecek.")
        return None


async def resolve_channel(client, chat_input):
    dialogs = await client.get_dialogs()

    channel = next(
        (
            dialog.entity
            for dialog in dialogs
            if str(dialog.id) == chat_input or dialog.name == chat_input
        ),
        None,
    )

    if channel is None:
        channel = next(
            (
                dialog.entity
                for dialog in dialogs
                if chat_input.lower() in (dialog.name or "").lower()
            ),
            None,
        )

    if channel is None:
        print(
            f"{Fore.RED}No matching chat was found. "
            f"(Eslesme bulunamadi.){Style.RESET_ALL}"
        )
        print(
            f"{Fore.YELLOW}Available chats: "
            f"(Mevcut sohbetlerin listesi:){Style.RESET_ALL}"
        )

        for dialog in dialogs:
            print(f"  {dialog.name}  ->  {dialog.id}")

        raise ValueError(
            f'Cannot find an entity corresponding to "{chat_input}".'
        )

    return channel


# ---------------------------------------------------------------------------
# Sadece hatali indirmeleri yeniden deneme
# ---------------------------------------------------------------------------
async def retry_failed_downloads(client):
    """Re-attempt only the messages recorded in failed_downloads.json,
    grouped by channel and destination folder, without re-scanning the
    whole chat."""
    records = load_failed_downloads()

    if not records:
        print(
            f"{Fore.GREEN}No previously failed downloads found. "
            f"(Kayitli hatali indirme yok.){Style.RESET_ALL}"
        )
        return

    by_channel = {}
    for record in records:
        by_channel.setdefault(record.get("channel_id"), []).append(record)

    for channel_id, channel_records in by_channel.items():
        try:
            channel = await client.get_entity(channel_id)
            channel_title = getattr(channel, "title", str(channel_id))
        except Exception as error:
            print(
                f"{Fore.RED}Could not resolve channel {channel_id}, "
                f"keeping its entries for next time. "
                f"(Kanal {channel_id} bulunamadi, kayitlar korunuyor.) "
                f"Error: {error}{Style.RESET_ALL}"
            )
            continue

        by_folder = {}
        for record in channel_records:
            by_folder.setdefault(record["folder_path"], []).append(record["message_id"])

        for folder_str, message_ids in by_folder.items():
            folder_path = Path(folder_str)
            folder_path.mkdir(parents=True, exist_ok=True)

            print(
                f"\n{Fore.CYAN}=== Retrying {len(message_ids)} failed message(s) "
                f"for '{channel_title}' -> {folder_path.name} ==="
                f"{Style.RESET_ALL}"
            )

            try:
                messages = await client.get_messages(channel, ids=message_ids)
            except Exception as error:
                print(
                    f"{Fore.RED}Could not fetch messages: {error}"
                    f"{Style.RESET_ALL}"
                )
                continue

            messages = [message for message in messages if message is not None]
            found_ids = {message.id for message in messages}

            for missing_id in set(message_ids) - found_ids:
                print(
                    f"{Fore.YELLOW}Message {missing_id} no longer exists "
                    f"(deleted?), removing from the retry list. "
                    f"(Mesaj {missing_id} artik mevcut degil, listeden "
                    f"cikariliyor.){Style.RESET_ALL}"
                )

            failed_records = await download_in_batches(
                messages,
                folder_path,
                batch_size,
                channel_id=channel_id,
            )

            update_failed_records(channel_id, folder_path, set(message_ids), failed_records)

    remaining = load_failed_downloads()
    if remaining:
        print(
            f"\n{Fore.YELLOW}{len(remaining)} file(s) still could not be "
            f"downloaded and remain in failed_downloads.json. "
            f"({len(remaining)} dosya hala indirilemedi, "
            f"failed_downloads.json icinde kayitli kaldi.){Style.RESET_ALL}"
        )
    else:
        print(
            f"\n{Fore.GREEN}All previously failed downloads succeeded! "
            f"(Tum hatali indirmeler basariyla tamamlandi!){Style.RESET_ALL}"
        )


async def main():
    session_path = BASE_DIR / session_name

    async with TelegramClient(str(session_path), api_id, api_hash) as client:
        print(f"{Fore.GREEN}Connected successfully!{Style.RESET_ALL}")

        failed_records = load_failed_downloads()
        if failed_records:
            retry_only = ask_yes_no(
                f"{len(failed_records)} previously failed download(s) found. "
                "Retry only those now?",
                f"Daha once basarisiz olan {len(failed_records)} dosya bulundu. "
                "Simdi sadece onlari yeniden denemek ister misiniz?",
            )

            if retry_only:
                await retry_failed_downloads(client)

                continue_with_new = ask_yes_no(
                    "Do you also want to start a new channel download?",
                    "Ayrica yeni bir kanal indirmesi baslatmak ister misiniz?",
                )

                if not continue_with_new:
                    return

        last_channel = load_last_channel()
        chat_input = None

        if last_channel:
            use_last_channel = ask_yes_no(
                f"Do you want to enter the same channel? ({last_channel})",
                f"Ayni kanala girmek ister misiniz? ({last_channel})",
            )

            if use_last_channel:
                chat_input = last_channel
            else:
                show_home_page()
                chat_input = get_new_channel_input()

        else:
            show_home_page()
            chat_input = get_new_channel_input()

        channel = await resolve_channel(client, chat_input)
        save_last_channel(chat_input)

        channel_title = getattr(channel, "title", "Private Chat")
        print(
            f"{Fore.YELLOW}Fetched channel: {channel_title} "
            f"(ID: {channel.id}){Style.RESET_ALL}"
        )

        selected_topics = await select_topics(client, channel)

        print(
            f"\n{Fore.CYAN}Choose the type of content to download:"
            f"{Style.RESET_ALL}\n"
            "1. Images\n"
            "2. Videos\n"
            "3. PDFs\n"
            "4. ZIP files\n"
            "5. All types\n"
        )

        choice = input(
            f"{Fore.CYAN}Enter your choice (1-5): {Style.RESET_ALL}"
        ).strip()

        filter_type = None

        if choice == "1":
            filter_type = InputMessagesFilterPhotos()
            base_folder = "images"

        elif choice == "2":
            filter_type = InputMessagesFilterVideo()
            base_folder = "videos"

        elif choice == "3":
            filter_type = InputMessagesFilterDocument()
            base_folder = "pdfs"

        elif choice == "4":
            filter_type = InputMessagesFilterDocument()
            base_folder = "zips"

        elif choice == "5":
            base_folder = "all_media"

        else:
            print(f"{Fore.RED}Invalid choice. Exiting...{Style.RESET_ALL}")
            return

        topics_to_process = selected_topics if selected_topics else [None]
        total_failed = 0

        for topic in topics_to_process:
            if topic:
                safe_topic_title = re.sub(r'[\\/*?:"<>|]', "_", topic.title)
                folder_path = DOWNLOADS_DIR / safe_topic_title / base_folder
                print(
                    f"\n{Fore.CYAN}=== Downloading: '{topic.title}' ==="
                    f"{Style.RESET_ALL}"
                )
            else:
                folder_path = DOWNLOADS_DIR / base_folder

            folder_path.mkdir(parents=True, exist_ok=True)

            print(f"{Fore.YELLOW}Fetching media messages...{Style.RESET_ALL}")

            if topic:
                media_messages = await get_topic_messages(
                    client,
                    channel,
                    topic.id,
                    filter_type,
                )
            else:
                media_messages = await client.get_messages(
                    channel,
                    filter=filter_type,
                    limit=2000,
                )

            if choice == "3":
                media_messages = [
                    message
                    for message in media_messages
                    if message.document
                    and getattr(message.document, "mime_type", "")
                    == "application/pdf"
                ]

            elif choice == "4":
                media_messages = [
                    message
                    for message in media_messages
                    if message.document
                    and getattr(message.document, "mime_type", "")
                    in ("application/zip", "application/x-zip-compressed")
                ]

            print(
                f"Found {len(media_messages)} messages matching your choice. "
                f"(Seciminizle eslesen {len(media_messages)} mesaj bulundu.)"
            )

            if media_messages:
                run_failed_records = await download_in_batches(
                    media_messages,
                    folder_path,
                    batch_size,
                    channel_id=channel.id,
                )
                attempted_ids = {message.id for message in media_messages}
                update_failed_records(
                    channel.id, folder_path, attempted_ids, run_failed_records
                )
                total_failed += len(run_failed_records)
            else:
                print(
                    f"{Fore.RED}No media found for the selected type. "
                    f"(Secilen turde medya bulunamadi.){Style.RESET_ALL}"
                )

        print(
            f"\n{Fore.GREEN}Downloads completed for all selected topics!"
            f"{Style.RESET_ALL}"
        )
        print("Tum secilen topic'ler icin indirme tamamlandi!")

        if total_failed:
            print(
                f"{Fore.YELLOW}{total_failed} file(s) could not be downloaded "
                f"this run. They were saved to failed_downloads.json - run the "
                f"script again and choose to retry only failed downloads. "
                f"({total_failed} dosya bu calistirmada indirilemedi ve "
                f"failed_downloads.json dosyasina kaydedildi; scripti tekrar "
                f"calistirip 'sadece hatali indirmeleri yeniden dene' "
                f"secenegini kullanabilirsiniz.){Style.RESET_ALL}"
            )


if __name__ == "__main__":
    asyncio.run(main())
