# Telegram Batch Media Downloader

Telegram Batch Media Downloader is a fork of [Telegram Bulk Media Downloader](https://github.com/vinodkr494/telegram-media-downloader), a Python-based tool that allows users to download various types of media files (videos, images, PDFs, ZIPs, etc.) from Telegram channels and groups. The downloader supports resumable downloads, batch processing, and progress tracking, making it ideal for managing large volumes of media efficiently.

## Features

- **Batch Processing**: Downloads media in configurable batches for better resource management.
- 🆕 **Use Chat_ID instead of chat/group name**: Automatically converts the Chat_ID number to the chat/group name.
- **Multi-Media Support**: Supports videos, images, PDFs, ZIP files, and more.
- 🆕 **File Autorenaming**: When a file e.g. a video is embedded (i.e. directly playable) from a chat, it uses the accompanying text as filename of the saved file.
- 🆕 **Smart Duplicate Skipping**: Checks each file's size in MB against files already in the download folder, so the same file is never downloaded twice, even if it was renamed.
- 🆕 **Automatic Retry**: If a file fails to download, the tool retries it once automatically before skipping it, just like the original version.
- 🆕 **Remembers Last Channel**: Saves the last entered channel locally and asks whether you want to use it again the next time you run the tool, so you don't have to type the channel name or ID every time.
- 🆕 **Channel/Group ID Finder**: Includes a helper script that lists every chat you're a member of along with its ID, so you don't have to guess it.
- **Progress Tracking**: Displays detailed progress bars for each download.
- **Configurable Settings**: Easily customizable batch size and session settings via `.env` file.
- **Cross-Platform**: Runs on Windows, macOS, and Linux.
- **Lightweight**: Requires only Python and a few libraries to run.

## Requirements

- Python 3.8+
- Telegram API credentials (API ID and API Hash)

## Installation

Clone the repository:

```
git clone https://github.com/proxrel/telegram-multiple-media-downlaoder.git
cd telegram-multiple-media-downlaoder
```

Install dependencies:

```
pip install -r requirements.txt
```

Create a .env file and configure it:

```
API_ID=your_api_id
API_HASH=your_api_hash
SESSION_NAME=default_session
BATCH_SIZE=5
```

Run the script:

```
python src/tbmd_fixed.py
```

## Usage

Start the script:

```
python src/tbmd_fixed.py
```

Choose whether to reuse the last saved channel, or enter the Telegram channel username, ID, or group link when prompted.

Select the type of media to download (e.g., videos, images, PDFs).

Watch as your files are downloaded with detailed progress bars!

## Advanced Configuration

### Finding a Channel/Group ID

If you don't know the numeric ID of the channel or group you want to download from, use the included helper script:

```
python src/find_channel.py
```

This logs in with your configured API credentials and lists every chat you're a member of, along with its ID:

```
AD: My Channel | ID: -1001234567890
```

Copy the ID next to the channel you want and use it when `tbmd_fixed.py` asks for the channel. If you'd rather search for one specific chat instead of scrolling through the full list, open `find_channel.py` and set the `TARGET_NAME` variable to that chat's exact name before running it.

### Resuming Downloads

The downloader automatically checks the file sizes of already downloaded files in the destination folder. To resume downloads, simply restart the script, and it will skip files that already exist based on matching size, even if the filename is different.

### Retry on Failure

If a file fails to download, the script waits a couple of seconds and tries once more automatically. If it still fails, it logs the error and moves on to the next file, just like before.

### Remembering the Last Channel

The tool saves the last entered channel or chat ID in a local `last_channel.json` file. On the next run, it will ask if you want to use the same channel again, so you don't need to retype it every time. This file is excluded from version control via `.gitignore`.

### Batch Size

To adjust the number of files downloaded in parallel, update the `BATCH_SIZE` value in the `.env` file.

## Supported Media Types

The tool supports the following media types:

- Videos
- Images
- PDFs
- ZIP files
- Any other Telegram media

## License

This project is licensed under GPLv3 License. See the [LICENSE](https://github.com/proxrel/telegram-multiple-downlaoder/blob/master/LICENSE) file for details.

## Acknowledgments

- [Vinod Kumar](https://github.com/vinodkr494) for the original code 🙏.
- [Matteo Di Cris](https://github.com/mdic) for further improvements to the original code.
- [Telethon](https://github.com/LonamiWebs/Telethon) - For making Telegram API integration easy.
- [TQDM](https://github.com/tqdm/tqdm) - For elegant progress bars.
- [Colorama](https://github.com/tartley/colorama) - For colorful console output.

---

Made with ❤️ by [Vinod Kumar](https://github.com/vinodkr494), edited with equal ❤️ by [Matteo Di Cris](https://github.com/mdic), and further improved by [proxrel](https://github.com/proxrel).
