#!/data/data/com.termux/files/usr/bin/bash
# Keeps ordered_mirror.py running until the job is really finished.
#
# ordered_mirror.py already heals itself (retries after network drops, saves
# your answers and restarts itself after a crash via auto_restart.py). This
# loop is the second safety net, e.g. when Android kills the whole python
# process. It stops by itself when the job is done or needs you.
cd ~/telegram-multiple-media-downloader || exit 1

# Stop Android from putting Termux to sleep while the transfer runs.
command -v termux-wake-lock >/dev/null 2>&1 && termux-wake-lock

args=()   # first start asks the menu questions; restarts reuse the saved answers
while true; do
    python src/ordered_mirror.py "${args[@]}"
    code=$?
    # 0 = finished, 2 = needs a human (permission, login, full disk, Ctrl+C),
    # 130/143 = you stopped it on purpose. Anything else is a crash: restart.
    case "$code" in
        0|2|130|143)
            echo "ordered_mirror.py exited with code $code - not restarting."
            break
            ;;
    esac
    echo "ordered_mirror.py died (exit code $code). Restarting in 30s..."
    args=(--auto)
    sleep 30
done

command -v termux-wake-unlock >/dev/null 2>&1 && termux-wake-unlock

exit "$code"
