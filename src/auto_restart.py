"""Tiny process supervisor: runs a script as a child process and starts it
again whenever it dies unexpectedly (crash, killed, power/sleep hiccup...).

Standard library only on purpose: it must be importable *before* the script's
own imports, because those can already prompt for credentials or fail.

Exit codes the child uses to talk to the supervisor:
    0  finished normally            -> supervisor stops
    2  needs a human / stopped by user -> supervisor stops, no restart
    anything else (1, crash, kill)  -> supervisor restarts it with --auto
"""
import os
import subprocess
import sys
import time

CHILD_ENV = "ORDERED_MIRROR_CHILD"
EXIT_DONE = 0
EXIT_NEEDS_HUMAN = 2

RESTART_DELAY = float(os.getenv("RESTART_DELAY", "10"))
# A child that dies sooner than this after starting counts as a "quick crash".
QUICK_CRASH_SECONDS = 30
# This many quick crashes in a row means something is broken for good (bad
# credentials, missing package...); restarting forever would only spam.
MAX_QUICK_CRASHES = int(os.getenv("MAX_QUICK_CRASHES", "8"))


def is_child() -> bool:
    return os.environ.get(CHILD_ENV) == "1"


def supervise(script: str, args=None) -> int:
    args = list(args or [])
    env = dict(os.environ, **{CHILD_ENV: "1"})
    quick_crashes = 0
    restarts = 0

    while True:
        command = [sys.executable, script] + args
        if restarts:
            command.append("--auto")  # resume the saved run without asking again
        started = time.monotonic()
        try:
            code = subprocess.call(command, env=env)
        except KeyboardInterrupt:
            print("\nStopped by user. (Kullanici durdurdu.)")
            return EXIT_NEEDS_HUMAN

        if code in (EXIT_DONE, EXIT_NEEDS_HUMAN):
            return code

        lived = time.monotonic() - started
        quick_crashes = quick_crashes + 1 if lived < QUICK_CRASH_SECONDS else 0
        restarts += 1
        if quick_crashes >= MAX_QUICK_CRASHES:
            print(
                f"\nThe program keeps crashing right after starting ({quick_crashes} times, "
                f"last exit code {code}). Giving up; fix the error shown above and start it again.\n"
                f"(Program acilir acilmaz surekli cokuyor, otomatik yeniden baslatma birakildi.)"
            )
            return EXIT_NEEDS_HUMAN

        print(
            f"\nProgram stopped unexpectedly (exit code {code}). Restarting in "
            f"{RESTART_DELAY:.0f}s - restart #{restarts}... "
            f"(Program beklenmedik sekilde durdu, {RESTART_DELAY:.0f} sn sonra kaldigi yerden yeniden baslatiliyor.)"
        )
        try:
            time.sleep(RESTART_DELAY)
        except KeyboardInterrupt:
            print("\nStopped by user. (Kullanici durdurdu.)")
            return EXIT_NEEDS_HUMAN
