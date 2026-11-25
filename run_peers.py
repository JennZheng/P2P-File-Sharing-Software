# Run all peers locally in the order listed in PeerInfo.cfg.
import subprocess
import sys
import time
from pathlib import Path


def wait_until_listening(process, console_path):
    deadline = time.monotonic() + 10

    while time.monotonic() < deadline:
        if "Server listening" in console_path.read_text():
            return
        if process.poll() is not None:
            raise RuntimeError(f"Peer failed to start. Check {console_path.name}")
        time.sleep(0.05)

    raise RuntimeError(f"Peer took too long to start. Check {console_path.name}")


def main():
    project_dir = Path(__file__).resolve().parent
    peer_info = project_dir / "project_config_file_large" / "PeerInfo.cfg"
    processes = []
    console_files = []

    try:
        peer_ids = []
        for line in peer_info.read_text().splitlines():
            if line.strip():
                peer_ids.append(int(line.split()[0]))

        if not peer_ids:
            raise ValueError("PeerInfo.cfg does not contain any peers")

        for peer_id in peer_ids:
            console_path = project_dir / f"log_peer_{peer_id}_console.log"
            console = console_path.open("w")
            console_files.append(console)

            print(f"Starting peer {peer_id}. Console: {console_path.name}", flush=True)
            process = subprocess.Popen(
                [sys.executable, "-u", str(project_dir / "peerProcess.py"), str(peer_id)],
                cwd=project_dir,
                stdout=console,
                stderr=subprocess.STDOUT,
            )
            processes.append((peer_id, process))
            # Earlier peers must be listening before later peers connect to them.
            wait_until_listening(process, console_path)

        print("All peers started. Waiting for downloads. Press Ctrl+C to stop.", flush=True)
        while True:
            all_finished = True
            for peer_id, process in processes:
                result = process.poll()
                if result is None:
                    all_finished = False
                elif result != 0:
                    raise RuntimeError(
                        f"Peer {peer_id} failed. Check log_peer_{peer_id}_console.log"
                    )

            if all_finished:
                print("All peers finished successfully.")
                return 0
            time.sleep(0.1)

    except KeyboardInterrupt:
        print("\nStopping all peers.")
        return 130
    except (OSError, ValueError, RuntimeError) as error:
        print(error)
        return 1
    finally:
        for peer_id, process in processes:
            if process.poll() is None:
                process.terminate()
        for peer_id, process in processes:
            try:
                process.wait(timeout=3)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
        for console in console_files:
            console.close()


if __name__ == "__main__":
    sys.exit(main())
