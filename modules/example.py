import asyncio
import logging
import shutil

logger = logging.getLogger("iptv-proxy.module.example")

class ExampleModule:
  """
  An example plugin module for Jellyfin-IPTV-Helper.
  Generates a continuous black frame video stream with silent audio using FFmpeg.
  """

  async def initialize(self):
    """
    Called once when the plugin is loaded during server startup.
    Can be a regular function or an async function.
    """
    self.ffmpeg_path = shutil.which("ffmpeg")
    logger.info("Example Module initialized (Black Frame Generator).")

  async def stream(self, arg: str):
    """
    The main streaming logic.
    'arg' is the part of the URL after 'module://example/'.
    Must be an async generator yielding bytes.
    """
    if not self.ffmpeg_path:
      logger.error("FFmpeg not found in PATH. Cannot generate black frame.")
      return

    # Escape special characters for the FFmpeg filter string (backslash, single quote, and colon)
    escaped_arg = arg.replace('\\', '\\\\').replace("'", r"\'").replace(':', r'\:')
    video_filter = f"color=c=black:s=1280x720:r=25,drawtext=text='{escaped_arg}':fontcolor=white:fontsize=48:x=(w-text_w)/2:y=(h-text_h)/2"

    # FFmpeg command:
    # -re: Read input at native frame rate (simulates a live stream)
    # -f lavfi -i color...drawtext: generates black video with white text overlay
    # -f lavfi -i anullsrc: generates silent audio
    # -c:v libx264: encode to H.264 using ultrafast preset for low CPU usage
    # -f mpegts: output as MPEG-TS for IPTV compatibility
    cmd = [
      self.ffmpeg_path,
      "-hide_banner", "-loglevel", "error",
      "-re",
      "-f", "lavfi", "-i", video_filter,
      "-f", "lavfi", "-i", "anullsrc=channel_layout=stereo:sample_rate=44100",
      "-c:v", "libx264", "-preset", "ultrafast", "-tune", "zerolatency",
      "-c:a", "aac",
      "-f", "mpegts",
      "pipe:1"
    ]

    process = await asyncio.create_subprocess_exec(
      *cmd,
      stdout=asyncio.subprocess.PIPE,
      stderr=asyncio.subprocess.DEVNULL
    )

    try:
      while True:
        chunk = await process.stdout.read(9400)
        if not chunk:
          break
        yield chunk
    except asyncio.CancelledError:
      pass
    except Exception as e:
      logger.error(f"Black frame generator error: {e}")
    finally:
      if process.returncode is None:
        try:
          process.kill()
          await process.wait()
        except Exception:
          pass
