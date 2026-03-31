#!/usr/bin/python

import asyncio
import configparser
import hashlib
import httpx
import logging
import re
import sys
import uvicorn

from collections import defaultdict
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import StreamingResponse, PlainTextResponse
from typing import Dict, List

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("iptv-proxy")

config = configparser.ConfigParser()
config.read("config.ini")

SERVER_HOST = config.get("server", "host", fallback="0.0.0.0")
SERVER_PORT = config.getint("server", "port", fallback=8000)

M3U_HOST = config.get("general", "m3u_host", fallback="http://127.0.0.1:8000")
TRACK_METRICS = config.getboolean("general", "track_metrics", fallback=False)
USE_FFMPEG = config.getboolean("general", "use_ffmpeg", fallback=True)
FFMPEG_PATH = config.get("general", "ffmpeg_path", fallback="ffmpeg")

PROVIDERS = {}

for section in config.sections():
  if section.startswith("provider:"):
    name = section.split(":", 1)[1]
    PROVIDERS[name] = {
      "file": config.get(section, "file"),
      "max_streams": config.getint(section, "max_streams", fallback=1),
      "priority": config.getint(section, "priority", fallback=10),
    }

CHANNEL_MAPPING = {}
if config.has_section("mapping"):
    for orig_id, val in config.items("mapping"):
        parts = val.split("|")
        if len(parts) == 3:
            idx, new_id, new_name = parts
            CHANNEL_MAPPING[orig_id] = (int(idx), new_id, new_name)

channel_index: Dict[str, List[dict]] = defaultdict(list)
active_sessions: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
locks: Dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
metrics: Dict[str, Dict[str, float]] = defaultdict(lambda: defaultdict(float))

http_client: httpx.AsyncClient = None

async def parse_m3u(path: str, provider_id: str):
  if path.startswith(("http://", "https://")):
    resp = await http_client.get(path)
    resp.raise_for_status()
    content = resp.text
  else:
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
      content = f.read()

  current_idx = None
  current_id = None
  current_name = None
  current_logo = None
  count = 0

  for line in content.splitlines():
    line = line.strip()
    if line.startswith("#EXTINF"):
      raw_name = line.split(",")[-1].strip()
      id_match = re.search(r'tvg-id="([^"]*)"', line)
      logo_match = re.search(r'tvg-logo="([^"]+)"', line)
      raw_id = hashlib.md5((id_match.group(1) if id_match else raw_name).encode()).hexdigest()

      if CHANNEL_MAPPING:
        if raw_id not in CHANNEL_MAPPING:
          logger.warning(f"Channel not found, id: '{raw_id}', name: '{raw_name}'")
          continue

        idx, new_id, new_name = CHANNEL_MAPPING[raw_id]
        if idx > 0:
          current_idx, current_id, current_name = idx, new_id, new_name
        else:
          current_idx, current_id, current_name = None, None, None
          continue
      else:
        current_idx, current_id, current_name = -1, raw_id, raw_name

      current_logo = logo_match.group(1) if logo_match else None
    elif line and not line.startswith("#") and current_idx and current_id and current_name:
      channel_index[current_id].append({
        "idx": current_idx,
        "name": current_name,
        "logo": current_logo,
        "url": line,
        "provider": provider_id
      })
      current_idx = None
      current_id = None
      current_name = None
      current_logo = None
      count += 1

  logger.info(f"Loaded {count} channels from provider '{provider_id}'")

async def load_all():
  channel_index.clear()
  for provider, cfg in PROVIDERS.items():
    try:
      await parse_m3u(cfg["file"], provider)
    except Exception as e:
      logger.error(f"Failed to load provider {provider}: {e}")

@asynccontextmanager
async def lifespan(app: FastAPI):
  global http_client
  http_client = httpx.AsyncClient(timeout=None)
  await load_all()
  yield
  await http_client.aclose()

app = FastAPI(lifespan=lifespan)

def pick_provider(channel: str):
  providers = channel_index[channel]

  def score(p):
    provider = p["provider"]
    usage = active_sessions[channel][provider]
    base = PROVIDERS[provider]["priority"]

    score_val = base + usage
    if TRACK_METRICS:
      score_val += metrics[channel][provider]

    return score_val

  scored_list = []
  for p in providers:
    scored_list.append((p, score(p)))

  sorted_providers = sorted(scored_list, key=lambda x: x[1])

  for p, s in sorted_providers:
    provider = p["provider"]
    if active_sessions[channel][provider] < PROVIDERS[provider]["max_streams"]:
      logger.info(f"Selected provider '{provider}' for '{channel}' (score: {s:.2f})")
      return p

  return None

@app.get("/{channel_key}/")
async def stream(channel_key: str, request: Request):
  if channel_key not in channel_index:
    raise HTTPException(404, "Channel not found")

  channel_name_display = channel_index[channel_key][0]["name"]

  async with locks[channel_key]:
    provider = pick_provider(channel_key)
    if not provider:
      raise HTTPException(503, "No available providers")

    pname = provider["provider"]
    active_sessions[channel_key][pname] += 1

  logger.info(f"Starting stream for '{channel_name_display}' (key: '{channel_key}') via '{pname}' [URL: {provider['url']}]")

  async def generator():
    start = asyncio.get_event_loop().time()
    bytes_count = 0
    process = None
    reason = "completed"

    try:
      if USE_FFMPEG:
        cmd = [
          FFMPEG_PATH,
          "-loglevel", "error",
          "-i", provider["url"],
          "-c", "copy",
          "-f", "mpegts",
          "pipe:1"
        ]

        process = await asyncio.create_subprocess_exec(
          *cmd,
          stdout=asyncio.subprocess.PIPE,
          stderr=asyncio.subprocess.DEVNULL
        )

        while True:
          chunk = await process.stdout.read(188 * 50)
          if not chunk:
            reason = "source EOF"
            break
          if await request.is_disconnected():
            reason = "client disconnected"
            break
          bytes_count += len(chunk)
          yield chunk

      else:
        async with http_client.stream("GET", provider["url"]) as resp:
          async for chunk in resp.aiter_bytes():
            if await request.is_disconnected():
              reason = "client disconnected"
              break
            bytes_count += len(chunk)
            yield chunk

    except Exception:
      reason = "error"
      if TRACK_METRICS:
        metrics[channel_key][pname] += 5
        logger.warning(f"Error during stream for '{channel_name_display}' (key: '{channel_key}') via '{pname}'. Metric penalty applied.")

    finally:
      if process:
        try:
          process.kill()
          await process.wait()
        except ProcessLookupError:
          logger.debug(f"FFmpeg process for '{channel_key}' already exited.")

      duration = asyncio.get_event_loop().time() - start
      mb_sent = bytes_count / (1024 * 1024)

      logger.info(
          f"Stream stopped: '{channel_name_display}' (key: '{channel_key}') [{pname}] (Source: {provider['url']}). "
          f"Duration: {duration:.1f}s, Sent: {mb_sent:.2f}MB, Reason: {reason}"
      )

      if TRACK_METRICS and duration > 0:
        bitrate = (bytes_count * 8) / duration
        metrics[channel_key][pname] = metrics[channel_key][pname] * 0.8 + (1 / max(bitrate, 1))
        logger.debug(f"Metrics updated for '{channel_key}' [{pname}]: {metrics[channel_key][pname]:.2f}")

      async with locks[channel_key]:
        active_sessions[channel_key][pname] = max(0, active_sessions[channel_key][pname] - 1)
        logger.debug(f"Active sessions for '{channel_key}' [{pname}]: {active_sessions[channel_key][pname]}")

  return StreamingResponse(generator(), media_type="video/mp2t")

@app.get("/playlist.m3u")
def merged_playlist():
  lines = ["#EXTM3U"]

  for key, entries in channel_index.items():
    entry = entries[0]
    idx = entry["idx"]
    name = entry["name"]
    logo = entry["logo"]
    url = f"{M3U_HOST}/{key}/"

    extinf = f'#EXTINF:{idx} tvg-id="{key}"'
    if logo:
      extinf += f' tvg-logo="{logo}"'
    extinf += f',{name}'

    lines.append(extinf)
    lines.append(url)

  return PlainTextResponse("\n".join(lines))

if __name__ == "__main__":
    uvicorn.run(app, host=SERVER_HOST, port=SERVER_PORT)
