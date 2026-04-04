# Jellyfin-IPTV-Helper

A high-performance IPTV proxy and aggregator designed to sit between your IPTV providers and media servers like Jellyfin or Plex. It provides stream stabilization via FFmpeg, channel mapping, and multi-provider load balancing.

## Features

- **Provider Aggregation**: Combine multiple M3U providers into a single unified playlist.
- **Failover & Priority**: Assign priorities to providers and set maximum concurrent stream limits.
- **Stream Stabilization**: Optional FFmpeg integration to proxy streams, ensuring better compatibility and recovery.
- **Plex/HDHomeRun Emulation**: Supports SSDP discovery and HDHomeRun API endpoints (`discover.json`, `lineup.json`) for easy integration with Plex DVR.
- **Channel Mapping**: Granular control over channel IDs, names, and numbering via a simple mapping configuration.
- **Smart Provider Selection**: Automatically picks the best provider based on current usage, priority, and optional performance metrics (bitrate/error history).
- **External App Support**: Ability to use external binaries as stream sources.

## Prerequisites

- Python 3.8+
- FFmpeg (recommended for stream proxying)

## Installation

1. Clone the repository.
2. Install dependencies:
   ```bash
   pip install fastapi uvicorn httpx
   ```
3. Create a `config.ini` file in the root directory (see Configuration below).
4. Run the server:
   ```bash
   python server.py
   ```

## Configuration (`config.ini`)

### `[server]`
- `host`: Listen address (default: `0.0.0.0`).
- `port`: Listen port (default: `8000`).

### `[general]`
- `m3u_host`: The base URL used for links in the generated M3U (e.g., `http://192.168.1.50:8000`).
- `use_ffmpeg`: (true/false) Use FFmpeg to proxy the stream.
- `ffmpeg_path`: Path to the FFmpeg executable.
- `track_metrics`: (true/false) Enable penalty-based provider selection based on stream quality.
- `enable_plex_support`: (true/false) Enable HDHomeRun emulation for Plex.
- `enable_ssdp`: (true/false) Enable network discovery for Plex.

### `[provider:Name]`
Define one or more providers:
```ini
[provider:MyProvider]
file = /path/to/playlist.m3u or http://url/to/m3u
max_streams = 2
priority = 10
```

### `[mapping]`
Map raw channel IDs to specific indices and names:
`raw_id = index|new_id|new_name`

Example:
```ini
[mapping]
abcdef123456 = 1|CNN|CNN HD
```
*Note: If mappings are defined, only mapped channels will appear in the output unless configured otherwise.*

## Usage

### Jellyfin
1. Point Jellyfin's M3U Tuner to: `http://<server-ip>:8000/playlist.m3u`
2. (Optional) Provide an EPG URL as you normally would in Jellyfin.

### Plex
1. Enable `enable_plex_support` and `enable_ssdp` in `config.ini`.
2. Plex should automatically discover the proxy as an "HDHomeRun" device.
3. If not discovered, manually add the IP: `http://<server-ip>:8000`.

## Endpoints

- `/playlist.m3u`: The aggregated M3U playlist.
- `/{channel_key}/`: The direct stream endpoint.
- `/discover.json`: HDHomeRun discovery data (Plex).
- `/lineup.json`: Channel lineup for Plex.

### Debugging
- `/debug/all-channels`: List all channels parsed from providers.
- `/debug/unmapped-channels`: List channels found in M3Us but not present in your `[mapping]` section.
- `/debug/unused-mappings`: List mappings defined in `config.ini` that weren't found in the provider M3Us.

## How Metrics Work

If `track_metrics` is enabled, the proxy monitors stream performance:
- **Errors**: If a stream fails to start or crashes, a penalty is applied to that provider for that specific channel.
- **Bitrate**: The proxy calculates the average bitrate. Providers offering higher bitrates for a specific channel are prioritized over time via a weighted decay algorithm.

## License

This project is licensed under the [Creative Commons Attribution-NonCommercial-ShareAlike 4.0 International (CC BY-NC-SA 4.0)](https://creativecommons.org/licenses/by-nc-sa/4.0/) license.

**Summary:** You are free to share and adapt the material, provided you give appropriate credit, do not use the material for commercial purposes, and distribute your contributions under the same license.
