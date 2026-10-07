# Jellyfin-IPTV-Helper

```bash
curl -s -w '\n' 'http://127.0.0.1:8000/playlist.m3u'

# Current provider-open failures, including counts by channel.
curl -s 'http://127.0.0.1:8000/debug/failed-count'

mpv 'http://127.0.0.1:8000/0170e7559eb1d241881cd07ca881c83a/'

```

Provider-open failures are remembered temporarily so another provider can be
preferred on the next request. The failures for a channel are cleared when any
provider starts delivering data, or after `provider_failure_timeout` seconds
(300 seconds by default).
