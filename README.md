Smart Billboard v6.0 ULTIMATE is a self-contained ESP32 firmware designed to control a relay (originally a billboard light/power) based on schedules, manual commands, and emergency modes.

Relay Logic (Original): The relay is active LOW – writing LOW to GPIO 25 turns the relay ON, HIGH turns it OFF.

Control Sources:

Telegram Bot (commands like /on, /off, /timer, schedules)

Web Dashboard (HTTP + live WebSocket at port 81)

Physical button (multiple click patterns)

Optional BLE, ESP‑NOW, and MQTT

Schedule:

Simple daily start/end hour, or

Advanced multi‑rule scheduler (time, day, season, expiry)

Dashboard: A built‑in HTML5 page with KPIs, charts, rule editor, file manager, user/audit log, and OTA update.

Safety & Logging:

Rate limiting, user roles, IP whitelist

Rotary audit log and event log

OLED display (128x64) cycles through status screens
