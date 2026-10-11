import os
import sys
import json
import logging
import asyncio
from datetime import datetime
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
import uvicorn
from TikTokLive import TikTokLiveClient
from TikTokLive.events import CommentEvent, ConnectEvent, GiftEvent, RoomUserSeqEvent

# ----------------------------------------------------
# 1. จัดการ Log ป้องกัน Error รก Console
# ----------------------------------------------------
logging.basicConfig(level=logging.CRITICAL)
for log_name in ["TikTokLive", "betterproto2", "pydantic", "urllib3", "httpx"]:
    log = logging.getLogger(log_name)
    log.setLevel(logging.CRITICAL)
    log.propagate = False
    log.handlers.clear()

class SuppressWebcastErrorFilter:
    def __init__(self, original_stderr):
        self.original_stderr = original_stderr
        self.suppressing = False

    def write(self, s):
        if "Failed to parse WebcastLinkLayerMessage payload" in s:
            self.suppressing = True
            return
        if self.suppressing:
            if "AttributeError:" in s or "Did you mean:" in s or s.strip() == "":
                self.suppressing = False
            return
        if self.original_stderr:
            self.original_stderr.write(s)

    def flush(self):
        if self.original_stderr:
            self.original_stderr.flush()

sys.stderr = SuppressWebcastErrorFilter(sys.stderr)


# ----------------------------------------------------
# 2. FastAPI Backend & TikTok Connection Manager
# ----------------------------------------------------
app = FastAPI(title="TikTok LIVE Web Dashboard")

class ConnectionManager:
    def __init__(self):
        self.active_websockets: set[WebSocket] = set()
        self.client: TikTokLiveClient = None
        self.is_running = False
        self.active_user = ""
        self.current_task: asyncio.Task = None

        # ที่เก็บประวัติบัญชี
        self.base_dir = os.path.dirname(os.path.abspath(__file__))
        self.history_file = os.path.join(self.base_dir, "account_history.json")
        self.config_file = os.path.join(self.base_dir, "config.json")

    def load_history(self):
        if os.path.exists(self.history_file):
            try:
                with open(self.history_file, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception:
                pass
        return ["mosfet1234"]

    def save_history(self, username: str):
        username = username.strip().replace("@", "")
        if not username:
            return
        history = self.load_history()
        if username in history:
            history.remove(username)
        history.insert(0, username)
        history = history[:15]
        try:
            with open(self.history_file, "w", encoding="utf-8") as f:
                json.dump(history, f, ensure_ascii=False, indent=2)
        except Exception:
            pass

    def load_config(self):
        if os.path.exists(self.config_file):
            try:
                with open(self.config_file, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception:
                pass
        return {}

    def save_config(self, api_key: str):
        try:
            cfg = self.load_config()
            cfg["sign_api_key"] = api_key.strip()
            with open(self.config_file, "w", encoding="utf-8") as f:
                json.dump(cfg, f, ensure_ascii=False, indent=2)
        except Exception:
            pass

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_websockets.add(websocket)
        # ส่งข้อมูลเริ่มต้นเมื่อ Client ต่อเข้ามา
        init_data = {
            "type": "init",
            "history": self.load_history(),
            "sign_api_key": self.load_config().get("sign_api_key", ""),
            "is_running": self.is_running,
            "active_user": self.active_user
        }
        await websocket.send_json(init_data)

    def disconnect(self, websocket: WebSocket):
        self.active_websockets.discard(websocket)

    async def broadcast(self, message: dict):
        for ws in list(self.active_websockets):
            try:
                await ws.send_json(message)
            except Exception:
                self.active_websockets.discard(ws)

    async def start_tiktok(self, username: str, api_key: str):
        await self.stop_tiktok()

        username = username.strip().replace("@", "")
        self.save_history(username)
        self.save_config(api_key)

        self.is_running = True
        self.active_user = username

        await self.broadcast({"type": "status", "status": "running", "username": username})
        await self.broadcast({"type": "log", "text": f"🔄 กำลังเชื่อมต่อไปยังห้อง LIVE ของ '{username}'..."})

        client_kwargs = {"unique_id": username}
        if api_key:
            client_kwargs["sign_api_key"] = api_key

        self.client = TikTokLiveClient(**client_kwargs)

        if hasattr(self.client, "logger"):
            self.client.logger.setLevel(logging.CRITICAL)
            self.client.logger.handlers.clear()
            self.client.logger.propagate = False

        @self.client.on(ConnectEvent)
        async def on_connect(event: ConnectEvent):
            if not self.is_running or self.active_user != username:
                return
            await self.broadcast({"type": "log", "text": f"🟢 เกาะห้อง LIVE สำเร็จแล้ว! (Room ID: {self.client.room_id})"})

        @self.client.on(RoomUserSeqEvent)
        async def on_user_seq(event: RoomUserSeqEvent):
            if not self.is_running or self.active_user != username:
                return
            viewers = getattr(event, "total", 0)
            await self.broadcast({"type": "stat_viewers", "count": viewers})

        @self.client.on(CommentEvent)
        async def on_comment(event: CommentEvent):
            if not self.is_running or self.active_user != username:
                return
            user_name = event.user.nickname or event.user.unique_id
            await self.broadcast({
                "type": "comment",
                "user": user_name,
                "comment": event.comment
            })

        @self.client.on(GiftEvent)
        async def on_gift(event: GiftEvent):
            if not self.is_running or self.active_user != username:
                return
            is_streak = getattr(event.gift, "streakable", False)
            streak_finished = getattr(event, "streak_finished", None)
            repeat_end = getattr(event.gift, "repeat_end", None)

            if not is_streak or streak_finished is True or repeat_end == 1 or repeat_end is None:
                gift_name = getattr(event.gift, "name", "Gift")
                gift_count = getattr(event.gift, "count", 1)
                user_name = event.user.nickname or event.user.unique_id

                await self.broadcast({
                    "type": "gift",
                    "user": user_name,
                    "gift_name": gift_name,
                    "count": gift_count
                })

        async def runner():
            try:
                await self.client.connect()
            except Exception as e:
                if self.is_running and self.active_user == username:
                    await self.broadcast({"type": "log", "text": f"❌ หลุดจากการเชื่อมต่อ: {e}"})
                    await self.stop_tiktok()

        self.current_task = asyncio.create_task(runner())

    async def stop_tiktok(self):
        self.is_running = False
        self.active_user = ""
        if self.client:
            try:
                await self.client.disconnect()
            except Exception:
                pass
            self.client = None

        if self.current_task and not self.current_task.done():
            self.current_task.cancel()
            self.current_task = None

        await self.broadcast({"type": "status", "status": "stopped"})
        await self.broadcast({"type": "log", "text": "✅ หยุดการทำงานเรียบร้อย"})


manager = ConnectionManager()

# ----------------------------------------------------
# 3. HTML + CSS + JS Frontend Template
# ----------------------------------------------------
HTML_CONTENT = """
<!DOCTYPE html>
<html lang="th">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>TikTok LIVE Real-time Dashboard & TTS</title>
    <link href="https://fonts.googleapis.com/css2?family=Sarabun:wght@300;400;600;700&display=swap" rel="stylesheet">
    <style>
        * {
            box-sizing: border-box;
            margin: 0;
            padding: 0;
            font-family: 'Sarabun', sans-serif;
        }
        body {
            background-color: #0f172a;
            color: #f8fafc;
            display: flex;
            justify-content: center;
            padding: 20px;
        }
        .container {
            width: 100%;
            max-width: 900px;
            background: #1e293b;
            border-radius: 16px;
            padding: 24px;
            box-shadow: 0 10px 25px rgba(0,0,0,0.4);
        }
        h1 {
            text-align: center;
            font-size: 26px;
            font-weight: 700;
            margin-bottom: 20px;
            color: #38bdf8;
        }
        .card {
            background: #0f172a;
            border-radius: 12px;
            padding: 16px;
            margin-bottom: 16px;
            border: 1px solid #334155;
        }
        .form-row {
            display: flex;
            align-items: center;
            margin-bottom: 12px;
            gap: 12px;
        }
        .form-row label {
            width: 140px;
            font-weight: 600;
            font-size: 16px;
            color: #cbd5e1;
        }
        .form-row input, .form-row select {
            flex: 1;
            padding: 10px 14px;
            border-radius: 8px;
            border: 1px solid #475569;
            background: #1e293b;
            color: #fff;
            font-size: 16px;
            outline: none;
        }
        .form-row input:focus, .form-row select:focus {
            border-color: #38bdf8;
        }
        .options-group {
            display: flex;
            flex-wrap: wrap;
            gap: 20px;
            margin-top: 10px;
            padding-left: 140px;
        }
        .checkbox-label {
            display: flex;
            align-items: center;
            gap: 8px;
            font-size: 15px;
            cursor: pointer;
            user-select: none;
        }
        .checkbox-label input {
            width: 18px;
            height: 18px;
            accent-color: #0284c7;
            cursor: pointer;
        }
        .stats-grid {
            display: grid;
            grid-template-columns: repeat(3, 1fr);
            gap: 12px;
            margin-bottom: 16px;
        }
        .stat-card {
            background: #0f172a;
            border: 1px solid #334155;
            border-radius: 12px;
            padding: 14px;
            text-align: center;
        }
        .stat-title {
            font-size: 14px;
            color: #94a3b8;
            margin-bottom: 4px;
        }
        .stat-value {
            font-size: 28px;
            font-weight: 700;
        }
        .btn-action {
            width: 100%;
            padding: 14px;
            border-radius: 10px;
            border: none;
            font-size: 18px;
            font-weight: 700;
            cursor: pointer;
            transition: all 0.2s;
            margin-bottom: 16px;
        }
        .btn-start {
            background: #0284c7;
            color: #fff;
        }
        .btn-start:hover {
            background: #0369a1;
        }
        .btn-stop {
            background: #e11d48;
            color: #fff;
        }
        .btn-stop:hover {
            background: #be123c;
        }
        .log-box {
            background: #090d16;
            border-radius: 12px;
            border: 1px solid #334155;
            height: 380px;
            overflow-y: auto;
            padding: 14px;
            font-size: 16px;
            line-height: 1.6;
        }
        .log-line {
            margin-bottom: 6px;
            word-break: break-word;
        }
        .log-time {
            color: #64748b;
            font-size: 13px;
            margin-right: 6px;
        }
        .comment-user {
            color: #38bdf8;
            font-weight: 600;
        }
        .gift-highlight {
            color: #f43f5e;
            font-weight: 600;
        }
    </style>
</head>
<body>
    <div class="container">
        <h1>TikTok LIVE Real-time Dashboard & TTS</h1>

        <!-- Settings Card -->
        <div class="card">
            <div class="form-row">
                <label for="usernameInput">Username:</label>
                <input type="text" id="usernameInput" list="historyList" placeholder="เช่น mosfet1234 (ไม่ต้องใส่ @)">
                <datalist id="historyList"></datalist>
            </div>
            <div class="form-row">
                <label for="apiKeyInput">Sign API Key:</label>
                <input type="text" id="apiKeyInput" placeholder="EulerStream API Key (เว้นว่างได้)">
            </div>
            <div class="options-group">
                <label class="checkbox-label">
                    <input type="checkbox" id="chkTTS" checked> เปิดเสียงอ่าน TTS (ไทย)
                </label>
                <label class="checkbox-label">
                    <input type="checkbox" id="chkGiftSound" checked> เสียงเตือนของขวัญ
                </label>
                <label class="checkbox-label">
                    <input type="checkbox" id="chkReadName"> อ่านชื่อคนส่งนำหน้า
                </label>
                <label class="checkbox-label">
                    <input type="checkbox" id="chkReadBrackets"> อ่านข้อความใน [ ]
                </label>
            </div>
        </div>

        <!-- Dashboard Stats -->
        <div class="stats-grid">
            <div class="stat-card">
                <div class="stat-title">ผู้ชมปัจจุบัน</div>
                <div class="stat-value" id="valViewers" style="color: #38bdf8;">0</div>
            </div>
            <div class="stat-card">
                <div class="stat-title">คอมเมนต์สะสม</div>
                <div class="stat-value" id="valComments" style="color: #4ade80;">0</div>
            </div>
            <div class="stat-card">
                <div class="stat-title">ของขวัญที่ได้รับ</div>
                <div class="stat-value" id="valGifts" style="color: #f43f5e;">0</div>
            </div>
        </div>

        <!-- Toggle Button -->
        <button id="btnToggle" class="btn-action btn-start">Start Running</button>

        <!-- Log Area -->
        <div class="log-box" id="logBox"></div>
    </div>

    <script>
        let ws;
        let isRunning = false;
        let totalComments = 0;
        let totalGifts = 0;

        // Web Audio Context สำหรับสังเคราะห์เสียง Chime
        let audioCtx = null;
        function playGiftChime() {
            if (!document.getElementById("chkGiftSound").checked) return;
            try {
                if (!audioCtx) {
                    audioCtx = new (window.AudioContext || window.webkitAudioContext)();
                }
                const now = audioCtx.currentTime;
                const osc = audioCtx.createOscillator();
                const gain = audioCtx.createGain();

                osc.type = "sine";
                osc.frequency.setValueAtTime(880, now);
                osc.frequency.setValueAtTime(1320, now + 0.15);

                gain.gain.setValueAtTime(0.3, now);
                gain.gain.exponentialRampToValueAtTime(0.001, now + 0.35);

                osc.connect(gain);
                gain.connect(audioCtx.destination);

                osc.start(now);
                osc.stop(now + 0.35);
            } catch(e) {}
        }

        // Web Speech API สำหรับอ่านออกเสียงภาษาไทย
        function speakThai(text) {
            if (!document.getElementById("chkTTS").checked) return;
            if (!('speechSynthesis' in window)) return;

            let cleaned = text.replace(/https?:\/\/\S+/g, '');
            if (!document.getElementById("chkReadBrackets").checked) {
                cleaned = cleaned.replace(/\[.*?\]/g, '');
            } else {
                cleaned = cleaned.replace(/[\[\]]/g, ' ');
            }
            cleaned = cleaned.trim();
            if (!cleaned) return;

            const utterance = new SpeechSynthesisUtterance(cleaned.substring(0, 40));
            utterance.lang = "th-TH";
            utterance.rate = 1.1; // ความเร็วเล็กน้อยให้ทันสตรีม

            // ให้เสียงล่าสุดแทรกพูดทันที
            window.speechSynthesis.cancel();
            window.speechSynthesis.speak(utterance);
        }

        function appendLog(htmlContent) {
            const logBox = document.getElementById("logBox");
            const now = new Date();
            const timeStr = now.toTimeString().split(' ')[0];
            const div = document.createElement("div");
            div.className = "log-line";
            div.innerHTML = `<span class="log-time">[${timeStr}]</span> ${htmlContent}`;
            logBox.appendChild(div);
            logBox.scrollTop = logBox.scrollHeight;
        }

        function connectWebSocket() {
            const protocol = location.protocol === 'https:' ? 'wss:' : 'ws:';
            ws = new WebSocket(`${protocol}//${location.host}/ws`);

            ws.onmessage = (event) => {
                const data = JSON.parse(event.data);
                
                if (data.type === "init") {
                    const dl = document.getElementById("historyList");
                    dl.innerHTML = "";
                    data.history.forEach(u => {
                        const opt = document.createElement("option");
                        opt.value = u;
                        dl.appendChild(opt);
                    });
                    if (data.history.length > 0) {
                        document.getElementById("usernameInput").value = data.history[0];
                    }
                    if (data.sign_api_key) {
                        document.getElementById("apiKeyInput").value = data.sign_api_key;
                    }
                    if (data.is_running) {
                        setRunningUI(true);
                    }
                } else if (data.type === "status") {
                    setRunningUI(data.status === "running");
                } else if (data.type === "log") {
                    appendLog(data.text);
                } else if (data.type === "stat_viewers") {
                    document.getElementById("valViewers").innerText = Number(data.count).toLocaleString();
                } else if (data.type === "comment") {
                    totalComments++;
                    document.getElementById("valComments").innerText = totalComments.toLocaleString();
                    appendLog(`<span class="comment-user">💬 ${data.user}:</span> ${data.comment}`);

                    const toSpeak = document.getElementById("chkReadName").checked ? 
                                    `${data.user.substring(0, 8)} ${data.comment}` : data.comment;
                    speakThai(toSpeak);
                } else if (data.type === "gift") {
                    totalGifts += data.count;
                    document.getElementById("valGifts").innerText = totalGifts.toLocaleString();
                    appendLog(`<span class="gift-highlight">🎁 ${data.user} มอบ ${data.gift_name} x${data.count}!</span>`);
                    playGiftChime();
                }
            };

            ws.onclose = () => {
                setTimeout(connectWebSocket, 2000);
            };
        }

        function setRunningUI(running) {
            isRunning = running;
            const btn = document.getElementById("btnToggle");
            const userInput = document.getElementById("usernameInput");
            const apiInput = document.getElementById("apiKeyInput");

            if (isRunning) {
                btn.innerText = "Stop";
                btn.className = "btn-action btn-stop";
                userInput.disabled = true;
                apiInput.disabled = true;
            } else {
                btn.innerText = "Start Running";
                btn.className = "btn-action btn-start";
                userInput.disabled = false;
                apiInput.disabled = false;
            }
        }

        document.getElementById("btnToggle").addEventListener("click", () => {
            if (!isRunning) {
                const user = document.getElementById("usernameInput").value.trim();
                const key = document.getElementById("apiKeyInput").value.trim();
                if (!user) {
                    alert("กรุณากรอก TikTok Username");
                    return;
                }
                totalComments = 0;
                totalGifts = 0;
                document.getElementById("valComments").innerText = "0";
                document.getElementById("valGifts").innerText = "0";
                document.getElementById("valViewers").innerText = "0";

                ws.send(JSON.stringify({
                    action: "start",
                    username: user,
                    api_key: key
                }));
            } else {
                ws.send(JSON.stringify({ action: "stop" }));
            }
        });

        // เชื่อมต่อเมื่อเปิดหน้าเว็บ
        connectWebSocket();
    </script>
</body>
</html>
"""

@app.get("/", response_class=HTMLResponse)
async def get_index():
    return HTMLResponse(content=HTML_CONTENT)

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            data = await websocket.receive_json()
            action = data.get("action")
            if action == "start":
                await manager.start_tiktok(data.get("username", ""), data.get("api_key", ""))
            elif action == "stop":
                await manager.stop_tiktok()
    except WebSocketDisconnect:
        manager.disconnect(websocket)
    except Exception:
        manager.disconnect(websocket)

# ----------------------------------------------------
# 4. Main Entry Point
# ----------------------------------------------------
if __name__ == "__main__":
    print("\n" + "="*50)
    print("🚀 TikTok LIVE WebApp พร้อมใช้งานแล้ว!")
    print("👉 เปิดเว็บเบราว์เซอร์ไปที่: http://127.0.0.1:8000")
    print("="*50 + "\n")
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="warning")
    