import os
import sys
import re
import json
import logging
import asyncio
import io
from datetime import datetime
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query
from fastapi.responses import HTMLResponse, Response
import uvicorn
from gtts import gTTS
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
# 2. Session Worker: คุม 1 TikTok Client ต่อ 1 Browser
# ----------------------------------------------------
app = FastAPI(title="TikTok LIVE Multi-User Dashboard with gTTS")

class UserLiveSession:
    """จัดการ Session ของแต่ละ Browser แยกอิสระ"""
    def __init__(self, websocket: WebSocket):
        self.ws = websocket
        self.client: TikTokLiveClient = None
        self.task: asyncio.Task = None
        self.is_running = False
        self.active_user = ""

    async def send_msg(self, message: dict):
        try:
            await self.ws.send_json(message)
        except Exception:
            pass

    async def start(self, username: str):
        await self.stop()

        username = username.strip().replace("@", "")
        if not username:
            return

        self.is_running = True
        self.active_user = username

        await self.send_msg({"type": "status", "status": "running", "username": username})
        await self.send_msg({"type": "log", "text": f"🔄 กำลังเชื่อมต่อไปยังห้อง LIVE ของ '{username}'..."})

        self.client = TikTokLiveClient(unique_id=username)

        if hasattr(self.client, "logger"):
            self.client.logger.setLevel(logging.CRITICAL)
            self.client.logger.handlers.clear()
            self.client.logger.propagate = False

        @self.client.on(ConnectEvent)
        async def on_connect(event: ConnectEvent):
            if not self.is_running or self.active_user != username:
                return
            await self.send_msg({"type": "log", "text": f"🟢 เกาะห้อง LIVE สำเร็จแล้ว! (Room ID: {self.client.room_id})"})

        @self.client.on(RoomUserSeqEvent)
        async def on_user_seq(event: RoomUserSeqEvent):
            if not self.is_running or self.active_user != username:
                return
            viewers = getattr(event, "total", 0)
            await self.send_msg({"type": "stat_viewers", "count": viewers})

        @self.client.on(CommentEvent)
        async def on_comment(event: CommentEvent):
            if not self.is_running or self.active_user != username:
                return
            user_name = event.user.nickname or event.user.unique_id
            await self.send_msg({
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

                await self.send_msg({
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
                    await self.send_msg({"type": "log", "text": f"❌ หลุดจากการเชื่อมต่อ: {e}"})
                    await self.stop()

        self.task = asyncio.create_task(runner())

    async def stop(self):
        self.is_running = False
        self.active_user = ""
        if self.client:
            try:
                await self.client.disconnect()
            except Exception:
                pass
            self.client = None

        if self.task and not self.task.done():
            self.task.cancel()
            self.task = None

        await self.send_msg({"type": "status", "status": "stopped"})
        await self.send_msg({"type": "log", "text": "✅ หยุดการทำงานเรียบร้อย"})


# ----------------------------------------------------
# 3. API แปลงข้อความเป็นเสียง gTTS
# ----------------------------------------------------
def generate_gtts_audio(text: str) -> bytes:
    tts = gTTS(text=text, lang="th", slow=False)
    buffer = io.BytesIO()
    tts.write_to_fp(buffer)
    buffer.seek(0)
    return buffer.read()

@app.get("/api/tts")
async def get_gtts_audio(text: str = Query(..., max_length=400)):
    cleaned = re.sub(r"http\S+|www\S+", "", text)
    cleaned = re.sub(r"\[.*?\]", "", cleaned).strip()
    if not cleaned:
        return Response(status_code=400)
    audio_bytes = await asyncio.to_thread(generate_gtts_audio, cleaned)
    return Response(content=audio_bytes, media_type="audio/mpeg")


# ----------------------------------------------------
# 4. HTML Frontend Template (พร้อม TikTok Credit Link & SVG Icon)
# ----------------------------------------------------
HTML_CONTENT = """
<!DOCTYPE html>
<html lang="th">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>TikTok LIVE Real-time Dashboard & gTTS</title>
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
            padding: 16px;
        }
        .container {
            width: 100%;
            max-width: 860px;
            background: #1e293b;
            border-radius: 14px;
            padding: 20px;
            box-shadow: 0 10px 25px rgba(0,0,0,0.4);
        }
        h1 {
            text-align: center;
            font-size: 21px;
            font-weight: 700;
            margin-bottom: 16px;
            color: #38bdf8;
        }
        .card {
            background: #0f172a;
            border-radius: 10px;
            padding: 14px;
            margin-bottom: 14px;
            border: 1px solid #334155;
        }
        .form-row {
            display: flex;
            align-items: center;
            margin-bottom: 10px;
            gap: 10px;
        }
        .form-row label {
            width: 120px;
            font-weight: 600;
            font-size: 13px;
            color: #cbd5e1;
        }
        .form-row input, .form-row select {
            flex: 1;
            padding: 8px 12px;
            border-radius: 6px;
            border: 1px solid #475569;
            background: #1e293b;
            color: #fff;
            font-size: 13px;
            outline: none;
        }
        .form-row input:focus, .form-row select:focus {
            border-color: #38bdf8;
        }
        .slider-container {
            flex: 1;
            display: flex;
            align-items: center;
            gap: 10px;
        }
        .slider-container input[type="range"] {
            flex: 1;
            accent-color: #38bdf8;
            cursor: pointer;
        }
        .slider-val {
            width: 45px;
            font-weight: 600;
            font-size: 13px;
            color: #38bdf8;
        }
        .options-group {
            display: flex;
            flex-wrap: wrap;
            gap: 16px;
            margin-top: 10px;
            padding-left: 120px;
        }
        .checkbox-label {
            display: flex;
            align-items: center;
            gap: 6px;
            font-size: 12px;
            cursor: pointer;
            user-select: none;
        }
        .checkbox-label input {
            width: 15px;
            height: 15px;
            accent-color: #0284c7;
            cursor: pointer;
        }
        .stats-grid {
            display: grid;
            grid-template-columns: repeat(3, 1fr);
            gap: 10px;
            margin-bottom: 14px;
        }
        .stat-card {
            background: #0f172a;
            border: 1px solid #334155;
            border-radius: 10px;
            padding: 12px;
            text-align: center;
        }
        .stat-title {
            font-size: 11px;
            color: #94a3b8;
            margin-bottom: 4px;
        }
        .stat-value {
            font-size: 22px;
            font-weight: 700;
        }
        .btn-action {
            width: 100%;
            padding: 11px;
            border-radius: 8px;
            border: none;
            font-size: 14px;
            font-weight: 700;
            cursor: pointer;
            transition: all 0.2s;
            margin-bottom: 14px;
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
            border-radius: 10px;
            border: 1px solid #334155;
            height: 400px;
            overflow-y: auto;
            padding: 14px;
            font-size: 18px;
            line-height: 1.6;
        }
        .log-line {
            margin-bottom: 8px;
            word-break: break-word;
        }
        .log-time {
            color: #64748b;
            font-size: 13px;
            margin-right: 8px;
        }
        .comment-user {
            color: #38bdf8;
            font-weight: 600;
        }
        .gift-highlight {
            color: #f43f5e;
            font-weight: 600;
        }

        /* --- Footer Credit --- */
        .footer-credit {
            display: flex;
            justify-content: center;
            align-items: center;
            gap: 8px;
            margin-top: 16px;
            font-size: 13px;
            color: #94a3b8;
        }
        .footer-credit a {
            display: inline-flex;
            align-items: center;
            gap: 6px;
            color: #f8fafc;
            text-decoration: none;
            font-weight: 600;
            padding: 4px 10px;
            border-radius: 20px;
            background: #0f172a;
            border: 1px solid #334155;
            transition: all 0.2s ease;
        }
        .footer-credit a:hover {
            color: #38bdf8;
            border-color: #38bdf8;
            background: #1e293b;
            transform: translateY(-1px);
        }
        .tiktok-icon {
            width: 15px;
            height: 15px;
            fill: currentColor;
        }
    </style>
</head>
<body>
    <div class="container">
        <h1>TikTok LIVE Comment Reader v1.0</h1>

        <div class="card">
            <div class="form-row">
                <label for="usernameInput">Username:</label>
                <input type="text" id="usernameInput" list="historyList" placeholder="เช่น mosfet1234 (ไม่ต้องใส่ @)">
                <datalist id="historyList"></datalist>
            </div>

            <div class="form-row">
                <label for="speedRange">ความเร็วเสียง:</label>
                <div class="slider-container">
                    <input type="range" id="speedRange" min="0.8" max="1.8" step="0.1" value="1.1">
                    <span class="slider-val" id="speedValText">1.1x</span>
                </div>
            </div>

            <div class="options-group">
                <label class="checkbox-label">
                    <input type="checkbox" id="chkTTS" checked> เปิดเสียงอ่าน gTTS (ไทย)
                </label>
                <label class="checkbox-label">
                    <input type="checkbox" id="chkGiftSound" checked> เสียงเตือนของขวัญ
                </label>
                <label class="checkbox-label">
                    <input type="checkbox" id="chkReadName"> อ่านชื่อคนส่งนำหน้า
                </label>
            </div>
        </div>

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

        <button id="btnToggle" class="btn-action btn-start">Start Running</button>

        <div class="log-box" id="logBox"></div>

        <!-- เครดิต TikTok พร้อมไอคอน -->
        <div class="footer-credit">
            <span>Created by</span>
            <a href="https://www.tiktok.com/@mosfet1234" target="_blank" rel="noopener noreferrer">
                <svg class="tiktok-icon" viewBox="0 0 448 512">
                    <path d="M448,209.91a210.06,210.06,0,0,1-122.77-39.25V349.38A162.55,162.55,0,1,1,185,188.31V278.2a74.62,74.62,0,1,0,52.23,71.18V0l88,0a121.18,121.18,0,0,0,1.86,22.17h0A122.18,122.18,0,0,0,381,102.39a121.43,121.43,0,0,0,67,20.14Z"/>
                </svg>
                <span>@mosfet1234</span>
            </a>
        </div>
    </div>

    <script>
        let ws;
        let isRunning = false;
        let totalComments = 0;
        let totalGifts = 0;

        const speedRange = document.getElementById("speedRange");
        const speedValText = document.getElementById("speedValText");
        speedRange.addEventListener("input", (e) => {
            speedValText.innerText = `${e.target.value}x`;
        });

        // จัดการประวัติ Username แยกตามเครื่องด้วย LocalStorage
        function loadLocalHistory() {
            try {
                const history = JSON.parse(localStorage.getItem("tiktok_history") || '["mosfet1234"]');
                const dl = document.getElementById("historyList");
                dl.innerHTML = "";
                history.forEach(u => {
                    const opt = document.createElement("option");
                    opt.value = u;
                    dl.appendChild(opt);
                });
                if (history.length > 0 && !document.getElementById("usernameInput").value) {
                    document.getElementById("usernameInput").value = history[0];
                }
            } catch(e) {}
        }

        function saveLocalHistory(username) {
            try {
                let history = JSON.parse(localStorage.getItem("tiktok_history") || '["mosfet1234"]');
                history = history.filter(u => u !== username);
                history.unshift(username);
                history = history.slice(0, 15);
                localStorage.setItem("tiktok_history", JSON.stringify(history));
                loadLocalHistory();
            } catch(e) {}
        }

        loadLocalHistory();

        // คิวเสียง gTTS
        const audioQueue = [];
        let isPlayingAudio = false;

        function playNextInQueue() {
            if (audioQueue.length === 0) {
                isPlayingAudio = false;
                return;
            }
            isPlayingAudio = true;
            const text = audioQueue.shift();
            const playbackRate = parseFloat(speedRange.value) || 1.1;

            const audio = new Audio(`/api/tts?text=${encodeURIComponent(text)}`);
            audio.playbackRate = playbackRate;

            audio.onended = () => { playNextInQueue(); };
            audio.onerror = () => { playNextInQueue(); };
            audio.play().catch(() => { playNextInQueue(); });
        }

        function speakGoogleTTS(text) {
            if (!document.getElementById("chkTTS").checked) return;

            let cleaned = text.replace(/https?:\/\/\S+/g, '');
            cleaned = cleaned.replace(/\[.*?\]/g, '').trim();
            if (!cleaned) return;

            if (audioQueue.length >= 6) {
                audioQueue.shift();
            }
            audioQueue.push(cleaned);

            if (!isPlayingAudio) {
                playNextInQueue();
            }
        }

        // Web Audio Context สำหรับเสียง Chime
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

                if (data.type === "status") {
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
                                    `${data.user} ${data.comment}` : data.comment;
                    speakGoogleTTS(toSpeak);
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

            if (isRunning) {
                btn.innerText = "Stop";
                btn.className = "btn-action btn-stop";
                userInput.disabled = true;
            } else {
                btn.innerText = "Start Running";
                btn.className = "btn-action btn-start";
                userInput.disabled = false;
            }
        }

        document.getElementById("btnToggle").addEventListener("click", () => {
            if (!isRunning) {
                const user = document.getElementById("usernameInput").value.trim();
                if (!user) {
                    alert("กรุณากรอก TikTok Username");
                    return;
                }
                saveLocalHistory(user);

                totalComments = 0;
                totalGifts = 0;
                document.getElementById("valComments").innerText = "0";
                document.getElementById("valGifts").innerText = "0";
                document.getElementById("valViewers").innerText = "0";

                ws.send(JSON.stringify({
                    action: "start",
                    username: user
                }));
            } else {
                ws.send(JSON.stringify({ action: "stop" }));
            }
        });

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
    await websocket.accept()
    session = UserLiveSession(websocket)
    try:
        while True:
            data = await websocket.receive_json()
            action = data.get("action")
            if action == "start":
                await session.start(data.get("username", ""))
            elif action == "stop":
                await session.stop()
    except WebSocketDisconnect:
        await session.stop()
    except Exception:
        await session.stop()

# ----------------------------------------------------
# 5. Main Entry Point
# ----------------------------------------------------
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    print("\n" + "="*50)
    print("🚀 TikTok LIVE Multi-Session WebApp พร้อมใช้งานแล้ว!")
    print(f"👉 เปิดเว็บเบราว์เซอร์ไปที่: http://127.0.0.1:{port}")
    print("="*50 + "\n")
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="warning")
    