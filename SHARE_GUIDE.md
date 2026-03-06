# DISCODATA Explorer - Share Guide

## Quick Start (Local)

Double-click `start.bat` or run:
```powershell
.\start.ps1
```

Open http://localhost:5000

---

## Share with Others (Public Link)

### Step 1: Install ngrok

```powershell
winget install ngrok.ngrok
```

### Step 2: Get ngrok token (free)

1. Sign up: https://dashboard.ngrok.com/signup
2. Get token: https://dashboard.ngrok.com/get-started/your-authtoken
3. Configure:
   ```powershell
   ngrok config add-authtoken YOUR_TOKEN
   ```

### Step 3: Run share script

```powershell
.\start-share.ps1
```

### Step 4: Share the link

Find the public URL in ngrok window:
```
https://xxxx-xxxx.ngrok-free.app
```

Share this link with others!

---

## Notes

- **ngrok free tier**: URL changes each time you restart
- **For permanent URL**: Upgrade ngrok or use other services (Cloudflare Tunnel, etc.)

## Stop Services

Close all PowerShell windows to stop.
