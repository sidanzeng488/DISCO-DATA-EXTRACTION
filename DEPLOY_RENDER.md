# Deploy to Render.com

## Step 1: Push to GitHub

First, create a GitHub repository and push your code:

```bash
cd "C:\Users\Sidan.Zeng\OneDrive - Media Analytics Limited\Desktop\DISCO link"

# Initialize git
git init
git add .
git commit -m "Initial commit"

# Create repo on GitHub, then:
git remote add origin https://github.com/YOUR_USERNAME/discodata-explorer.git
git branch -M main
git push -u origin main
```

## Step 2: Deploy on Render

1. Go to https://render.com and sign up/login

2. Click **"New +"** → **"Web Service"**

3. Connect your GitHub repository

4. Configure:
   - **Name**: `discodata-explorer`
   - **Runtime**: `Python`
   - **Build Command**: `pip install -r requirements.txt`
   - **Start Command**: `gunicorn app:app --bind 0.0.0.0:$PORT`

5. Click **"Create Web Service"**

6. Wait for deployment (2-3 minutes)

7. Your app will be live at: `https://discodata-explorer.onrender.com`

## Notes

- **Free tier**: App sleeps after 15 min of inactivity (first request will be slow)
- **Paid tier**: Always on, faster performance

## Files needed for Render

- `requirements.txt` - Python dependencies
- `render.yaml` - Render configuration (optional)
- `Procfile` - Process configuration (optional)
