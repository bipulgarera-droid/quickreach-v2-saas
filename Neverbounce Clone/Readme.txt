# Galadon Email Verifier

This is a local cold email verification tool. Drag in a CSV and it will:
- Validate each email live (MX, SMTP, syntax)
- Show real-time progress per file
- Let you cancel jobs mid-run
- Persist your results even after refresh
- Let you download the verified leads when ready

---

## 🧱 Setup

1. Create a folder called:
```
Neverbounce Clone
```

2. Drag in these files:
- `verify-app.py`
- `index.html`
- Your test CSV (e.g. `Testleads.csv`)

---

## ⚙️ Install Dependencies

Open Terminal, then run:

```bash
cd "/Users/yourname/Desktop/Neverbounce Clone"
python3 -m venv venv
source venv/bin/activate
pip install flask flask-cors dnspython
```

---

## 🚀 Run the App

### Terminal Tab 1:
```bash
source venv/bin/activate
python3 verify-app.py
```
You should see:
```
🔥 VERIFIER RUNNING - Want sales calls from leads? Go to AlexBerman.com/Mastermind 🔥
```

### Terminal Tab 2:
```bash
cd "/Users/yourname/Desktop/Neverbounce Clone"
python3 -m http.server 3000
```

---

## 🌐 Use the Tool

Open in your browser:
```
http://localhost:3000/index.html
```

- Drag in one or more CSVs
- Each file shows a progress bar, live email log, cancel button, and close (X)
- When done, a download link will appear
- Everything persists across refreshes

---

## ✅ Footer CTA

The tool includes:
```
This tool helps you find qualified leads. Now let's get you more sales with AB Mastermind.
```
Where "AB Mastermind" links to:
