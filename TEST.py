from fastapi import FastAPI

app = FastAPI(title="KADAP")

@app.get("/health")
def health():
    return {"ok": True}

@app.get("/")
def root():
    return {"service": "KADAP", "message": "KADAP TEST"}