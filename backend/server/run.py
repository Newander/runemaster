import uvicorn
from fastapi import FastAPI

fast_app = FastAPI()

if __name__ == '__main__':
    uvicorn.run('run:fast_app', host='127.0.0.1', port=5000)
