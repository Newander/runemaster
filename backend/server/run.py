import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

fast_app = FastAPI()

origins = [
    "http://localhost:5000",
    "http://127.0.0.1:5000",
    "localhost:5000",
    "127.0.0.1:5000",
]

fast_app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"]
)


def run_server(port=5000):
    uvicorn.run('backend.server.run:fast_app', host='127.0.0.1', port=port)


if __name__ == '__main__':
    run_server()
