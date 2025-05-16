import sys
import config

def app_flask():
    from app.flaskapp import app
    app.run(host="0.0.0.0")

def app_fastapi():
    import uvicorn
    uvicorn.run("app.fastapi:app", host="0.0.0.0", port=5001, log_level="info")

cli_tree = {
    "app": {
        "flask": app_flask,
        "fastapi": app_fastapi
    },
}

def interpret_argv(argv, tree):
    tree = tree[argv.pop(0)]
    if len(argv) != 0:
        interpret_argv(argv, tree)
    else:
        tree()

if __name__ == "__main__":
    interpret_argv(sys.argv[1:], cli_tree)
