import sys
from app.flaskapp import app

def app_flask():
    app.run(host="0.0.0.0")

cli_tree = {
        "app": {
            "flask": app_flask
            }
        }

def interpret_argv(argv, tree):
    tree = tree[argv.pop(0)]
    if len(argv) != 0:
        interpret_argv(argv, tree)
    else:
        tree()

if __name__ == "__main__":
    interpret_argv(sys.argv[1:], cli_tree)
