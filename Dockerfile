FROM archlinux
WORKDIR /code
RUN pacman --noconfirm -Syu gcc make bash python-pipenv musl linux-headers
COPY Pipfile Pipfile.lock .
RUN pipenv install
COPY . .
CMD ["pipenv", "run", "flask", "--app", "main.py", "run", "--host", "0.0.0.0"]
