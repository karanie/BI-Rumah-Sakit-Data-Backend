FROM archlinux
WORKDIR /code
RUN pacman --noconfirm -Syu gcc make bash python-pipenv musl linux-headers postgresql-libs
COPY Pipfile Pipfile.lock .
RUN pipenv install
COPY . .
#CMD ["pipenv", "run", "flask", "--app", "./app/flask/main.py", "run", "--host", "0.0.0.0"]
CMD ["pipenv", "run", "python", "main.py", "app", "flask" ]
