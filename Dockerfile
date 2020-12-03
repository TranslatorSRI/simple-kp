FROM python:3.8.1-buster

# set up murphy
ARG UID=1000
ARG GID=1000
RUN groupadd -o -g $GID murphy
RUN useradd -m -u $UID -g $GID -s /bin/bash murphy

# set up requirements
WORKDIR /home/murphy
ADD --chown=murphy:murphy ./requirements.txt /home/murphy/requirements.txt
RUN pip install -r /home/murphy/requirements.txt

# get package
ADD --chown=murphy:murphy . .

# become murphy
ENV HOME=/home/murphy
ENV USER=murphy
USER murphy

# set up entrypoint
ENTRYPOINT ["/home/murphy/main.sh"]