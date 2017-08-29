FROM zoltannz/hadoop-ubuntu:2.8.1

RUN apt-get install maven -y

# Define mountable directories.
VOLUME ["/aol"]
VOLUME ["/wordcount"]

# Define working directory.
WORKDIR /