FROM registry.access.redhat.com/ubi9/ubi

# Install openjdk
RUN yum install -y java-17-openjdk-headless && \
    export JAVA_HOME="$(dirname $(dirname $(readlink -f $(which java))))" && \
    echo "$JAVA_HOME/lib/server" | tee /etc/ld.so.conf.d/jdk.conf && \
    ldconfig

# Install postgres
RUN yum install -y https://download.postgresql.org/pub/repos/yum/reporpms/EL-9-x86_64/pgdg-redhat-repo-latest.noarch.rpm && \
    yum install -y postgresql14-server postgresql14-devel

# Install node and npm
RUN yum install -y nodejs npm

# Install openssl
RUN yum install -y openssl openssl-devel

# Install ecs-cli
RUN curl -Lo /usr/local/bin/ecs-cli https://ansilo-dev-tmp.s3.ap-southeast-2.amazonaws.com/ecs-cli  && \
    chmod +x /usr/local/bin/ecs-cli  && \
    ecs-cli configure --cluster dev-cluster --region ap-southeast-2

# Install awscli
RUN yum install -y zip
RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" && \
    unzip awscliv2.zip && \
    ./aws/install --update

# Switch to non-root user
RUN adduser build
RUN mkdir /build && chown build:build /build
USER build

# Add source
WORKDIR /build

# Install rust
RUN curl --proto '=https' --tlsv1.3 -sSf https://sh.rustup.rs | sh -s -- -y

# Install cargo pgx
RUN source $HOME/.cargo/env && cargo install cargo-pgx --version 0.5.0-beta.0 
RUN source $HOME/.cargo/env && cargo pgx init --pg14 /usr/pgsql-14/bin/pg_config

# Add source
COPY . /build
