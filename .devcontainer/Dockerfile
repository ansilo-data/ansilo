ARG VARIANT="bullseye"
FROM mcr.microsoft.com/vscode/devcontainers/rust:1-${VARIANT}

RUN apt-get update && export DEBIAN_FRONTEND=noninteractive && \
    apt-get -y install --no-install-recommends \
    curl vim htop ssh openssl libssl-dev sudo \
    # postgres reqs
    build-essential pkg-config \
    libreadline-dev zlib1g-dev flex bison libxml2-dev libxslt-dev libssl-dev libxml2-utils xsltproc clang

RUN mkdir -p /run/sshd

USER vscode

RUN mkdir -p ~/.ssh && touch ~/.ssh/authorized_keys \
    && sudo chmod 700 ~/.ssh && sudo chmod 600 ~/.ssh/authorized_keys

# First load user-specific init scripts...
RUN touch ~/.initenvrc && echo 'source ~/.initenvrc' | tee -a ~/.bashrc ~/.zshrc 

RUN cargo install cargo-pgx --version 0.6.0
RUN cargo pgx init --pg15 download

RUN cargo install cargo-edit

RUN echo 'export CARGO_HOME="${WORKSPACE_HOME}/ansilo/.cargo"' | tee -a ~/.bashrc ~/.zshrc && \
    echo 'if [[ "${PATH}" != *"${CARGO_HOME}/bin"* ]]; then export PATH="${CARGO_HOME}/bin:${PATH}"; fi' | tee -a ~/.bashrc ~/.zshrc

# Install OpenJDK
RUN sudo apt-get update && export DEBIAN_FRONTEND=noninteractive && \
    sudo apt-get -y install openjdk-17-jdk maven
RUN echo 'export JAVA_HOME="$(dirname $(dirname $(readlink -f $(which java))))"' | tee -a ~/.bashrc ~/.zshrc 
RUN echo 'export MAVEN_OPTS="-Dmaven.repo.local=${WORKSPACE_HOME}/ansilo/.m2 -Djava.net.preferIPv4Stack=true"' | tee -a ~/.bashrc ~/.zshrc 
# Ensure libjvm.so is in path
RUN echo "$(dirname $(dirname $(readlink -f $(which java))))/lib/server" | sudo tee /etc/ld.so.conf.d/jdk.conf && sudo ldconfig

# Maintain shell history between instances
RUN echo 'export HISTFILE="${WORKSPACE_HOME}/.sh_history"' | tee -a ~/.bashrc ~/.zshrc 

# Install mold linker
RUN cd ~/ && \
    git clone https://github.com/rui314/mold.git && \
    cd mold && \
    sudo ./install-build-deps.sh && \
    git checkout v1.3.0 && \
    make -j$(nproc) CXX=clang++ && \
    sudo make install && \
    cd .. && sudo rm -rf ./mold/
COPY .cargo /.cargo

# Install llvm linker
RUN sudo apt-get update && export DEBIAN_FRONTEND=noninteractive && \
    sudo apt-get -y install lld

# Install ecs-cli
# TODO: Hopefully https://github.com/aws/amazon-ecs-cli/pull/1150 gets merged and can use mainline
RUN sudo curl -Lo /usr/local/bin/ecs-cli https://ansilo-dev-tmp.s3.ap-southeast-2.amazonaws.com/ecs-cli && \
    sudo chmod +x /usr/local/bin/ecs-cli && \
    echo 'export $(strings /proc/1/environ | grep -P "(AWS|ECS)")' | tee -a ~/.bashrc ~/.zshrc && \
    ecs-cli configure --cluster dev-cluster --region ap-southeast-2

# Install aws cli
RUN cd /tmp/ && curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" && \
    unzip awscliv2.zip && \
    sudo ./aws/install && \
    rm -rf awscliv2.zip ./aws/

# Install azure cli
RUN curl -sL https://packages.microsoft.com/keys/microsoft.asc | \
    gpg --dearmor | \
    sudo tee /etc/apt/trusted.gpg.d/microsoft.gpg > /dev/null && \
    echo "deb [arch=amd64] https://packages.microsoft.com/repos/azure-cli/ $(lsb_release -cs) main" | \
    sudo tee /etc/apt/sources.list.d/azure-cli.list && \
    sudo apt-get update && \
    sudo apt-get install -y azure-cli

# Install netcat & socat
RUN sudo apt-get update && export DEBIAN_FRONTEND=noninteractive && \
    sudo apt-get -y install netcat socat

# Install postgres 15 server
RUN sudo sh -c 'echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list' && \
    wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add - && \
    sudo apt-get update -y && \
    sudo apt install -y postgresql-15

# Install https://github.com/TimeToogo/lazyprox
RUN curl --fail https://lazyprox.elliotlevin.dev/lazyprox-x86_64-unknown-linux-musl -o ~/lazyprox && \
    chmod +x ~/lazyprox && \
    sudo mv ~/lazyprox /usr/local/bin/lazyprox

# Install gdb
RUN sudo apt install -y gdb

# Install facl and add some perms
RUN sudo apt install -y acl

# Install mkcert
RUN sudo apt install -y libnss3-tools && \
    curl --fail "https://dl.filippo.io/mkcert/latest?for=linux/amd64" -o ~/mkcert && \
    chmod +x ~/mkcert && \
    sudo mv ~/mkcert /usr/local/bin/mkcert

# Install perf
RUN cd /tmp && \
    git clone --depth 1 https://git.kernel.org/pub/scm/linux/kernel/git/torvalds/linux.git && \
    cd linux/tools/perf && \
    make && \
    sudo cp perf /usr/bin && \
    cd / && sudo rm -rf /tmp/linux

# Install docker
RUN curl --fail https://get.docker.com | bash

# Install node.js
RUN curl -fsSL https://deb.nodesource.com/setup_16.x | sudo -E bash - && \
    sudo apt-get install -y nodejs && \
    sudo npm install -g npm@latest

# Add boot.sh file
COPY ./boot.sh /boot.sh
