FROM registry.access.redhat.com/ubi9/ubi

# Install openjdk
RUN yum install -y java-17-openjdk-headless maven-openjdk17 && \
    alternatives --set java java-17-openjdk.x86_64
ENV JAVA_HOME="/etc/alternatives/jre_17_openjdk/"
RUN echo "$JAVA_HOME/lib/server" | tee /etc/ld.so.conf.d/jdk.conf && \
    ldconfig

# Install postgres
RUN yum install -y https://download.postgresql.org/pub/repos/yum/reporpms/EL-9-x86_64/pgdg-redhat-repo-latest.noarch.rpm && \
    yum install -y postgresql14-server postgresql14-devel redhat-rpm-config
RUN sed -i 's/logging_collector = on/logging_collector = off/g' /usr/pgsql-14/share/postgresql.conf.sample
ENV PATH="${PATH}:/usr/pgsql-14/bin/"

# Install node and npm
RUN yum install -y nodejs npm

# Install openssl
RUN yum install -y openssl openssl-devel

# Install ecs-cli
RUN curl -Lo /usr/local/bin/ecs-cli https://ansilo-dev-tmp.s3.ap-southeast-2.amazonaws.com/ecs-cli  && \
    chmod +x /usr/local/bin/ecs-cli

# Install awscli
RUN yum install -y zip
RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" && \
    unzip awscliv2.zip && \
    ./aws/install --update

# Install azure cli
RUN rpm --import https://packages.microsoft.com/keys/microsoft.asc && \
    dnf install -y https://packages.microsoft.com/config/rhel/9.0/packages-microsoft-prod.rpm && \
    dnf install -y azure-cli

# Install utils
RUN yum install -y procps jq lld

# Add .cargo
ADD .github/workflows/docker/.cargo /.cargo

# Switch to non-root user
# Use the same uid/gid as github actions so we can mount in volumes without changing perms
RUN yum install -y sudo
RUN echo "%sudoers        ALL=(ALL)       NOPASSWD: ALL" >> /etc/sudoers.d/sudeors
RUN groupadd sudoers
RUN groupadd build -g 121
RUN adduser build -u 1001 -g build -G sudoers
RUN mkdir /build && chown build:build /build
USER build
ENV USER build

# Add source
WORKDIR /build

# Install rust
RUN curl --proto '=https' --tlsv1.3 -sSf https://sh.rustup.rs | sh -s -- -y
ENV PATH="/home/build/.cargo/bin:${PATH}"
ENV CARGO_INCREMENTAL="false"
