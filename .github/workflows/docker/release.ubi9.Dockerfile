FROM ansilo-source as source

RUN mvn --version
# Run build
RUN source $HOME/.cargo/env && cargo build --release
RUN source $HOME/.cargo/env && cargo pgx package -p ansilo-pgx --out-dir target/release/ansilo-pgx/

# Copy release artifacts
RUN mkdir artifacts
RUN cp target/release/ansilo-main artifacts && \
    cp target/release/*.jar artifacts && \
    cp -r target/release/frontend/** artifacts && \
    cp -r target/release/ansilo-pgx artifacts/pgx

# Create runtime image
FROM registry.access.redhat.com/ubi9/ubi

# Install openjdk
RUN yum install -y java-17-openjdk-headless && \
    export JAVA_HOME="$(dirname $(dirname $(readlink -f $(which java))))" && \
    echo "$JAVA_HOME/lib/server" | tee /etc/ld.so.conf.d/jdk.conf && \
    ldconfig

# Install postgres
RUN yum install -y https://download.postgresql.org/pub/repos/yum/reporpms/EL-9-x86_64/pgdg-redhat-repo-latest.noarch.rpm && \
    yum install -y postgresql14-server

# Install openssl
RUN yum install -y openssl

# Copy artifacts
RUN mkdir /ansilo/
COPY --from=source /build/artifacts/* /ansilo/
# Install postgres extension
RUN cp -r /ansilo/pgx / && rm -rf /ansilo/pgx

# Clean up
RUN yum clean all && \
    rpm -q java-17-openjdk-headless postgresql14-server openssl && \
    rm -rf /var/cache/yum && \
    rm -rf /tmp/

RUN mkdir /ansilo

EXPOSE 80 443
ENTRYPOINT [ "/ansilo/ansilo-main" ]
