FROM ubi9/openjdk-17 as build

# Install rust
RUN curl --proto '=https' --tlsv1.3 -sSf https://sh.rustup.rs | sh

# Install postgres
RUN yum install -y https://download.postgresql.org/pub/repos/yum/reporpms/EL-9-x86_64/pgdg-redhat-repo-latest.noarch.rpm && \
    yum -qy module disable postgresql && \
    yum install -y postgresql14-server postgresql14-devel
