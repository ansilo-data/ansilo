FROM ansilo-source as source

ENV AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID}
ENV AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY}
ENV RUSTFLAGS=-Copt-level=0
ENV RUST_BACKTRACE=1
ENV CARGO_INCREMENTAL=false
ENV ANSILO_TESTS_ECS_USE_PUBLIC_IP=true
ENV ANSILO_TEST_PG_DIR=/usr/pgsql-14/

# Configure ecs-cli
RUN ecs-cli configure \
        --cluster dev-cluster \
        --region ap-southeast-2
RUN ecs-cli configure profile --profile-name default \
        --profile-name default \
        --access-key $AWS_ACCESS_KEY_ID \
        --secret-key $AWS_SECRET_ACCESS_KEY

# Run tests
RUN source $HOME/.cargo/env && cargo test
# Run benches
RUN source $HOME/.cargo/env && cargo bench
