FROM ansilo-build as build

ARG AWS_ACCESS_KEY_ID
ARG AWS_SECRET_ACCESS_KEY
ENV AWS_PAGER=""
ENV AWS_DEFAULT_REGION=ap-southeast-2
ENV AWS_REGION=ap-southeast-2
ENV RUST_BACKTRACE=1
ENV ANSILO_TESTS_ECS_USE_PUBLIC_IP=true
ENV ANSILO_TEST_PG_DIR=/usr/pgsql-14/

# Configure ecs-cli
RUN ecs-cli configure \
        --cluster dev-cluster \
        --region ${AWS_REGION}
RUN ecs-cli configure profile \
        --profile-name default \
        --access-key ${AWS_ACCESS_KEY_ID} \
        --secret-key ${AWS_SECRET_ACCESS_KEY}

# Run tests
RUN source $HOME/.cargo/env && cargo test
# Run benches
RUN source $HOME/.cargo/env && cargo bench
