FROM ansilo-source as source

ENV ANSILO_TEST_PG_DIR=/usr/pgsql-14/

# Configure ecs-cli
RUN ecs-cli configure profile --profile-name default \
        --profile-name default \
        --cluster dev-cluster \
        --region ap-southeast-2 \
        --access-key $AWS_ACCESS_KEY_ID \
        --secret-key $AWS_SECRET_ACCESS_KEY

# Run tests
RUN source $HOME/.cargo/env && cargo test
# Run benches
RUN source $HOME/.cargo/env && cargo bench
