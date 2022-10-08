---
sidebar_position: 2
---

# Installation

:::info
Once you have acquired your credentials you may follow these instructions to download Ansilo
for development or production use.

If you do not have credentials please contact us on [get@ansilo.io](mailto:get@ansilo.io).
:::

Ansilo is deployed as a sidecar container alongside your services or data stores.

### 1. Install [Docker](https://docs.docker.com/get-docker/)

Install docker on your machine using the [instructions provided](https://docs.docker.com/get-docker/).

### 2. Login to the Ansilo Registry

Using the credentials provided to you, login to the private Ansilo Docker registry.

```bash
docker login https://get.ansilo.tech
```

You will be prompted for the username and password provided to you.
Upon entering those you should see: `Login succeeded!`.

:::caution

If you are not able to login, please contact us at [support@ansilo.io](mailto:support@ansilo.io)

:::

### 3. Pull the Ansilo base image

Now pull the Ansilo base image to your local machine:

```bash
docker pull get.ansilo.tech/ansilo-prod
```

:::tip
This will pull the latest version of Ansilo by default.
To specify a specify version use the following command: 

`docker pull get.ansilo.tech/ansilo-prod:1.2.3`
:::

### 4. Verify the image runs correctly

Run the following command to check that your base image is working.

```bash
docker run --rm get.ansilo.tech/ansilo-prod help
```

And you should see the following output:

```txt
[INFO ansilo_main] Hi, thanks for using Ansilo!
Arguments for running the Ansilo main program

Usage: ansilo <COMMAND>
...
```

You should be ready to start developing with Ansilo! [Check out the fundamentals to get started](/docs/fundamentals/architecture/).