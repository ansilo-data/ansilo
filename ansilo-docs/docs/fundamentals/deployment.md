---
sidebar_position: 6
---
import Diagram from './diagrams/deployment.svg'

# Deployment

In order to deploy an Ansilo node we need to create an image which has all the configuration baked in.

<center>
    <Diagram width="90%" height="auto" className="auto-invert" />
</center>

### Build process

The [boilerplate repo](https://github.com/ansilo-data/template/) shows a working example of building
a production-ready image.

```yml
# By default we use the Ansilo base image
# Your organisation may have a custom base image that you want to reference here instead
FROM get.ansilo.tech/ansilo-prod

# Copy the configuration files into the image
ADD . /app/

# Optimise the image for fast startup
RUN ansilo build

# By default, the container will start the Ansilo server
# ENTRYPOINT [ "ansilo" ]
# CMD [ "run" ]
```