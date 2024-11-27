# Render Quarto

## Instructions

This is a base image that can be used to render Quarto Documents. The Quarto version this was built with is `1.5.57`.

For sites, it is recommended to mount the target location on the webserver to the docker container and generate an `.sh` or `.Rscript` code to execute all rendering steps and then copy the output into the mounted folder location.
