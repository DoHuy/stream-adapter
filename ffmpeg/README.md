ffmpeg base image
=================

The Dockerfile generates an alpine base image containing a custom build of ffmpeg 4.1 with libxml2 support (this is to support MPEG-DASH manifests).

Build and push to the Veritone DockerHub repo:
```
docker build -t ffmpeg-base .
docker tag ffmpeg-base veritone/ffmpeg-base:custom-tag
docker login ...
docker push veritone/ffmpeg-base:custom-tag
```

Then use `FROM veritone/ffmpeg-base:custom-tag` in the adapter's Dockerfile to specify this image as the base image.