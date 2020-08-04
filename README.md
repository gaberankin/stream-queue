dockerfiles and docker-compose file permits local running without external dependencies.  to run, 

```
docker-compose up
```

there are 2 apps in this - a producer which pushes messages into the queue, and a consumer, which pulls items off the queue and processes them.

To stop, `CTRL+c`, and then probably best to do

```
docker-compose down --remove-orphans
```
