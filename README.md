# loopback4-example-kafka

A LoopBack 4 example application for Kafka integration.

See related issues:

- https://github.com/strongloop/loopback-next/issues/1925
- https://github.com/strongloop/loopback-next/issues/1884

## Getting started

### Start a Kafka instance

```sh
docker-compose up
```

### Start the application

```sh
npm start
```

Now you can try out on http://localhost:3000/explorer.

To use `curl`:

- Create new topics

```sh
curl -X POST "http://127.0.0.1:3000/topics" -H "accept: */*" -H "Content-Type: application/json" -d "[\"demo\"]"
```

- Publish messages to `demo` topic:

```sh
curl -X POST "http://127.0.0.1:3000/topics/demo/messages" -H "accept: */*" -H "Content-Type: application/json" -d "[\"test messsage\"]"
```

- Receive messages from `demo` topic:

```sh
curl -X GET "http://127.0.0.1:3000/topics/demo/messages?limit=3" -H "accept: */*"
```

[![LoopBack](<https://github.com/strongloop/loopback-next/raw/master/docs/site/imgs/branding/Powered-by-LoopBack-Badge-(blue)-@2x.png>)](http://loopback.io/)
