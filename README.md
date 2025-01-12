# Adding a Heartbeat to the Firehose Client

While implementing the Feed Generator on Heroku I was running into issues where the Firehose Client would silently stop receiving messages. I'd have to restart the server once or twice a day when this happened.

In this fork I added a heartbeat monitor to the `FirehoseClient` class to detect if the client has stopped receiving messages. If the time since the last message exceeds the timeout threshold then the client will be restarted.

Adding the heartbeat monitor to the generator resolved the issue with the silent failures. When the heartbeat monitor triggers the reconnect the client closes its existing connection and will open a new one. The client will resume receiving messages from where it left off.

There is an open issue on the [atproto SDK repo](https://github.com/MarshalX/atproto/issues/489) to add a timeout to the socket connection. So maybe this will be fixed at the SDK level in the future. Since I'm already maintaining my own fork for the feed generator, I decided to implement the solution as a part of the feed generator instead of also maintaining a fork of the SDK. 

# ATProto Feed Generator powered by [The AT Protocol SDK for Python](https://github.com/MarshalX/atproto)

> Feed Generators are services that provide custom algorithms to users through the AT Protocol.

Official overview (read it first): https://github.com/bluesky-social/feed-generator#overview

## Getting Started

We've set up this simple server with SQLite to store and query data. Feel free to switch this out for whichever database you prefer.

Next, you will need to do two things:

1. Implement filtering logic in `server/data_filter.py`.
2. Copy `.env.example` to `.env`
3. Optionally implement custom feed generation logic in `server/algos`.

We've taken care of setting this server up with a did:web. However, you're free to switch this out for did:plc if you like - you may want to if you expect this Feed Generator to be long-standing and possibly migrating domains.

## Publishing your feed

To publish your feed, simply run `python publish_feed.py`.

To update your feed's display data (name, avatar, description, etc.), just update the relevant variables in `.env` and re-run the script.

After successfully running the script, you should be able to see your feed from within the app, as well as share it by embedding a link in a post (similar to a quote post).

## Running the Server

Install Python 3.7+.

Run `setupvenv.sh` to setup a virtual environment and install the dependencies:

```shell
./setupvenv.sh
```

**Note**: To get value for `FEED_URI` you need to publish the feed first

To run a development Flask server:

```shell
flask run
```

**Warning** The Flask development server is not designed for production use. In production, you should use production WSGI server such as [`waitress`](https://flask.palletsprojects.com/en/stable/deploying/waitress/) behind a reverse proxy such as NGINX instead.

```shell
pip install waitress
waitress-serve --listen=127.0.0.1:8080 server.app:app
```

To run a development server with debugging:

```shell
flask --debug run
```

**Note**: Duplication of data stream instances in debug mode is fine.

**Warning**: If you want to run server in many workers, you should run Data Stream (Firehose) separately.

### Endpoints

- `/.well-known/did.json`
- `/xrpc/app.bsky.feed.describeFeedGenerator`
- `/xrpc/app.bsky.feed.getFeedSkeleton`

## License

MIT
