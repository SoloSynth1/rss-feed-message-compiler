import base64
import time

from flask import Flask, request, json

import subscriptions
import publisher

PORT = 8080
HOST = "0.0.0.0"

app = Flask(__name__)


def compile_and_publish_requests(time_after):
    all_subscriptions = subscriptions.get_all()
    feed_space_map = compile_feed_space_map(all_subscriptions)
    for feed, space_list in feed_space_map.items():
        message = {
            'feed': feed,
            'spaces': space_list,
            'onlyFetchAfter': time_after,
        }
        message_bytes = str.encode(json.dumps(message))
        publisher.publish(message_bytes)


def compile_feed_space_map(all_subscriptions):
    feed_space_map = {}
    for subscription in all_subscriptions:
        feed = subscription['feed']
        if feed not in feed_space_map:
            feed_space_map[feed] = []
        feed_space_map[feed].append(subscription['space'])
    return feed_space_map


@app.route('/', methods=['POST'])
def home_post():
    envelope = request.get_json()
    if not envelope:
        msg = 'no Pub/Sub message received'
        print(f'error: {msg}')
        return f'Bad Request: {msg}', 400

    if not isinstance(envelope, dict) or 'message' not in envelope:
        msg = 'invalid Pub/Sub message format'
        print(f'error: {msg}')
        return f'Bad Request: {msg}', 400

    pubsub_message = envelope['message']

    if isinstance(pubsub_message, dict) and 'frequency' in pubsub_message:

        # get 'frequency' param from pubsub payload to determine how far in time we need to go back
        frequency = base64.b64decode(pubsub_message['frequency']).decode('utf-8').strip()
        time_after = time.time() - (int(frequency) * 60)  # 'frequency' is provided in minutes, converting to seconds

        compile_and_publish_requests(time_after)

        return json.jsonify({}), 204
    else:
        msg = 'missing "frequency" parameter'
        return f'Bad Request: {msg}', 400


if __name__ == '__main__':
    app.run(host=HOST, port=PORT, debug=True)
