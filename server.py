from exchange import wsgi_app
import config
from flask import Flask, make_response, request

app = Flask(__name__)


@app.route('/')
def get_start_date():
    start_date = request.args.get('start_date', '')
    status = wsgi_app(start_date)
    content = b""
    resp = make_response(content, status)
    resp.headers['Content-type'] = 'text/plain'
    return resp


if __name__ == '__main__':
    app.run(host=config.SERVER_IP, port=config.SERVER_PORT, debug=config.DEBUG)
