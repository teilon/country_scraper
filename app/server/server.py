# server.py
import subprocess

from flask import Flask
app = Flask(__name__)

@app.route('/t')
def home():
    return {'message': 'hello'}

@app.route('/start')
def start():
    spider_name = "pop_crawler"

    # subprocess.check_output(['scrapy', 'crawl', spider_name])
    subprocess.Popen(['scrapy', 'crawl', spider_name])
    return {'message': 'start crawl'}

if __name__ == '__main__':
    app.run(debug=True)