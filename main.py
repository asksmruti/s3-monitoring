from app import s3v
from mailer import Mailer, Message
from lib import htmlTemplate, jsonRenderer
import flask
from flask import Flask
from apscheduler.schedulers.background import BackgroundScheduler

___author__ = "Smruti Sahoo"

app = Flask(__name__)

# Set AWS profile name
profile = "<AWS profile name>"
email_receiver = "Receviers email address"
email_sender = "Senders email address"
email_subject = "ALERT!! Outdated buckets"
email_server = "<Email Server>"
smtp_port = "<Mail server port>"


def __getstatus__():
    gc, sc = s3v.init_session(profile)
    db_list = s3v.get_db(gc)
    unhealthy_datasource = s3v.get_tab(sc, gc, db_list)
    return unhealthy_datasource


def email_notification():
    msg = htmlTemplate.generate(__getstatus__())
    message = Message(From=email_sender,
                      To=email_receiver,
                      charset="utf-8")
    message.Subject = email_subject
    message.Html = msg
    sender = Mailer(email_server, smtp_port)
    sender.send(message)


@app.route('/v1/api/validator')
def raw_response():
    return flask.jsonify(jsonRenderer.json_response(__getstatus__()))


@app.route('/')
def html_response():
    return htmlTemplate.generate(__getstatus__())


if __name__ == '__main__':
    sched = BackgroundScheduler(daemon=True)
    sched.add_job(email_notification, 'cron', day_of_week='0-4', hour='18', minute='55')
    sched.start()
    app.run(debug=True, use_reloader=False, port=3000)
