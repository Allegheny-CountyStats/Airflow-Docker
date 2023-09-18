import smtplib
from email import encoders
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
import os

# to_emails = 'daniel.andrus@alleghenycounty.us'
attachment = 'EditRecord_resize.png'


def send_email(subject, to_emails, message):
    s = smtplib.SMTP(host='mailrelay.allegheny.local', port=25)
    msg = MIMEMultipart()
    msg['From'] = 'CountyStat@alleghenycounty.us'
    msg['To'] = to_emails
    msg['Subject'] = subject
    msg.attach(MIMEText(message % (attachment), 'html'))
    with open(attachment, 'rb') as fp:
        img = MIMEImage(fp.read())
    img.add_header('Content-ID', '<{}>'.format(attachment))
    msg.attach(img)
    # Send Email Message
    s.send_message(msg)
    del msg
    s.quit()
