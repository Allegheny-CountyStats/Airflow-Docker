#!/usr/bin/env python
# coding: utf-8
import smtplib
from email import encoders
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
import os

subject = os.getenv('email_subject')
to_emails = os.getenv('to_emails')
filename = os.getenv('outfile')
mail_relay  = os.getenv('mail_relay')

def send_email():
    message="Hello, attached is your CountyStat Report"
    s = smtplib.SMTP(host=mail_relay, port=25)
    msg = MIMEMultipart()
    msg['From']='CountyStat@alleghenycounty.us'
    msg['To']=to_emails
    msg['Subject']=subject
    msg.attach(MIMEText(message, 'plain'))
    
    # Open PDF file in binary mode
    with open(filename, "rb") as attachment:
        # Add file as application/octet-stream
        part = MIMEBase("application", "octet-stream")
        part.set_payload(attachment.read())
    # Encode file in ASCII characters to send by email    
    encoders.encode_base64(part)
    # Add header as key/value pair to attachment part
    part.add_header(
        "Content-Disposition",
        f"attachment; filename= {filename}",
    )
    # Add attachment to message and convert message to string
    msg.attach(part)
    # Send Email Message
    s.send_message(msg)
    del msg
    s.quit()

send_email()