import smtplib
from email import encoders
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
import os

to_emails = 'daniel.andrus@alleghenycounty.us'

def send_email(subject, to_emails, filename, mail_relay):
    message = "Hello, attached is your CountyStat Report"
    s = smtplib.SMTP(host=mail_relay, port=25)
    msg = MIMEMultipart()
    msg['From'] = 'CountyStat@alleghenycounty.us'
    msg['To'] = to_emails
    msg['Subject'] = subject
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
