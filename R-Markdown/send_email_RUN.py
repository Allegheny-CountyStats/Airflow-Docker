import smtplib
from email import encoders
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
import os

e_to_emails = os.getenv('TO_EMAILS')
e_subject = os.getenv('SUBJECT')
e_message = os.getenv('MESSAGE')
e_attachment = os.getenv('ATTACHMENT_NAME', None)  # specify image attachment with % within html/message
e_mailrelay = os.getenv('MAILRELAY')


def send_email(subject, from_email, to_emails, message, mailrelay, attachment=None):
    s = smtplib.SMTP(host=mailrelay, port=25)
    msg = MIMEMultipart()
    msg['From'] = from_email
    msg['To'] = to_emails
    msg['Subject'] = subject
    # Attachment
    if attachment is not None:
        msg.attach(MIMEText(message % attachment, 'html'))
        with open(attachment, 'rb') as fp:
            img = MIMEImage(fp.read())
        img.add_header('Content-ID', '<{}>'.format(attachment))
        msg.attach(img)
    # Send Email Message
    s.send_message(msg)
    del msg
    s.quit()


send_email(e_subject, e_to_emails, e_message, e_mailrelay, e_attachment)
