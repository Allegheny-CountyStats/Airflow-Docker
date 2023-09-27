import smtplib
from email import encoders
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
import os

e_to_emails = os.getenv('TO_EMAILS')
e_from_email = os.getenv('FROM_EMAIL')
e_subject = os.getenv('SUBJECT')
e_message = os.getenv('MESSAGE')
e_img_attachment = os.getenv('ATTACHMENT_NAME', None)  # specify image attachment with % within html/message
e_file_attachment = os.getenv('FILE_ATTACHMENT', None)
e_mailrelay = os.getenv('MAILRELAY')


def attach_file(filename, msg_mime):
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
    msg_mime.attach(part)


def send_email(subject, from_email, to_emails, message, mailrelay, file_attach=None, img_attachment=None):
    s = smtplib.SMTP(host=mailrelay, port=25)
    msg = MIMEMultipart()
    msg['From'] = from_email
    msg['To'] = to_emails
    msg['Subject'] = subject
    if file_attach is not None:
        if isinstance(file_attach, list):
            for f_attach in file_attach:
                attach_file(f_attach, msg_mime=msg)
        else:
            attach_file(file_attach, msg_mime=msg)
    if img_attachment is not None:
        msg.attach(MIMEText(message % img_attachment, 'html')) # used for in line insert
        with open(img_attachment, 'rb') as fp:
            img = MIMEImage(fp.read())
        img.add_header('Content-ID', '<{}>'.format(img_attachment))
        msg.attach(img)
    # Send Email Message
    s.send_message(msg)
    del msg
    s.quit()


send_email(subject=e_subject, from_email=e_from_email, to_emails=e_to_emails, message=e_message,
           mailrelay=e_mailrelay, file_attach=e_file_attachment, img_attachment=e_img_attachment)
