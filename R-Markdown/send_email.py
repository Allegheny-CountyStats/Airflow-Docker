import smtplib
from email import encoders
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
import os

# specify single image attachment with % within html/message
# speciffy single pdf attachment as string name of path/filename in file_attach parameter, or provide python list
# formated as ['file1.pdf','file2.pdf']


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

