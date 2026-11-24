import os
import smtplib
import time
from datetime import datetime
from email.header import Header
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from config_constants import (
    EMAIL_PASSWORD,
    EMAIL_RECIVER,
    EMAIL_SMTP_PORT,
    EMAIL_SMTP_SERVER,
    EMAIL_USERNAME,
)

# 发件人和收件人的邮箱地址
sender = EMAIL_USERNAME
pass_code = EMAIL_PASSWORD
# 发件人和收件人的邮箱地址

receiver = EMAIL_RECIVER

cache = {"last_email_time_by_subject": {}}
SEND_LOG_FILE = "data/email_send_log.txt"


def write_send_log(status, subject="", error=""):
    os.makedirs(os.path.dirname(SEND_LOG_FILE), exist_ok=True)
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    error_part = f"\terror={error}" if error else ""
    with open(SEND_LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"{timestamp}\tstatus={status}\tsubject={subject}{error_part}\n")


# 邮件主题和内容
def send_email(subject="", content="", html_content=None):
    now = time.time()
    last_email_time = cache["last_email_time_by_subject"].get(subject, 0)
    if (now - last_email_time) < 1800:
        print(f"同一主题30分钟内只能发送一次: {subject}")
        write_send_log("skipped_rate_limit", subject)
        return
    # 创建邮件对象：带 HTML 时发 multipart/alternative，客户端优先渲染 HTML，
    # 纯文本仍作为兜底（手机邮箱是比例字体，纯文本的空格对齐会塌）
    if html_content:
        message = MIMEMultipart("alternative")
        message.attach(MIMEText(content, "plain", "utf-8"))
        message.attach(MIMEText(html_content, "html", "utf-8"))
    else:
        message = MIMEText(content, "plain", "utf-8")
    message["From"] = Header(f"<{sender}>")
    message["To"] = Header(receiver, "utf-8")
    message["Subject"] = Header(subject, "utf-8")
    try:
        smtp_obj = smtplib.SMTP_SSL(EMAIL_SMTP_SERVER, EMAIL_SMTP_PORT)
        smtp_obj.login(sender, pass_code)
        smtp_obj.sendmail(sender, [receiver], message.as_string())
        print("邮件发送成功")
        cache["last_email_time_by_subject"][subject] = now
        write_send_log("sent", subject)
    except smtplib.SMTPException as e:
        print("Error: 无法发送邮件", e)
        write_send_log("send_error", subject, str(e))


if __name__ == "__main__":
    subject = "Python SMTP 邮件测试"
    content = "这是通过 Python 发送的测试邮件。"
    send_email(subject, content)
