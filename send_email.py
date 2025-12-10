import smtplib
from email.mime.text import MIMEText
from email.header import Header
import time
from config_constants import (
    EMAIL_SMTP_SERVER,
    EMAIL_SMTP_PORT,
    EMAIL_USERNAME,
    EMAIL_PASSWORD,
    EMAIL_RECIVER,
)

# 发件人和收件人的邮箱地址
sender = EMAIL_USERNAME
pass_code = EMAIL_PASSWORD
# 发件人和收件人的邮箱地址

receiver = EMAIL_RECIVER

cache = {"last_email_time": 0}


# 邮件主题和内容
def send_email(subject="", content=""):
    now = time.time()
    if (now - cache["last_email_time"]) < 1800:
        print("30分钟内只能发送一次！")
        return
    # 创建邮件对象
    message = MIMEText(content, "plain", "utf-8")
    message["From"] = Header(f"<{sender}>")
    message["To"] = Header(receiver, "utf-8")
    message["Subject"] = Header(subject, "utf-8")
    try:
        smtp_obj = smtplib.SMTP_SSL(EMAIL_SMTP_SERVER, EMAIL_SMTP_PORT)
        smtp_obj.login(sender, pass_code)
        smtp_obj.sendmail(sender, [receiver], message.as_string())
        print("邮件发送成功")
        cache["last_email_time"] = now
    except smtplib.SMTPException as e:
        print("Error: 无法发送邮件", e)


if __name__ == "__main__":
    subject = "Python SMTP 邮件测试"
    content = "这是通过 Python 发送的测试邮件。"
    send_email(subject, content)
