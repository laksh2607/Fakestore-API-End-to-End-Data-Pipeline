import json
import smtplib
from email.message import EmailMessage

def lambda_handler(event, context):
    message = event['message']
    subject = event['subject']
    recevier = event['recevier']
    res = send_mail(subject,message,recevier)
    return res

def send_mail(subject,message,recevier) :
    try :
        s = smtplib.SMTP('smtp.gmail.com', 587)
        s.starttls()
        s.login("nerdistaan4@gmail.com", "bcen oemq hrln onhg")
        msg = EmailMessage()
        msg['Subject'] = subject
        msg['From'] = "nerdistaan4@gmail.com"
        msg['To'] = recevier
        msg.set_content(message)
        s.send_message(msg)
        s.quit()
        return {
        'statusCode': 200,
        'Message': 'Mail Sent Successfully'
    }
    except Exception as e:
            return {
        'statusCode': 400,
        'Message': str(e)
    }