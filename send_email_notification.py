import base64
import logging
import os

import sendgrid


# receiver address and name
reciever_email = "receiver@example.com"
reciever_name = "Reciever Name"

# sender address and name
sender_email = "sender@example.com"
sender_name = "example@example.com"

# subject Line
subject = "Data Pipeline Notification"


def send_email(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    For sending email using sendgrid api


    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    # get message and prepare email text
    pubsub_message = base64.b64decode(event["data"]).decode("utf-8")
    message = {
        "personalizations": [
            {
                "to": [{"email": reciever_email, "name": reciever_name}],
                "subject": subject,
            }
        ],
        "from": {"email": sender_email, "name": sender_name},
        "reply_to": {"email": sender_email, "name": sender_name},
        "content": [
            {"type": "text/plain", "value": pubsub_message}
        ],  # plaintext message
    }
    # Set sendgrid api key as environment variable
    sg = sendgrid.SendGridAPIClient(os.environ["API_KEY"])
    response = sg.send(message)
    if response.status_code == 202:
        logging.info("Successfully sent email")
        return
    logging.error("Something went wrong")
    return
