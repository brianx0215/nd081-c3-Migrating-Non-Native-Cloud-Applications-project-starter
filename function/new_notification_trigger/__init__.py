import logging
import azure.functions as func
import psycopg2
import os
from datetime import datetime
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

def main(msg: func.ServiceBusMessage):
    notification_id = int(msg.get_body().decode('utf-8'))
    logging.info('Python ServiceBus queue trigger processed message: %s',notification_id)

    # Get connection to database
    POSTGRES_URL = os.environ['POSTGRES_URL']
    POSTGRES_USER = os.environ['POSTGRES_USER']
    POSTGRES_PW = os.environ['POSTGRES_PW']
    POSTGRES_DB = os.environ['POSTGRES_DB']
    connection = psycopg2.connect(host = POSTGRES_URL, dbname = POSTGRES_DB, user = POSTGRES_USER, password = POSTGRES_PW)
    cursor = connection.cursor()
    
    try:
        # Get notification message and subject from database using the notification_id
        # You need to parse cursor.execute() a sequence as second argument instead of buiding the query directly.
        cursor.execute("SELECT message, subject FROM notification WHERE id = %s;",(notification_id,))
        message, subject = cursor.fetchone()
        # Get attendees email and name
        cursor.execute("SELECT first_name, last_name, email FROM attendee;")
        attendees = cursor.fetchall()
        # Loop through each attendee and send an email with a personalized subject
        for (first_name, last_name, email) in attendees:
            Mail(
                from_email='admin@techconf.com',
                to_emails = email,
                subject = "Techconf <{subject}>",
                html_content = "Hello {first_name} {last_name},<br>" \
                                "{message} <br>" \
                                "Regards,<br> Techconf Team"
            )
        # Update the notification table by setting the completed date and updating the status with the total number of attendees notified
        notification_date = datetime.utcnow()
        notification_info = 'Notified {} attendees'.format(len(attendees))
        # Excellent DB Table
        cursor.execute("UPDATE notification SET status = %s, completed_date = %s WHERE id = %s;",(notification_info, notification_date, notification_id))
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(error)
    finally:
        # Close connection
        connection.commit()
        cursor.close()
        connection.close()
        