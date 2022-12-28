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

    # TODO: Get connection to database
    userNamePostgres = 'admin_pg_3@migrate-postgresql-udacity-3'
    passwordPostgres = 'azurelogin3.'
    serverNamePostgres = 'migrate-postgresql-udacity-3.postgres.database.azure.com'
    portPostGres = '5432'
    databaseName = 'techconfdb'
    connectionString = "host=%s dbname=%s user=%s password=%s port=%s"%(serverNamePostgres, databaseName, userNamePostgres, passwordPostgres, portPostGres)

    connectDatabase = psycopg2.connect(connectionString)
    cursorDatabase = connectDatabase.cursor()

    try:
        # TODO: Get notification message and subject from database using the notification_id
        notification_query = """SELECT message, subject FROM notification WHERE id = %d;""" % (notification_id)
        cursorDatabase.execute(notification_query)
        notification_result = cursorDatabase.fetchone()

        notification_message = notification_result[0]
        notification_subject = notification_result[1]

        # TODO: Get attendees email and name
        attendees_query = """SELECT email, first_name FROM attendee;"""
        cursorDatabase.execute(attendees_query)
        attendees_result = cursorDatabase.fetchall()

        # TODO: Loop through each attendee and send an email with a personalized subject
        for attendee in attendees_result:
            Mail('info@techconf.com', attendee[0], notification_subject, notification_message)

        # TODO: Update the notification table by setting the completed date and updating the status with the total number of attendees notified
        completed_date = datetime.utcnow()
        status = 'Notified ' + str(len(attendees_result)) + ' attendees'
        notification_update_command = """UPDATE notification SET completed_date = '%s', status = '%s' WHERE id = %d;""" %(completed_date, status, notification_id)
        cursorDatabase.execute(notification_update_command)
        connectDatabase.commit()

    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(error)
    finally:
        # TODO: Close connection
        connectDatabase.close()

