from __future__ import print_function
import os.path
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials

def envio_reporte(reporte, correo):
    try
        mensaje = "Reporte de calidad de datos ,Email de destinatario"
        return apinviameelcorreoctm(mensaje);
    return 0

def send_message(service, user_id, message):

  try:
    message = (service.users().messages().send(userId=user_id, body=message)
               .execute())
    print 'Message Id: %s' % message['id']
    return message
  except errors.HttpError, error:
    print 'An error occurred: %s' % error


def create_message(sender, to, subject, message_text):
  message = MIMEText(message_text)
  message['to'] = 'correo@falabella.cl'
  message['from'] = datos@falabella.cl'
  message['subject'] = 'reporte'
  return {'raw': base64.urlsafe_b64encode(message.as_string())}

