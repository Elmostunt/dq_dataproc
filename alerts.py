import requests
from google.cloud import pubsub_v1

def format_msg(v_s, v_f, v_o, p_s, p_f, pr_o):
    message="La ejecución del componente de calidad ha finalizado \n"
    if len(v_s)>0:
        add="Las siguientes tablas han sido validadas exitosamente: \n"
        add=add+',\n '.join(v_s)
        message = message+add+"\n"
    if len(v_f)>0:
        add="Las siguientes tablas han fallado durante el proceso de validación: \n"
        add=add+',\n '.join(v_f)
        message = message+add+"\n"
    if len(v_o)>0:
        add="Las siguientes tablas han sido omitidas durante el proceso de validación: \n"
        add=add+',\n '.join(v_o)
        message = message+add+"\n"
    if len(p_s)>0:
        add="Las siguientes tablas han sido perfiladas exitosamente: \n"
        add=add+',\n '.join(p_s)
        message = message+add+"\n"
    if len(p_f)>0:
        add="Las siguientes tablas han fallado durante el processo de perfilamiento: \n"
        add=add+',\n '.join(p_f)
        message = message+add+"\n"
    if len(pr_o)>0:
        add="Las siguientes tablas han sido omitidas: \n"
        add=add+',\n '.join(pr_o)
        message = message+add+"\n"
    return message

#-msm_email_attached
#    Genera un correo de notificación.
#def send_completion_pubsub(domain,key,alert_targets,message):
#    atts={}
#    atts["subject"]=
#    atts["notification_channel"]=
#    atts["subject"]=
#    atts["notification_type"]=
#    atts["secret_manager_project_id"]=
#    atts["sender_email_secret"]=
#    atts["sender_password_secret"]=
#
#    publisher = pubsub_v1.PublisherClient()
#    topic_path = publisher.topic_path(project_id, topic)
#    publisher.publish(topic_path, data=out_payload.encode("utf-8"), **atts)



