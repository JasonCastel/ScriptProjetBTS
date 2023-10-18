import paho.mqtt.client as mqtt
import json as json
import base64
import psycopg2


def on_connect(client, userdata, flags, rc):
    print("Connected with result code " + str(rc))
    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    client.subscribe("#")


# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
    resultat_complet = json.loads(msg.payload.decode())
    print(resultat_complet)
    payload = resultat_complet['uplink_message']['frm_payload']
    date = resultat_complet
    print("Information reçu :", base64.b64decode(payload).decode(), "\nà la date : ", resultat_complet['received_at'])
    varPrincip = base64.b64decode(payload).decode()
    i = 0
    try:

        while i < len(varPrincip):

              if varPrincip[1] != "#":
                  print("Il manque la donnée de température")
                  break
              elif varPrincip[3] != "$":
                  print("Il manque la donnée du volume")
                  break
              elif varPrincip[5] != "%":
                  print("Il manque la donnée de la hauteur")
                  break
              elif varPrincip[7] != "&":
                  print("Il manque l'id du bassin")
                  break
              else:
                if varPrincip[i] == "#":  # Valeur Hexa de "#"=23
                 temp = ord(varPrincip[i - 1])
                 print(temp)
                 if temp >= 80:
                     print("Le capteur de température doit avoir un problème")
                     break
                elif varPrincip[i] == "$":  # Valeur Hexa de "$"=24
                 vol = ord(varPrincip[i - 1])
                 print(vol)
                 if vol >120:
                     print("Le capteur de volume doit avoir un problème")
                     break
                elif varPrincip[i] == "%":  # Valeur Hexa de "%"=25
                 haut = ord(varPrincip[i - 1])
                 print(haut)
                 if haut >42:
                     print("Le capteur de hauteur doit avoir un problème")
                     break
                elif varPrincip[i] == "&":  # Valeur Hexa de "&"=26
                 id = ord(varPrincip[i - 1])
                 print(id)
                i += 1

        conn = psycopg2.connect(
            user="postgres",
            password="admin",
            host="172.20.10.3",
            port="5432",
            database="eaux"
            )
        cur = conn.cursor()
        cur.execute("INSERT INTO Mesure(temperature,volume,hauteur,id_bassin) VALUES(%s,%s,%s,%s)",
                    (temp, vol, haut, id))
        conn.commit()
        conn.close()

    except (Exception, psycopg2.Error) as error:
        print(error)


client = mqtt.Client()
client.username_pw_set("recupeauxcapt-1@ttn",
                       "NNSXS.4CMRYPQ5HSOOO536ROSQLXW3JPX6EOB7RTCSUUY.M7JAMDT7PG2PVT6LR2Q5KSKUC6W5XY3F3YLS5RRTU72RR5P4ACAQ")

client.on_connect = on_connect
client.on_message = on_message

client.connect("eu1.cloud.thethings.network", 1883, 60)


client.loop_forever()
