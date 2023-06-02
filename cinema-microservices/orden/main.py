from fastapi import FastAPI
import uvicorn
import psycopg2
import urllib.request, json
import pika

app = FastAPI()

connection = pika.BlockingConnection(
pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

@app.get("/order")
async def order(client_id, n_seats):

   print(client_id, n_seats)

   conn = psycopg2.connect(
        database="orders", user='postgres', password='1234', host='127.0.0.1', port= '5432'
   )
   
   cur = conn.cursor()

   cur.execute("INSERT INTO public.\"Order\" (\"NumberOfSeats\", \"ClientID\", \"Status\") VALUES (%s, %s, %s)", (n_seats, client_id, "SUCCESS"))

   conn.commit()

   cur.execute("SELECT MAX(\"ID\") FROM public.\"Order\";")
   order_id = cur.fetchone()

   cur.close()
   conn.close()

   channel.queue_declare(queue='order')
   
   data = {
      "order_id" : str(order_id),
      "message" : "Success"
   }

   body = json.dumps(data)

   channel.basic_publish(exchange='', routing_key='payment', body=body)
   print(" [x] Mensaje Enviado ")

   # DO NOT CHANGE THIS: Lets assume that this send to a kafka and we dont know if fails or not
   try:
      send_order(client_id, n_seats, order_id)
   except Exception as e:
      print(e)


   return {"Result": "Success"}

def send_order(client_id, n_seats, order_id):
   print(client_id, n_seats, order_id[0])
   url = "http://localhost:7001/payment?order_id={}&n_seats={}".format(order_id[0], n_seats)

   response = urllib.request.urlopen(url)
   data = response.read()

   
   channel.queue_declare(queue='payment')

   def callback(ch, method, properties, body):
        print(" [x] Mensaje recibido !! %r" % body)
        message = json.loads(body)
        data = message['message']
        if data == "FAIL":
            print("Ocurrio un error!!!")

   channel.basic_consume(queue='payment', on_message_callback=callback, auto_ack=True)

   channel.start_consuming()

   channel.close()

   print(data)

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=7000)


# To excecute: python3 -m uvicorn main:app --reload --port 7000