from fastapi import FastAPI
import uvicorn
import psycopg2
import urllib.request, json
import pika

app = FastAPI()

connection = pika.BlockingConnection(
pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

@app.get("/payment")
async def payment(order_id, n_seats):

   print(order_id, n_seats)

   conn = psycopg2.connect(
        database="payments", user='postgres', password='1234', host='127.0.0.1', port= '5432'
   )
   
   cur = conn.cursor()

   # 50 is the price of the seat -- this is hardcoded for educational purposes
   total = int(n_seats) * 50
   print(total)
   cur.execute("INSERT INTO public.\"Payment\" (\"OrderId\", \"Total\", \"Status\") VALUES (%s, %s, %s)", (order_id, total, "SUCCESS"))

   conn.commit()

   cur.close()
   conn.close()

   channel.queue_declare(queue='payment')

   data = {
      "order_id" : str(order_id),
      "message" : "Success"
   }

   body = json.dumps(data)
   
   channel.basic_publish(exchange='', routing_key='seats', body=body)
   print(" [x] Mensaje Enviado ")
   
   # DO NOT CHANGE THIS: Lets assume that this send to a kafka and we dont know if fails or not
   try:
      send_payment(n_seats, order_id)
   except Exception as e:
      print(e)
      channel.basic_publish(
         exchange='',
         routing_key='order',
         body=json.dumps({"message" : "FAIL"}),
         properties=pika.BasicProperties(
         delivery_mode=2
         )
      )

   return {"Result": "Success"}

def send_payment(n_seats, order_id):
   print(n_seats, order_id)
   url = "http://localhost:7002/seats?order_id={}&n_seats={}".format(order_id, n_seats)

   response = urllib.request.urlopen(url)
   data = response.read()

   channel.queue_declare(queue='seats')

   def callback(ch, method, properties, body):
        print(" [x] Mensaje recibido !! %r" % body)
        message = json.loads(body)
        data = message['message']
        print(data)
        if data == "FAIL":
         channel.basic_publish(
               exchange='',
               routing_key='order',
               body=json.dumps({"message" : "FAIL"}),
               properties=pika.BasicProperties(
               delivery_mode=2
               )
            )

   channel.basic_consume(queue='seats', on_message_callback=callback, auto_ack=True)

   channel.start_consuming()

   channel.close()

   print(data)

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=7001)


# To excecute: python3 -m uvicorn main:app --reload --port 7000