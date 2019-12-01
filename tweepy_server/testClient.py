#!/usr/bin/env python3
"""Script for Tkinter GUI chat client."""
from socket import AF_INET, socket, SOCK_STREAM
from threading import Thread
import tkinter
import json
import  os
from datetime import datetime
def receive():
    """Handles receiving of messages."""
    now = datetime.now()
    here = os.path.dirname(os.path.abspath(__file__))
    filename = os.path.join(here, 'tweets_sample_ ' + now.strftime("%d_%m_%Y-%H:%M:%S")+'.json')   
    while True:
        try:
            msg = client_socket.recv(BUFSIZ).decode("utf-8")
           # msg = encoder.convert(msg)
            #msg_list.insert(tkinter.END, msg )
            with open(filename,"a", encoding='utf-8') as f :
                f.write(msg)
                f.write(", \n")
            
                
        except OSError:  # Possibly client has left the chat.
            pass


def send(event=None):  # event is passed by binders.
    """Handles sending of messages."""
    msg = my_msg.get()
    my_msg.set("")  # Clears input field.
    client_socket.send(bytes(msg, "utf8"))
    if msg == "{quit}":
        client_socket.close()
        top.quit()


def on_closing(event=None):
    """This function is to be called when the window is closed."""
    my_msg.set("{quit}")
    send()

top = tkinter.Tk()
top.title("Chatter")

messages_frame = tkinter.Frame(top)
my_msg = tkinter.StringVar()  # For the messages to be sent.
#my_msg.set("Type your messages here.")
scrollbar = tkinter.Scrollbar(messages_frame)  # To navigate through past messages.
# Following will contain the messages.
msg_list = tkinter.Listbox(messages_frame, height=15, width=50, yscrollcommand=scrollbar.set)
scrollbar.pack(side=tkinter.RIGHT, fill=tkinter.Y)
msg_list.pack(side=tkinter.LEFT, fill=tkinter.BOTH)
msg_list.pack()
messages_frame.pack()

entry_field = tkinter.Entry(top, textvariable=my_msg)
entry_field.bind("<Return>", send)
entry_field.pack()
send_button = tkinter.Button(top, text="Send", command=send)
send_button.pack()

top.protocol("WM_DELETE_WINDOW", on_closing)

#----Now comes the sockets part----
HOST = '127.0.0.1'
PORT = 1998
if not PORT:
    PORT = 1998
else:
    PORT = int(PORT)

BUFSIZ = 100000
ADDR = (HOST, PORT)

client_socket = socket(AF_INET, SOCK_STREAM)
client_socket.connect(ADDR)

receive_thread = Thread(target=receive)
receive_thread.start()
tkinter.mainloop()  # Starts GUI execution.