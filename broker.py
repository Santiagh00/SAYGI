from topic import Topic

class MessageBroker:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(MessageBroker, cls).__new__(cls)  #
            cls._instance.topics = {}
        return cls._instance

    #Singleton: garantiza que, sin importar cuántas veces hagas MessageBroker(), siempre obtienes el mismo objeto
    #si todavía no existe una instancia guardada, créala. Si ya existe, devuelve siempre la misma.

    def create_topic(self, topic_name):
        if topic_name not in self.topics:
            self.topics[topic_name] = Topic(topic_name)
            print(f" [BROKER] Topic '{topic_name}' registrado.")

    def subscribe(self, topic_name, callback):
        if topic_name not in self.topics:
            self.create_topic(topic_name)
        self.topics[topic_name].subscribe(callback)

    def publish(self, topic_name, message, pub_name):
        print(f" [BROKER] Recibiendo mensaje de '{pub_name}' para TOPIC: '{topic_name}'...")
        if topic_name in self.topics:
            subs = self.topics[topic_name].get_subscribers()
            if not subs:
                print(f" [BROKER] Sin suscriptores para '{topic_name}'.")
            else:
                print(f" [BROKER] Distribuyendo a {len(subs)} suscriptores...")
                for s in subs:
                    s(message)
        else:
            print(f" [BROKER] Topic '{topic_name}' no existe.")

broker_instance = MessageBroker()