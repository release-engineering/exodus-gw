import logging
import select
import threading

import backoff
from dramatiq import Middleware

LOG = logging.getLogger("exodus-gw")


class PostgresNotifyMiddleware(Middleware):
    """Middleware using postgres LISTEN/NOTIFY to notify all connected consumers
    on certain events.
    """

    def __init__(self, db_engine, interval=5.0):
        self.__db_engine = db_engine
        self.__listener = None
        self.__listener_thread = None
        self.__listener_interval = interval

    def before_worker_boot(self, broker, worker):
        # As worker boots, we start a thread which is continuosly doing a LISTEN.
        self.__listener = Listener(
            broker, self.__db_engine, self.__listener_interval
        )

        self.__listener_thread = threading.Thread(
            name="pg-listener", target=self.__listener, daemon=True
        )
        self.__listener_thread.start()

    def do_pg_notify(self, broker):
        # Do a NOTIFY either using the broker's current session, or our
        # own if needed.
        if broker.session:
            return self.do_notify_with_db(broker.session)

        with self.__db_engine.connect() as connection:
            return self.do_notify_with_db(connection)

    def do_notify_with_db(self, db):
        db.execute("NOTIFY dramatiq")

    def before_worker_shutdown(self, broker, worker):
        # As worker shuts down we should shut down the listener thread.
        self.__listener.running = False
        self.__listener_thread.join()

    def after_ack(self, broker, message):
        self.do_pg_notify(broker)

    def after_nack(self, broker, message):
        self.do_pg_notify(broker)

    def after_enqueue(self, broker, message, delay):
        self.do_pg_notify(broker)


class Listener:
    def __init__(self, broker, db_engine, interval):
        self.broker = broker
        self.db_engine = db_engine
        # Note: interval just controls how often the listener checks 'running'
        # to see if we should shut down yet.
        self.interval = interval
        self.running = True

    @backoff.on_exception(
        backoff.expo,
        Exception,
        logger=LOG,
    )
    def __call__(self):
        with self.db_engine.connect().execution_options(
            isolation_level="AUTOCOMMIT"
        ) as connection:
            # This tells the server we're interested in notifications.
            connection.execute("LISTEN dramatiq")

            # For the next step we need to unwrap the sqlalchemy
            # connection facade and get down to the native psycopg2
            # connection, which is two levels deeper...
            c = connection.connection.connection

            while self.running:
                (readable, _, _) = select.select([c], [], [], self.interval)
                if readable:
                    # Got some notifications
                    c.poll()
                    # There is no payload so just drop the notifications
                    c.notifies = []

                    LOG.debug("PG listen notifying broker")
                    self.broker.notify()
