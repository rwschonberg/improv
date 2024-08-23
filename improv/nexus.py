from __future__ import annotations

import multiprocessing
import time
import uuid
import signal
import logging
import asyncio
import concurrent
import subprocess

from queue import Full
from datetime import datetime
from multiprocessing import Process, get_context
from importlib import import_module

import zmq.asyncio as zmq
from zmq import PUB, REP, REQ, SocketOption

from improv import log
from improv.broker import bootstrap_broker
from improv.log import bootstrap_log_server
from improv.messaging import (
    ActorStateMsg,
    ActorStateReplyMsg,
    ActorSignalMsg,
    BrokerInfoReplyMsg,
    BrokerInfoMsg,
    LogInfoMsg,
    LogInfoReplyMsg,
)
from improv.store import StoreInterface, RedisStoreInterface
from improv.actor import Signal, Actor, LinkInfo
from improv.config import Config
from improv.link import Link

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# TODO: actors should retry setup/state comms every N seconds
#  nexus should be able to poll actors for state
#  (and send them state updates?) helpful for getting
#  stragglers through config

# TODO: nexus actually only really provides information to actors.
#  in this way, we can actually just allow arbitrary actors to connect,
#  send nexus their information, and get a reply back of the broker and
#  logger ports. long-term plan though.

# TODO: links were implicitly assumed to be one connection per link.
#  It turns out a link can be present in many connections so I need
#  to think through exactly what this is going to mean for the design.
#  We can subscribe to multiple topics easy enough, but we can't
#  really pub to multiple topics at once without multiple messages.
#  but then again, this is what we would have done anyway before with
#  multiple downstreams connected to one upstream?

# TODO: hook Nexus up to logger - just add a handler once we're back from setup


class ActorState:
    def __init__(
        self, actor_name, status, nexus_in_port, hostname="localhost", sig_socket=None
    ):
        self.actor_name = actor_name
        self.status = status
        self.nexus_in_port = nexus_in_port
        self.hostname = hostname
        self.sig_socket = None


class Nexus:
    """Main server class for handling objects in improv"""

    def __init__(self, name="Server"):
        self.actor_in_socket_port: int | None = None
        self.actor_in_socket: zmq.Socket | None = None
        self.in_socket: zmq.Socket | None = None
        self.out_socket: zmq.Socket | None = None
        self.zmq_context: zmq.Context | None = None
        self.logger_pub_port: int | None = None
        self.logger_pull_port: int | None = None
        self.logger_in_socket: zmq.Socket | None = None
        self.p_logger: multiprocessing.Process | None = None
        self.broker_pub_port = None
        self.broker_sub_port = None
        self.broker_in_port = None
        self.broker_in_socket = None
        self.actor_states: dict[str, ActorState] = dict()
        self.redis_fsync_frequency = None
        self.store = None
        self.config = None
        self.name = name
        self.aof_dir = None
        self.redis_saving_enabled = False
        self.allow_setup = False
        self.outgoing_topics = dict()
        self.incoming_topics = dict()

    def __str__(self):
        return self.name

    def create_nexus(
        self,
        file=None,
        use_watcher=None,
        store_size=10_000_000,
        control_port=0,
        output_port=0,
        actor_in_port=None,
    ):
        """Function to initialize class variables based on config file.

        Starts a store of class Limbo, and then loads the config file.
        The config file specifies the specific actors that nexus will
        be connected to, as well as their links.

        Args:
            file (string): Name of the config file.
            use_watcher (bool): Whether to use watcher for the store.
            store_size (int): initial store size
            control_port (int): port number for input socket
            output_port (int): port number for output socket
            actor_in_port (int): port number for the socket which receives
                                 actor communications

        Returns:
            string: "Shutting down", to notify start() that pollQueues has completed.
        """

        curr_dt = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(f"************ new improv server session {curr_dt} ************")

        if file is None:
            logger.exception("Need a config file!")
            raise Exception  # TODO
        else:
            logger.info(f"Loading configuration file {file}:")
            self.load_config(file=file)
            with open(file, "r") as f:  # write config file to log
                logger.info(f.read())

        # set config options loaded from file
        # in Python 3.9, can just merge dictionaries using precedence
        cfg = self.config.settings
        if "use_watcher" not in cfg:
            cfg["use_watcher"] = use_watcher
        if "store_size" not in cfg:
            cfg["store_size"] = store_size
        if "control_port" not in cfg or control_port != 0:
            cfg["control_port"] = control_port
        if "output_port" not in cfg or output_port != 0:
            cfg["output_port"] = output_port
        if "actor_in_port" not in cfg and actor_in_port is None:
            actor_in_port = 0
        else:
            actor_in_port = (
                (int(cfg["actor_in_port"])) if "actor_in_port" in cfg else actor_in_port
            )

        # set up socket in lieu of printing to stdout
        self.zmq_context = zmq.Context()
        self.zmq_context.setsockopt(SocketOption.LINGER, 0)
        self.out_socket = self.zmq_context.socket(PUB)
        self.out_socket.bind("tcp://*:%s" % cfg["output_port"])
        out_port_string = self.out_socket.getsockopt_string(SocketOption.LAST_ENDPOINT)
        cfg["output_port"] = int(out_port_string.split(":")[-1])

        self.in_socket = self.zmq_context.socket(REP)
        self.in_socket.bind("tcp://*:%s" % cfg["control_port"])
        in_port_string = self.in_socket.getsockopt_string(SocketOption.LAST_ENDPOINT)
        cfg["control_port"] = int(in_port_string.split(":")[-1])

        self.actor_in_socket = self.zmq_context.socket(REP)
        self.actor_in_socket.bind(f"tcp://*:{actor_in_port}")
        in_port_string = self.actor_in_socket.getsockopt_string(
            SocketOption.LAST_ENDPOINT
        )
        self.actor_in_socket_port = int(in_port_string.split(":")[-1])

        self.broker_in_socket = self.zmq_context.socket(REP)
        self.broker_in_socket.bind("tcp://*:0")
        broker_in_port_string = self.broker_in_socket.getsockopt_string(
            SocketOption.LAST_ENDPOINT
        )
        self.broker_in_port = int(broker_in_port_string.split(":")[-1])

        loop = asyncio.get_event_loop()

        loop.set_debug(True)

        loop.run_until_complete(self.start_logger())

        logger.addHandler(log.ZmqLogHandler("localhost", self.logger_pull_port))

        loop.run_until_complete(self.start_message_broker())

        self.configure_redis_persistence()

        # default size should be system-dependent
        self._start_store_interface(store_size)
        logger.info("Redis server started")

        self.out_socket.send_string("StoreInterface started")

        # connect to store and subscribe to notifications
        logger.info("Create new store object")
        self.store = StoreInterface(server_port_num=self.store_port)
        logger.info(f"Redis server connected on port {self.store_port}")

        self.store.subscribe()

        # TODO: Better logic/flow for using watcher as an option
        self.p_watch = None
        if cfg["use_watcher"]:
            self.start_watcher()

        # Create dicts for reading config and creating actors
        self.comm_queues = {}
        self.sig_queues = {}
        self.data_queues = {}
        self.actors = {}
        self.flags = {}
        self.processes = []

        self.init_config()

        self.flags.update({"quit": False, "run": False, "load": False})
        self.allowStart = False
        self.stopped = False

        return (cfg["control_port"], cfg["output_port"], self.logger_pub_port)

    def load_config(self, file):
        """Load configuration file.
        file: a YAML configuration file name
        """
        self.config = Config(configFile=file)

    def init_config(self):
        """For each connection:
        create a Link with a name (purpose), start, and end
        Start links to one actor's name, end to the other.
        Nexus gives start_actor the Link as a q_in,
        and end_actor the Link as a q_out.
        Nexus maintains dict of name and associated Link.
        Nexus also has list of Links that it is itself connected to
        for communication purposes.

        OR
        For each connection, create 2 Links. Nexus acts as intermediary.

        Args:
            file (string): input config filepath
        """
        # TODO load from file or user input, as in dialogue through FrontEnd?

        flag = self.config.create_config()
        if flag == -1:
            logger.error(
                "An error occurred when loading the configuration file. "
                "Please see the log file for more details."
            )

        # create all data links requested from Config config
        self.create_connections()

        if self.config.hasGUI:
            # Have to load GUI first (at least with Caiman)
            name = self.config.gui.name
            m = self.config.gui  # m is ConfigModule
            # treat GUI uniquely since user communication comes from here
            try:
                visualClass = m.options["visual"]
                # need to instantiate this actor
                visualActor = self.config.actors[visualClass]
                self.create_actor(visualClass, visualActor)
                # then add links for visual
                for k, l in {
                    key: self.data_queues[key]
                    for key in self.data_queues.keys()
                    if visualClass in key
                }.items():
                    self.assign_link(k, l)

                # then give it to our GUI
                self.create_actor(name, m)
                self.actors[name].setup(visual=self.actors[visualClass])

                self.p_GUI = Process(target=self.actors[name].run, name=name)
                self.p_GUI.daemon = True
                self.p_GUI.start()

            except Exception as e:
                logger.error(f"Exception in setting up GUI {name}: {e}")

        else:
            # have fake GUI for communications
            q_comm = Link("GUI_comm", "GUI", self.name)
            self.comm_queues.update({q_comm.name: q_comm})

        # First set up each class/actor
        for name, actor in self.config.actors.items():
            if name not in self.actors.keys():
                # Check for actors being instantiated twice
                try:
                    self.create_actor(name, actor)
                    logger.info(f"Setting up actor {name}")
                except Exception as e:
                    logger.error(f"Exception in setting up actor {name}: {e}.")
                    self.quit()

        # Second set up each connection b/t actors
        # TODO: error handling for if a user tries to use q_in without defining it
        for name, link in self.data_queues.items():
            self.assign_link(name, link)

        if self.config.settings["use_watcher"]:
            watchin = []
            for name in self.config.settings["use_watcher"]:
                watch_link = Link(name + "_watch", name, "Watcher")
                self.assign_link(name + ".watchout", watch_link)
                watchin.append(watch_link)
            self.createWatcher(watchin)

    def configure_redis_persistence(self):
        # invalid configs: specifying filename and using an ephemeral filename,
        # specifying that saving is off but providing either filename option
        aof_dirname = self.config.get_redis_aof_dirname()
        generate_unique_dirname = self.config.generate_ephemeral_aof_dirname()
        redis_saving_enabled = self.config.redis_saving_enabled()
        redis_fsync_frequency = self.config.get_redis_fsync_frequency()

        if aof_dirname and generate_unique_dirname:
            logger.error(
                "Cannot both generate a unique dirname and use the one provided."
            )
            raise Exception("Cannot use unique dirname and use the one provided.")

        if aof_dirname or generate_unique_dirname or redis_fsync_frequency:
            if redis_saving_enabled is None:
                redis_saving_enabled = True
            elif not redis_saving_enabled:
                logger.error(
                    "Invalid configuration. Cannot save to disk with saving disabled."
                )
                raise Exception("Cannot persist to disk with saving disabled.")

        self.redis_saving_enabled = redis_saving_enabled

        if redis_fsync_frequency and redis_fsync_frequency not in [
            "every_write",
            "every_second",
            "no_schedule",
        ]:
            logger.error("Cannot use unknown fsync frequency ", redis_fsync_frequency)
            raise Exception(
                "Cannot use unknown fsync frequency ", redis_fsync_frequency
            )

        if redis_fsync_frequency is None:
            redis_fsync_frequency = "no_schedule"

        if redis_fsync_frequency == "every_write":
            self.redis_fsync_frequency = "always"
        elif redis_fsync_frequency == "every_second":
            self.redis_fsync_frequency = "everysec"
        elif redis_fsync_frequency == "no_schedule":
            self.redis_fsync_frequency = "no"
        else:
            logger.error("Unknown fsync frequency ", redis_fsync_frequency)
            raise Exception("Unknown fsync frequency ", redis_fsync_frequency)

        if aof_dirname:
            self.aof_dir = aof_dirname
        elif generate_unique_dirname:
            self.aof_dir = "improv_persistence_" + str(uuid.uuid1())

        if self.redis_saving_enabled and self.aof_dir is not None:
            logger.info(
                "Redis saving enabled. Saving to directory "
                + self.aof_dir
                + " on schedule "
                + "'{}'".format(self.redis_fsync_frequency)
            )
        elif self.redis_saving_enabled:
            logger.info(
                "Redis saving enabled with default directory "
                + "on schedule "
                + "'{}'".format(self.redis_fsync_frequency)
            )
        else:
            logger.info("Redis saving disabled.")

        return

    def start_nexus(self):
        """
        Puts all actors in separate processes and begins polling
        to listen to comm queues
        """
        for name, m in self.actors.items():
            if "GUI" not in name:  # GUI already started
                if "method" in self.config.actors[name].options:
                    meth = self.config.actors[name].options["method"]
                    logger.info("This actor wants: {}".format(meth))
                    ctx = get_context(meth)
                    p = ctx.Process(target=m.run, name=name)
                else:
                    ctx = get_context("fork")
                    p = ctx.Process(target=self.run_actor, name=name, args=(m,))
                    if "Watcher" not in name:
                        if "daemon" in self.config.actors[name].options:
                            p.daemon = self.config.actors[name].options["daemon"]
                            logger.info("Setting daemon for {}".format(name))
                        else:
                            p.daemon = True  # default behavior
                self.processes.append(p)

        self.start()

        loop = asyncio.get_event_loop()
        res = ""
        try:
            self.out_socket.send_string("Awaiting input:")
            res = loop.run_until_complete(self.poll_queues())
        except asyncio.CancelledError:
            logger.info("Loop is cancelled")

        try:
            logger.info(f"Result of run_until_complete: {res}")
        except Exception as e:
            logger.info(f"Res failed to await: {e}")

        logger.info(f"Current loop: {asyncio.get_event_loop()}")

        loop.stop()
        loop.close()
        logger.info("Shutdown loop")

    def start(self):
        """
        Start all the processes in Nexus
        """
        logger.info("Starting processes")

        for p in self.processes:
            logger.info(str(p))
            p.start()

        logger.info("All processes started")

    def destroy_nexus(self):
        """Method that calls the internal method
        to kill the processes running the store
        and the message broker
        """
        logger.warning("Destroying Nexus")
        self._closeStoreInterface()

        if self.out_socket:
            self.out_socket.close(linger=0)
        if self.in_socket:
            self.in_socket.close(linger=0)
        if self.actor_in_socket:
            self.actor_in_socket.close(linger=0)
        if self.broker_in_socket:
            self.broker_in_socket.close(linger=0)
        if self.logger_in_socket:
            self.logger_in_socket.close(linger=0)
        if self.zmq_context:
            self.zmq_context.destroy(linger=0)

        self._shutdown_broker()
        self._shutdown_logger()

    async def poll_queues(self):
        """
        Listens to links and processes their signals.

        For every communications queue connected to Nexus, a task is
        created that gets from the queue. Throughout runtime, when these
        queues output a signal, they are processed by other functions.
        At the end of runtime (when the gui has been closed), polling is
        stopped.

        Returns:
            string: "Shutting down", Notifies start() that pollQueues has completed.
        """
        self.actorStates = dict.fromkeys(self.actors.keys())
        self.actor_states = dict.fromkeys(self.actors.keys(), None)
        if not self.config.hasGUI:
            # Since Visual is not started, it cannot send a ready signal.
            try:
                del self.actorStates["Visual"]
            except Exception as e:
                logger.info("Visual is not started: {0}".format(e))
                pass

        self.tasks = []
        self.tasks.append(asyncio.create_task(self.process_actor_message()))
        self.tasks.append(asyncio.create_task(self.remote_input()))

        self.early_exit = False

        # add signal handlers
        loop = asyncio.get_event_loop()
        signals = (signal.SIGHUP, signal.SIGTERM, signal.SIGINT)
        for s in signals:
            loop.add_signal_handler(s, lambda s=s: self.stop_polling_and_quit(s))

        logger.info("Nexus signal handler added")

        while not self.flags["quit"]:
            try:
                done, pending = await asyncio.wait(
                    self.tasks, return_when=concurrent.futures.FIRST_COMPLETED
                )
            except asyncio.CancelledError:
                pass

            # sort through tasks to see where we got input from
            # (so we can choose a handler)
            for i, t in enumerate(self.tasks):
                if i == 0:
                    if t in done:
                        self.tasks[i] = asyncio.create_task(
                            self.process_actor_message()
                        )
                elif t in done:
                    self.tasks[i] = asyncio.create_task(self.remote_input())

        return "Shutting Down"

    def stop_polling_and_quit(self, signal):
        """
        quit the process and stop polling signals from queues

        Args:
            signal (signal): Signal for handling async polling.
                             One of: signal.SIGHUP, signal.SIGTERM, signal.SIGINT
            queues (improv.link.AsyncQueue): Comm queues for links.
        """
        logger.warning(
            "Shutting down via signal handler due to {}. \
                Steps may be out of order or dirty.".format(
                signal
            )
        )
        asyncio.ensure_future(self.stop_polling(signal))
        self.flags["quit"] = True
        self.early_exit = True
        self.quit()

    def process_actor_state_update(self, msg: ActorStateMsg):
        actor_state = None
        if msg.actor_name in self.actor_states.keys():
            actor_state = self.actor_states[msg.actor_name]
        if not actor_state:
            logger.info(
                f"Received state message from new actor {msg.actor_name}"
                f" with info: {msg.info}\n"
            )
            self.actor_states[msg.actor_name] = ActorState(
                msg.actor_name, msg.status, msg.nexus_in_port
            )
            actor_state = self.actor_states[msg.actor_name]
        else:
            logger.info(
                f"Received state message from actor {msg.actor_name}"
                f" with info: {msg.info}\n"
                f"Current state:\n"
                f"Name: {actor_state.actor_name}\n"
                f"Status: {actor_state.status}\n"
                f"Nexus control port: {actor_state.nexus_in_port}\n"
            )
            if msg.nexus_in_port != actor_state.nexus_in_port:
                pass
                # TODO: this actor's signal socket changed

            actor_state.actor_name = msg.actor_name
            actor_state.status = msg.status
            actor_state.nexus_in_port = msg.nexus_in_port

        logger.info(
            "Updated actor state:\n"
            f"Name: {actor_state.actor_name}\n"
            f"Status: {actor_state.status}\n"
            f"Nexus control port: {actor_state.nexus_in_port}\n"
        )

        if all(
            [
                actor_state is not None and actor_state.status == Signal.ready()
                for actor_state in self.actor_states.values()
            ]
        ):
            self.allowStart = True

        return True

    async def process_actor_message(self):
        msg = await self.actor_in_socket.recv_pyobj()
        if isinstance(msg, ActorStateMsg):
            if self.process_actor_state_update(msg):
                await self.actor_in_socket.send_pyobj(
                    ActorStateReplyMsg(
                        msg.actor_name, "OK", "actor state updated successfully"
                    )
                )
            else:
                await self.actor_in_socket.send_pyobj(
                    ActorStateReplyMsg(
                        msg.actor_name, "ERROR", "actor state update failed"
                    )
                )
            if (not self.allow_setup) and all(
                [
                    actor_state is not None and actor_state.status == Signal.waiting()
                    for actor_state in self.actor_states.values()
                ]
            ):
                logger.info("All actors connected to Nexus. Allowing setup.")
                self.allow_setup = True

            if (not self.allowStart) and all(
                [
                    actor_state is not None and actor_state.status == Signal.ready()
                    for actor_state in self.actor_states.values()
                ]
            ):
                logger.info("All actors ready. Allowing run.")
                self.allowStart = True

    async def remote_input(self):
        msg = await self.in_socket.recv_multipart()
        command = msg[0].decode("utf-8")
        await self.in_socket.send_string("Awaiting input:")
        if command == Signal.quit():
            await self.out_socket.send_string("QUIT")
        await self.process_gui_signal([command], "TUI_Nexus")

    async def process_gui_signal(self, flag, name):
        """Receive flags from the Front End as user input"""
        name = name.split("_")[0]
        if flag:
            logger.info("Received signal from user: " + flag[0])
            if flag[0] == Signal.run():
                logger.info("Begin run!")
                # self.flags['run'] = True
                await self.run()
            elif flag[0] == Signal.setup():
                logger.info("Running setup")
                await self.setup()
            elif flag[0] == Signal.ready():
                logger.info("GUI ready")
                self.actorStates[name] = flag[0]
            elif flag[0] == Signal.quit():
                logger.warning("Quitting the program!")
                await self.stop_polling_and_quit(Signal.quit())
                self.flags["quit"] = True
            elif flag[0] == Signal.load():
                logger.info("Loading Config config from file " + flag[1])
                self.load_config(flag[1])
            elif flag[0] == Signal.pause():
                logger.info("Pausing processes")
                # TODO. Also resume, reset

            # temporary WiP
            elif flag[0] == Signal.kill():
                # TODO: specify actor to kill
                list(self.processes)[0].kill()
            elif flag[0] == Signal.revive():
                dead = [p for p in list(self.processes) if p.exitcode is not None]
                for pro in dead:
                    name = pro.name
                    m = self.actors[pro.name]
                    actor = self.config.actors[name]
                    if "GUI" not in name:  # GUI hard to revive independently
                        if "method" in actor.options:
                            meth = actor.options["method"]
                            logger.info("This actor wants: {}".format(meth))
                            ctx = get_context(meth)
                            p = ctx.Process(target=m.run, name=name)
                        else:
                            ctx = get_context("fork")
                            p = ctx.Process(target=self.run_actor, name=name, args=(m,))
                            if "Watcher" not in name:
                                if "daemon" in actor.options:
                                    p.daemon = actor.options["daemon"]
                                    logger.info("Setting daemon for {}".format(name))
                                else:
                                    p.daemon = True

                    # Setting the stores for each actor to be the same
                    # TODO: test if this works for fork -- don't think it does?
                    al = [act for act in self.actors.values() if act.name != pro.name]
                    m.set_store_interface(al[0].client)
                    m.client = None
                    m._get_store_interface()

                    self.processes.append(p)
                    p.start()
                    m.q_sig.put_nowait(Signal.setup())
                    # TODO: ensure waiting for ready before run?
                    m.q_sig.put_nowait(Signal.run())

                self.processes = [p for p in list(self.processes) if p.exitcode is None]
            elif flag[0] == Signal.stop():
                logger.info("Nexus received stop signal")
                await self.stop()
        elif flag:
            logger.error("Unknown signal received from Nexus: {}".format(flag))

    async def setup(self):
        if not self.allow_setup:
            logger.error(
                "Not all actors connected to Nexus. Please wait, then try again."
            )
            return

        for actor in self.actor_states.values():
            logger.info("Starting setup: " + str(actor.actor_name))
            actor.sig_socket = self.zmq_context.socket(REQ)
            actor.sig_socket.connect(f"tcp://{actor.hostname}:{actor.nexus_in_port}")
            await actor.sig_socket.send_pyobj(
                ActorSignalMsg(actor.actor_name, Signal.setup(), "")
            )
            await actor.sig_socket.recv_pyobj()

    async def run(self):
        if self.allowStart:
            for actor in self.actor_states.values():
                await actor.sig_socket.send_pyobj(
                    ActorSignalMsg(actor.actor_name, Signal.run(), "")
                )
                await actor.sig_socket.recv_pyobj()
        else:
            logger.error("Not all actors ready yet, please wait and then try again.")

    def quit(self):
        logger.warning("Killing child processes")
        self.out_socket.send_string("QUIT")

        for q in self.sig_queues.values():
            try:
                q.put_nowait(Signal.quit())
            except Full:
                logger.warning("Signal queue {} full, cannot quit".format(q.name))
            except FileNotFoundError:
                logger.warning("Queue {} corrupted.".format(q.name))

        if self.config.hasGUI:
            self.processes.append(self.p_GUI)

        if self.p_watch:
            self.processes.append(self.p_watch)

        for p in self.processes:
            p.terminate()
            p.join()

        logger.warning("Actors terminated")

        self.destroy_nexus()

    async def stop(self):
        logger.warning("Starting stop procedure")
        self.allowStart = False

        for actor in self.actor_states.values():
            try:
                await actor.sig_socket.send_pyobj(
                    ActorSignalMsg(
                        actor.actor_name, Signal.stop(), "Nexus sending stop signal"
                    )
                )
                await actor.sig_socket.recv_pyobj()
            except Exception as e:
                logger.info(
                    f"Unable to send stop message "
                    f"to actor {actor.actor_name}: "
                    f"{e}"
                )
        self.allowStart = True

    def revive(self):
        logger.warning("Starting revive")

    async def stop_polling(self, stop_signal):
        """Cancels outstanding tasks and fills their last request.

        Puts a string into all active queues, then cancels their
        corresponding tasks. These tasks are not fully cancelled until
        the next run of the event loop.

        Args:
            stop_signal (improv.actor.Signal): Signal for signal handler.
            queues (improv.link.AsyncQueue): Comm queues for links.
        """
        logger.info("Received shutdown order")

        logger.info(f"Stop signal: {stop_signal}")
        shutdown_message = Signal.quit()
        for actor in self.actor_states.values():
            try:
                await actor.sig_socket.send_pyobj(
                    ActorSignalMsg(
                        actor.actor_name, shutdown_message, "Nexus sending quit signal"
                    )
                )
                await actor.sig_socket.recv_pyobj()
            except Exception as e:
                logger.info(
                    f"Unable to send shutdown message "
                    f"to actor {actor.actor_name}: "
                    f"{e}"
                )

        logger.info("Canceling outstanding tasks")

        [task.cancel() for task in self.tasks]

        logger.info("Polling has stopped.")

    def create_store_interface(self):
        """Creates StoreInterface"""
        return RedisStoreInterface(server_port_num=self.store_port)

    def _start_store_interface(self, size, attempts=20):
        """Start a subprocess that runs the redis store
        Raises a RuntimeError exception if size is undefined
        Raises an Exception if the redis store doesn't start

        Args:
            size: in bytes

        Raises:
            RuntimeError: if the size is undefined
            Exception: if the store doesn't start

        """
        if size is None:
            raise RuntimeError("Server size needs to be specified")

        logger.info("Setting up Redis store.")
        self.store_port = (
            self.config.get_redis_port()
            if self.config and self.config.redis_port_specified()
            else Config.get_default_redis_port()
        )
        if self.config and self.config.redis_port_specified():
            logger.info(
                "Attempting to connect to Redis on port {}".format(self.store_port)
            )
            # try with failure, incrementing port number
            self.p_StoreInterface = self.start_redis(size)
            time.sleep(3)
            if self.p_StoreInterface.poll():
                logger.error("Could not start Redis on specified port number.")
                raise Exception("Could not start Redis on specified port.")
        else:
            logger.info("Redis port not specified. Searching for open port.")
            for attempt in range(attempts):
                logger.info(
                    "Attempting to connect to Redis on port {}".format(self.store_port)
                )
                # try with failure, incrementing port number
                self.p_StoreInterface = self.start_redis(size)
                time.sleep(3)
                if self.p_StoreInterface.poll():  # Redis could not start
                    logger.info("Could not connect to port {}".format(self.store_port))
                    self.store_port = str(int(self.store_port) + 1)
                else:
                    break
            else:
                logger.error("Could not start Redis on any tried port.")
                raise Exception("Could not start Redis on any tried ports.")

        logger.info(f"StoreInterface start successful on port {self.store_port}")

    def start_redis(self, size):
        subprocess_command = [
            "redis-server",
            "--port",
            str(self.store_port),
            "--maxmemory",
            str(size),
            "--save",  # this only turns off RDB, which we want permanently off
            '""',
        ]

        if self.aof_dir is not None and len(self.aof_dir) == 0:
            raise Exception("Persistence directory specified but no filename given.")

        if self.aof_dir is not None:  # use specified (possibly pre-existing) file
            # subprocess_command += ["--save", "1 1"]
            subprocess_command += [
                "--appendonly",
                "yes",
                "--appendfsync",
                self.redis_fsync_frequency,
                "--appenddirname",
                self.aof_dir,
            ]
            logger.info("Redis persistence directory set to {}".format(self.aof_dir))
        elif (
            self.redis_saving_enabled
        ):  # just use the (possibly preexisting) default aof dir
            subprocess_command += [
                "--appendonly",
                "yes",
                "--appendfsync",
                self.redis_fsync_frequency,
            ]
            logger.info("Proceeding with using default Redis dump file.")

        logger.info(
            "Starting Redis server with command: \n {}".format(subprocess_command)
        )

        return subprocess.Popen(
            subprocess_command,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

    def _closeStoreInterface(self):
        """Internal method to kill the subprocess
        running the store
        """
        if hasattr(self, "p_StoreInterface"):
            try:
                self.p_StoreInterface.send_signal(signal.SIGINT)
                try:
                    self.p_StoreInterface.wait(timeout=120)
                    logger.info(
                        "StoreInterface close successful: {}".format(self.store_port)
                    )
                except subprocess.TimeoutExpired as e:
                    logger.error(e)
                    self.p_StoreInterface.send_signal(signal.SIGKILL)
                    logger.info("Killed datastore process")

            except Exception as e:
                logger.exception("Cannot close store {}".format(e))

    def create_actor(self, name, actor):
        """Function to instantiate actor, add signal and comm Links,
        and update self.actors dictionary

        Args:
            name: name of the actor
            actor: improv.actor.Actor
        """
        # Instantiate selected class
        mod = import_module(actor.packagename)
        clss = getattr(mod, actor.classname)
        outgoing_links = (
            self.outgoing_topics[actor.name]
            if actor.name in self.outgoing_topics
            else []
        )
        incoming_links = (
            self.incoming_topics[actor.name]
            if actor.name in self.incoming_topics
            else []
        )
        instance = clss(
            name=actor.name,
            nexus_comm_port=self.actor_in_socket_port,
            broker_sub_port=self.broker_sub_port,
            broker_pub_port=self.broker_pub_port,
            log_pull_port=self.logger_pull_port,
            outgoing_links=outgoing_links,
            incoming_links=incoming_links,
            **actor.options,
        )

        if "method" in actor.options.keys():
            # check for spawn
            if "fork" == actor.options["method"]:
                # Add link to StoreInterface store
                store = self.create_store_interface()
                instance.set_store_interface(store)
            else:
                # spawn or forkserver; can't pickle plasma store
                logger.info("No store for this actor yet {}".format(name))
        else:
            # Add link to StoreInterface store
            store = self.create_store_interface()
            instance.set_store_interface(store)

        # Update information
        self.actors.update({name: instance})

    def run_actor(self, actor: Actor):
        """Run the actor continually; used for separate processes
        #TODO: hook into monitoring here?

        Args:
            actor:
        """
        actor.run()

    def create_connections(self):
        for name, connection in self.config.connections.items():
            sources = connection["sources"]
            if not isinstance(sources, list):
                sources = [sources]
            sinks = connection["sinks"]
            if not isinstance(sinks, list):
                sinks = [sinks]

            name_input = tuple(
                ["inputs:"]
                + [name for name in sources]
                + ["outputs:"]
                + [name for name in sinks]
            )
            name = str(
                hash(name_input)
            )  # space-efficient key for uniquely referring to this connection
            logger.info(f"Created link name {name} for link {name_input}")

            for source in sources:
                source_actor = source.split(".")[0]
                source_link = source.split(".")[1]
                if source_actor not in self.outgoing_topics.keys():
                    self.outgoing_topics[source_actor] = [LinkInfo(source_link, name)]
                else:
                    self.outgoing_topics[source_actor].append(
                        LinkInfo(source_link, name)
                    )

            for sink in sinks:
                sink_actor = sink.split(".")[0]
                sink_link = sink.split(".")[1]
                if sink_actor not in self.incoming_topics.keys():
                    self.incoming_topics[sink_actor] = [LinkInfo(sink_link, name)]
                else:
                    self.incoming_topics[sink_actor].append(LinkInfo(sink_link, name))

    def assign_link(self, name, link):
        """Function to set up Links between actors
        for data location passing
        Actor must already be instantiated

        #NOTE: Could use this for reassigning links if actors crash?

        #TODO: Adjust to use default q_out and q_in vs being specified
        """
        classname = name.split(".")[0]
        linktype = name.split(".")[1]
        if linktype == "q_out":
            self.actors[classname].set_link_out(link)
        elif linktype == "q_in":
            self.actors[classname].set_link_in(link)
        elif linktype == "watchout":
            self.actors[classname].set_link_watch(link)
        else:
            self.actors[classname].add_link(linktype, link)

    # TODO: StoreInterface access here seems wrong, need to test
    def start_watcher(self):
        from improv.watcher import Watcher

        self.watcher = Watcher("watcher", self.create_store_interface("watcher"))
        q_sig = Link("watcher_sig", self.name, "watcher")
        self.watcher.setLinks(q_sig)
        self.sig_queues.update({q_sig.name: q_sig})

        self.p_watch = Process(target=self.watcher.run, name="watcher_process")
        self.p_watch.daemon = True
        self.p_watch.start()
        self.processes.append(self.p_watch)

    async def start_logger(self):
        self.p_logger = multiprocessing.Process(
            target=bootstrap_log_server,
            args=("localhost", self.broker_in_port, "remote.global.log"),
        )
        self.p_logger.start()
        logger_info: LogInfoMsg = await self.broker_in_socket.recv_pyobj()
        self.logger_pull_port = logger_info.pull_port
        self.logger_pub_port = logger_info.pub_port
        await self.broker_in_socket.send_pyobj(
            LogInfoReplyMsg(logger_info.name, "OK", "registered logger information")
        )

    async def start_message_broker(self):
        self.p_broker = multiprocessing.Process(
            target=bootstrap_broker, args=("localhost", self.broker_in_port)
        )
        self.p_broker.start()
        broker_info: BrokerInfoMsg = await self.broker_in_socket.recv_pyobj()
        self.broker_sub_port = broker_info.sub_port
        self.broker_pub_port = broker_info.pub_port
        await self.broker_in_socket.send_pyobj(
            BrokerInfoReplyMsg(broker_info.name, "OK", "registered broker information")
        )

    def _shutdown_broker(self):
        """Internal method to kill the subprocess
        running the message broker
        """
        if hasattr(self, "p_broker"):
            try:
                self.p_broker.terminate()
                try:
                    self.p_broker.join(timeout=120)
                    logger.info("Broker shutdown successful")
                except subprocess.TimeoutExpired as e:
                    logger.error(e)
                    self.p_broker.kill()
                    logger.info("Killed broker process")

            except Exception as e:
                logger.exception(f"Unable to close broker {e}")

    def _shutdown_logger(self):
        """Internal method to kill the subprocess
        running the logger
        """
        if self.p_logger:
            try:
                self.p_logger.terminate()
                try:
                    self.p_logger.join(timeout=120)
                    logger.info("Logger shutdown successful")
                except subprocess.TimeoutExpired as e:
                    logger.error(e)
                    self.p_logger.kill()
                    logger.info("Killed logger process")

            except Exception as e:
                logger.exception(f"Unable to close logger: {e}")
