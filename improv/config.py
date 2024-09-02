import yaml
import logging
from inspect import signature
from importlib import import_module

logger = logging.getLogger(__name__)


class CannotCreateConfigException(Exception):
    def __init__(self):
        super().__init__("Cannot create config")


class Config:
    """Handles configuration and logs of configs for
    the entire server/processing pipeline.
    """

    def __init__(self, config_file):
        self.actors = {}
        self.connections = {}
        self.hasGUI = False
        self.config_file = config_file

        with open(self.config_file, "r") as ymlfile:
            self.config = yaml.safe_load(ymlfile)

        if self.config is None:
            logger.error("The config file is empty")
            raise CannotCreateConfigException

        if type(self.config) is not dict:
            logger.error("Error: The config file is not in dictionary format")
            raise TypeError

        self.populate_defaults()

        self.settings = self.config["settings"]

    def populate_defaults(self):
        if "settings" not in self.config:
            self.config["settings"] = {}

        if "use_watcher" not in self.config["settings"]:
            self.config["settings"]["use_watcher"] = False

    def create_config(self):
        """Read yaml config file and create config for Nexus
        TODO: check for config file compliance, error handle it
        beyond what we have below.
        """
        cfg = self.config

        for name, actor in cfg["actors"].items():
            if name in self.actors.keys():
                raise RepeatedActorError(name)

            packagename = actor.pop("package")
            classname = actor.pop("class")

            try:
                __import__(packagename, fromlist=[classname])
                mod = import_module(packagename)

                actor_class = getattr(mod, classname)
                sig = signature(actor_class)
                config_module = ConfigModule(
                    name, packagename, classname, options=actor
                )
                sig.bind(config_module.options)

            except SyntaxError as e:
                logger.error(f"Error: syntax error when initializing actor {name}: {e}")
                return -1

            except ModuleNotFoundError as e:
                logger.error(
                    f"Error: failed to import packages, {e}. Please check both each "
                    f"actor's imports and the package name in the yaml file."
                )

                return -1

            except AttributeError:
                logger.error("Error: Classname not valid within package")
                return -1

            except TypeError:
                logger.error("Error: Invalid arguments passed")
                params = ""
                for param_name, param in sig.parameters.items():
                    params = params + ", " + param.name
                logger.warning("Expected Parameters:" + params)
                return -1

            except Exception as e:  # TODO: figure out how to test this
                logger.error(f"Error: {e}")
                return -1

            if "GUI" in name:
                logger.info(f"Config detected a GUI actor: {name}")
                self.hasGUI = True
                self.gui = config_module
            else:
                self.actors.update({name: config_module})

        for name, conn in cfg["connections"].items():
            self.connections.update({name: conn})

        return 0

    def save_actors(self):
        """Saves the actors config to a specific file."""
        wflag = True
        saveFile = self.config_file.split(".")[0]
        pathName = saveFile + "_actors.yaml"

        for a in self.actors.values():
            wflag = a.save_config_modules(pathName, wflag)

    def get_redis_port(self):
        if self.redis_port_specified():
            return self.config["redis_config"]["port"]
        else:
            return Config.get_default_redis_port()

    def redis_port_specified(self):
        if "redis_config" in self.config.keys():
            return "port" in self.config["redis_config"]
        return False

    def redis_saving_enabled(self):
        if "redis_config" in self.config.keys():
            return (
                self.config["redis_config"]["enable_saving"]
                if "enable_saving" in self.config["redis_config"]
                else None
            )

    def generate_ephemeral_aof_dirname(self):
        if "redis_config" in self.config.keys():
            return (
                self.config["redis_config"]["generate_ephemeral_aof_dirname"]
                if "generate_ephemeral_aof_dirname" in self.config["redis_config"]
                else None
            )
        return False

    def get_redis_aof_dirname(self):
        if "redis_config" in self.config.keys():
            return (
                self.config["redis_config"]["aof_dirname"]
                if "aof_dirname" in self.config["redis_config"]
                else None
            )
        return None

    def get_redis_fsync_frequency(self):
        if "redis_config" in self.config.keys():
            frequency = (
                self.config["redis_config"]["fsync_frequency"]
                if "fsync_frequency" in self.config["redis_config"]
                else None
            )

            return frequency

    @staticmethod
    def get_default_redis_port():
        return "6379"


class ConfigModule:
    def __init__(self, name, packagename, classname, options=None):
        self.name = name
        self.packagename = packagename
        self.classname = classname
        self.options = options

    def save_config_modules(self, path_name, wflag):
        """Loops through each actor to save the modules to the config file.

        Args:
            path_name:
            wflag (bool):

        Returns:
            bool: wflag
        """

        if wflag:
            writeOption = "w"
            wflag = False
        else:
            writeOption = "a"

        cfg = {self.name: {"package": self.packagename, "class": self.classname}}

        for key, value in self.options.items():
            cfg[self.name].update({key: value})

        with open(path_name, writeOption) as file:
            yaml.dump(cfg, file)

        return wflag


class RepeatedActorError(Exception):
    def __init__(self, repeat):
        super().__init__()

        self.name = "RepeatedActorError"
        self.repeat = repeat

        self.message = 'Actor name has already been used: "{}"'.format(repeat)

    def __str__(self):
        return self.message


class RepeatedConnectionsError(Exception):
    def __init__(self, repeat):
        super().__init__()
        self.name = "RepeatedConnectionsError"
        self.repeat = repeat

        self.message = 'Connection name has already been used: "{}"'.format(repeat)

    def __str__(self):
        return self.message
