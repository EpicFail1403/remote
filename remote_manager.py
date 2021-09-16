#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from typing import Dict, List
import fabric
import configparser
import os
import logging

logger = logging.getLogger(__name__)


class Console(fabric.Connection):
    def __init__(
        self,
        host,
        user=None,
        port=None,
        config=None,
        gateway=None,
        forward_agent=None,
        connect_timeout=None,
        connect_kwargs=None,
        inline_ssh_env=None,
        name=None,
    ):
        super().__init__(
            host,
            user=user,
            port=port,
            config=config,
            gateway=gateway,
            forward_agent=forward_agent,
            connect_timeout=connect_timeout,
            connect_kwargs=connect_kwargs,
            inline_ssh_env=inline_ssh_env,
        )
        if name is None:
            self.name = "{}:{}".format(host, port)
        else:
            self.name = name

    def run(self, command, **kwargs):
        hide = kwargs.get("hide")
        # @see http://docs.pyinvoke.org/en/latest/api/runners.html#invoke.runners.Runner.run
        if hide in [None, True, "both", "stdout"]:
            print("[{}@{} {}]$ {}".format(self.user, self.name, self.cwd, command))
        return super().run(command, **kwargs)


class HostManager:
    def __init__(self, config) -> None:
        if isinstance(config, (str, bytes, os.PathLike)):
            self.config = configparser.ConfigParser()
            self.config.read(config)
        elif isinstance(config, configparser.ConfigParser):
            self.config: configparser.ConfigParser = config
        else:
            raise "invalid config"
        self.connections: Dict[str, fabric.Connection] = {}
        self.jump_server: fabric.Connection = None
        self.list: List[Console] = []

        try:
            self.jump_server = self.connect(config["jump_server"], gateway=None)
            logger.info("default jump server: {}".format(self.jump_server.host))
        except:
            self.jump_server = None
            logger.debug("no default jump server configured")

        logger.info("loading hosts from {}".format(config))
        for section_name in self.config.sections():
            if section_name == "DEFAULT":
                continue
            self.list.append(self.connect(section_name))
            logger.info(section_name)

    def connect(
        self, target_config: dict, gateway: fabric.Connection = None
    ) -> Console:
        if isinstance(target_config, str):
            name = target_config
            target_config = self.config[target_config]
        else:
            name = None

        key = "{}:{}".format(target_config["host"], target_config["port"])
        if key in self.connections:
            return self.connections[key]

        connection = Console(
            target_config["host"],
            user=target_config["user"],
            port=target_config["port"],
            connect_kwargs={
                "key_filename": [
                    target_config["key"],
                ],
                "passphrase": target_config["passphrase"],
                "password": target_config["password"],
            },
            gateway=gateway,
            name=name,
        )
        self.connections[key] = connection
        return connection

    def drop_connection(self, connection: fabric.Connection):
        if connection is None:
            return

        for key in self.connections:
            if self.connections[key] is connection:
                del self.connections[key]
        try:
            connection.close()
        except:
            pass

    def get_default_connection(self) -> Console:
        return self.connect(
            self.config[self.config["DEFAULT"]["target"]], gateway=self.jump_server
        )
