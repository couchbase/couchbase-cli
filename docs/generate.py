#!/usr/bin/python3
# Copyright (C) Couchbase, Inc 2022 - All Rights Reserved
# Unauthorized copying of this file, via any medium is strictly prohibited
# Proprietary and confidential

from __future__ import annotations

import argparse
import os
import re
import subprocess
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from itertools import repeat
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

FILE = Path(__file__).resolve(strict=True)
DIR = FILE.parent

# "couchbase-cli" is a matching exception because it is the only command that has a dash in it
# "backup-service" is a matching exception because it is the only command with sub-sub-commands
# (!) Adding new commands with dashes in them or with sub-sub-commands will likely break this regular expression so
# it needs to be adjusted accordingly
ADOC_META = r"= (?P<command>couchbase-cli|\w+)-?(?P<subcommand>backup-service|(\w+-?)*)-?(?P<subsubcommand>(\w+-?)*)\((?P<section>\d)\)"

AUTHOR = "Couchbase"
PRODUCT = "Couchbase Server"
DEFAULT_BUILD_VERSION = "0.0.0-0000"

HIDDEN_COMMANDS = []
HIDDEN_SUBCOMMANDS = {"couchbase-cli": ["help", "server-eshell"]}


class ADocMeta():
    def __init__(self, command: str, subcommand: str, subsubcommand: str, section: int):
        self.command = command
        self.subcommand = subcommand
        self.subsubcommand = subsubcommand
        self.section = section


class ADoc():
    def __init__(self, path: Path) -> None:
        self.path = path
        self.meta = self._meta(path)

    def _meta(self, path: Path) -> Optional[ADocMeta]:
        with open(path, "r", encoding="utf-8") as file:
            return self.__meta(file.readline())

    @staticmethod
    def __meta(line: str) -> Optional[ADocMeta]:
        match = re.search(ADOC_META, line)
        if match is None:
            return None

        return ADocMeta(
            match.group("command"),
            match.group("subcommand"),
            match.group("subsubcommand"),
            int(match.group("section")),
        )

    def with_suffix(self, suffix: str) -> Path:
        return self.path.with_suffix(suffix)

    def filename_with_suffix(self, suffix: str) -> str:
        return self.with_suffix(suffix).name

    def __eq__(self, other: object) -> bool:
        return isinstance(other, ADoc) and self.path == other.path

    def __lt__(self, other: ADoc) -> bool:
        return self.path < other.path


class Generator:
    def __init__(self, module: str) -> None:
        self.module = module

    def adocs(self) -> List[ADoc]:
        adocs = Path(DIR, "modules", self.module, "pages").glob("**/*.adoc")
        return list(filter(lambda adoc: adoc.meta is not None, [ADoc(adoc) for adoc in adocs]))

    def generate(self, build_version: str, man: bool = False) -> List[Optional[Path]]:
        with ThreadPoolExecutor() as pool:
            return flatten(list(pool.map(lambda args: self._generate(args[0], *args[1]),
                                         zip(self.adocs(), repeat((build_version, man))))))

    def _generate(self, adoc: ADoc, build_version: str, man: bool = False) -> List[Path]:
        paths = [self._generate_html(adoc, build_version)]

        if man:
            paths.append(self._generate_man(adoc, build_version))

        return paths

    def _generate_html(self, adoc: ADoc, build_version: str) -> Path:
        path = DIR / 'generated' / 'doc' / adoc.meta.command / adoc.filename_with_suffix('.html')

        args = (
            "asciidoctor "
            "-b html "
            "-d manpage "
            f"-a mansource='{PRODUCT} {build_version}' "
            f"-a manmanual='{PRODUCT} Manual' "
            f"-a author='{AUTHOR}' "
            f"-a partialsdir={DIR / 'modules' / self.module / 'pages' / '_partials' } "
            "-a nofooter "
            f"-r {DIR / 'asciidoctor-ext.rb'} "
            f"-o {path} "
            f"{adoc.path}"
        )

        subprocess.run(args, shell=True, check=True)

        return path

    def _generate_man(self, adoc: ADoc, build_version: str) -> Path:
        path = DIR / 'generated' / 'man' / ('man' + str(adoc.meta.section)) / \
            adoc.filename_with_suffix(f".{adoc.meta.section}")

        args = (
            "asciidoctor "
            "-b manpage "
            "-d manpage "
            f"-a mansource='{PRODUCT} {build_version}' "
            f"-a manmanual='{PRODUCT} Manual' "
            f"-a author='{AUTHOR}' "
            f"-a partialsdir={DIR / 'modules' / self.module / 'pages' / '_partials' } "
            f"-r {DIR / 'asciidoctor-ext.rb'} "
            f"-o {path} "
            f"{adoc.path}"
        )

        subprocess.run(args, shell=True, check=True)

        return path

    def generate_navs(self) -> List[Optional[Path]]:
        commands = []
        subcommands = defaultdict(list)
        subsubcommands = defaultdict(lambda: defaultdict(list))

        for adoc in self.adocs():
            if adoc.meta.subcommand == "":
                commands.append(adoc)
            elif adoc.meta.subsubcommand == "":
                subcommands[adoc.meta.command].append(adoc)
            else:
                subsubcommands[adoc.meta.command][adoc.meta.subcommand].append(adoc)

        return [self._generate_nav(command, subcommands[command.meta.command],
                                   subsubcommands[command.meta.command]) for command in commands]

    def _generate_nav(self, command: ADoc, subcommands: List[ADoc], subsubcommands: Dict[List]) -> Optional[Path]:
        if self._command_hidden(command):
            return None

        path = DIR / "modules" / self.module / "pages" / "_partials"

        nav_filename = self._nav_filename(command.meta.command)
        if nav_filename == "nav.adoc":
            path /= "cbcli"

        path /= nav_filename

        with open(path, "w", encoding="utf-8") as nav:
            nav.write(self.__generate_nav(command, subcommands, subsubcommands))

        return path

    def __generate_nav(self, command: ADoc, subcommands: List[ADoc], subsubcommands: Dict[List]) -> str:
        nav = (f"* xref:{self.module}:{'cbcli/' if command.meta.command == 'couchbase-cli' else ''}"
               f"{command.path.name}[{command.meta.command}]\n")

        for subcommand in sorted(subcommands):
            if subcommand.meta.section != 1 or self._subcommand_hidden(subcommand):
                continue

            nav += (f" ** xref:{self.module}:{'cbcli/' if command.meta.command == 'couchbase-cli' else ''}"
                    f"{subcommand.path.name}[{subcommand.meta.command} {subcommand.meta.subcommand}]\n")

            for subsubcommand in sorted(subsubcommands[subcommand.meta.subcommand]):
                nav += (f" ** xref:{self.module}:{'cbcli/' if command.meta.command == 'couchbase-cli' else ''}"
                        f"{subsubcommand.path.name}[{subcommand.meta.command} {subcommand.meta.subcommand} "
                        f"{subsubcommand.meta.subsubcommand}]\n")

        return nav

    @staticmethod
    def _command_hidden(s: ADoc) -> bool:
        return s.meta.command in HIDDEN_COMMANDS

    @staticmethod
    def _subcommand_hidden(s: ADoc) -> bool:
        return s.meta.command in HIDDEN_SUBCOMMANDS and s.meta.subcommand in HIDDEN_SUBCOMMANDS[s.meta.command]

    @staticmethod
    def _nav_filename(command: str) -> str:
        if command == "couchbase-cli":
            return "nav.adoc"

        return f"nav-{command}.adoc"


def flatten(lst: List[List[Any]]) -> List[Any]:
    return [e for l in lst for e in l]


def modules() -> List[str]:
    return list(map(lambda m: m.name, Path(DIR, "modules").iterdir()))


def generate(fn: Callable[[str], List[Optional[Path]]], verbose: bool) -> None:
    generated = []

    for module in modules():
        generated += fn(module)

    if verbose and len(generated) > 0:
        print(" ".join(map(str, filter(lambda p: p is not None, generated))))


def generate_docs(build_version: str, verbose: bool) -> None:
    def fn(module: str) -> List[Optional[Path]]:
        return Generator(module=module).generate(build_version, man=os.name != "nt")

    generate(fn, verbose)


def generate_navs(verbose: bool) -> None:
    def fn(module: str) -> List[Optional[Path]]:
        return Generator(module=module).generate_navs()

    generate(fn, verbose)


def main() -> None:
    parser = argparse.ArgumentParser(description="Automatically generate repository documentation")

    parser.add_argument("-n", "--navs", dest="navs", action="store_true", help="Generate Antora nav files")
    parser.add_argument("-b", "--build-version", dest="build_version", action="store", type=str,
                        default=DEFAULT_BUILD_VERSION, help="The build version")
    parser.add_argument("-v", "--verbose", dest="verbose", action="store_true", help="Output the documents generated")

    args = parser.parse_args()

    if args.navs:
        generate_navs(args.verbose)
    else:
        generate_docs(args.build_version, args.verbose)


if __name__ == "__main__":
    main()
