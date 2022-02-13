# Couchbase Command Line Tools

This repository contains a number of command line tools which perform actions against a local/remote Couchbase Cluster.

## Maintained
- cbrecovery (Recover missing vBuckets from a remote cluster)
- cbworkloadgen (Workload generator)
- couchbase-cli (Manage a local/remote Couchbase Cluster)

## Deprecated
- cbbackup (Online backup of a Couchbase cluster or server node)
- cbbackupwrapper (Multi-processing wrapper for `cbbackup`)
- cbrestore (Restore data backed up using `cbbackup` and `cbbackupwrapper`)
- cbrestorewrapper (Multi-processing wrapper for `cbrestore`)
- cbtransfer (Transfer data from source to destination)

# Running

When developing, the fastest way to get up-and-running with the command line tools in this repository is to use our
provided `Makefile`. The use this `Makefile` you will need to have [`make`](https://www.gnu.org/software/make) and
[`pipenv`](https://github.com/pypa/pipenv) installed.

Running `make shell`, will use `pipenv` to create a virtual environment for the project with all the required
dependencies (including development tools) and will launch a sub-shell in that environment. You should then be able to
run the desired tools by directly executing the Python files e.g. `./cbbackup` or `python3 cbbackup`.

For a more comprehensive environment, you may build the whole of `Couchbase Server` (which includes `couchbase-cli`)
using the [tlm](https://github.com/couchbase/tlm), in this case, you'd want to run the version of the chosen command
tools which was build by `cmake` (usually contained in `install/bin`).

## Documentation

The documentation for all the publicly available tools is stored in the `docs` directory and are written in AsciiDoc
format which is used to generate HTML/Man page files.

The documentation may be generated using the `generate.sh` script located in the `docs` directory. Please note that this
script will generate *all* the documentation; when submitting patches, it's common practice to comment out all but the
updated documentation.

Note that the directory structure in `docs` is deeply nested in order for these documents to be incorporated directly
into the main documentation for Couchbase Server, hosted at https://docs.couchbase.com.

# Testing

Running the unit tests contained in this repository can be done by running `make test`, this will setup a Python
virtual environment using `pipenv` and run any unit tests contained in the `test` directory. This may be run inside or
outside of a shell created using `make shell`.

# Contributing

The following sections cover some basics of how to contribute to `couchbase-cli` whilst following some of our common
practices/conventions.

## Gerrit

To contribute to this codebase you can upload patches through [Gerrit](http://review.couchbase.org). Make sure you have
configured the git hooks as described in the [Git Hooks](#git-hooks) section so that the code is linted and formatted
before uploading the patch.

Once you are ready to make your first commit note that *all* commits must be linked to an MB. This is done by making
sure that the commit title has the following format `MB-XXXXX Commit title` where `MB-XXXXX` is a valid issue in
[Jira](https://issues.couchbase.com).

## Git Hooks

Before contributing any patches, the Git hooks should be configured to ensure code is correctly linted and formatted.

The Git hooks require the following dependencies:
- aspell (Interactive spell checker)
- pylint (Static code analysis tool which looks for programming errors)
- sponge (Binary provided by `moreutils` which "soaks" all input before writing output)
- wget (Used to download the `commit-msg` hook from Gerrit)

Once installed, the Git hooks may be setup using the following command:

```sh
git config core.hooksPath .githooks
```

If the Git hooks have been setup correctly, the Gerrit `commit-msg` hook will be downloaded automatically when creating
your first commit. However, this can also be done manually by downloading the
[commit-msg](http://review.couchbase.org/tools/hooks/commit-msg) script, marking it as executable and placing it into
the `.githooks` directory.

## Coding style

In this section we will cover notes on the exact coding style to use for this codebase. Most of the style rules are
enforced by the linters, so here we will only cover ones that are not.

### Documenting

- All public functions should have a matching docstring.
- Any non-trivial unexported function should also have a matching docstring. Note this is left up to the developer and
  reviewer consideration.
- Comments must be wrapped at 120 characters.
- Notes on interesting/unexpected behavior should have a newline before them and use the `# NOTE:` prefix.

Please note that not all the code in the repository follows these rules, however, newly added/updated code should
generally adhere to them.

# Related Projects
- [`backup`](https://github.com/couchbase/backup)
- [`cbbs`](https://github.com/couchbase/cbbs)
- [`tools-common`](https://github.com/couchbase/tools-common)

# License
Copyright 2021 Couchbase Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
