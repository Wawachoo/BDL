import sys
import argparse
import re
import logging
import time
import threading
import bdl.repository
import bdl.engine
import bdl.exceptions


# =============================================================================
# UTILITY & GENERIC FUNCTIONS
# =============================================================================

def process_config(config):
    """Returns a valid configuration object from a list of elements.
    """
    _config = {}
    for cfg in config:
        _fields = cfg.split('=')
        if len(_fields) != 2:
            print("Invalid config syntax: {}".format(cfg), file=sys.stderr)
            print("Expected syntax: --config name=value, ...", file=sys.stderr)
            sys.exit(1)
        _config[_fields[0]] = _fields[1]
    return _config


def run_in_thread(repository, method, *args, **kwargs):
    """Run a repository call in a thread.
    """
    def run():
        cur_name = None
        try:
            method()
        except bdl.exceptions.BDLError as error:
            print("\nBDL error: {}".format(str(error)), file=sys.stderr)
        except Exception as error:
            print("\nError: {}".format(str(error)), file=sys.stderr)

    def print_progress(progress):
        print("\r{:.0f}% (count: {}, finished: {}, failed: {})"
              .format(progress["percentage"], progress["count"],
                      progress["finished"], progress["failed"]),
              end='')

    try:
        thread = threading.Thread(target=run)
        thread.start()
        while thread.is_alive():
            print_progress(repository.get_progress())
            time.sleep(0.05)
        print_progress(repository.get_progress())
    except (Exception, KeyboardInterrupt):
        repository.stop()
        repository.unload()
        print()
        sys.exit(1)
    repository.unload()
    print()


# =============================================================================
# REPOSITORY API CALLS WRAPPER & UTILITY COMMANDS
# =============================================================================

def command_connect(address, path, **kwargs):
    """Connect to a repository.
    """
    repo = bdl.repository.connect(url=address, path=path)
    repo.load()
    repo.rename(template=kwargs.get("template"))
    repo.unload()


def command_clone(address, path, **kwargs):
    """Clone a repository.
    """
    print("Clone: {}".format(address))
    repo = bdl.repository.connect(url=address, path=path)
    repo.load()
    run_in_thread(repo, repo.update)


def command_update(paths, **kwargs):
    """Update a repository.
    """
    for path in paths:
        print("Update: {}".format(path))
        repo = bdl.repository.Repository(path=path)
        repo.load()
        run_in_thread(repo, repo.update)


def command_stash(paths, **kwargs):
    """Stash a repository.
    """
    for path in paths:
        print("Stash: {}".format(path))
        repo = bdl.repository.Repository(path=path)
        repo.load()
        run_in_thread(repo, repo.stash)


def command_reset(paths, **kwargs):
    """Reset a repository.
    """
    for path in paths:
        print("Reset: {}".format(path))
        repo = bdl.repository.Repository(path=path)
        repo.load()
        run_in_thread(repo, repo.reset)


def command_checkout(paths, **kwargs):
    """Connect to a repository.
    """
    for path in paths:
        print("Checkout: {}".format(path))
        repo = bdl.repository.Repository(path=path)
        repo.load()
        run_in_thread(repo, repo.checkout)


def command_status(paths, **kwargs):
    """Status of the specified repositories.
    """
    for path in paths:
        repo = bdl.repository.Repository(path=path)
        repo.load()
        stats = repo.get_status()
        print(("reachable: {}, indexed: {}, new: {}, missing: {}")
              .format(stats["reachable"], stats["indexed"],
                      stats["new"], stats["missing"]))
        repo.unload()


def command_diff(paths, **kwargs):
    """Diff. between indexed and local files.
    """
    for path in paths:
        repo = bdl.repository.Repository(path=path)
        repo.load()
        items = repo.get_missing()
        if len(items) > 0:
            print("Found {} missing items :".format(len(items)))
            for storename, url in items:
                print("{} ({})".format(storename, url))
        repo.unload()


def command_rename(paths, **kwargs):
    """Diff. between indexed and local files.
    """
    for path in paths:
        repo = bdl.repository.Repository(path=path)
        repo.load()
        repo.rename(template=kwargs.get("template"))
        repo.unload()


def command_about(req=""):
    """Displays information about `req`.

    Arguments:
        req (list): Display information about this item.
    """
    avails = [("(engine|site)(s){0,1}", "engine"),
              ("version", "version")]
    # Normalize.
    for regex, value in avails:
        item = re.match(regex, req) and value or None
        if item is not None:
            break
    # Error case.
    if item is None:
        print("Available items:", file=sys.stderr)
        print("{}".format(", ".join(y for x, y in avails)), file=sys.stderr)
    # Print informations.
    if item == "engine":
        for sitename, engine in bdl.engine.by_netloc.items():
            print("{} <-> {}".format(sitename, engine[0].engine_name))
    elif item == "version":
        print("Unknow !")


# =============================================================================
# PARSING & COMMAND PROCESSING
# =============================================================================

def parse():
    """Parse arguments.

    Returns:
        An argparse.Namespace object with the parsed arguments.
    """
    # Replace command alias.
    if len(sys.argv) >= 2:
        for alias, command in [("co", "connect"), ("up", "update")]:
            if sys.argv[1] == alias:
                sys.argv[1] = command
    # Prepare argument parsing.
    parser = argparse.ArgumentParser(prog="bdl")
    subparsers = parser.add_subparsers(dest="command", title="command")
    subparsers.required = True
    # Single-repository commands.
    for cmd, desc in [("connect", "Create a local repository."),
                      ("clone", "Create and update a local repository.")]:
        _p = subparsers.add_parser(cmd, description=desc)
        _p.add_argument("address", type=str, help="Remote repository URL")
        _p.add_argument("path", type=str, nargs='?', default=None,
                        help="Local repository path")
        _p.add_argument("--template", type=str, default=None,
                        help="Files name template")
        _p.add_argument("--loglevel", type=str, default="WARNING",
                        choices=["DEBUG", "INFO", "WARNING", "ERROR",
                                 "CRITICAL"])
    # Multi-repositories commands.
    for cmd, desc in [("update", "Download the new files."),
                      ("stash", "Re-download current and missing files."),
                      ("reset", "Download missing files"),
                      ("checkout", "Reset and re-download all files."),
                      ("status", "Display status informations."),
                      ("diff", "Shows the list of missing files"),
                      ("rename", "Rename files and update template")]:
        _p = subparsers.add_parser(cmd, description=desc)
        _p.add_argument("paths", type=str, nargs='+',
                        help="{} these repositories".format(cmd))
        _p.add_argument("--template", type=str, default=None,
                        help="Files name template")
        _p.add_argument("--loglevel", type=str, default="WARNING",
                        choices=["DEBUG", "INFO", "WARNING", "ERROR",
                                 "CRITICAL"])
    # 'about' command.
    _p = subparsers.add_parser("about", description="Information about BDL.")
    _p.add_argument("item", type=str, nargs='?', default="",
                    help="Get information about this property".format(cmd))
    _p.add_argument("--loglevel", type=str, default="WARNING",
                    choices=["DEBUG", "INFO", "WARNING", "ERROR",
                             "CRITICAL"])
    # Done.
    return parser.parse_args()


def run_command(args):
    """Run the specified command.

    Arguments:
        args (argparse.Namespace): Parsed arguments.
    """
    # Pre-load engines (this is required for all commmands).
    bdl.engine.preload()
    # Run command.
    try:
        if args.command in ["connect", "clone"]:
            kwargs = {"template": args.template, }
            globals()["command_{}".format(args.command)](args.address,
                                                         args.path,
                                                         **kwargs)
        elif args.command in ["update", "stash", "reset", "checkout",
                              "status", "diff", "rename"]:
            kwargs = {"template": args.template, }
            globals()["command_{}".format(args.command)](args.paths,
                                                         **kwargs)
        elif args.command in ["about", ]:
            globals()["command_{}".format(args.command)](args.item)
        else:
            print("{} not implemented.".format(args.command), file=sys.stderr)
    except bdl.exceptions.BDLError as error:
        print("\nBDL error: {}".format(str(error)), file=sys.stderr)
    except Exception as error:
        raise
        exit(1)


# =============================================================================
# MAIN
# =============================================================================

def main():
    # Parse arguments.
    args = parse()
    # Setup log handler.
    logger = logging.getLogger("bdl")
    formatter = logging.Formatter(
        "{levelname} - {name} - {message}", style='{')
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.getLevelName(args.loglevel))
    # Run selected command.
    run_command(args)


if __name__ == "__main__":
    main()
