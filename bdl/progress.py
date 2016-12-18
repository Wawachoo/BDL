import threading
import copy
import time
from collections import namedtuple


ProgressState = namedtuple("ProgressState", ["count", "finished", "failed",
                                             "percentage"])


class Progress:

    def __init__(self, count=0, name=None):
        """Initializes object.

        Arguments:
            count (int, optional): Number of items to download. `< 1` means
                that the number of items cannot be deduced.
            name (str, optional): Name of the current operation.
        """
        self.__lock = threading.Lock()
        self.reset()
        self.__count = count
        self.__name = name

    def reset(self):
        """Reset progress state.
        """
        with self.__lock:
            self.__entries = []
            self.__currents = []
            self.__finished = []
            self.__failed = []
            self.__count = 0
            self.__name = None

    @property
    def count(self):
        with self.__lock:
            return copy.copy(self.__count)

    @count.setter
    def count(self, value):
        with self.__lock:
            self.__count = value

    @property
    def name(self):
        with self.__lock:
            return copy.copy(self.__name)

    @name.setter
    def name(self, value):
        with self.__lock:
            self.__name = value

    def add(self, url, percentage=0):
        """Add an `url` to the progress list.

        Arguments:
            url (str): URL to add.
            percentage (int, optional): URL download progress.
        """
        with self.__lock:
            if len(self.__entries) > 0:
                curpos = (len(self.__entries) - 1)
            else:
                curpos = 0
            curtime = time.time()
            self.__entries.append({
                "url": url,
                "begin": percentage > 0 and curtime or -1,
                "end": percentage < 100 and curtime or -1,
                "percentage": percentage})
            self.__currents.append(curpos)

    def __mark(self, url, new_container, **kwargs):
        """Add `url` in `new_container`.

        Arguments:
            url (str): Item to update.
            new_container (object, None): Item new container.
            **kwargs: Item values to update.
        """
        with self.__lock:
            pos = 0
            for entry in self.__entries:
                pos += 1
                if entry["url"] == url:
                    # Removes from `current` and add to specified container.
                    if new_container is not None:
                        try:
                            self.__currents.remove(pos)
                        except ValueError:
                            pass
                        new_container.append(pos)
                    # Update values.
                    for key, value in kwargs.items():
                        entry[key] = value

    def update(self, url, percentage=0):
        """Update an `url` progress percentage.

        Arguments:
            url (str): URL to update.
            percentage (int, optional): Download progress.
        """
        self.__mark(url, None, percentage=percentage)

    def mark_finished(self, url):
        """Marks `url` as failed.

        Arguments:
            url (str): URL to mark as finished.
        """
        self.__mark(url, self.__finished)

    def mark_failed(self, url):
        """Marks `url` as failed.

        Arguments:
            url (str): URL to mark as failed.
        """
        self.__mark(url, self.__failed)

    @property
    def total(self):
        """Returns global state.
        """
        with self.__lock:
            if self.__count > 0:
                percentage = len(self.__entries) / self.__count * 100
            else:
                percentage = 0
            return ProgressState(count=self.__count,
                                 finished=len(self.__finished),
                                 failed=len(self.__failed),
                                 percentage=percentage)

    def __get_container(self, container):
        """Returns state of selected `container`.
        """
        with self.__lock:
            items = []
            for entry_pos in container:
                items.append(copy.copy(self.__entries[entry_pos]))
            return items

    @property
    def currents(self):
        return self.__get_container(self.__currents)

    @property
    def finished(self):
        return self.__get_container(self.__finished)

    @property
    def failed(self):
        return self.__get_container(self.__failed)
