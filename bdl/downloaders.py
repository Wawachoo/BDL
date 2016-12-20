import requests
import urllib.parse
import tempfile
import bdl.item
import bdl.exceptions


def generic(urls, timeout=5.000, progress=None, headers={}, session=None):
    """Generic file downloader function.

    Arguments:
        urls: Iterable object yielding the URL to download.
        timeout (float, optional): Timeout in seconds.
        progress (bdl.progress.Progress, optional): progress object.
        headers (dict, optional): Requests HTTP headers.
        session (requests.Session, optional): Shared session object.

    Yields:
        An bdl.item.Item object.

    Raises:
        DownloadTimeoutError: Download timeout.
        DownloadError: Any other error.
    """
    if session is None:
        session = requests.Session()
    number = 0
    if progress is None:
        progress = bdl.progress.Progress()
    for url in urls:
        try:
            progress.add(url, 0)
            r = session.get(url, timeout=timeout, stream=True, headers=headers)
            if not r.ok:
                raise bdl.exceptions.DownloadError(url, r.reason)
            filetype, extension = r.headers["Content-Type"].split('/')
            length = int(r.headers["Content-Length"])
            #content = bytes()
            written = 0
            with tempfile.NamedTemporaryFile(delete=False) as temp:
                for chunk in r.iter_content(4096):
                    written += len(chunk)
                    #content += chunk
                    temp.write(chunk)
                    progress.update(url, written*100/length)
            progress.mark_finished(url)
            yield bdl.item.Item(url=url,
                                filename=None,
                                extension=extension,
                                storename=None,
                                content=None,
                                hashed=None,
                                tempfile=temp.name)
        except requests.exceptions.Timeout as err:
            raise bdl.exceptions.DownloadTimeoutError(url, str(err)) from err
        except requests.exceptions.InvalidSchema as err:
            progress.mark_failed(url)
            yield None
        except Exception as err:
            raise bdl.exceptions.DownloadError(url, str(err)) from err


def fake(urls, timeout=5.000, progress=None):
    """Fake file downloader function.

    This function returns void items (not `None`).

    Arguments:
        urls: Iterable object yielding the URL to download.
        timeout (float, optional): Timeout in seconds.
        progress (bdl.progress.Progress, optional): progress object.

    Yields:
        An bdl.item.Item object, with no filename nor extension nor content.
    """
    for url in urls:
        progress.add = (url, 100)
        progress.mark_finished(url)
        yield bdl.item.Item(url=url,
                            filename=None,
                            extension=None,
                            storename=None,
                            content=None,
                            hashed=None)
