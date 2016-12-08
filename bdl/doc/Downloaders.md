# Downloaders
BDL includes a module named `downloaders` which contains sample functions
which can be used to download files.

## Available downloaders

### `generic`
This function is the simplest downloader possible. It takes a list of URLs
as argument, and yield new `Item` (`bdl.item.Item`).
