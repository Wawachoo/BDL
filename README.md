# BDL
**BDL core version**: `2.0.0`  
**Engine API version**: `2.0.0`

Extensible download tool.


## Principle
BDL is a download tool. It *connects* to *repositories* (online
ressources such as forums thread, imageboards, etc.), and provides a set of
methods to *update* (download) the repositories files locally.

BDL by itself supports no site; It uses *engines* to find and download the
available files from the remote repositories.


## Engines
BDL engines are Python modules which are dynamically loaded by BDL. They are in
charge of finding new files, and may optionnaly provides their own
function to download those files.


## Supported sites
This is a short list of engines and their supported sites:
* **4Chan**: [BDL_4Chan](https://github.com/Wawachoo/BDL_4Chan)
* **JoyReactor**: [BDL_Joyreactor](https://github.com/Wawachoo/BDL_Joyreactor)


## Installation
```shell
$> git clone https://github.com/Wawachoo/BDL
$> cd BDL
$> python3.5 setup.py install
```


## Usage

### Connect to or clone a repository
**Syntax**  
`bdl {connect|clone} <url> [name] [--template "template"]`

**Description**  
The `connect` command simply initialize a local repository, while `clone`
initialize the repository and update it in same time. *url* is the remote
repository URL.
If *name* is not provided, BDL will try to set it after the repository name.

**Examples**  
```shell
$> bdl connect https://address.of/repository
$> bdl connect https://address.of/repository "local_path"
$> bdl clone https://address.of/repository
$> bdl clone https://address.of/repository "local_path"
```


### Update a local repository
**Syntax**  
`bdl {update|stash|reset|checkout} <repositories ...>`

**Description**  
Download new or missing files from the selected `repositories`.
* `update` downloads only the newset files available;
* `stash` re-downloadeds all previously indexed files;
* `reset` downloads only the deleted files;
* `checkout`: (re)downloads everything (current, deleted and new files).


### Rename repository files
**Syntax**  
`bdl rename <repositories ...> [--template "template"]`

**Description**  
Change the specified repositories file's name according to `template`, and
update the repositories default template to `template`. If `--template` is not
specified, revert to the default template `{position}.{extension}`.

**Template syntax**  
The `template` argument is a string which consists in Python string formatters
and keywords, like `{position:02d}.{extension}`.


**Default template keywords**  
* `{position}`: The item position in index database (numeric);
* `{filename}`: The original file name without extension (text);
* `{extension}`: The file extension (text);

**Extra template keywords**  
* BDL engines may export metadata, like `{thread_name}` or `{file_likes}`.
  Refers to engine documentation.
* If a requested keyword doesn't exists, an empty string is returned.


### Get information about repositories
**Syntax**
`bdl {status|diff} <repositories ...>`

**Description**    
Display information about the selected `repositories`.
* `status` displays general information about he repositories;
* `diff` lists the missing (deleted) files.
