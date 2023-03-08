# ERD Generate and update script

This script is used to auto generate a database schemas diagram and update a confluence page where it is displayed
for external use

Confluence page links:
- [Production db schema](https://hellobink.atlassian.net/wiki/spaces/BPL/pages/3430219777/Cosmos+Production+Schema+git+tag)
- [Staging db schema](https://hellobink.atlassian.net/wiki/spaces/BPL/pages/3429498885/Cosmos+Staging+Schema+git+tag)


Pre-requisite:

MacOs:
```sh
brew install graphviz
```

Ubuntu:
```sh
sudo apt-get install graphviz graphviz-dev
```

To use this script, need to have the optional `erd` poetry group install

```sh
poetry install --with erd
```


## Local

This script can also be used locally to generate different formats of diagram i.e `.dot|.png|.jpg|.er|.md`
without updating the confluence pages

i.e

```sh
python scripts/erd/generate_upload_erd.py -o md
```

For dot format, use graphviz viewer to render graph. For example, [vscode extension](https://marketplace.visualstudio.com/items?itemName=tintinweb.graphviz-interactive-preview)