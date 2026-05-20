# Karp pipeline

This is Språkbankens tool for turning structural data, mainly lexicons, into uniform JSON data, optionally augmented with
UD tags. It supports adding modules for generating additional files and installation in external systems.

## Documentation

The pipeline is centered around:
- importers - currently [JSONL](https://jsonlines.org/) and some variants of CSV
- converters - currently tag conversion (to UD), excluding fields and renaming fields.
- exporters - for example, JSONL output and SQL and configuration files for the backend
- installers - for example, install resource in an instance of the Karp-S backend

The main commands that can be invoked are:
- **run** - do the needed modifications to each entry and output data in new formats (*converters*, *exporters*).
- **install** - runs commands and moves files, such as adding data to a database, running a command in another tool etc. (*installers*).

## Preparing a resource

Create a directory for the resource with the following structure:

```
<resource_dir>:
    source/<data_file1>.jsonl
    source/<data_file2>.jsonl
    source/...
    config.yaml
```

The possible fields in `config.yaml` are (optional unless noted):
- `parent` - see [Configuration inheritance](#configuration-inheritance)
- `root`- see [Configuration inheritance](#configuration-inheritance)
- `resource_id` - required
- `name` - may be required by some modules ([multi-lang](#multi-lang))
- `description` - may be required by some modules ([multi-lang](#multi-lang))
- `export`
    - `default` - a list of exporters to run when no argument is given to `karp-pipeline run`
    - `fields` - an array of fields settings, the default behavior is to include all fields 
    in the export. The possible patterns are:
        - `<field name> as <new field name>` - rename field
        - `<field name>:<module>.<function> as <new field name>` - modify field with module
        - `<field name>:<function> as <new field name>` - modify field with local plugin
        - `not <field name>` - exclude field
        - `...` - add all fields from the source file that have not been explicitly excluded
- `install` - a list of installers to run when no argument is given to `karp-pipeline install`
- `fields`: output field settings, needed to force categorical values and to add labels.
  The jsonl module only uses categorical data. The format is as follows:
    - `name: <field name>`
    - `label` - of type [multi-lang](#multi-lang)
    - `type: text/integer/float/bool` - the type of the field
    - `collection: true/false` - if the value of the field is a list or not
    - `categorical: true/false` - if categories are given, the pipeline checks that the field contains a value from the categories. Otherwise categories may be generated for usage in modules.
    - `categories: <list>`- a list of categories in this field
    - `category_labels: <object>` - an object of category values to the [multi-lang](#multi-lang) type

### Multi-lang

In the description above "multi-lang" means an object that may contain language codes or just a string if there is no translation:

```
name: "Same in all languages"
description:
  eng: "Description in English"
  swe: "Description in Swedish"
```

## Configuration inheritance

There is an inheritance system for configurations. A parent configuration will populate all unset values in the child configuration. Also the top-level `fields` array
will be merged, so that default fields can be put in a parent and resource-specific fields in
a child.

There are two ways for a resource to inherit configuration:
- `config.yaml` can declare a parent, which must be called `config.yaml`
- The pipeline will look in the parent directory for a `config.yaml` and use that. This works recursively and may be turned off by adding `root: true` to a configuration.

## CLI

The CLI tool `karp-pipeline` has additional documentation that can be accessed using `karp-pipeline --help` or `karp-pipeline <cmd> --help`

### run

The most basic command is `karp-pipeline run jsonl` which generates one JSONL file from the data, 
with the requested modifications from the configuration file.

The generated data is put in a directory called `output`.

### install

SBX uses installers for adding resources to our applications and repositories, for example [Karp sök backend](https://github.com/spraakbanken/karp-s-backend).

## modules

Many of these are SBX specific, but are documented here for inspiration about
what the system can do. The code is also available and may be tweaked or 
generalized to fit another organization's use case. New modules
are added to the source code in this repository.

Each (exporter) module adds its data to a subfolder in `output/`.

Modules also have a dependency system. For example, `jsonl` declares `dependencies = [Dependency("schema")]`.

### schema

Only an exporter. Used internally for everything related to the `export.fields` settings
and automatic generation of field settings, such as if a field is a collection or not.
Despite its name, this is the module that calls converters and creates the final entries.

### jsonl

Only an exporter, simply writes each entry to a JSONL file.

### dataupload

Only an installer. Will move the JSONL file to the specified location (on a server or locally).

### generate_categorical_values

Only an exporter. If a field has `categorical: true`, but no categories are given, this plugin will write a file for the field with the possible values, which can be added to `config.yaml` or be used as a basis for a module/converter.

### sbxmetadata

Only an exporter. Fetches metadata from the SBX metadata API.

### sbxrepo

**Export** creates an SBX metadata file using data from module sbxmetadata
and settings from the pipeline configuration.

**Install** adds the file to a Git repo and commits.

### karp

**Export** generates the needed JSONL and configuration files needed for the
[Karp red](https://github.com/spraakbanken/karp-backend) (previously just Karp) backend.

**Install** calls `karp-cli resource create <config file>` and `karp-cli entries add <jsonl file>`.

### karps

**Export** generates the needed SQL and configuration files for the [Karp sök backend](https://github.com/spraakbanken/karp-s-backend). 

**Install** uses the mysql console to add the data to a database and moves the
configuration files to the Karp sök configuration folder and calls `karp-s-cli add <resource>` which will integrate the configuration and make sure the backend sees the
new resource.

## Multiple instances of installers

Installers may be configured many times and used by instance name rather than module name.
This can be useful if there is a system with a production backend and a staging backend, for example.

Normally, the module `karps` is defined something like this:

```
karps:
  entry_word: ...
  output_config_dir: "../karp-s-backend/prod/config/incoming"
  cli_path: "../karp-s-backend/prod/.venv/bin/karp-s-cli"
  db_database: "karps_prod"
```

but if multiple instances are needed we can use a new name and the type setting:

```
karps-prod:
  entry_word: <lots of additional settings here>
  output_config_dir: "../karp-s-backend/prod/config/incoming"
  cli_path: "../karp-s-backend/prod/.venv/bin/karp-s-cli"
  db_database: "karps_prod"
  type: karps
karps-stage:
  entry_word: <lots of additional settings here>
  output_config_dir: "../karp-s-backend/stage/config/incoming"
  cli_path: "../karp-s-backend/stage/.venv/bin/karp-s-cli"
  db_database: "karps_stage"
  type: karps
```

It is even possible to use inheritance, so that common values are only defined once:

```
karps:
  entry_word: <lots of additional settings here>
  type: karps
karps-prod:
  output_config_dir: "../karp-s-backend/prod/config/incoming"
  cli_path: "../karp-s-backend/prod/.venv/bin/karp-s-cli"
  db_database: "karps_prod"
  parent: karps
karps-stage:
  output_config_dir: "../karp-s-backend/stage/config/incoming"
  cli_path: "../karp-s-backend/stage/.venv/bin/karp-s-cli"
  db_database: "karps_stage"
  parent: karps
```

and the `karps` configuration can be added in a child and the
instances in a parent, to allow each resource to have their
own `karps` configuration, but still be installable in
multiple instances.

## Converters

Converters take a value from a field, or a whole entry, for each entry in the resource
and runs a function to produce a new value. For example it may be used to map a tagset
to another tagset or generate a link. In addition to the transformation code, the converter code
also needs to define a schema function, to tell the pipeline if the value changes any properties
like type or max length. A converter is defined by adding `mymod.py` to `src/karppipeline/converters` with code of this form:

```
from karppipeline.common import PipelineException

def to_ud_update_schema(field):
    field.extra["length"] = 4
    return field

def to_ud(_, pos):
    """
    convert tagset to ud
    """
    if pos == "adj":
        return "ADJ"
    if pos == "adv":
        return "ADV"
    if pos == "intj":
        return "INTJ"
    if pos[0] == "n":
        return "NOUN"
    if pos == "v":
        return "VERB"
    raise PipelineException(f"Could not convert {pos} to `upos`")
```

And to use this in a field: `my_pos:mymod.to_ud as upos`

Converters can be implemented outside the codebase by adding a
directory called `plugins` to the resource directory, with a Python file
called `converters.py` and by adding the used methods within. When adding converters this way, the field setting should only contain
the function name and not module name (`<field name>:<converter> as <new_field_name>`).
There is no way to reuse a converter in many resources except copying.

### Built-in converters

- `util.to_int` - takes a value and converts it to int.
- `saldo.to_ud` - Saldo tagset to UPOS
- `suc.to_ud` - SUC tagset to UPOS
